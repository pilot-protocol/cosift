package crawler

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"testing"
)

// siteRT serves in-memory sites keyed by "scheme://host/path" so redirect
// chains across hostnames and schemes need no sockets.
type siteRT struct {
	mu     sync.Mutex
	hits   map[string]int
	routes map[string]string // URL -> "redirect:<location>" or an HTML body
}

func (s *siteRT) RoundTrip(r *http.Request) (*http.Response, error) {
	key := r.URL.Scheme + "://" + r.URL.Host + r.URL.Path
	s.mu.Lock()
	s.hits[key]++
	s.mu.Unlock()
	route, ok := s.routes[key]
	switch {
	case !ok:
		return &http.Response{StatusCode: 404, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("")), Request: r}, nil
	case strings.HasPrefix(route, "redirect:"):
		return &http.Response{StatusCode: 302, Header: http.Header{"Location": {strings.TrimPrefix(route, "redirect:")}}, Body: io.NopCloser(strings.NewReader("")), Request: r}, nil
	}
	return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/html"}}, Body: io.NopCloser(strings.NewReader(route)), Request: r}, nil
}

func (s *siteRT) hitsFor(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hits[key]
}

func redirectFixture(t *testing.T, routes map[string]string, cfgMod func(*Crawler)) (*Crawler, *siteRT, *stubEmbedder) {
	t.Helper()
	cfg := testCrawlerCfg()
	cfg.RespectRobots = false
	cfg.PerHostDelayMs = 0
	cfg.DisableLinkFollowing = true
	emb := &stubEmbedder{dim: 8}
	c := New(cfg, newStoreT(t)).WithEmbedder(emb)
	rt := &siteRT{hits: map[string]int{}, routes: routes}
	c.http.Transport = rt
	if cfgMod != nil {
		cfgMod(c)
	}
	c.contributionOnce.Do(func() { c.contributionCrawler = c; c.contributionSlots = make(chan struct{}, 2) })
	return c, rt, emb
}

func approvedFor(t *testing.T, raw string) string {
	t.Helper()
	parsed, err := Parse([]byte(sampleHTML), raw)
	if err != nil {
		t.Fatal(err)
	}
	return ApprovedContentHash(parsed.Title, parsed.Text)
}

func TestContributionRedirectToDisallowedDomainIsRejectedBeforeFetch(t *testing.T) {
	routes := map[string]string{
		"http://front.example/r":       "redirect:http://mid.example/hop",
		"http://mid.example/hop":       "redirect:http://target.example/page",
		"http://target.example/page":   sampleHTML,
		"http://front.example/to-lone": "redirect:http://unlisted.example/page",
		"http://unlisted.example/page": sampleHTML,
	}
	for name, tc := range map[string]struct {
		mod   func(*Crawler)
		start string
		last  string
	}{
		"excluded final hop":   {func(c *Crawler) { c.cfg.ExcludeDomains = []string{"target.example"} }, "http://front.example/r", "http://target.example/page"},
		"unlisted final hop":   {func(c *Crawler) { c.cfg.IncludeDomains = []string{"front.example", "mid.example"} }, "http://front.example/r", "http://target.example/page"},
		"excluded middle hop":  {func(c *Crawler) { c.cfg.ExcludeDomains = []string{"mid.example"} }, "http://front.example/r", "http://target.example/page"},
		"lookalike of allowed": {func(c *Crawler) { c.cfg.IncludeDomains = []string{"front.example", "listed.example"} }, "http://front.example/to-lone", "http://unlisted.example/page"},
	} {
		t.Run(name, func(t *testing.T) {
			c, rt, _ := redirectFixture(t, routes, tc.mod)
			_, err := c.FetchContribution(context.Background(), tc.start, nil, approvedFor(t, tc.last))
			if !errors.Is(err, ErrContributionRejected) {
				t.Fatalf("want a terminal rejection, got %v", err)
			}
			if rt.hitsFor(tc.last) != 0 {
				t.Fatal("the disallowed target was requested")
			}
			if doc, derr := c.store.GetDocByURL(context.Background(), tc.last); derr == nil && doc != nil {
				t.Fatal("the disallowed target was indexed")
			}
		})
	}
}

func TestContributionSameSiteRedirectsComplete(t *testing.T) {
	for name, tc := range map[string]struct{ start, final string }{
		"http to https":  {"http://site.example/guide", "https://site.example/guide"},
		"www prefix":     {"http://site.example/guide", "http://www.site.example/guide"},
		"trailing slash": {"http://site.example/guide", "http://site.example/guide/"},
	} {
		t.Run(name, func(t *testing.T) {
			routes := map[string]string{tc.start: "redirect:" + tc.final, tc.final: sampleHTML}
			c, _, emb := redirectFixture(t, routes, nil)
			ctx := context.Background()
			approved := approvedFor(t, tc.final)
			if _, err := c.FetchContribution(ctx, tc.start, nil, approved); err != nil {
				t.Fatalf("redirecting submission did not complete: %v", err)
			}
			if doc, err := c.store.GetDocByURL(ctx, tc.final); err != nil || doc == nil {
				t.Fatalf("document not stored under the final URL: %v", err)
			}
			if _, err := c.FetchContribution(ctx, tc.start, nil, approved); err != nil {
				t.Fatalf("re-submission failed: %v", err)
			}
			if emb.calls != 1 {
				t.Fatalf("re-submission re-indexed the page: %d embeds", emb.calls)
			}
		})
	}
}

type frontRT struct {
	front, to string
	next      http.RoundTripper
}

func (f frontRT) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.Host == f.front {
		return &http.Response{StatusCode: 302, Header: http.Header{"Location": {f.to}}, Body: io.NopCloser(strings.NewReader("")), Request: r}, nil
	}
	return f.next.RoundTrip(r)
}

func TestContributionRedirectToPrivateHostIsTerminal(t *testing.T) {
	c, _, _ := redirectFixture(t, nil, nil)
	dialed := false
	dialer := publicDialer{
		lookup: func(context.Context, string, string) ([]netip.Addr, error) {
			return []netip.Addr{netip.MustParseAddr("10.0.0.5")}, nil
		},
		dial: func(context.Context, string, string) (net.Conn, error) {
			dialed = true
			return nil, errors.New("must not dial")
		},
	}
	c.http.Transport = frontRT{front: "front.example", to: "http://internal.example/page", next: &http.Transport{DialContext: dialer.DialContext}}
	_, err := c.FetchContribution(context.Background(), "http://front.example/r", nil, approvedFor(t, "http://internal.example/page"))
	if !errors.Is(err, ErrContributionRejected) || dialed {
		t.Fatalf("private redirect target: err=%v dialed=%v", err, dialed)
	}
}
