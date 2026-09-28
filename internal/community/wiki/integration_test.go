package wiki_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/community"
	"github.com/pilot-protocol/cosift/internal/community/wiki"
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) Add(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func newClock() *clock { return &clock{t: time.Now()} }

var nextIP atomic.Int64

func get(h http.Handler, path string, headers ...string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodGet, path, nil)
	n := nextIP.Add(1)
	r.RemoteAddr = fmt.Sprintf("10.%d.%d.%d:4000", n>>16&255, n>>8&255, n&255)
	for i := 0; i+1 < len(headers); i += 2 {
		if headers[i] == "RemoteAddr" {
			r.RemoteAddr = headers[i+1]
		} else {
			r.Header.Set(headers[i], headers[i+1])
		}
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	return rec
}

var titleRE = regexp.MustCompile(`<h1>([^<]*)</h1>`)

func TestStackPages(t *testing.T) {
	s := newStack(t)
	s.seed()
	srv := s.pages(t, true, nil)
	wiki.SetPageSizes(srv.Wiki(), 200, 2)
	if err := srv.Wiki().Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	var promoted []string
	for _, f := range fixtures(t) {
		rec := get(srv, "/wiki/"+f.slug)
		body := rec.Body.String()
		switch f.kind {
		case "promoted", "ok":
			if rec.Code != 200 || titleRE.FindStringSubmatch(body)[1] != f.title || !strings.Contains(body, "AI-generated") || !strings.Contains(body, `id="cite-2"`) {
				t.Fatalf("%s: %d\n%s", f.slug, rec.Code, body)
			}
			if !wiki.Known(srv.Wiki(), f.slug) {
				t.Fatalf("%s is not in the known set", f.slug)
			}
		case "stub":
			if rec.Code != 200 || !strings.Contains(body, "Being researched") || !strings.Contains(body, "<h1>"+f.title+"</h1>") || strings.Contains(body, "wiki-body") {
				t.Fatalf("stub: %d\n%s", rec.Code, body)
			}
		case "tombstoned":
			if rec.Code != 410 || strings.Contains(body, f.title) {
				t.Fatalf("tombstone: %d\n%s", rec.Code, body)
			}
		case "held", "rejected", "other-env":
			if rec.Code != 404 || strings.Contains(body, f.title) {
				t.Fatalf("%s: %d\n%s", f.kind, rec.Code, body)
			}
		}
		if f.promoted() {
			promoted = append(promoted, f.slug)
		}
	}
	hub := get(srv, "/wiki").Body.String()
	var listed []string
	for _, m := range regexp.MustCompile(`<li><a href="/wiki/([a-z0-9-]+)">`).FindAllStringSubmatch(hub, -1) {
		listed = append(listed, m[1])
	}
	slices.Sort(promoted)
	if !slices.Equal(listed, promoted) {
		t.Fatalf("the A–Z hub lists %v, want %v", listed, promoted)
	}
	perVertical := map[string]int{}
	for _, f := range fixtures(t) {
		if f.promoted() {
			perVertical[f.vertical]++
		}
	}
	for _, f := range fixtures(t) {
		if !f.promoted() {
			continue
		}
		v := get(srv, "/wiki/v/"+f.vertical).Body.String()
		if !strings.Contains(v, `href="/wiki/`+f.slug+`"`) || strings.Count(v, `<li><a href="/wiki/`) != perVertical[f.vertical] {
			t.Fatalf("vertical %s:\n%s", f.vertical, v)
		}
	}
	var mapped []string
	for n := 1; ; n++ {
		rec := get(srv, fmt.Sprintf("/sitemaps/wiki-%d.xml", n))
		if rec.Code == 404 {
			if n-1 != (len(promoted)+1)/2 {
				t.Fatalf("%d sitemap files for %d promoted at 2 a file", n-1, len(promoted))
			}
			break
		}
		for _, m := range regexp.MustCompile(`<loc>https://cosift\.example/wiki/([a-z0-9-]+)</loc>`).FindAllStringSubmatch(rec.Body.String(), -1) {
			mapped = append(mapped, m[1])
		}
	}
	slices.Sort(mapped)
	if !slices.Equal(mapped, promoted) {
		t.Fatalf("the sitemaps list %v, want exactly %v", mapped, promoted)
	}
	if idx := get(srv, "/sitemap.xml").Body.String(); strings.Count(idx, "<sitemap>") != (len(promoted)+1)/2 {
		t.Fatalf("sitemap index:\n%s", idx)
	}
}

func TestStackTakedown(t *testing.T) {
	s := newStack(t)
	f := fixtures(t)[1]
	s.put("synth-prod", f)
	c := newClock()
	srv := s.pages(t, true, c.Now)
	w := srv.Wiki()
	if err := w.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if rec := get(srv, "/wiki/"+f.slug); rec.Code != 200 || rec.Header().Get("Cache-Control") != "public, max-age=300, s-maxage=300" {
		t.Fatalf("article: %d %s", rec.Code, rec.Header().Get("Cache-Control"))
	}
	c.Add(250 * time.Second)
	if err := w.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(get(srv, "/wiki").Body.String(), f.title) {
		t.Fatal("setup: the hub does not list the article")
	}
	s.moderate(f.id, "tombstone")
	c.Add(51 * time.Second)
	rec := get(srv, "/wiki/"+f.slug)
	if rec.Code != 410 || strings.Contains(rec.Body.String(), f.title) {
		t.Fatalf("after the takedown: %d", rec.Code)
	}
	hub := get(srv, "/wiki")
	if hub.Code != 200 || strings.Contains(hub.Body.String(), f.title) || hub.Header().Get("Cache-Control") != "public, max-age=249, s-maxage=249" {
		t.Fatalf("the hub still lists the tombstoned title: %d %s", hub.Code, hub.Header().Get("Cache-Control"))
	}
	if strings.Contains(get(srv, "/sitemaps/wiki-1.xml").Body.String(), f.slug) {
		t.Fatal("the sitemap still lists the tombstoned slug")
	}
	if status, live := wiki.EntryStatus(w, f.slug); status != 410 || !live {
		t.Fatalf("the slug's cache entry is %d (live %v), want the 410 entry", status, live)
	}
}

func securityHeaders(t *testing.T, path string, rec *httptest.ResponseRecorder, ga bool) {
	t.Helper()
	h := rec.Header()
	csp := h.Get("Content-Security-Policy")
	if h.Get("X-Content-Type-Options") != "nosniff" || h.Get("Referrer-Policy") != "no-referrer" || !strings.Contains(csp, "script-src 'self'") || !strings.Contains(csp, "style-src 'self'") {
		t.Fatalf("%s: security headers %v", path, h)
	}
	if ga != strings.Contains(csp, "https://www.googletagmanager.com/gtag/js") {
		t.Fatalf("%s: CSP %q", path, csp)
	}
	if _, ok := h["Set-Cookie"]; ok {
		t.Fatalf("%s sets a cookie", path)
	}
	if strings.Contains(strings.ToLower(strings.Join(h.Values("Vary"), ",")), "cookie") {
		t.Fatalf("%s varies on cookies", path)
	}
}

func TestStackSecurityHeadersOnEveryPage(t *testing.T) {
	s := newStack(t)
	s.seed()
	for _, ga := range []string{"", "G-TEST123"} {
		c := newClock()
		srv := s.pages(t, true, c.Now, func(cfg *community.Config) { cfg.GAMeasurementID = ga })
		if err := srv.Wiki().Refresh(context.Background()); err != nil {
			t.Fatal(err)
		}
		paths := []string{"/wiki", "/wiki/v/research", "/sitemap.xml", "/sitemaps/wiki-1.xml", "/robots.txt", "/wiki.css", "/wiki.js",
			"/wiki/Bad", "/wiki/index/9", "/wiki/nothing-here"}
		for _, f := range fixtures(t) {
			paths = append(paths, "/wiki/"+f.slug)
		}
		seen := map[int]bool{}
		for _, p := range paths {
			c.Add(time.Second)
			rec := get(srv, p, "Cookie", "cosift_session=abc")
			seen[rec.Code] = true
			securityHeaders(t, p, rec, ga != "")
		}
		for range 200 {
			rec := get(srv, "/wiki/flood-"+fmt.Sprint(nextIP.Add(1)), "RemoteAddr", "192.0.2.99:1")
			if rec.Code == 429 {
				seen[429] = true
				securityHeaders(t, "429", rec, ga != "")
				break
			}
		}
		c.Add(400 * time.Second)
		rec := get(srv, "/wiki")
		seen[rec.Code] = true
		securityHeaders(t, "stale hub", rec, ga != "")
		for _, code := range []int{200, 404, 410, 429, 503} {
			if !seen[code] {
				t.Fatalf("no %d page was checked", code)
			}
		}
		if api := get(srv, "/api/limits"); api.Header().Get("Cache-Control") != "no-store" {
			t.Fatal("a non-wiki route lost no-store")
		}
	}
}

// counting is a /v1 stand-in that counts every request.
func counting(t *testing.T) (*httptest.Server, *atomic.Int64) {
	var n atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"items":[],"next_cursor":null}`))
	}))
	t.Cleanup(srv.Close)
	return srv, &n
}

const offKey = "csk_0123456789abcdef_ABCDEFGHIJKLMNOPQRSTUVWXYZ234567ABCDEFGHIJKLMNOPQRST"

func TestSwitchOffIsDarkWithNoEngineCalls(t *testing.T) {
	for _, public := range []bool{false, true} {
		engine, calls := counting(t)
		c := newClock()
		srv, err := community.Open(community.Config{DataDir: t.TempDir(), Backend: "http://127.0.0.1:9", PublicURL: "https://cosift.example", AdminToken: "x",
			Wiki: wiki.Config{Public: public, EngineURL: engine.URL, EngineKey: offKey, ReportMailto: "reports@example.org", Now: c.Now, Logf: t.Logf}})
		if err != nil {
			t.Fatal(err)
		}
		if public {
			wiki.SetPoll(srv.Wiki(), time.Millisecond)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { defer close(done); srv.Run(ctx) }()
		for _, p := range []string{"/wiki", "/wiki/index/2", "/wiki/v/dev-docs", "/wiki/v/dev-docs/2", "/wiki/rust-async-runtimes", "/sitemap.xml",
			"/sitemaps/wiki-1.xml", "/robots.txt", "/wiki.css", "/wiki.js"} {
			rec := get(srv, p)
			if !public && (rec.Code != 404 || rec.Header().Get("Cache-Control") != "no-store") {
				t.Fatalf("switch off: %s answered %d", p, rec.Code)
			}
		}
		for range 3 {
			c.Add(241 * time.Second)
			time.Sleep(30 * time.Millisecond)
		}
		cancel()
		<-done
		srv.Close()
		if !public && calls.Load() != 0 {
			t.Fatalf("switch off made %d engine calls", calls.Load())
		}
		if public && calls.Load() < 3 {
			t.Fatalf("switch on made only %d engine calls; the check is vacuous", calls.Load())
		}
	}
}

func TestSearchConsoleTagOnHome(t *testing.T) {
	srv, err := community.Open(community.Config{DataDir: t.TempDir(), Backend: "http://127.0.0.1:9", PublicURL: "https://cosift.example", AdminToken: "x",
		GSCVerification: "gsc-token_0123456789"})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	for _, p := range []string{"/", "/login", "/signup"} {
		body := get(srv, p).Body.String()
		if strings.Count(body, `<meta name="google-site-verification" content="gsc-token_0123456789" />`) != 1 || !strings.Contains(body, "</head>") {
			t.Fatalf("%s lacks the Search Console tag", p)
		}
	}
	plain, err := community.Open(community.Config{DataDir: t.TempDir(), Backend: "http://127.0.0.1:9", PublicURL: "https://cosift.example", AdminToken: "x"})
	if err != nil {
		t.Fatal(err)
	}
	defer plain.Close()
	if strings.Contains(get(plain, "/").Body.String(), "google-site-verification") {
		t.Fatal("a tag without a token")
	}
	if _, err := community.Open(community.Config{DataDir: t.TempDir(), Backend: "http://127.0.0.1:9", PublicURL: "https://cosift.example", AdminToken: "x",
		GSCVerification: `bad"><script>`}); err == nil || !strings.Contains(err.Error(), "COSIFT_GSC_VERIFICATION") {
		t.Fatalf("a bad token was accepted: %v", err)
	}
}

func TestWikiLimiterLeavesLoginAlone(t *testing.T) {
	engine, _ := counting(t)
	srv, err := community.Open(community.Config{DataDir: t.TempDir(), Backend: "http://127.0.0.1:9", PublicURL: "https://cosift.example", AdminToken: "x",
		Wiki: wiki.Config{Public: true, EngineURL: engine.URL, EngineKey: offKey, ReportMailto: "reports@example.org", Logf: func(string, ...any) {}}})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	if err := srv.Wiki().Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	limited := 0
	for i := range 10001 {
		rec := get(srv, fmt.Sprintf("/wiki/visitor-%d", i), "RemoteAddr", fmt.Sprintf("10.%d.%d.%d:1", i>>16&255, i>>8&255, i&255))
		if rec.Code == 429 {
			limited++
		}
	}
	if limited != 0 || wiki.LimiterSize(srv.Wiki()) != 10000 {
		t.Fatalf("%d of 10,001 distinct clients were limited (%d tracked); a full map evicts the oldest", limited, wiki.LimiterSize(srv.Wiki()))
	}
	b, _ := json.Marshal(map[string]string{"email": "someone@example.org", "password": "a wrong password"})
	r := httptest.NewRequest(http.MethodPost, "/api/login", bytes.NewReader(b))
	r.RemoteAddr = "172.16.0.1:1"
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("X-Cosift-Client", "community")
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, r)
	if rec.Code != 401 {
		t.Fatalf("a new login IP got %d, want its own limit to pass (401)", rec.Code)
	}
}

func TestWikiLimiterKeysByForwardedClient(t *testing.T) {
	engine, _ := counting(t)
	c := newClock()
	srv, err := community.Open(community.Config{DataDir: t.TempDir(), Backend: "http://127.0.0.1:9", PublicURL: "https://cosift.example", AdminToken: "x",
		TrustedProxies: []string{"127.0.0.1/32"},
		Wiki:           wiki.Config{Public: true, EngineURL: engine.URL, EngineKey: offKey, ReportMailto: "reports@example.org", Now: c.Now, Logf: func(string, ...any) {}}})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	if err := srv.Wiki().Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	port := 40000
	miss := func(client string) *httptest.ResponseRecorder {
		port++
		c.Add(10 * time.Millisecond)
		return get(srv, fmt.Sprintf("/wiki/miss-%d", port), "RemoteAddr", fmt.Sprintf("127.0.0.1:%d", port), "X-Forwarded-For", client)
	}
	for i := range 120 {
		if miss("203.0.113.7").Code == 429 {
			t.Fatalf("miss %d refused below the limit", i)
		}
	}
	got := miss("203.0.113.7")
	if got.Code != 429 || got.Header().Get("Retry-After") != "59" {
		t.Fatalf("one forwarded client over 120 misses through changing proxy ports: %d, Retry-After %q", got.Code, got.Header().Get("Retry-After"))
	}
	if miss("203.0.113.8").Code == 429 {
		t.Fatal("another client behind the same proxy was limited")
	}
	c.Add(57 * time.Second)
	if miss("203.0.113.7").Code != 429 {
		t.Fatal("the window closed before 60 s")
	}
	c.Add(2 * time.Second)
	if miss("203.0.113.7").Code == 429 {
		t.Fatal("the window did not reset at 60 s")
	}
}

func TestStackPromotionTierReload(t *testing.T) {
	if os.Getuid() != 0 {
		t.Skip("articles.json must belong to root")
	}
	s := newStack(t)
	all := fixtures(t)
	strong, ok := all[0], all[4]
	s.put("synth-prod", strong)
	s.put("synth-prod", ok)
	c := newClock()
	srv := s.pages(t, true, c.Now)
	listed := func() (hub, sitemap bool) {
		t.Helper()
		c.Add(time.Second)
		if err := srv.Wiki().Refresh(context.Background()); err != nil {
			t.Fatal(err)
		}
		h, m := get(srv, "/wiki").Body.String(), get(srv, "/sitemaps/wiki-1.xml").Body.String()
		if !strings.Contains(h, strong.title) || !strings.Contains(m, strong.slug) {
			t.Fatal("the strong article left the hub or the sitemap")
		}
		return strings.Contains(h, ok.title), strings.Contains(m, "/wiki/"+ok.slug+"<")
	}
	reload := func(body string) {
		t.Helper()
		if err := os.WriteFile(s.thresholds, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := s.store.ReloadThresholds(); err != nil {
			t.Fatal(err)
		}
	}
	if hub, sitemap := listed(); !hub || !sitemap {
		t.Fatalf("default ok: the ok article is on the hub %v, in the sitemap %v", hub, sitemap)
	}
	reload(`{"schema_version": 1, "theta_covered": 0.78, "theta_related": 0.74, "promotion_min_tier": "strong"}`)
	if hub, sitemap := listed(); hub || sitemap {
		t.Fatalf("strong: the ok article is on the hub %v, in the sitemap %v", hub, sitemap)
	}
	reload(`{"schema_version": 1, "theta_covered": 0.78, "theta_related": 0.74, "promotion_min_tier": "ok"}`)
	if hub, sitemap := listed(); !hub || !sitemap {
		t.Fatalf("back to ok: the ok article is on the hub %v, in the sitemap %v", hub, sitemap)
	}
}
