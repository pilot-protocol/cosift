package crawler

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestParseProxies(t *testing.T) {
	in := []string{
		"http://proxy.example.com:8080",
		"http://user:pass@proxy.example.com:8080",
		"socks5://socks.local:1080",
		"",                  // empty — skipped
		"  ",                // whitespace-only — skipped
		"not-a-url",         // no scheme/host — skipped
		"http://",           // empty host — skipped
		"https://valid:443", // ok
	}
	out := parseProxies(in)
	if len(out) != 4 {
		t.Errorf("parseProxies: got %d want 4 (%+v)", len(out), out)
	}
	if out[0].Host != "proxy.example.com:8080" {
		t.Errorf("first proxy host: %q", out[0].Host)
	}
}

func TestParseProxiesEmpty(t *testing.T) {
	out := parseProxies(nil)
	if len(out) != 0 {
		t.Errorf("nil input: got len %d", len(out))
	}
	out = parseProxies([]string{})
	if len(out) != 0 {
		t.Errorf("empty input: got len %d", len(out))
	}
}

// --- Robots ---

func TestRobotsAllowedHTTPServer(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/robots.txt" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte("User-agent: *\nDisallow: /private\nAllow: /private/public\nCrawl-delay: 2\nSitemap: https://example/sitemap.xml\n"))
	}))
	defer ts.Close()

	r := NewRobots(&http.Client{Timeout: 5 * time.Second}, "TestUA/1.0")

	// Disallowed.
	ok, delay, err := r.Allowed(t.Context(), ts.URL+"/private/secret")
	if err != nil {
		t.Fatalf("Allowed: %v", err)
	}
	if ok {
		t.Errorf("/private/secret should be disallowed")
	}
	if delay <= 0 {
		t.Errorf("expected crawl-delay > 0, got %v", delay)
	}

	// Allowed (more specific allow overrides disallow).
	ok, _, _ = r.Allowed(t.Context(), ts.URL+"/private/public/foo")
	if !ok {
		t.Errorf("/private/public/foo should be allowed (longer allow rule)")
	}

	// Sitemaps endpoint.
	sitemaps := r.Sitemaps(t.Context(), ts.URL)
	if len(sitemaps) != 1 || sitemaps[0] != "https://example/sitemap.xml" {
		t.Errorf("Sitemaps: %v", sitemaps)
	}

	// Bad URL.
	_, _, err = r.Allowed(t.Context(), "::not a url")
	if err == nil {
		t.Errorf("Allowed should error on malformed URL")
	}
}

func TestRobotsSitemapsBadURL(t *testing.T) {
	r := NewRobots(&http.Client{Timeout: 1 * time.Second}, "T/1")
	if out := r.Sitemaps(t.Context(), "::bad"); out != nil {
		t.Errorf("bad URL: got %v", out)
	}
}

func TestRobotsCacheHit(t *testing.T) {
	calls := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/robots.txt" {
			calls++
		}
		_, _ = w.Write([]byte("User-agent: *\nDisallow:\n"))
	}))
	defer ts.Close()

	r := NewRobots(&http.Client{Timeout: 2 * time.Second}, "U/1")
	for i := 0; i < 3; i++ {
		if _, _, err := r.Allowed(t.Context(), ts.URL+"/p"); err != nil {
			t.Fatalf("Allowed: %v", err)
		}
	}
	if calls != 1 {
		t.Errorf("robots.txt fetched %d times, want 1 (cache miss path)", calls)
	}
}

func TestRobotsFetch404TreatedAsEmpty(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer ts.Close()
	r := NewRobots(&http.Client{Timeout: 2 * time.Second}, "U/1")
	ok, _, err := r.Allowed(t.Context(), ts.URL+"/anything")
	if err != nil {
		t.Fatalf("Allowed: %v", err)
	}
	if !ok {
		t.Errorf("404 robots.txt should leave host fully allowed")
	}
}

// Quick sanity: parseRobots tolerates a body with comments + blank lines
// + malformed lines (already covered in robots_test.go but exercises a
// different combination here).
func TestParseRobotsCornerCases(t *testing.T) {
	body := strings.Join([]string{
		"# leading comment",
		"",
		"User-agent: SpecificBot",
		"User-agent: AnotherBot",
		"Disallow: /no",
		"Allow: /no/yes",
		"Crawl-delay: 5",
		"",
		"User-agent: *",
		"Disallow: /",
		"",
		"Sitemap: https://x/sm1.xml",
		"Sitemap: https://x/sm2.xml",
		"unknown-directive: ignored",
		"no-colon-line",
	}, "\n")
	rules := parseRobots(body)
	if len(rules.sitemaps) != 2 {
		t.Errorf("sitemaps: got %d want 2", len(rules.sitemaps))
	}
	if len(rules.groups) == 0 {
		t.Errorf("expected groups parsed")
	}
}
