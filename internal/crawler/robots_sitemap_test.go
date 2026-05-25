package crawler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestParseRobotsExtractsSitemap(t *testing.T) {
	// Iter 131: Sitemap directive is group-independent per the sitemaps.org spec.
	body := `# example
User-agent: *
Disallow: /admin/

User-agent: CosiftBot
Allow: /

Sitemap: https://example.com/sitemap.xml
`
	r := parseRobots(body)
	if len(r.sitemaps) != 1 {
		t.Fatalf("want 1 sitemap, got %d: %+v", len(r.sitemaps), r.sitemaps)
	}
	if r.sitemaps[0] != "https://example.com/sitemap.xml" {
		t.Errorf("sitemap URL: got %q", r.sitemaps[0])
	}
	// Sitemap directive doesn't attach to either group — it's top-level.
	if len(r.groups) != 2 {
		t.Errorf("groups: got %d want 2", len(r.groups))
	}
}

func TestParseRobotsMultipleSitemaps(t *testing.T) {
	// Some hosts split sitemaps by section (news, docs, products). All should be captured.
	body := `User-agent: *
Disallow: /admin/

Sitemap: https://example.com/sitemap-news.xml
Sitemap: https://example.com/sitemap-docs.xml
Sitemap: https://example.com/sitemap-products.xml
`
	r := parseRobots(body)
	if len(r.sitemaps) != 3 {
		t.Fatalf("want 3 sitemaps, got %d: %+v", len(r.sitemaps), r.sitemaps)
	}
}

func TestParseRobotsSitemapEmpty(t *testing.T) {
	// `Sitemap: ` with empty value is dropped (no point storing "").
	body := `User-agent: *
Sitemap:
Sitemap: https://example.com/sitemap.xml
`
	r := parseRobots(body)
	if len(r.sitemaps) != 1 {
		t.Errorf("empty sitemap should be skipped: got %+v", r.sitemaps)
	}
}

func TestParseRobotsNoSitemap(t *testing.T) {
	// Backward compat: robots.txt without Sitemap: directive parses fine,
	// sitemaps slice is nil.
	body := `User-agent: *
Disallow: /admin/
`
	r := parseRobots(body)
	if len(r.sitemaps) != 0 {
		t.Errorf("no sitemap directive: want nil/empty, got %+v", r.sitemaps)
	}
}

func TestRobotsSitemapsHTTPFlow(t *testing.T) {
	// Full HTTP path: fetch robots.txt via Robots.Sitemaps(), verify return.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/robots.txt" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(`User-agent: *
Allow: /
Sitemap: ` + "https://srv.test/sitemap-1.xml" + `
Sitemap: ` + "https://srv.test/sitemap-2.xml" + `
`))
	}))
	defer srv.Close()

	r := NewRobots(srv.Client(), "CosiftBot/test")
	sitemaps := r.Sitemaps(context.Background(), srv.URL)
	if len(sitemaps) != 2 {
		t.Fatalf("want 2 sitemaps from robots.txt, got %d: %+v", len(sitemaps), sitemaps)
	}
	if sitemaps[0] != "https://srv.test/sitemap-1.xml" {
		t.Errorf("first sitemap: got %q", sitemaps[0])
	}
}

func TestRobotsSitemapsReturnsCopy(t *testing.T) {
	// Mutating the returned slice should NOT affect the cached entry.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("Sitemap: https://x/sm.xml\n"))
	}))
	defer srv.Close()

	r := NewRobots(srv.Client(), "CosiftBot/test")
	first := r.Sitemaps(context.Background(), srv.URL)
	if len(first) != 1 {
		t.Fatalf("first call: want 1 sitemap, got %d", len(first))
	}
	first[0] = "MUTATED"

	second := r.Sitemaps(context.Background(), srv.URL)
	if len(second) != 1 || second[0] != "https://x/sm.xml" {
		t.Errorf("second call should return original cached value; got %+v", second)
	}
}

func TestRobotsSitemapsNoRobots(t *testing.T) {
	// 404 robots.txt → empty sitemaps (graceful, matches Allowed() degradation).
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer srv.Close()

	r := NewRobots(srv.Client(), "CosiftBot/test")
	sitemaps := r.Sitemaps(context.Background(), srv.URL)
	if len(sitemaps) != 0 {
		t.Errorf("missing robots.txt: want empty, got %+v", sitemaps)
	}
}
