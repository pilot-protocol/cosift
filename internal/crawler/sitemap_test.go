package crawler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pilot-protocol/cosift/internal/config"
)

const urlsetXML = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>https://example.com/a</loc><lastmod>2024-01-01</lastmod></url>
  <url><loc>https://example.com/b</loc></url>
  <url><loc>https://example.com/c</loc><priority>0.5</priority></url>
</urlset>`

const indexXML = `<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <sitemap><loc>%s/sub1.xml</loc></sitemap>
  <sitemap><loc>%s/sub2.xml</loc></sitemap>
</sitemapindex>`

func TestSeedSitemapURLSet(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(urlsetXML))
	}))
	defer srv.Close()

	s := newStoreT(t)
	cfg := config.Default().Crawler
	cfg.RespectRobots = false
	c := New(cfg, s)

	n, err := c.SeedSitemap(context.Background(), srv.URL+"/sitemap.xml")
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	if n != 3 {
		t.Errorf("seeded: got %d want 3", n)
	}

	stats, _ := s.GetFrontierStats(context.Background())
	if stats.Queued != 3 {
		t.Errorf("frontier queued: got %d want 3", stats.Queued)
	}
}

func TestSeedSitemapIndex(t *testing.T) {
	// A sitemap index pointing at two child urlsets.
	var srvURL string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		switch r.URL.Path {
		case "/sitemap.xml":
			_, _ = w.Write([]byte("<?xml version=\"1.0\"?><sitemapindex xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">" +
				"<sitemap><loc>" + srvURL + "/sub1.xml</loc></sitemap>" +
				"<sitemap><loc>" + srvURL + "/sub2.xml</loc></sitemap>" +
				"</sitemapindex>"))
		case "/sub1.xml":
			_, _ = w.Write([]byte(`<?xml version="1.0"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
				<url><loc>https://example.com/sub1/a</loc></url>
				<url><loc>https://example.com/sub1/b</loc></url>
			</urlset>`))
		case "/sub2.xml":
			_, _ = w.Write([]byte(`<?xml version="1.0"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
				<url><loc>https://example.com/sub2/a</loc></url>
			</urlset>`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	srvURL = srv.URL

	s := newStoreT(t)
	cfg := config.Default().Crawler
	cfg.RespectRobots = false
	c := New(cfg, s)

	n, err := c.SeedSitemap(context.Background(), srv.URL+"/sitemap.xml")
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	if n != 3 {
		t.Errorf("index recursion: got %d want 3 URLs", n)
	}
}

func TestSeedSitemapRespectsDomainFilters(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(urlsetXML))
	}))
	defer srv.Close()

	s := newStoreT(t)
	cfg := config.Default().Crawler
	cfg.RespectRobots = false
	cfg.IncludeDomains = []string{"example.com"} // matches the test URLs
	c := New(cfg, s)

	n, _ := c.SeedSitemap(context.Background(), srv.URL+"/sitemap.xml")
	if n != 3 {
		t.Errorf("with allow-list match: got %d want 3", n)
	}

	// Now exclude — should drop all.
	s2 := newStoreT(t)
	cfg2 := cfg
	cfg2.IncludeDomains = nil
	cfg2.ExcludeDomains = []string{"example.com"}
	c2 := New(cfg2, s2)
	n2, _ := c2.SeedSitemap(context.Background(), srv.URL+"/sitemap.xml")
	if n2 != 0 {
		t.Errorf("with exclude: got %d want 0", n2)
	}
}

func TestSitemapMalformedReturnsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("not xml at all"))
	}))
	defer srv.Close()

	s := newStoreT(t)
	cfg := config.Default().Crawler
	cfg.RespectRobots = false
	c := New(cfg, s)

	_, err := c.SeedSitemap(context.Background(), srv.URL+"/bad.xml")
	if err == nil {
		t.Errorf("expected error on malformed sitemap")
	}
}
