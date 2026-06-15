package crawler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/index"
	"github.com/pilot-protocol/cosift/internal/store"
)

// TestSeedSitemapLane confirms SeedSitemapLane lands every <loc> URL in the
// requested frontier lane (here the high-priority submitted lane, lane 0),
// while plain SeedSitemap keeps using the refresh lane (lane 1). This is the
// plumbing behind /admin/site-submit's "submit a whole site to the priority
// queue".
func TestSeedSitemapLane(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(urlsetXML)) // 3 URLs, defined in sitemap_test.go
	}))
	defer srv.Close()

	dir := filepath.Join(t.TempDir(), "pebble")
	ps, err := store.OpenPebble(dir)
	if err != nil {
		t.Fatalf("OpenPebble: %v", err)
	}
	defer ps.Close()

	cfg := config.Default().Crawler
	cfg.RespectRobots = false
	c := NewWithBackend(cfg, ps, index.NewPebbleBM25(ps))

	n, err := c.SeedSitemapLane(context.Background(), srv.URL+"/sitemap.xml", store.LaneSubmitted)
	if err != nil {
		t.Fatalf("SeedSitemapLane: %v", err)
	}
	if n != 3 {
		t.Fatalf("queued: got %d want 3", n)
	}

	stats, err := ps.GetLaneStats(context.Background())
	if err != nil {
		t.Fatalf("GetLaneStats: %v", err)
	}
	if got := stats.Lanes[store.LaneSubmitted].Queued; got != 3 {
		t.Errorf("submitted lane queued: got %d want 3", got)
	}
	if got := stats.Lanes[store.LaneRefresh].Queued; got != 0 {
		t.Errorf("refresh lane queued: got %d want 0 (should not leak into refresh)", got)
	}
}

// TestSeedSitemapDefaultLane confirms the plain wrapper still uses the refresh
// lane — a regression guard so site-submit's lane change doesn't silently
// alter sitemap-import behavior.
func TestSeedSitemapDefaultLane(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(urlsetXML))
	}))
	defer srv.Close()

	dir := filepath.Join(t.TempDir(), "pebble")
	ps, err := store.OpenPebble(dir)
	if err != nil {
		t.Fatalf("OpenPebble: %v", err)
	}
	defer ps.Close()

	cfg := config.Default().Crawler
	cfg.RespectRobots = false
	c := NewWithBackend(cfg, ps, index.NewPebbleBM25(ps))

	if _, err := c.SeedSitemap(context.Background(), srv.URL+"/sitemap.xml"); err != nil {
		t.Fatalf("SeedSitemap: %v", err)
	}
	stats, err := ps.GetLaneStats(context.Background())
	if err != nil {
		t.Fatalf("GetLaneStats: %v", err)
	}
	if got := stats.Lanes[store.LaneRefresh].Queued; got != 3 {
		t.Errorf("refresh lane queued: got %d want 3", got)
	}
}
