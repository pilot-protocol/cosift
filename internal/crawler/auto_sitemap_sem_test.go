package crawler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/config"
)

// TestMaybeAutoSitemapDropsWhenSemSaturated locks in the GH200 fix: when
// the auto-sitemap concurrency cap is saturated (spam-discovery storm), new
// hosts are dropped instead of leaking another 5-min goroutine parked on
// PebbleStore.mu.
func TestMaybeAutoSitemapDropsWhenSemSaturated(t *testing.T) {
	cfg := config.Default().Crawler
	cfg.AutoSitemap = true
	cfg.RespectRobots = false

	c := &Crawler{
		cfg:        cfg,
		sitemapSem: make(chan struct{}, 2),
	}
	c.sitemapSem <- struct{}{}
	c.sitemapSem <- struct{}{}

	before := runtime.NumGoroutine()
	u, _ := url.Parse("https://saturated-spam.example/page")
	c.maybeAutoSitemap(context.Background(), u)

	time.Sleep(50 * time.Millisecond)
	after := runtime.NumGoroutine()
	if after > before {
		t.Errorf("expected no new goroutine when sem saturated; before=%d after=%d", before, after)
	}

	c.autoSitemapMu.Lock()
	_, seen := c.autoSitemapSeen["saturated-spam.example"]
	c.autoSitemapMu.Unlock()
	if !seen {
		t.Errorf("host should still be marked seen so we don't retry on every URL")
	}
}

// TestMaybeAutoSitemapReleasesSlotOnCompletion verifies the deferred slot
// release. Without it the semaphore would leak permanently.
func TestMaybeAutoSitemapReleasesSlotOnCompletion(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	cfg := config.Default().Crawler
	cfg.AutoSitemap = true
	cfg.RespectRobots = false

	s := newStoreT(t)
	c := New(cfg, s)
	c.sitemapSem = make(chan struct{}, 1)

	u, _ := url.Parse(srv.URL + "/page")
	c.maybeAutoSitemap(context.Background(), u)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(c.sitemapSem) == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("sem slot never released; len=%d", len(c.sitemapSem))
}
