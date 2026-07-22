package crawler

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/config"
)

func robotsCrawlerT(t *testing.T, crawlDelay string, maxDelayMs int) (*Crawler, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/robots.txt" {
			fmt.Fprintf(w, "User-agent: *\nCrawl-delay: %s\n", crawlDelay)
			return
		}
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprintf(w, "<html><head><title>p</title></head><body><p>page %s body text</p></body></html>", r.URL.Path)
	}))
	t.Cleanup(srv.Close)

	cfg := config.Default().Crawler
	cfg.MaxDepth = 0
	cfg.PerHostDelayMs = 0
	cfg.MaxConcurrent = 2
	cfg.RespectRobots = true
	cfg.MaxCrawlDelayMs = maxDelayMs
	return New(cfg, newStoreT(t)).WithEmbedder(&stubEmbedder{dim: 8}), srv
}

func TestRobotsCrawlDelayPacesConcurrentWorkers(t *testing.T) {
	c, srv := robotsCrawlerT(t, "0.5", 0)
	for _, p := range []string{"/a", "/b", "/c"} {
		if err := c.Seed(srv.URL + p); err != nil {
			t.Fatalf("seed %s: %v", p, err)
		}
	}

	start := time.Now()
	if err := c.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	// Three same-host fetches at 0.5s Crawl-delay: slots at 0 / 0.5s / 1s.
	if elapsed := time.Since(start); elapsed < 900*time.Millisecond {
		t.Errorf("Crawl-delay not paced through the gate: 3 fetches in %v, want ≥900ms", elapsed)
	}
	for _, p := range []string{"/a", "/b", "/c"} {
		if doc, _ := c.store.GetDocByURL(context.Background(), srv.URL+p); doc == nil {
			t.Errorf("%s not indexed", p)
		}
	}
}

func TestRobotsCrawlDelayClamped(t *testing.T) {
	c, srv := robotsCrawlerT(t, "30", 200) // hostile 30s delay, clamped to 200ms
	for _, p := range []string{"/a", "/b"} {
		if err := c.Seed(srv.URL + p); err != nil {
			t.Fatalf("seed %s: %v", p, err)
		}
	}

	start := time.Now()
	if err := c.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("clamp not applied: 2 fetches took %v with a 30s Crawl-delay", elapsed)
	}
}

func TestClaimTimeDropCountedAndTerminal(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("disallowed URL was fetched: %s", r.URL)
	}))
	defer srv.Close()

	cfg := config.Default().Crawler
	cfg.MaxDepth = 0
	cfg.PerHostDelayMs = 0
	cfg.MaxConcurrent = 1
	cfg.RespectRobots = false
	cfg.IncludeDomains = []string{"allowed.example"}
	c := New(cfg, newStoreT(t)).WithEmbedder(&stubEmbedder{dim: 8})

	// Push directly: Seed rejects disallowed domains up front, but frontier
	// entries can predate an allowlist change — the claim-time check is
	// what handles those.
	for _, p := range []string{"/one", "/two"} {
		if err := c.store.PushFrontierLane(context.Background(), srv.URL+p, 0, 2, 1.0); err != nil {
			t.Fatalf("push: %v", err)
		}
	}
	if err := c.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}

	dropped, _ := c.PolitenessStats()
	if dropped != 2 {
		t.Errorf("dropped_disallowed: got %d want 2", dropped)
	}
}
