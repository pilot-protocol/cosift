package crawler

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/config"
)

const rateLimitSampleHTML = `<html><head><title>Recovered</title></head>
<body><p>Served after the throttle lifted. Enough text to index.</p></body></html>`

func rateLimitedCrawlerT(t *testing.T, fail429 int64) (*Crawler, *httptest.Server, *atomic.Int64) {
	t.Helper()
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) <= fail429 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(rateLimitSampleHTML))
	}))
	t.Cleanup(srv.Close)

	cfg := config.Default().Crawler
	cfg.MaxDepth = 0
	cfg.PerHostDelayMs = 0
	cfg.MaxConcurrent = 1
	cfg.RespectRobots = false
	return New(cfg, newStoreT(t)).WithEmbedder(&stubEmbedder{dim: 8}), srv, &hits
}

func TestRateLimitedURLRequeuesAndRecovers(t *testing.T) {
	c, srv, hits := rateLimitedCrawlerT(t, 2)

	if err := c.Seed(srv.URL); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := c.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}

	doc, err := c.store.GetDocByURL(context.Background(), srv.URL)
	if err != nil || doc == nil {
		t.Fatalf("doc not indexed after 429 recovery: %v", err)
	}
	if doc.Title != "Recovered" {
		t.Errorf("title: got %q", doc.Title)
	}
	if got := hits.Load(); got != 3 {
		t.Errorf("server hits: got %d want 3 (two 429s, one success)", got)
	}
	if _, deferred := c.PolitenessStats(); deferred != 2 {
		t.Errorf("rate_limited_deferrals: got %d want 2", deferred)
	}
	// Throttling must not poison the success-ratio blacklist: the two 429s
	// stay out of hostStats entirely, only the final success is recorded.
	host := strings.TrimPrefix(srv.URL, "http://")
	if v, ok := c.hostStats.Load(host); ok {
		s := v.(*hostFetchStats)
		if s.attempts.Load() != 1 || s.successes.Load() != 1 {
			t.Errorf("hostStats polluted by 429s: attempts=%d successes=%d",
				s.attempts.Load(), s.successes.Load())
		}
	}
}

func TestRateLimitedURLGivesUpAfterMaxRequeues(t *testing.T) {
	c, srv, hits := rateLimitedCrawlerT(t, 1<<30) // never recovers

	if err := c.Seed(srv.URL); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := c.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}

	// Initial attempt + maxRateLimitRequeues requeues, then FailFrontier.
	if got := hits.Load(); got != maxRateLimitRequeues+1 {
		t.Errorf("server hits: got %d want %d", got, maxRateLimitRequeues+1)
	}
	if doc, _ := c.store.GetDocByURL(context.Background(), srv.URL); doc != nil {
		t.Error("doc should not be indexed for a permanently throttled URL")
	}
}

func TestRateLimitedBackoffWithoutRetryAfter(t *testing.T) {
	// No Retry-After header → the exponential fallback paces the host.
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(rateLimitSampleHTML))
	}))
	defer srv.Close()

	cfg := config.Default().Crawler
	cfg.MaxDepth = 0
	cfg.PerHostDelayMs = 0
	cfg.MaxConcurrent = 1
	cfg.RespectRobots = false
	c := New(cfg, newStoreT(t)).WithEmbedder(&stubEmbedder{dim: 8})

	if err := c.Seed(srv.URL); err != nil {
		t.Fatalf("seed: %v", err)
	}
	start := time.Now()
	if err := c.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	// backoffDelay floors at 1s; first 429 defers the host 1s<<1 = 2s.
	if elapsed := time.Since(start); elapsed < 2*time.Second {
		t.Errorf("fallback backoff not applied: recovered in %v, want ≥2s", elapsed)
	}
	if doc, _ := c.store.GetDocByURL(context.Background(), srv.URL); doc == nil {
		t.Error("doc not indexed after fallback backoff")
	}
}

func TestFetch503RetryAfterSemantics(t *testing.T) {
	// 503 WITH Retry-After is a throttle; without the header it stays a
	// generic failure.
	for _, withHeader := range []bool{true, false} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if withHeader {
				w.Header().Set("Retry-After", "3")
			}
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
		cfg := config.Default().Crawler
		c := New(cfg, newStoreT(t))
		_, err := c.fetch(context.Background(), srv.URL, nil)
		var rle *rateLimitedError
		if got := errors.As(err, &rle); got != withHeader {
			t.Errorf("503 withHeader=%v: rateLimitedError=%v (err %v)", withHeader, got, err)
		}
		if withHeader && rle.retryAfter != 3*time.Second {
			t.Errorf("retryAfter: got %v want 3s", rle.retryAfter)
		}
		srv.Close()
	}
}

func TestMaxCrawlDelayDefaultAndOverride(t *testing.T) {
	cfg := config.Default().Crawler
	c := New(cfg, newStoreT(t))
	if d := c.maxCrawlDelay(); d != 2*time.Minute {
		t.Errorf("default clamp: got %v want 2m", d)
	}
	cfg.MaxCrawlDelayMs = 500
	c = New(cfg, newStoreT(t))
	if d := c.maxCrawlDelay(); d != 500*time.Millisecond {
		t.Errorf("configured clamp: got %v want 500ms", d)
	}
}

func TestParseRetryAfter(t *testing.T) {
	if d := parseRetryAfter("7"); d != 7*time.Second {
		t.Errorf("delta-seconds: got %v", d)
	}
	if d := parseRetryAfter(time.Now().Add(30 * time.Second).UTC().Format(http.TimeFormat)); d < 25*time.Second || d > 30*time.Second {
		t.Errorf("http-date: got %v", d)
	}
	for _, v := range []string{"", "garbage", "-5", time.Now().Add(-time.Minute).UTC().Format(http.TimeFormat)} {
		if d := parseRetryAfter(v); d != 0 {
			t.Errorf("parseRetryAfter(%q): got %v want 0", v, d)
		}
	}
}
