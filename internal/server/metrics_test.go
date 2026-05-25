package server

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/calinteodor/cosift/internal/index"
	"github.com/calinteodor/cosift/internal/store"
)

func TestMetricsHistogramBuckets(t *testing.T) {
	m := NewMetrics()
	// Observations at: 1ms, 50ms, 200ms, 800ms, 3000ms.
	m.RecordRequest("/x", 1*time.Millisecond)
	m.RecordRequest("/x", 50*time.Millisecond)
	m.RecordRequest("/x", 200*time.Millisecond)
	m.RecordRequest("/x", 800*time.Millisecond)
	m.RecordRequest("/x", 3000*time.Millisecond)

	var buf bytes.Buffer
	m.WritePrometheus(&buf)
	out := buf.String()

	for _, want := range []string{
		`cosift_requests_total{path="/x"} 5`,
		`cosift_request_duration_seconds_bucket{path="/x",le="+Inf"} 5`,
		`cosift_request_duration_seconds_count{path="/x"} 5`,
		// 1ms → le=0.005,0.01,0.025,0.05,... all include it.
		`cosift_request_duration_seconds_bucket{path="/x",le="0.005"} 1`,
		// 50ms → falls into le=0.05 onwards. Cumulative count there = 2 (1ms + 50ms).
		`cosift_request_duration_seconds_bucket{path="/x",le="0.05"} 2`,
		// 200ms (le=0.25) → 3
		`cosift_request_duration_seconds_bucket{path="/x",le="0.25"} 3`,
		// 800ms (le=1) → 4
		`cosift_request_duration_seconds_bucket{path="/x",le="1"} 4`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing line: %q\noutput was:\n%s", want, out)
		}
	}
}

func TestMetricsRateLimitCounter(t *testing.T) {
	m := NewMetrics()
	m.RecordRateLimit()
	m.RecordRateLimit()
	m.RecordRateLimit()
	var buf bytes.Buffer
	m.WritePrometheus(&buf)
	if !strings.Contains(buf.String(), "cosift_rate_limit_denied_total 3") {
		t.Errorf("rate-limit counter: %s", buf.String())
	}
}

func TestMetricsEndpointThroughServer(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	s, _ := store.Open(dir)
	t.Cleanup(func() { s.Close() })

	srv := New(s)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	// Hit a couple of endpoints so there's something to scrape.
	http.Get(httpSrv.URL + "/healthz")
	http.Get(httpSrv.URL + "/stats")
	http.Get(httpSrv.URL + "/stats")

	resp, err := http.Get(httpSrv.URL + "/metrics")
	if err != nil {
		t.Fatalf("get metrics: %v", err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("Content-Type: %q", ct)
	}
	body, _ := io.ReadAll(resp.Body)
	out := string(body)
	if !strings.Contains(out, `cosift_requests_total{path="/healthz"} 1`) {
		t.Errorf("expected /healthz counter, got:\n%s", out)
	}
	if !strings.Contains(out, `cosift_requests_total{path="/stats"} 2`) {
		t.Errorf("expected /stats counter == 2, got:\n%s", out)
	}
	// /metrics itself should NOT be counted on this scrape (we scrape AFTER
	// the request is recorded; the next scrape would see /metrics: 1).
	// Just verify the histogram lines are present.
	if !strings.Contains(out, "cosift_request_duration_seconds_bucket") {
		t.Errorf("missing histogram in /metrics output")
	}
}

func TestCorpusSizeGauge(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	s, _ := store.Open(dir)
	t.Cleanup(func() { s.Close() })

	// Empty store → gauge reads zero.
	srv := New(s)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	resp, _ := http.Get(httpSrv.URL + "/metrics")
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	for _, want := range []string{
		`cosift_corpus_size{type="documents"} 0`,
		`cosift_corpus_size{type="passages"} 0`,
		`cosift_corpus_size{type="paraphrases"} 0`,
	} {
		if !strings.Contains(string(body), want) {
			t.Errorf("missing zero-state line: %q\n%s", want, body)
		}
	}

	// Ingest 3 docs + 2 passages + 1 paraphrase. Gauge should reflect.
	idx := index.NewBM25(s)
	for _, url := range []string{"a", "b", "c"} {
		id, _ := s.UpsertDocument(context.Background(), &store.Document{
			URL: "https://x/" + url, Title: url, Text: "body " + url,
			Source: "test", FetchedAt: time.Now(),
		})
		_ = idx.IndexDocument(context.Background(), id, url, "body "+url)
		if url == "a" {
			_ = s.UpsertPassage(context.Background(), &store.Passage{
				DocID: id, Offset: 0, Model: "m", Embedding: []float32{1, 0, 0, 0},
			})
			_ = s.UpsertPassage(context.Background(), &store.Passage{
				DocID: id, Offset: 100, Model: "m", Embedding: []float32{0, 1, 0, 0},
			})
		}
	}
	_ = s.SaveParaphrases(context.Background(), "stub", "test query", []string{"para"})

	resp, _ = http.Get(httpSrv.URL + "/metrics")
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	for _, want := range []string{
		`cosift_corpus_size{type="documents"} 3`,
		`cosift_corpus_size{type="passages"} 2`,
		`cosift_corpus_size{type="paraphrases"} 1`,
	} {
		if !strings.Contains(string(body), want) {
			t.Errorf("missing post-ingest line: %q\n%s", want, body)
		}
	}
}

func TestMetricsCountsRateLimitDenial(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	s, _ := store.Open(dir)
	t.Cleanup(func() { s.Close() })

	srv := New(s).WithLLMLimiter(1, time.Minute) // 1/min so the second hit denies
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	// First request: allowed (404-ish — chat not configured, but limiter checked first).
	_, _ = http.Get(httpSrv.URL + "/answer?q=hi")
	// Second: should be denied (429).
	resp, _ := http.Get(httpSrv.URL + "/answer?q=hi")
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Errorf("second hit: got %d want 429", resp.StatusCode)
	}

	metricsResp, _ := http.Get(httpSrv.URL + "/metrics")
	body, _ := io.ReadAll(metricsResp.Body)
	metricsResp.Body.Close()
	if !strings.Contains(string(body), "cosift_rate_limit_denied_total 1") {
		t.Errorf("rate-limit denial counter not incremented; got:\n%s", string(body))
	}
}

// context import keeper — eliminates the "imported and not used" complaint
// if the file ends up referencing it later.
var _ = context.Background

func TestInfoMetricShape(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	s, _ := store.Open(dir)
	t.Cleanup(func() { s.Close() })

	srv := New(s).WithAdminToken("k")
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	resp, err := http.Get(httpSrv.URL + "/metrics")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	out := string(body)

	// Must contain the HELP/TYPE preamble.
	if !strings.Contains(out, "# HELP cosift_info") {
		t.Errorf("missing cosift_info HELP line\n%s", out)
	}
	if !strings.Contains(out, "# TYPE cosift_info gauge") {
		t.Errorf("missing cosift_info TYPE line")
	}
	// Must contain the version label (defaults to "dev" when not stamped).
	if !strings.Contains(out, `version="dev"`) {
		t.Errorf("missing version=dev label")
	}
	// Capability flags on a bare BM25-only server.
	if !strings.Contains(out, `dense_enabled="false"`) {
		t.Errorf("expected dense_enabled=false on bm25-only server")
	}
	if !strings.Contains(out, `answer_enabled="false"`) {
		t.Errorf("expected answer_enabled=false on bm25-only server")
	}
	if !strings.Contains(out, `admin_enabled="true"`) {
		t.Errorf("expected admin_enabled=true (WithAdminToken was called)")
	}
	// Value is always 1.
	if !strings.Contains(out, "} 1\n") {
		t.Errorf("info metric value should be 1")
	}
}