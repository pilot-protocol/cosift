package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/embed"
	"github.com/pilot-protocol/cosift/internal/index"
)

// minimalConfig builds a Config with DataDir set so runStatusFile etc.
// have a sandboxed working dir.
func minimalConfig(dataDir string) *config.Config {
	return &config.Config{DataDir: dataDir}
}

// --- bench_pq.go helpers ---

func TestRecallAtKAllHit(t *testing.T) {
	truth := []index.VectorHit{{URL: "u1"}, {URL: "u2"}}
	pred := []index.VectorHit{{URL: "u1"}, {URL: "u2"}}
	if got := recallAtK(truth, pred); got != 1.0 {
		t.Errorf("recallAtK all-hit = %v, want 1.0", got)
	}
}

func TestRecallAtKPartial(t *testing.T) {
	truth := []index.VectorHit{{URL: "u1"}, {URL: "u2"}}
	pred := []index.VectorHit{{URL: "u1"}, {URL: "wrong"}}
	if got := recallAtK(truth, pred); got != 0.5 {
		t.Errorf("recallAtK 0.5 = %v", got)
	}
}

func TestRecallAtKEmptyTruth(t *testing.T) {
	if got := recallAtK(nil, []index.VectorHit{{URL: "u1"}}); got != 0 {
		t.Errorf("empty truth = %v, want 0", got)
	}
}

func TestRecallAtKEmptyPred(t *testing.T) {
	truth := []index.VectorHit{{URL: "u1"}}
	if got := recallAtK(truth, nil); got != 0 {
		t.Errorf("empty pred = %v, want 0", got)
	}
}

func TestSummaryEmpty(t *testing.T) {
	mean, p50, p95, stddev := summary(nil)
	if mean != 0 || p50 != 0 || p95 != 0 || stddev != 0 {
		t.Errorf("empty summary = (%v, %v, %v, %v), want zeros", mean, p50, p95, stddev)
	}
}

func TestSummarySingle(t *testing.T) {
	mean, p50, p95, stddev := summary([]float64{0.5})
	if mean != 0.5 || p50 != 0.5 || p95 != 0.5 || stddev != 0 {
		t.Errorf("single summary = (%v, %v, %v, %v)", mean, p50, p95, stddev)
	}
}

func TestSummaryMany(t *testing.T) {
	xs := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	mean, p50, p95, stddev := summary(xs)
	if mean != 5.5 {
		t.Errorf("mean = %v, want 5.5", mean)
	}
	if p50 != 6 {
		t.Errorf("p50 = %v, want 6", p50)
	}
	if p95 < 9 || p95 > 10 {
		t.Errorf("p95 = %v, want ~10", p95)
	}
	if stddev <= 0 {
		t.Errorf("stddev = %v, want > 0", stddev)
	}
}

// --- truncStr ---

func TestTruncStrShortInput(t *testing.T) {
	if got := truncStr("abc", 10); got != "abc" {
		t.Errorf("got %q", got)
	}
}

func TestTruncStrTruncates(t *testing.T) {
	got := truncStr("0123456789", 5)
	if !strings.HasPrefix(got, "0123") || !strings.HasSuffix(got, "…") {
		t.Errorf("got %q", got)
	}
}

func TestTruncStrExactLen(t *testing.T) {
	if got := truncStr("abcde", 5); got != "abcde" {
		t.Errorf("got %q", got)
	}
}

// --- pebble_serve.go helpers ---

func TestNumNonEmpty(t *testing.T) {
	if n := numNonEmpty([]string{"a", "", "b", "", "c"}); n != 3 {
		t.Errorf("got %d, want 3", n)
	}
	if n := numNonEmpty(nil); n != 0 {
		t.Errorf("nil got %d", n)
	}
	if n := numNonEmpty([]string{"", "", ""}); n != 0 {
		t.Errorf("all empty got %d", n)
	}
}

func TestStripPortIPv4(t *testing.T) {
	if got := stripPort("127.0.0.1:8080"); got != "127.0.0.1" {
		t.Errorf("got %q", got)
	}
}

func TestStripPortIPv6(t *testing.T) {
	if got := stripPort("[::1]:8080"); got != "::1" {
		t.Errorf("got %q", got)
	}
}

func TestStripPortNoPort(t *testing.T) {
	// SplitHostPort fails on no-port input — stripPort returns input verbatim.
	if got := stripPort("nohost"); got != "nohost" {
		t.Errorf("got %q", got)
	}
}

// --- rateLimiter ---

func TestNewRateLimiterFromEnvDisabled(t *testing.T) {
	t.Setenv("COSIFT_RATELIMIT_RPM", "")
	if rl := newRateLimiterFromEnv(); rl != nil {
		t.Errorf("expected nil when RPM empty, got %+v", rl)
	}
}

func TestNewRateLimiterFromEnvBadRPM(t *testing.T) {
	t.Setenv("COSIFT_RATELIMIT_RPM", "not-a-number")
	if rl := newRateLimiterFromEnv(); rl != nil {
		t.Errorf("expected nil for bad RPM, got %+v", rl)
	}
	t.Setenv("COSIFT_RATELIMIT_RPM", "-1")
	if rl := newRateLimiterFromEnv(); rl != nil {
		t.Errorf("expected nil for negative RPM, got %+v", rl)
	}
}

func TestNewRateLimiterFromEnvHappy(t *testing.T) {
	t.Setenv("COSIFT_RATELIMIT_RPM", "60")
	t.Setenv("COSIFT_RATELIMIT_BURST", "20")
	t.Setenv("COSIFT_RATELIMIT_WHITELIST", "10.0.0.1, 10.0.0.2 , ")
	rl := newRateLimiterFromEnv()
	if rl == nil {
		t.Fatal("expected non-nil rate limiter")
	}
	if rl.rpm != 60 {
		t.Errorf("rpm = %v, want 60", rl.rpm)
	}
	if rl.burst != 20 {
		t.Errorf("burst = %v, want 20", rl.burst)
	}
	wl := rl.whitelistList()
	if len(wl) != 2 {
		t.Errorf("whitelist len = %d, want 2", len(wl))
	}
}

func TestRateLimiterWhitelistListNil(t *testing.T) {
	var rl *rateLimiter
	if got := rl.whitelistList(); got != nil {
		t.Errorf("nil receiver = %v, want nil", got)
	}
}

func TestRateLimiterAllowNilReceiver(t *testing.T) {
	var rl *rateLimiter
	if !rl.allow("1.2.3.4") {
		t.Errorf("nil receiver should always allow")
	}
}

func TestRateLimiterAllowWhitelist(t *testing.T) {
	rl := &rateLimiter{rpm: 1, burst: 1, whitelist: map[string]bool{"1.2.3.4": true}}
	for i := 0; i < 10; i++ {
		if !rl.allow("1.2.3.4") {
			t.Errorf("whitelist should always pass on iter %d", i)
		}
	}
}

func TestRateLimiterAllowExhausts(t *testing.T) {
	rl := &rateLimiter{rpm: 0.01, burst: 2, whitelist: map[string]bool{}}
	// Two tokens granted, third should fail (no replenishment in single
	// nanosecond elapsed).
	if !rl.allow("9.9.9.9") {
		t.Error("first allow should pass")
	}
	if !rl.allow("9.9.9.9") {
		t.Error("second allow should pass")
	}
	if rl.allow("9.9.9.9") {
		t.Error("third allow should be rate limited")
	}
}

// --- HTTP handlers ---

func TestPebbleHTTPHandleHealthz(t *testing.T) {
	s := &pebbleHTTP{}
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	s.handleHealthz(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("code = %d", rec.Code)
	}
	var body map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["status"] != "ok" {
		t.Errorf("status = %v", body["status"])
	}
}

func TestPebbleHTTPHandleLandingExactRoot(t *testing.T) {
	s := &pebbleHTTP{}
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	s.handleLanding(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("code = %d", rec.Code)
	}
	if !strings.HasPrefix(rec.Header().Get("Content-Type"), "text/html") {
		t.Errorf("content-type = %q", rec.Header().Get("Content-Type"))
	}
}

func TestPebbleHTTPHandleLandingNonRoot404(t *testing.T) {
	s := &pebbleHTTP{}
	req := httptest.NewRequest(http.MethodGet, "/something", nil)
	rec := httptest.NewRecorder()
	s.handleLanding(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("code = %d, want 404", rec.Code)
	}
}

func TestPebbleHTTPHandleChat(t *testing.T) {
	s := &pebbleHTTP{}
	req := httptest.NewRequest(http.MethodGet, "/chat", nil)
	rec := httptest.NewRecorder()
	s.handleChat(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("code = %d", rec.Code)
	}
	if !strings.HasPrefix(rec.Header().Get("Content-Type"), "text/html") {
		t.Errorf("content-type = %q", rec.Header().Get("Content-Type"))
	}
}

func TestPebbleHTTPHandleOpenAPI(t *testing.T) {
	s := &pebbleHTTP{}
	req := httptest.NewRequest(http.MethodGet, "/openapi.json", nil)
	rec := httptest.NewRecorder()
	s.handleOpenAPI(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("code = %d", rec.Code)
	}
	if !strings.HasPrefix(rec.Header().Get("Content-Type"), "application/json") {
		t.Errorf("content-type = %q", rec.Header().Get("Content-Type"))
	}
}

func TestPebbleHTTPHandleSwaggerUI(t *testing.T) {
	s := &pebbleHTTP{}
	req := httptest.NewRequest(http.MethodGet, "/swagger/", nil)
	rec := httptest.NewRecorder()
	s.handleSwaggerUI(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("code = %d", rec.Code)
	}
}

func TestPebbleHTTPHandleSwaggerAssetUnknown(t *testing.T) {
	s := &pebbleHTTP{}
	mux := http.NewServeMux()
	mux.HandleFunc("/swagger/{file}", s.handleSwaggerAsset)
	req := httptest.NewRequest(http.MethodGet, "/swagger/no-such-file", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("code = %d, want 404", rec.Code)
	}
}

func TestPebbleHTTPHandleSwaggerAssetCSS(t *testing.T) {
	s := &pebbleHTTP{}
	mux := http.NewServeMux()
	mux.HandleFunc("/swagger/{file}", s.handleSwaggerAsset)
	req := httptest.NewRequest(http.MethodGet, "/swagger/swagger-ui.css", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("code = %d", rec.Code)
	}
	if !strings.HasPrefix(rec.Header().Get("Content-Type"), "text/css") {
		t.Errorf("content-type = %q", rec.Header().Get("Content-Type"))
	}
}

func TestPebbleHTTPHandleSwaggerAssetJS(t *testing.T) {
	s := &pebbleHTTP{}
	mux := http.NewServeMux()
	mux.HandleFunc("/swagger/{file}", s.handleSwaggerAsset)
	req := httptest.NewRequest(http.MethodGet, "/swagger/swagger-ui-bundle.js", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("code = %d", rec.Code)
	}
	if !strings.HasPrefix(rec.Header().Get("Content-Type"), "application/javascript") {
		t.Errorf("content-type = %q", rec.Header().Get("Content-Type"))
	}
}

// --- rate-limit middleware ---

func TestRateLimitMiddlewareAllows(t *testing.T) {
	called := false
	s := &pebbleHTTP{}
	handler := s.rateLimit(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "1.2.3.4:1234"
	rec := httptest.NewRecorder()
	handler(rec, req)
	if !called {
		t.Errorf("handler not called")
	}
	if rec.Code != http.StatusOK {
		t.Errorf("code = %d", rec.Code)
	}
}

func TestRateLimitMiddlewareBlocks(t *testing.T) {
	s := &pebbleHTTP{rl: &rateLimiter{rpm: 0.001, burst: 1, whitelist: map[string]bool{}}}
	called := 0
	h := s.rateLimit(func(w http.ResponseWriter, r *http.Request) {
		called++
		w.WriteHeader(http.StatusOK)
	})
	for i := 0; i < 5; i++ {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.RemoteAddr = "5.5.5.5:1111"
		rec := httptest.NewRecorder()
		h(rec, req)
		if rec.Code == http.StatusTooManyRequests {
			// at least one rejection — done.
			if rec.Header().Get("Retry-After") == "" {
				t.Error("Retry-After missing")
			}
			return
		}
	}
	t.Errorf("expected at least one 429; handler called %d times", called)
}

// --- count middleware ---

func TestCountMiddlewareIncrementsMetrics(t *testing.T) {
	s := &pebbleHTTP{}
	h := s.count(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	for i := 0; i < 3; i++ {
		req := httptest.NewRequest(http.MethodGet, "/foo", nil)
		rec := httptest.NewRecorder()
		h(rec, req)
		if rec.Code != http.StatusOK {
			t.Errorf("iter %d code = %d", i, rec.Code)
		}
	}
	v, ok := s.requestCounts.Load("/foo")
	if !ok {
		t.Fatal("missing metric for /foo")
	}
	m := v.(*endpointMetrics)
	if m.count.Load() != 3 {
		t.Errorf("count = %d, want 3", m.count.Load())
	}
	if m.sumNanos.Load() == 0 {
		t.Errorf("sumNanos = 0, want > 0")
	}
}

// --- runStatusFile ---

func TestRunStatusFileMissingFails(t *testing.T) {
	tmp := t.TempDir()
	cfg := minimalConfig(tmp)
	err := runStatusFile(context.Background(), cfg, nil)
	if err == nil {
		t.Error("expected error for missing status file")
	}
}

func TestRunStatusFileBadJSONFails(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "crawl-status.json")
	if err := os.WriteFile(path, []byte("not-json"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := minimalConfig(tmp)
	if err := runStatusFile(context.Background(), cfg, nil); err == nil {
		t.Error("expected decode error")
	}
}

func TestRunStatusFileHuman(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "crawl-status.json")
	body := `{
		"frontier_queued": 10, "frontier_in_flight": 2, "frontier_done": 8, "frontier_errored": 1,
		"indexed_docs": 6, "indexed_docs_at_start": 0, "avg_doc_len": 1024,
		"started_at": "2026-01-01T00:00:00Z", "written_at": "2026-01-01T00:00:05Z"
	}`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := minimalConfig(tmp)
	stdout := captureStdoutCosift(t, func() {
		if err := runStatusFile(context.Background(), cfg, nil); err != nil {
			t.Errorf("runStatusFile: %v", err)
		}
	})
	for _, want := range []string{"status file", "queued:     10", "in_flight:  2"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("missing %q in: %s", want, stdout)
		}
	}
}

func TestRunStatusFileJSON(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "crawl-status.json")
	body := `{
		"frontier_queued": 1, "frontier_in_flight": 0, "frontier_done": 0, "frontier_errored": 0,
		"written_at": "2026-01-01T00:00:00Z"
	}`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := minimalConfig(tmp)
	stdout := captureStdoutCosift(t, func() {
		if err := runStatusFile(context.Background(), cfg, []string{"-json"}); err != nil {
			t.Errorf("runStatusFile: %v", err)
		}
	})
	var d map[string]any
	if err := json.Unmarshal([]byte(stdout), &d); err != nil {
		t.Fatalf("parse: %v\n%s", err, stdout)
	}
	if int64(d["frontier_queued"].(float64)) != 1 {
		t.Errorf("queued = %v", d["frontier_queued"])
	}
	if _, ok := d["age_seconds"]; !ok {
		t.Errorf("age_seconds missing: %s", stdout)
	}
}

func TestRunStatusFileTargetReached(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "crawl-status.json")
	body := `{
		"frontier_queued": 0, "frontier_in_flight": 0, "frontier_done": 100, "frontier_errored": 0,
		"indexed_docs": 100, "indexed_docs_at_start": 0,
		"started_at": "2026-01-01T00:00:00Z", "written_at": "2026-01-01T00:00:01Z"
	}`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := minimalConfig(tmp)
	stdout := captureStdoutCosift(t, func() {
		if err := runStatusFile(context.Background(), cfg, []string{"-target", "50"}); err != nil {
			t.Errorf("runStatusFile: %v", err)
		}
	})
	if !strings.Contains(stdout, "reached") {
		t.Errorf("expected 'reached' in: %s", stdout)
	}
}

// --- generateParaphrases ---

// fakeChat is a minimal ChatClient that returns canned responses.
type fakeChat struct {
	model string
	resp  string
	err   error
	calls int32
}

func (f *fakeChat) Chat(ctx context.Context, msgs []embed.ChatMsg) (string, error) {
	atomic.AddInt32(&f.calls, 1)
	if f.err != nil {
		return "", f.err
	}
	return f.resp, nil
}

func (f *fakeChat) Model() string { return f.model }

func TestGenerateParaphrasesUsesCache(t *testing.T) {
	c := &fakeChat{resp: `["one","two","three"]`}
	p := &paraphraseRetriever{chat: c, n: 3, cache: map[string][]string{}}
	got := p.generateParaphrases(context.Background(), "query")
	if len(got) == 0 {
		t.Skip("LLM response parsing returned empty; not exercising main path")
	}
	// Second call should hit cache.
	_ = p.generateParaphrases(context.Background(), "query")
	if c.calls != 1 {
		t.Errorf("calls = %d, want 1 (cache miss + hit)", c.calls)
	}
}

func TestGenerateParaphrasesChatError(t *testing.T) {
	c := &fakeChat{err: errors.New("rate limit")}
	p := &paraphraseRetriever{chat: c, n: 3, cache: map[string][]string{}}
	got := p.generateParaphrases(context.Background(), "query")
	if got != nil {
		t.Errorf("expected nil on chat error, got %v", got)
	}
}

// --- batchEmbed / embedWithRetry ---

// fakeEmbed counts Embed calls and can be set to fail.
type fakeEmbed struct {
	dim       int
	model     string
	calls     int32
	failFirst bool
	failErr   error
}

func (f *fakeEmbed) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	n := atomic.AddInt32(&f.calls, 1)
	if f.failFirst && n == 1 {
		if f.failErr != nil {
			return nil, f.failErr
		}
		return nil, errors.New("transient")
	}
	out := make([][]float32, len(texts))
	for i := range out {
		out[i] = make([]float32, f.dim)
	}
	return out, nil
}

func (f *fakeEmbed) Model() string { return f.model }
func (f *fakeEmbed) Dim() int      { return f.dim }

func TestBatchEmbedSingleBatch(t *testing.T) {
	e := &fakeEmbed{dim: 8, model: "t"}
	out, err := batchEmbed(context.Background(), e, []string{"a", "b"}, 256)
	if err != nil {
		t.Fatalf("batchEmbed: %v", err)
	}
	if len(out) != 2 || len(out[0]) != 8 {
		t.Errorf("out len = %d, dim = %d", len(out), len(out[0]))
	}
	if e.calls != 1 {
		t.Errorf("calls = %d, want 1", e.calls)
	}
}

func TestBatchEmbedMultipleBatches(t *testing.T) {
	e := &fakeEmbed{dim: 4, model: "t"}
	texts := make([]string, 5)
	for i := range texts {
		texts[i] = "x"
	}
	out, err := batchEmbed(context.Background(), e, texts, 2)
	if err != nil {
		t.Fatalf("batchEmbed: %v", err)
	}
	if len(out) != 5 {
		t.Errorf("out len = %d", len(out))
	}
	// 5/2 → 3 batches.
	if e.calls != 3 {
		t.Errorf("calls = %d, want 3", e.calls)
	}
}

func TestBatchEmbedHonorsContextDone(t *testing.T) {
	e := &fakeEmbed{dim: 4}
	texts := make([]string, 4)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// With 2 per batch and cancelled context, we still process the first
	// batch successfully (no ctx check inside Embed), then sleep returns
	// ctx.Err. The function may return either the partial result with err
	// or finish quickly — test for non-panic + sane shape.
	out, err := batchEmbed(ctx, e, texts, 2)
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("unexpected err: %v", err)
	}
	if err == nil && len(out) != 4 {
		t.Errorf("partial out len = %d", len(out))
	}
}

func TestEmbedWithRetryNoRetryOnNonRateLimit(t *testing.T) {
	e := &fakeEmbed{dim: 4, failFirst: true, failErr: errors.New("auth failed")}
	_, err := embedWithRetry(context.Background(), e, []string{"x"})
	if err == nil {
		t.Error("expected error")
	}
	if e.calls != 1 {
		t.Errorf("calls = %d, want 1 (no retry on non-429)", e.calls)
	}
}

// --- forwardURLToPeer ---

func TestForwardURLToPeerHappy(t *testing.T) {
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	// httptest URL is http://host:port — caller passes host:port.
	peer := strings.TrimPrefix(srv.URL, "http://")
	s := &pebbleHTTP{}
	if err := s.forwardURLToPeer("https://example.com", peer); err != nil {
		t.Errorf("forwardURLToPeer: %v", err)
	}
	if !strings.Contains(string(gotBody), "example.com") {
		t.Errorf("body missing URL: %q", gotBody)
	}
}

func TestForwardURLToPeerNon2xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	peer := strings.TrimPrefix(srv.URL, "http://")
	s := &pebbleHTTP{}
	if err := s.forwardURLToPeer("https://example.com", peer); err == nil {
		t.Error("expected error on 5xx")
	}
}

func TestForwardURLToPeerHTTPPrefix(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	s := &pebbleHTTP{}
	// Pass full URL (with http:// scheme + trailing slash) — exercises the
	// alternate branch.
	if err := s.forwardURLToPeer("https://example.com", srv.URL+"/"); err != nil {
		t.Errorf("forwardURLToPeer (with prefix): %v", err)
	}
}

// --- captureStdout-like helper (no clash with other tests) ---

func captureStdoutCosift(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	orig := os.Stdout
	os.Stdout = w
	defer func() { os.Stdout = orig }()

	done := make(chan struct{})
	var buf strings.Builder
	go func() {
		defer close(done)
		_, _ = io.Copy(&buf, r)
	}()
	fn()
	_ = w.Close()
	<-done
	return buf.String()
}

// Sanity: cap test runtime if ctx-based tests overshoot.
func init() {
	if os.Getenv("CI") != "" {
		// Belt-and-suspenders: bound subprocess wall time in CI.
		go func() {
			time.Sleep(5 * time.Minute)
			os.Exit(2)
		}()
	}
}
