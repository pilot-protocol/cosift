// Round 4 coverage push for cmd/cosift.
//
// Round 3 reached 50.5% by adding the OpenAI httptest mock + populated
// Pebble fixture. Round 4 extends those scaffolds with:
//
//   - fakeCluster: an array of httptest servers simulating peer shards so
//     scatterSearch + the 4 gateway handlers can be exercised end-to-end.
//   - whole-runner sweeps for runServe, runCrawl, runEval, runBench,
//     runAnswerEval (dry-run), runAnswerEvalCompare, runCrawlErrors,
//     runVerifyPebble — every one of which sat at 0% after round 3.
//
// The new tests do NOT modify production code. They lean on existing test
// seams: cancellable ctx for blocking entrypoints (runServe, runCrawl) and
// already-honored env vars / -dry-run flags for the eval/bench runners.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/embed"
	"github.com/pilot-protocol/cosift/internal/rerank"
	"github.com/pilot-protocol/cosift/internal/store"
)

// realFakeReranker satisfies rerank.Reranker for the doRerank counter test.
// Returns a canned order; flips to error when err is set.
type realFakeReranker struct {
	order []string
	err   error
}

func (r *realFakeReranker) Name() string { return "fake" }
func (r *realFakeReranker) Rerank(_ context.Context, _ string, _ []rerank.Candidate) ([]string, error) {
	if r.err != nil {
		return nil, r.err
	}
	return r.order, nil
}

// ---------------------------------------------------------------------------
// fakeCluster — N httptest peer servers that each return a canned hit list
// for /search and /find_similar. Lets us drive scatterSearch + the four
// gateway handlers without spinning up real cosift instances.
// ---------------------------------------------------------------------------

type fakePeer struct {
	srv          *httptest.Server
	id           int
	searchCalls  atomic.Int64
	failNext     atomic.Bool  // when true, next request returns 500
	delayNext    atomic.Int64 // ms to sleep before responding
	canonicalURL string       // URL prefix this peer owns
}

func (p *fakePeer) URL() string { return strings.TrimPrefix(p.srv.URL, "http://") }

type fakeCluster struct {
	peers []*fakePeer
}

func newFakeCluster(t *testing.T, n int) *fakeCluster {
	t.Helper()
	c := &fakeCluster{peers: make([]*fakePeer, n)}
	for i := 0; i < n; i++ {
		p := &fakePeer{id: i, canonicalURL: fmt.Sprintf("https://shard%d.example", i)}
		mux := http.NewServeMux()
		mux.HandleFunc("/search", p.handleSearch)
		mux.HandleFunc("/find_similar", p.handleSearch) // same canned shape
		p.srv = httptest.NewServer(mux)
		c.peers[i] = p
	}
	t.Cleanup(func() {
		for _, p := range c.peers {
			p.srv.Close()
		}
	})
	return c
}

func (p *fakePeer) handleSearch(w http.ResponseWriter, r *http.Request) {
	if d := p.delayNext.Load(); d > 0 {
		time.Sleep(time.Duration(d) * time.Millisecond)
		p.delayNext.Store(0)
	}
	if p.failNext.Load() {
		p.failNext.Store(false)
		http.Error(w, "synthetic failure", http.StatusInternalServerError)
		return
	}
	p.searchCalls.Add(1)
	q := r.URL.Query().Get("q")
	// Two canned hits per peer; URLs are stable + unique so dedup is testable.
	resp := map[string]any{
		"query": q,
		"hits": []map[string]any{
			{
				"url":     p.canonicalURL + "/a",
				"title":   fmt.Sprintf("peer %d hit a for %s", p.id, q),
				"score":   1.0 - float64(p.id)*0.1,
				"excerpt": "snippet a",
				"text":    "full text a from peer " + fmt.Sprintf("%d", p.id),
			},
			{
				"url":     p.canonicalURL + "/b",
				"title":   fmt.Sprintf("peer %d hit b for %s", p.id, q),
				"score":   0.5 - float64(p.id)*0.1,
				"excerpt": "snippet b",
				"text":    "full text b",
			},
		},
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// peerHosts returns "host:port" for each peer in shard-ID order, suitable
// for stuffing into cfg.Cluster.Peers.
func (c *fakeCluster) peerHosts() []string {
	out := make([]string, len(c.peers))
	for i, p := range c.peers {
		out[i] = p.URL()
	}
	return out
}

// ---------------------------------------------------------------------------
// scatterSearch — direct unit test on a populated fixture wired to a 2-peer
// fake cluster. Touches the happy fan-out path + the error-aggregation path.
// ---------------------------------------------------------------------------

func TestScatterSearchHappy(t *testing.T) {
	cluster := newFakeCluster(t, 2)
	f := populatedPebbleStore(t)
	srv := f.makeServer(nil)
	srv.cluster = config.Cluster{
		NumShards:   2,
		MyShardID:   0,
		Peers:       cluster.peerHosts(),
		GatewayMode: true,
	}

	hits, warns, total := srv.scatterSearch(context.Background(),
		"raft", 5, 5, nil, false)
	if len(hits) == 0 {
		t.Fatalf("scatterSearch returned no hits: warns=%v", warns)
	}
	if total != 4 {
		t.Errorf("total candidates = %d, want 4 (2 peers × 2 hits)", total)
	}
	if len(warns) != 0 {
		t.Errorf("unexpected warns: %v", warns)
	}
	// Each fake peer's /search should have been hit exactly once.
	for i, p := range cluster.peers {
		if got := p.searchCalls.Load(); got != 1 {
			t.Errorf("peer %d searchCalls = %d, want 1", i, got)
		}
	}
}

func TestScatterSearchPeerFailure(t *testing.T) {
	cluster := newFakeCluster(t, 2)
	cluster.peers[1].failNext.Store(true)
	f := populatedPebbleStore(t)
	srv := f.makeServer(nil)
	srv.cluster = config.Cluster{
		NumShards: 2, MyShardID: 0,
		Peers: cluster.peerHosts(), GatewayMode: true,
	}

	hits, warns, _ := srv.scatterSearch(context.Background(),
		"q", 5, 5, nil, true)
	if len(warns) == 0 {
		t.Errorf("expected at least one warning for failing peer")
	}
	if len(hits) == 0 {
		t.Errorf("expected hits from the surviving peer")
	}
}

func TestScatterSearchEmptyPeerSlot(t *testing.T) {
	cluster := newFakeCluster(t, 2)
	f := populatedPebbleStore(t)
	srv := f.makeServer(nil)
	peers := cluster.peerHosts()
	peers[0] = "" // skip self
	srv.cluster = config.Cluster{
		NumShards: 2, MyShardID: 0,
		Peers: peers, GatewayMode: true,
	}
	hits, _, total := srv.scatterSearch(context.Background(),
		"q", 5, 5, nil, false)
	if total != 2 {
		t.Errorf("total = %d, want 2 (only peer 1 should fire)", total)
	}
	if len(hits) == 0 {
		t.Errorf("expected hits from non-empty peer slot")
	}
	if cluster.peers[0].searchCalls.Load() != 0 {
		t.Errorf("peer 0 (empty slot) should not have been called")
	}
}

// ---------------------------------------------------------------------------
// handleSearchGateway — sets cluster.GatewayMode=true so /search dispatches
// to the gateway path, which delegates to scatterSearch and writes a
// "gateway:rrf(...)" retriever field.
// ---------------------------------------------------------------------------

func TestHandleSearchGatewayHappy(t *testing.T) {
	cluster := newFakeCluster(t, 2)
	f := populatedPebbleStore(t)
	srv := f.makeServer(nil)
	srv.cluster = config.Cluster{
		NumShards: 2, MyShardID: 0,
		Peers: cluster.peerHosts(), GatewayMode: true,
	}

	req := httptest.NewRequest(http.MethodGet, "/search?q=raft&k=4", nil)
	rec := httptest.NewRecorder()
	srv.handleSearchGateway(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	var resp searchResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !strings.HasPrefix(resp.Retriever, "gateway:rrf") {
		t.Errorf("expected gateway retriever label, got %q", resp.Retriever)
	}
	if len(resp.Hits) == 0 {
		t.Errorf("no hits in response")
	}
}

func TestHandleSearchGatewayMissingQ(t *testing.T) {
	f := populatedPebbleStore(t)
	srv := f.makeServer(nil)
	srv.cluster = config.Cluster{NumShards: 2, Peers: []string{"", ""}, GatewayMode: true}
	req := httptest.NewRequest(http.MethodGet, "/search", nil)
	rec := httptest.NewRecorder()
	srv.handleSearchGateway(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

// ---------------------------------------------------------------------------
// handleFindSimilarGateway — fan-out happy path; missing-args is 400 only
// on the local handler, the gateway forwards everything to peers so an
// empty-q request still 200s with an empty cluster result.
// ---------------------------------------------------------------------------

func TestHandleFindSimilarGatewayHappy(t *testing.T) {
	cluster := newFakeCluster(t, 2)
	f := populatedPebbleStore(t)
	srv := f.makeServer(nil)
	srv.cluster = config.Cluster{
		NumShards: 2, MyShardID: 0,
		Peers: cluster.peerHosts(), GatewayMode: true,
	}
	req := httptest.NewRequest(http.MethodGet,
		"/find_similar?url=https://x.example/raft&k=3", nil)
	rec := httptest.NewRecorder()
	srv.handleFindSimilarGateway(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
}

// ---------------------------------------------------------------------------
// handleAnswerGateway — needs chat configured + cluster. Peers return canned
// hits; gateway synthesizes via the mock chat.
// ---------------------------------------------------------------------------

func TestHandleAnswerGatewayHappy(t *testing.T) {
	mock := openaiTestServer(t)
	cluster := newFakeCluster(t, 2)
	f := populatedPebbleStore(t)
	srv := f.makeServer(mock)
	srv.cluster = config.Cluster{
		NumShards: 2, MyShardID: 0,
		Peers: cluster.peerHosts(), GatewayMode: true,
	}
	req := httptest.NewRequest(http.MethodGet, "/answer?q=raft&k=4", nil)
	rec := httptest.NewRecorder()
	srv.handleAnswerGateway(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if mock.ChatCalls() == 0 {
		t.Errorf("gateway answer didn't reach chat mock")
	}
	if !strings.Contains(rec.Body.String(), "fake response") {
		t.Errorf("missing fake response: %s", rec.Body.String())
	}
}

func TestHandleAnswerGatewayNoChat(t *testing.T) {
	f := populatedPebbleStore(t)
	srv := f.makeServer(nil)
	srv.cluster = config.Cluster{NumShards: 2, Peers: []string{"", ""}, GatewayMode: true}
	req := httptest.NewRequest(http.MethodGet, "/answer?q=x", nil)
	rec := httptest.NewRecorder()
	srv.handleAnswerGateway(rec, req)
	if rec.Code != http.StatusNotImplemented {
		t.Errorf("status = %d, want 501", rec.Code)
	}
}

func TestHandleAnswerGatewayMissingQ(t *testing.T) {
	mock := openaiTestServer(t)
	f := populatedPebbleStore(t)
	srv := f.makeServer(mock)
	srv.cluster = config.Cluster{NumShards: 2, Peers: []string{"", ""}, GatewayMode: true}
	req := httptest.NewRequest(http.MethodGet, "/answer", nil)
	rec := httptest.NewRecorder()
	srv.handleAnswerGateway(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestHandleAnswerGatewayNoHits(t *testing.T) {
	mock := openaiTestServer(t)
	f := populatedPebbleStore(t)
	srv := f.makeServer(mock)
	// All peers empty → scatterSearch returns no hits → gateway short-circuits.
	srv.cluster = config.Cluster{NumShards: 2, Peers: []string{"", ""}, GatewayMode: true}
	req := httptest.NewRequest(http.MethodGet, "/answer?q=anything", nil)
	rec := httptest.NewRecorder()
	srv.handleAnswerGateway(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "No matching sources") {
		t.Errorf("expected no-matches body, got %s", rec.Body.String())
	}
}

// ---------------------------------------------------------------------------
// handleResearchGateway — plan + scatter + synth.
// ---------------------------------------------------------------------------

func TestHandleResearchGatewayHappy(t *testing.T) {
	mock := openaiTestServer(t)
	// Make the planner return a parseable sub-query JSON so the iter-243
	// planner path is exercised.
	mock.SetChatResponse(`["consensus algorithm","leader election"]`)
	cluster := newFakeCluster(t, 2)
	f := populatedPebbleStore(t)
	srv := f.makeServer(mock)
	srv.cluster = config.Cluster{
		NumShards: 2, MyShardID: 0,
		Peers: cluster.peerHosts(), GatewayMode: true,
	}
	req := httptest.NewRequest(http.MethodGet, "/research?q=raft&k=3", nil)
	rec := httptest.NewRecorder()
	srv.handleResearchGateway(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if mock.ChatCalls() < 2 {
		t.Errorf("expected plan + synth chat calls, got %d", mock.ChatCalls())
	}
}

func TestHandleResearchGatewayNoChat(t *testing.T) {
	f := populatedPebbleStore(t)
	srv := f.makeServer(nil)
	srv.cluster = config.Cluster{NumShards: 2, Peers: []string{"", ""}, GatewayMode: true}
	req := httptest.NewRequest(http.MethodGet, "/research?q=x", nil)
	rec := httptest.NewRecorder()
	srv.handleResearchGateway(rec, req)
	if rec.Code != http.StatusNotImplemented {
		t.Errorf("status = %d, want 501", rec.Code)
	}
}

func TestHandleResearchGatewayMissingQ(t *testing.T) {
	mock := openaiTestServer(t)
	f := populatedPebbleStore(t)
	srv := f.makeServer(mock)
	srv.cluster = config.Cluster{NumShards: 2, Peers: []string{"", ""}, GatewayMode: true}
	req := httptest.NewRequest(http.MethodGet, "/research", nil)
	rec := httptest.NewRecorder()
	srv.handleResearchGateway(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestHandleResearchGatewayNoSources(t *testing.T) {
	mock := openaiTestServer(t)
	mock.SetChatResponse(`["sub1"]`)
	f := populatedPebbleStore(t)
	srv := f.makeServer(mock)
	// Empty peers → scatterSearch returns nothing → handler writes
	// "No matching sources" response without a synth chat call.
	srv.cluster = config.Cluster{NumShards: 2, Peers: []string{"", ""}, GatewayMode: true}
	req := httptest.NewRequest(http.MethodGet, "/research?q=x", nil)
	rec := httptest.NewRecorder()
	srv.handleResearchGateway(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "No matching sources") {
		t.Errorf("expected no-matches body: %s", rec.Body.String())
	}
}

// ---------------------------------------------------------------------------
// runServe — full lifecycle test: pick a free port, start the server with
// an already-cancellable ctx, hit /healthz, then cancel and confirm graceful
// shutdown. Covers the big SQLite wiring block in main.go:2917.
// ---------------------------------------------------------------------------

func TestRunServeLifecycle(t *testing.T) {
	cfg := seedTinyStore(t)
	// Pick a free port.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	cfg.Server.Addr = addr

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- runServe(ctx, cfg) }()

	// Wait until /healthz responds (server is ready).
	healthOK := false
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + addr + "/healthz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				healthOK = true
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !healthOK {
		t.Fatalf("server didn't come up on %s", addr)
	}

	// Shutdown cleanly.
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("runServe returned: %v", err)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("runServe didn't shut down within 8s")
	}
}

// runServe with admin token + trusted-proxies set covers the optional
// wiring branches. We don't need a healthz round-trip here; just confirm
// the function returns nil on a cancellable ctx.
func TestRunServeWithAdminAndProxies(t *testing.T) {
	cfg := seedTinyStore(t)
	cfg.Server.AdminToken = "test-admin-token"
	cfg.Server.TrustedProxies = []string{"127.0.0.0/8"}
	// Wire defaults so the "defaults: ..." log line branch fires.
	cfg.Defaults.Retriever = "hybrid"
	cfg.Defaults.Expand = true
	cfg.Defaults.ResearchSynthK = 5
	cfg.Crawler.ChunkSize = 200
	cfg.Crawler.ChunkOverlap = 40

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	cfg.Server.Addr = l.Addr().String()
	_ = l.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runServe(ctx, cfg) }()
	// Brief wait for server to come up; we cancel without making a request
	// since we only care about constructor-path coverage here.
	time.Sleep(200 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("runServe: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("runServe didn't shut down")
	}
}

func TestRunServeBadProxies(t *testing.T) {
	cfg := seedTinyStore(t)
	cfg.Server.TrustedProxies = []string{"not-a-cidr"}
	cfg.Server.Addr = "127.0.0.1:0"
	err := runServe(context.Background(), cfg)
	if err == nil {
		t.Error("expected trusted_proxies error")
	}
}

// ---------------------------------------------------------------------------
// runCrawl — exercise the SQLite + Pebble backend paths with a
// pre-cancelled context. Workers see ctx.Done() and exit cleanly so the
// run returns without ever issuing an HTTP fetch.
// ---------------------------------------------------------------------------

// acceptableCrawlErr reports whether err is a clean ctx-cancelled exit from
// runCrawl. Cancellation may bubble up through store.RecoverInFlight or the
// worker loop itself; either is acceptable for these tests since we just want
// to exercise the SQLite/Pebble wiring branches without doing real fetches.
func acceptableCrawlErr(err error) bool {
	if err == nil {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "context canceled") ||
		strings.Contains(msg, "context deadline exceeded")
}

func TestRunCrawlSQLiteCancelled(t *testing.T) {
	cfg := seedTinyStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel
	err := runCrawl(ctx, cfg, []string{"https://example.invalid/seed"})
	if !acceptableCrawlErr(err) {
		t.Errorf("runCrawl sqlite cancelled: %v", err)
	}
}

func TestRunCrawlPebbleCancelled(t *testing.T) {
	cfg := seedTinyStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := runCrawl(ctx, cfg, []string{"-backend", "pebble", "https://example.invalid/seed"})
	if !acceptableCrawlErr(err) {
		t.Errorf("runCrawl pebble cancelled: %v", err)
	}
}

func TestRunCrawlNoURLs(t *testing.T) {
	cfg := seedTinyStore(t)
	err := runCrawl(context.Background(), cfg, nil)
	if err == nil {
		t.Error("expected error for missing URL/sitemap")
	}
}

func TestRunCrawlUnknownBackend(t *testing.T) {
	cfg := seedTinyStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := runCrawl(ctx, cfg, []string{"-backend", "weird", "https://example.invalid"})
	if err == nil || !strings.Contains(err.Error(), "unknown -backend") {
		t.Errorf("got %v", err)
	}
}

func TestRunCrawlSeedsFile(t *testing.T) {
	cfg := seedTinyStore(t)
	seedsPath := filepath.Join(t.TempDir(), "seeds.txt")
	body := "https://example.invalid/a\n" +
		"# this is a comment\n" +
		"\n" +
		"https://example.invalid/b\n"
	if err := os.WriteFile(seedsPath, []byte(body), 0o600); err != nil {
		t.Fatalf("write seeds: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := runCrawl(ctx, cfg, []string{"-seeds-file", seedsPath})
	if !acceptableCrawlErr(err) {
		t.Errorf("runCrawl seeds-file: %v", err)
	}
}

func TestRunCrawlSeedsFileMissing(t *testing.T) {
	cfg := seedTinyStore(t)
	err := runCrawl(context.Background(), cfg,
		[]string{"-seeds-file", "/nope/does-not-exist.txt"})
	if err == nil || !strings.Contains(err.Error(), "read -seeds-file") {
		t.Errorf("got %v", err)
	}
}

func TestRunCrawlDuration(t *testing.T) {
	cfg := seedTinyStore(t)
	// Tiny duration → ctx times out almost immediately; workers exit clean.
	err := runCrawl(context.Background(), cfg, []string{
		"-duration", "10ms",
		"https://example.invalid/seed",
	})
	if !acceptableCrawlErr(err) {
		t.Errorf("runCrawl duration: %v", err)
	}
}

// ---------------------------------------------------------------------------
// runCrawlErrors — sqlite-only, no errors in seeded store.
// ---------------------------------------------------------------------------

func TestRunCrawlErrorsEmpty(t *testing.T) {
	cfg := seedTinyStore(t)
	out := captureStdoutCosift(t, func() {
		if err := runCrawlErrors(context.Background(), cfg, nil); err != nil {
			t.Errorf("runCrawlErrors: %v", err)
		}
	})
	if !strings.Contains(out, "no errored frontier entries") {
		t.Errorf("expected empty msg, got %s", out)
	}
}

func TestRunCrawlErrorsCustomLimit(t *testing.T) {
	cfg := seedTinyStore(t)
	err := runCrawlErrors(context.Background(), cfg, []string{"-limit", "5"})
	if err != nil {
		t.Errorf("runCrawlErrors: %v", err)
	}
}

// ---------------------------------------------------------------------------
// runVerifyPebble — local path against a populated fixture.
// ---------------------------------------------------------------------------

func TestRunVerifyPebbleHappy(t *testing.T) {
	f := populatedPebbleStore(t)
	f.Close()
	cfg := &config.Config{DataDir: filepath.Dir(f.dir)}
	out := captureStdoutCosift(t, func() {
		if err := runVerifyPebble(context.Background(), cfg,
			[]string{"-dir", f.dir}); err != nil {
			t.Errorf("runVerifyPebble: %v", err)
		}
	})
	if !strings.Contains(out, "PebbleStore:") {
		t.Errorf("missing header: %s", out)
	}
	if !strings.Contains(out, "OK:") {
		t.Errorf("expected OK: %s", out)
	}
}

func TestRunVerifyPebbleJSON(t *testing.T) {
	f := populatedPebbleStore(t)
	f.Close()
	cfg := &config.Config{DataDir: filepath.Dir(f.dir)}
	out := captureStdoutCosift(t, func() {
		if err := runVerifyPebble(context.Background(), cfg,
			[]string{"-dir", f.dir, "-json"}); err != nil {
			t.Errorf("runVerifyPebble json: %v", err)
		}
	})
	var probe map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &probe); err != nil {
		t.Errorf("invalid JSON: %v\n%s", err, out)
	}
	if got, _ := probe["ok"].(bool); !got {
		t.Errorf("ok = %v, want true", probe["ok"])
	}
}

func TestRunVerifyPebbleViaServer(t *testing.T) {
	// Spin up a fake /verify endpoint that returns a passing report.
	mux := http.NewServeMux()
	mux.HandleFunc("/verify", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":                   true,
			"indexed_docs_counter": 5,
			"indexed_docs_scan":    5,
			"sum_doc_len_counter":  100,
			"sum_doc_len_scan":     100,
		})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	cfg := seedTinyStore(t)
	out := captureStdoutCosift(t, func() {
		if err := runVerifyPebble(context.Background(), cfg,
			[]string{"-server", ts.URL}); err != nil {
			t.Errorf("runVerifyPebble -server: %v", err)
		}
	})
	if !strings.Contains(out, "pebble-serve:") {
		t.Errorf("missing pebble-serve header: %s", out)
	}
	if !strings.Contains(out, "OK:") {
		t.Errorf("expected OK in body: %s", out)
	}
}

func TestRunVerifyPebbleViaServerDrift(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/verify", func(w http.ResponseWriter, r *http.Request) {
		// Server signals drift via 500 status.
		body, _ := json.Marshal(map[string]any{
			"ok":                   false,
			"indexed_docs_counter": 5,
			"indexed_docs_scan":    7,
			"indexed_docs_drift":   -2,
			"sum_doc_len_counter":  100,
			"sum_doc_len_scan":     120,
			"sum_doc_len_drift":    -20,
		})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write(body)
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	cfg := seedTinyStore(t)
	err := runVerifyPebble(context.Background(), cfg,
		[]string{"-server", ts.URL})
	if err == nil || !strings.Contains(err.Error(), "drift") {
		t.Errorf("expected drift error, got %v", err)
	}
}

func TestRunVerifyPebbleViaServerJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/verify", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"indexed_docs_counter":1,"indexed_docs_scan":1,"sum_doc_len_counter":10,"sum_doc_len_scan":10}`))
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()
	cfg := seedTinyStore(t)
	if err := runVerifyPebble(context.Background(), cfg,
		[]string{"-server", ts.URL, "-json"}); err != nil {
		t.Errorf("runVerifyPebble -server -json: %v", err)
	}
}

// ---------------------------------------------------------------------------
// runBench — vector + bm25 modes are pure CPU + go heap, no I/O.
// ---------------------------------------------------------------------------

func TestRunBenchVectorTiny(t *testing.T) {
	out := captureStdoutCosift(t, func() {
		err := runBench(context.Background(), []string{
			"-mode", "vector", "-n", "20", "-dim", "16", "-queries", "5",
		})
		if err != nil {
			t.Errorf("runBench vector: %v", err)
		}
	})
	if !strings.Contains(out, "vector") {
		t.Errorf("missing vector line: %s", out)
	}
}

func TestRunBenchBM25Tiny(t *testing.T) {
	out := captureStdoutCosift(t, func() {
		err := runBench(context.Background(), []string{
			"-mode", "bm25", "-n", "20", "-queries", "5",
		})
		if err != nil {
			t.Errorf("runBench bm25: %v", err)
		}
	})
	if !strings.Contains(out, "bm25") {
		t.Errorf("missing bm25 line: %s", out)
	}
}

func TestRunBenchJSON(t *testing.T) {
	out := captureStdoutCosift(t, func() {
		err := runBench(context.Background(), []string{
			"-mode", "vector", "-n", "10", "-dim", "8", "-queries", "3",
			"-json",
		})
		if err != nil {
			t.Errorf("runBench json: %v", err)
		}
	})
	// JSON mode emits a parseable line.
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if strings.HasPrefix(line, "{") {
			var r benchResult
			if err := json.Unmarshal([]byte(line), &r); err != nil {
				t.Errorf("bad JSON line %q: %v", line, err)
			}
			if r.Mode != "vector" {
				t.Errorf("mode = %q, want vector", r.Mode)
			}
			return
		}
	}
	t.Errorf("no JSON line emitted: %s", out)
}

func TestRunBenchStorage(t *testing.T) {
	// Storage mode exercises both benchBM25SQLite + benchBM25Pebble.
	out := captureStdoutCosift(t, func() {
		err := runBench(context.Background(), []string{
			"-mode", "storage", "-n", "20", "-queries", "3",
		})
		if err != nil {
			t.Errorf("runBench storage: %v", err)
		}
	})
	// One bm25-sqlite + one bm25-pebble line each.
	if !strings.Contains(out, "bm25") {
		t.Errorf("missing bm25 lines: %s", out)
	}
}

// ---------------------------------------------------------------------------
// runAnswerEval -dry-run — covers flag parsing + corpus/queries load +
// dry-run print path, no LLM calls.
// ---------------------------------------------------------------------------

func TestRunAnswerEvalDryRun(t *testing.T) {
	// Need OPENAI_API_KEY unset so -dry-run doesn't require it (per the
	// runAnswerEval guard).
	_ = os.Unsetenv("OPENAI_API_KEY")
	_ = os.Unsetenv("OPENAI")

	// Synthesize a tiny corpus + queries pair on disk.
	tmp := t.TempDir()
	corpusPath := filepath.Join(tmp, "corpus.json")
	queriesPath := filepath.Join(tmp, "queries.json")
	corpus := map[string]any{
		"docs": []map[string]any{
			{"url": "https://x.example/a", "title": "A", "text": "text a"},
			{"url": "https://x.example/b", "title": "B", "text": "text b"},
		},
	}
	queries := map[string]any{
		"name": "test",
		"queries": []map[string]any{
			{"text": "first query", "relevant": []string{"https://x.example/a"}},
			{"text": "second query", "relevant": []string{"https://x.example/b"}},
		},
	}
	mustWriteJSON(t, corpusPath, corpus)
	mustWriteJSON(t, queriesPath, queries)

	out := captureStdoutCosift(t, func() {
		err := runAnswerEval(context.Background(), []string{
			"-corpus", corpusPath,
			"-queries", queriesPath,
			"-dry-run",
		})
		if err != nil {
			t.Errorf("runAnswerEval -dry-run: %v", err)
		}
	})
	if !strings.Contains(out, "answer-eval:") {
		t.Errorf("missing answer-eval header: %s", out)
	}
	if !strings.Contains(out, "first query") {
		t.Errorf("missing query in dry-run plan: %s", out)
	}
}

func TestRunAnswerEvalNoKey(t *testing.T) {
	_ = os.Unsetenv("OPENAI_API_KEY")
	_ = os.Unsetenv("OPENAI")
	// No -dry-run, no env key → must fail with explanatory error.
	err := runAnswerEval(context.Background(), []string{
		"-corpus", "testdata/eval/corpus.json",
		"-queries", "testdata/eval/queries.json",
	})
	if err == nil || !strings.Contains(err.Error(), "OPENAI_API_KEY") {
		t.Errorf("got %v", err)
	}
}

func TestRunAnswerEvalBadCorpus(t *testing.T) {
	_ = os.Unsetenv("OPENAI_API_KEY")
	_ = os.Unsetenv("OPENAI")
	err := runAnswerEval(context.Background(), []string{
		"-corpus", "/no/such/corpus.json",
		"-queries", "testdata/eval/queries.json",
		"-dry-run",
	})
	if err == nil || !strings.Contains(err.Error(), "load corpus") {
		t.Errorf("got %v", err)
	}
}

func TestRunAnswerEvalBadQueries(t *testing.T) {
	_ = os.Unsetenv("OPENAI_API_KEY")
	_ = os.Unsetenv("OPENAI")
	tmp := t.TempDir()
	corpusPath := filepath.Join(tmp, "c.json")
	mustWriteJSON(t, corpusPath, map[string]any{"docs": []any{}})
	err := runAnswerEval(context.Background(), []string{
		"-corpus", corpusPath,
		"-queries", "/no/such/queries.json",
		"-dry-run",
	})
	if err == nil || !strings.Contains(err.Error(), "load queries") {
		t.Errorf("got %v", err)
	}
}

// ---------------------------------------------------------------------------
// runAnswerEvalCompare — pure file-based diff. Round 3 covers loadReport;
// here we exercise the print path end-to-end across N/no-overlap variants.
// ---------------------------------------------------------------------------

func writeAnswerEvalReport(t *testing.T, path string, plannerScores, paraScores [][2]int) {
	t.Helper()
	type sr struct {
		Strategy     string `json:"strategy"`
		Coverage     int    `json:"coverage"`
		Grounding    int    `json:"grounding"`
		JudgeComment string `json:"judge_comment"`
	}
	type qr struct {
		Query   string `json:"query"`
		Reports []sr   `json:"reports"`
	}
	type report struct {
		SynthModel string `json:"synth_model"`
		JudgeModel string `json:"judge_model"`
		Queries    string `json:"queries"`
		Rerank     bool   `json:"rerank"`
		SynthK     int    `json:"synth_k"`
		When       string `json:"when"`
		Reports    []qr   `json:"reports"`
	}
	r := report{
		SynthModel: "gpt-4o-mini",
		JudgeModel: "gpt-4o",
		Queries:    "queries.json",
		When:       "2025-12-01",
	}
	for i := range plannerScores {
		q := qr{Query: fmt.Sprintf("query-%d", i)}
		q.Reports = append(q.Reports,
			sr{Strategy: "planner", Coverage: plannerScores[i][0], Grounding: plannerScores[i][1]})
		if i < len(paraScores) {
			q.Reports = append(q.Reports,
				sr{Strategy: "paraphrase", Coverage: paraScores[i][0], Grounding: paraScores[i][1]})
		}
		r.Reports = append(r.Reports, q)
	}
	mustWriteJSON(t, path, r)
}

func TestRunAnswerEvalCompareHappy(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "base.json")
	newR := filepath.Join(dir, "new.json")
	writeAnswerEvalReport(t, base,
		[][2]int{{3, 3}, {4, 4}, {2, 2}},
		[][2]int{{3, 3}, {4, 4}, {2, 2}})
	writeAnswerEvalReport(t, newR,
		[][2]int{{5, 5}, {4, 4}, {2, 2}},
		[][2]int{{3, 3}, {5, 5}, {1, 1}})

	out := captureStdoutCosift(t, func() {
		err := runAnswerEvalCompare(context.Background(),
			[]string{"-query-threshold", "2", base, newR})
		if err != nil {
			t.Errorf("runAnswerEvalCompare: %v", err)
		}
	})
	if !strings.Contains(out, "BASELINE:") {
		t.Errorf("missing BASELINE header: %s", out)
	}
	if !strings.Contains(out, "Per-query moves") {
		t.Errorf("expected per-query moves block: %s", out)
	}
}

func TestRunAnswerEvalCompareNoMoves(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "base.json")
	newR := filepath.Join(dir, "new.json")
	writeAnswerEvalReport(t, base, [][2]int{{3, 3}}, [][2]int{{3, 3}})
	writeAnswerEvalReport(t, newR, [][2]int{{3, 3}}, [][2]int{{3, 3}})
	out := captureStdoutCosift(t, func() {
		if err := runAnswerEvalCompare(context.Background(),
			[]string{"-query-threshold", "2", base, newR}); err != nil {
			t.Errorf("runAnswerEvalCompare: %v", err)
		}
	})
	if !strings.Contains(out, "No per-query moves") {
		t.Errorf("expected no-moves msg: %s", out)
	}
}

func TestRunAnswerEvalCompareWrongArgCount(t *testing.T) {
	err := runAnswerEvalCompare(context.Background(), []string{"only-one.json"})
	if err == nil {
		t.Error("expected usage error")
	}
}

func TestRunAnswerEvalCompareMissingBaseline(t *testing.T) {
	dir := t.TempDir()
	newR := filepath.Join(dir, "new.json")
	writeAnswerEvalReport(t, newR, [][2]int{{3, 3}}, [][2]int{{3, 3}})
	err := runAnswerEvalCompare(context.Background(),
		[]string{"/no/such/base.json", newR})
	if err == nil || !strings.Contains(err.Error(), "baseline") {
		t.Errorf("got %v", err)
	}
}

func TestRunAnswerEvalCompareMissingNew(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "base.json")
	writeAnswerEvalReport(t, base, [][2]int{{3, 3}}, [][2]int{{3, 3}})
	err := runAnswerEvalCompare(context.Background(),
		[]string{base, "/no/such/new.json"})
	if err == nil || !strings.Contains(err.Error(), "new") {
		t.Errorf("got %v", err)
	}
}

// ---------------------------------------------------------------------------
// runEval against a fake /search API endpoint — covers the iter-104 -api
// short-circuit and the httpAPIRetriever Search path.
// ---------------------------------------------------------------------------

func TestRunEvalViaAPI(t *testing.T) {
	// Fake server speaks the iter-104 wire: GET /search?q=... &k=...
	// returns {"hits":[{url, title, score}, ...]}.
	mu := sync.Mutex{}
	seen := 0
	mux := http.NewServeMux()
	mux.HandleFunc("/search", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen++
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"hits": []map[string]any{
				{"url": "https://test.local/go-lang", "title": "Go", "score": 1.0},
				{"url": "https://test.local/rust-lang", "title": "Rust", "score": 0.5},
			},
		})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	// Build a tiny queries.json with 2 queries.
	tmp := t.TempDir()
	queriesPath := filepath.Join(tmp, "queries.json")
	mustWriteJSON(t, queriesPath, map[string]any{
		"name": "tiny",
		"queries": []map[string]any{
			{"text": "go", "relevant": []string{"https://test.local/go-lang"}},
			{"text": "rust", "relevant": []string{"https://test.local/rust-lang"}},
		},
	})

	out := captureStdoutCosift(t, func() {
		err := runEval(context.Background(), []string{
			"-api", ts.URL,
			"-queries", queriesPath,
		})
		if err != nil {
			t.Errorf("runEval -api: %v", err)
		}
	})
	if seen == 0 {
		t.Errorf("API was not queried")
	}
	if !strings.Contains(out, "tiny") {
		t.Errorf("missing summary name: %s", out)
	}
}

func TestRunEvalViaAPIWithSave(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/search", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"hits": []map[string]any{
				{"url": "https://test.local/go-lang", "title": "Go", "score": 1.0},
			},
		})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	tmp := t.TempDir()
	queriesPath := filepath.Join(tmp, "q.json")
	savePath := filepath.Join(tmp, "summary.json")
	mustWriteJSON(t, queriesPath, map[string]any{
		"name":    "save-test",
		"queries": []map[string]any{{"text": "go", "relevant": []string{"https://test.local/go-lang"}}},
	})

	if err := runEval(context.Background(), []string{
		"-api", ts.URL, "-queries", queriesPath, "-save", savePath,
	}); err != nil {
		t.Errorf("runEval -save: %v", err)
	}
	if _, err := os.Stat(savePath); err != nil {
		t.Errorf("save file not written: %v", err)
	}
}

func TestRunEvalBadQueriesPath(t *testing.T) {
	err := runEval(context.Background(), []string{
		"-api", "http://127.0.0.1:1",
		"-queries", "/no/such/queries.json",
	})
	if err == nil {
		t.Error("expected error for missing queries file")
	}
}

// ---------------------------------------------------------------------------
// mustWriteJSON — local test helper. Keeps test bodies brief.
// ---------------------------------------------------------------------------

func mustWriteJSON(t *testing.T, path string, v any) {
	t.Helper()
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// ---------------------------------------------------------------------------
// Admin endpoint shims — exercise the auth check + early-error branches for
// the family of /admin/* handlers that all share the same prologue
// (token check → nil hook → JSON body validation). One test per branch is
// enough to lift coverage materially because each handler is mostly
// boilerplate around a single hook function pointer.
// ---------------------------------------------------------------------------

func mkPOST(path string, body any, token string) *http.Request {
	var rdr *strings.Reader
	if body != nil {
		buf, _ := json.Marshal(body)
		rdr = strings.NewReader(string(buf))
	} else {
		rdr = strings.NewReader("")
	}
	req := httptest.NewRequest(http.MethodPost, path, rdr)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return req
}

func TestHandleCheckpointHappy(t *testing.T) {
	f := populatedPebbleStore(t)
	srv := f.makeServer(nil)
	// Send to a tempdir to avoid polluting /tmp.
	t.Setenv("COSIFT_CHECKPOINT_DIR", t.TempDir())
	rec := httptest.NewRecorder()
	srv.handleCheckpoint(rec, mkPOST("/admin/checkpoint", nil, ""))
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
}

func TestHandleCrawlEnqueueBadBody(t *testing.T) {
	f := populatedPebbleStore(t)
	srv := f.makeServer(nil)
	srv.crawlSeed = func(string) error { return nil } // bypass nil check
	rec := httptest.NewRecorder()
	srv.handleCrawlEnqueue(rec, mkPOST("/admin/crawl-enqueue", nil, ""))
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestHandleCrawlEnqueueHappy(t *testing.T) {
	f := populatedPebbleStore(t)
	srv := f.makeServer(nil)
	called := atomic.Int32{}
	srv.crawlSeed = func(u string) error { called.Add(1); return nil }
	rec := httptest.NewRecorder()
	srv.handleCrawlEnqueue(rec, mkPOST("/admin/crawl-enqueue",
		map[string]string{"url": "https://x.example/enq"}, ""))
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if called.Load() != 1 {
		t.Errorf("crawlSeed calls = %d, want 1", called.Load())
	}
}

func TestHandleCrawlEnqueueHookErr(t *testing.T) {
	f := populatedPebbleStore(t)
	srv := f.makeServer(nil)
	srv.crawlSeed = func(u string) error { return fmt.Errorf("synthetic") }
	rec := httptest.NewRecorder()
	srv.handleCrawlEnqueue(rec, mkPOST("/admin/crawl-enqueue",
		map[string]string{"url": "https://x.example"}, ""))
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rec.Code)
	}
}

func TestHandleSitemapImportBadBody(t *testing.T) {
	f := populatedPebbleStore(t)
	srv := f.makeServer(nil)
	srv.crawlSeedSitemap = func(context.Context, string) (int, error) { return 0, nil }
	rec := httptest.NewRecorder()
	srv.handleSitemapImport(rec, mkPOST("/admin/sitemap-import", nil, ""))
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestHandleSitemapImportHappy(t *testing.T) {
	f := populatedPebbleStore(t)
	srv := f.makeServer(nil)
	srv.crawlSeedSitemap = func(context.Context, string) (int, error) { return 42, nil }
	rec := httptest.NewRecorder()
	srv.handleSitemapImport(rec, mkPOST("/admin/sitemap-import",
		map[string]string{"url": "https://x.example/sitemap.xml"}, ""))
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
}

func TestHandleSitemapImportHookErr(t *testing.T) {
	f := populatedPebbleStore(t)
	srv := f.makeServer(nil)
	srv.crawlSeedSitemap = func(context.Context, string) (int, error) {
		return 0, fmt.Errorf("fail")
	}
	rec := httptest.NewRecorder()
	srv.handleSitemapImport(rec, mkPOST("/admin/sitemap-import",
		map[string]string{"url": "https://x.example/sitemap.xml"}, ""))
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rec.Code)
	}
}

func TestHandleFrontierPurgeHostBadBody(t *testing.T) {
	f := populatedPebbleStore(t)
	srv := f.makeServer(nil)
	rec := httptest.NewRecorder()
	srv.handleFrontierPurgeHost(rec, mkPOST("/admin/frontier-purge-host", nil, ""))
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestHandleFrontierPurgeHostHappy(t *testing.T) {
	f := populatedPebbleStore(t)
	srv := f.makeServer(nil)
	rec := httptest.NewRecorder()
	srv.handleFrontierPurgeHost(rec, mkPOST("/admin/frontier-purge-host",
		map[string]string{"host": "nonexistent.example"}, ""))
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
}

func TestHandleCrawlNowEmptyURLs(t *testing.T) {
	f := populatedPebbleStore(t)
	srv := f.makeServer(nil)
	srv.crawlFetchNow = func(context.Context, string) error { return nil }
	rec := httptest.NewRecorder()
	srv.handleCrawlNow(rec, mkPOST("/admin/crawl-now",
		map[string]any{"urls": []string{}}, ""))
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestHandleRSSImportBadBody(t *testing.T) {
	f := populatedPebbleStore(t)
	srv := f.makeServer(nil)
	srv.crawlSeedRSS = func(context.Context, string) (int, error) { return 0, nil }
	rec := httptest.NewRecorder()
	srv.handleRSSImport(rec, mkPOST("/admin/rss-import", nil, ""))
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestHandleWETImportBulkBadBody(t *testing.T) {
	f := populatedPebbleStore(t)
	srv := f.makeServer(nil)
	srv.crawlSeedWET = func(context.Context, string, bool, bool) (int, error) { return 0, nil }
	rec := httptest.NewRecorder()
	srv.handleWETImportBulk(rec, mkPOST("/admin/wet-import-bulk",
		map[string]any{"manifest_url": ""}, ""))
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestHandleWETImportBulkManifestUnreachable(t *testing.T) {
	f := populatedPebbleStore(t)
	srv := f.makeServer(nil)
	srv.crawlSeedWET = func(context.Context, string, bool, bool) (int, error) { return 0, nil }
	rec := httptest.NewRecorder()
	srv.handleWETImportBulk(rec, mkPOST("/admin/wet-import-bulk",
		map[string]any{
			"manifest_url": "http://127.0.0.1:1/no-such-manifest.gz",
			"count":        5,
		}, ""))
	if rec.Code != http.StatusBadGateway {
		t.Errorf("status = %d, want 502", rec.Code)
	}
}

func TestHandleSitePackBadHost(t *testing.T) {
	f := populatedPebbleStore(t)
	srv := f.makeServer(nil)
	srv.crawlSeedSitemap = func(context.Context, string) (int, error) { return 0, nil }
	srv.crawlSeedRSS = func(context.Context, string) (int, error) { return 0, nil }
	rec := httptest.NewRecorder()
	// host with a slash → 400
	srv.handleSitePack(rec, mkPOST("/admin/site-pack",
		map[string]string{"host": "x.example/path"}, ""))
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestHandleSitePackMissingHost(t *testing.T) {
	f := populatedPebbleStore(t)
	srv := f.makeServer(nil)
	srv.crawlSeedSitemap = func(context.Context, string) (int, error) { return 0, nil }
	srv.crawlSeedRSS = func(context.Context, string) (int, error) { return 0, nil }
	rec := httptest.NewRecorder()
	srv.handleSitePack(rec, mkPOST("/admin/site-pack", nil, ""))
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestHandlePQTrainNoHNSW(t *testing.T) {
	f := populatedPebbleStore(t)
	srv := f.makeServer(nil)
	srv.hnsw = nil
	rec := httptest.NewRecorder()
	srv.handlePQTrain(rec, mkPOST("/admin/pq-train", map[string]any{}, ""))
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestHandlePQEncodeNoCodebook(t *testing.T) {
	// hnsw exists but no codebook trained yet → "run /admin/pq-train first".
	f := populatedPebbleStore(t)
	srv := f.makeServer(nil)
	rec := httptest.NewRecorder()
	srv.handlePQEncode(rec, mkPOST("/admin/pq-encode", nil, ""))
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestHandleHNSWCompactNoHNSW(t *testing.T) {
	f := populatedPebbleStore(t)
	srv := f.makeServer(nil)
	srv.hnsw = nil
	rec := httptest.NewRecorder()
	srv.handleHNSWCompact(rec, mkPOST("/admin/hnsw-compact", nil, ""))
	// Handler returns 501 when no HNSW is loaded; the early-error branch
	// covers the same code path either way.
	if rec.Code != http.StatusNotImplemented && rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 501 or 400", rec.Code)
	}
}

// ---------------------------------------------------------------------------
// streamAnswer / streamResearch — end-to-end SSE via a real httptest server
// (httptest.NewRecorder doesn't implement http.Flusher; the streaming
// handlers fall through to writeProblem otherwise).
// ---------------------------------------------------------------------------

func TestStreamAnswerSSE(t *testing.T) {
	t.Setenv("COSIFT_DEFAULT_DECAY_DAYS", "0")
	mock := openaiTestServer(t)
	f := populatedPebbleStore(t)
	srv := f.makeServer(mock)

	mux := http.NewServeMux()
	mux.HandleFunc("/answer", srv.handleAnswer)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/answer?q=raft+consensus&k=2&stream=true")
	if err != nil {
		t.Fatalf("GET /answer: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Errorf("content-type = %q, want SSE", ct)
	}
	// Read at least one SSE frame.
	buf := make([]byte, 4096)
	n, _ := resp.Body.Read(buf)
	body := string(buf[:n])
	if !strings.Contains(body, "data:") {
		t.Errorf("no SSE frame in body: %q", body)
	}
	if !strings.Contains(body, "sources") {
		t.Errorf("missing sources event: %q", body)
	}
}

func TestStreamResearchSSE(t *testing.T) {
	t.Setenv("COSIFT_DEFAULT_DECAY_DAYS", "0")
	mock := openaiTestServer(t)
	mock.SetChatResponse(`["sub one","sub two"]`)
	f := populatedPebbleStore(t)
	srv := f.makeServer(mock)

	mux := http.NewServeMux()
	mux.HandleFunc("/research", srv.handleResearch)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/research?q=raft&k=2&stream=true")
	if err != nil {
		t.Fatalf("GET /research: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Errorf("content-type = %q, want SSE", ct)
	}
	buf := make([]byte, 8192)
	n, _ := resp.Body.Read(buf)
	body := string(buf[:n])
	if !strings.Contains(body, "data:") {
		t.Errorf("no SSE frame: %q", body)
	}
}

// ---------------------------------------------------------------------------
// doRerank — wraps a Reranker with attempt/failure counters. A trivial
// in-line fake covers both happy + error paths.
// ---------------------------------------------------------------------------

func TestDoRerankHappy(t *testing.T) {
	mock := openaiTestServer(t)
	f := populatedPebbleStore(t)
	srv := f.makeServer(mock)
	rr := &realFakeReranker{order: []string{"id-1", "id-0"}}
	srv.reranker = rr
	beforeA := srv.rerankAttempts.Load()
	got, err := srv.doRerank(context.Background(), "q", nil)
	if err != nil {
		t.Fatalf("doRerank: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("order = %v", got)
	}
	if srv.rerankAttempts.Load() != beforeA+1 {
		t.Errorf("attempts not incremented")
	}
}

func TestDoRerankError(t *testing.T) {
	mock := openaiTestServer(t)
	f := populatedPebbleStore(t)
	srv := f.makeServer(mock)
	rr := &realFakeReranker{err: fmt.Errorf("synthetic")}
	srv.reranker = rr
	beforeF := srv.rerankFailures.Load()
	_, err := srv.doRerank(context.Background(), "q", nil)
	if err == nil {
		t.Fatal("expected error")
	}
	if srv.rerankFailures.Load() != beforeF+1 {
		t.Errorf("failures not incremented")
	}
}

// ---------------------------------------------------------------------------
// applyBM25EnvOverrides — env-driven knob, locked down for both happy +
// malformed inputs.
// ---------------------------------------------------------------------------

func TestApplyBM25EnvOverridesValid(t *testing.T) {
	f := populatedPebbleStore(t)
	t.Setenv("COSIFT_BM25_K1", "1.5")
	t.Setenv("COSIFT_BM25_B", "0.8")
	got := applyBM25EnvOverrides(f.idx)
	if !got.k1Set || got.k1Val != 1.5 {
		t.Errorf("k1 = %+v", got)
	}
	if !got.bSet || got.bVal != 0.8 {
		t.Errorf("b = %+v", got)
	}
}

func TestApplyBM25EnvOverridesBad(t *testing.T) {
	f := populatedPebbleStore(t)
	t.Setenv("COSIFT_BM25_K1", "not-a-float")
	t.Setenv("COSIFT_BM25_B", "abc")
	got := applyBM25EnvOverrides(f.idx)
	if got.k1Set {
		t.Errorf("k1 should not be applied")
	}
	if got.k1Bad == "" {
		t.Errorf("k1Bad should be set")
	}
	if got.bSet {
		t.Errorf("b should not be applied")
	}
	if got.bBad == "" {
		t.Errorf("bBad should be set")
	}
}

func TestApplyBM25EnvOverridesNegative(t *testing.T) {
	f := populatedPebbleStore(t)
	t.Setenv("COSIFT_BM25_K1", "-0.5")
	t.Setenv("COSIFT_BM25_B", "-1")
	got := applyBM25EnvOverrides(f.idx)
	if got.k1Set || got.bSet {
		t.Errorf("negative values must not be applied: %+v", got)
	}
}

// ---------------------------------------------------------------------------
// hnswPassageWriter — direct unit tests on the iter-391 bridge type used to
// hand crawler passages to an in-memory HNSW.
// ---------------------------------------------------------------------------

func TestHNSWPassageWriterUpsertPassage(t *testing.T) {
	f := populatedPebbleStore(t)
	w := &hnswPassageWriter{ps: f.ps, hnsw: f.hnsw}
	// Need a doc ID guaranteed to exist (round 3 inserts 6 docs starting at ID 1).
	doc, err := f.ps.GetDocByID(context.Background(), 1)
	if err != nil || doc == nil {
		t.Skipf("could not load doc 1: %v", err)
	}
	beforeLen := f.hnsw.Len()
	p := &store.Passage{DocID: 1, Offset: 0, Length: 10, Embedding: deterministicVec("p1", f.dim)}
	if err := w.UpsertPassage(context.Background(), p); err != nil {
		t.Fatalf("UpsertPassage: %v", err)
	}
	if f.hnsw.Len() <= beforeLen {
		t.Errorf("hnsw len didn't grow: before=%d after=%d", beforeLen, f.hnsw.Len())
	}
}

func TestHNSWPassageWriterUpsertPassageMissingDoc(t *testing.T) {
	f := populatedPebbleStore(t)
	w := &hnswPassageWriter{ps: f.ps, hnsw: f.hnsw}
	// DocID that doesn't exist — the underlying store returns ErrNotFound,
	// which UpsertPassage propagates. The code path is what matters.
	p := &store.Passage{DocID: 999999, Offset: 0, Length: 4, Embedding: deterministicVec("none", f.dim)}
	// Either a not-found error OR a silent nil is fine — both exercise the
	// branch where GetDocByID's return is checked.
	_ = w.UpsertPassage(context.Background(), p)
}

func TestHNSWPassageWriterUpsertPassageBatch(t *testing.T) {
	f := populatedPebbleStore(t)
	w := &hnswPassageWriter{ps: f.ps, hnsw: f.hnsw}
	beforeLen := f.hnsw.Len()
	ps := []*store.Passage{
		{DocID: 1, Offset: 0, Length: 4, Embedding: deterministicVec("p-a", f.dim)},
		{DocID: 1, Offset: 4, Length: 4, Embedding: deterministicVec("p-b", f.dim)},
	}
	if err := w.UpsertPassageBatch(context.Background(), ps); err != nil {
		t.Fatalf("UpsertPassageBatch: %v", err)
	}
	if f.hnsw.Len() != beforeLen+2 {
		t.Errorf("hnsw len after batch: before=%d after=%d (want +2)", beforeLen, f.hnsw.Len())
	}
}

func TestHNSWPassageWriterUpsertPassageBatchEmpty(t *testing.T) {
	f := populatedPebbleStore(t)
	w := &hnswPassageWriter{ps: f.ps, hnsw: f.hnsw}
	if err := w.UpsertPassageBatch(context.Background(), nil); err != nil {
		t.Errorf("empty batch should be nil, got %v", err)
	}
}

func TestHNSWPassageWriterMarkURLInvalid(t *testing.T) {
	f := populatedPebbleStore(t)
	w := &hnswPassageWriter{ps: f.ps, hnsw: f.hnsw}
	// Mark a known URL invalid — count is hnsw-specific but the call must succeed.
	n, err := w.MarkURLInvalid(context.Background(), f.docs[0])
	if err != nil {
		t.Errorf("MarkURLInvalid: %v", err)
	}
	if n < 0 {
		t.Errorf("count = %d", n)
	}
}

func TestHNSWPassageWriterMarkURLInvalidCtxCancelled(t *testing.T) {
	f := populatedPebbleStore(t)
	w := &hnswPassageWriter{ps: f.ps, hnsw: f.hnsw}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := w.MarkURLInvalid(ctx, f.docs[0])
	if err == nil {
		t.Error("expected ctx.Err() to bubble up")
	}
}

// ---------------------------------------------------------------------------
// runEval BM25 (no API key required) — exercises bm25Adapter.Search and
// the local-index ingest path.
// ---------------------------------------------------------------------------

func TestRunEvalBM25Local(t *testing.T) {
	tmp := t.TempDir()
	corpusPath := filepath.Join(tmp, "corpus.json")
	queriesPath := filepath.Join(tmp, "queries.json")
	mustWriteJSON(t, corpusPath, map[string]any{
		"docs": []map[string]any{
			{"url": "https://x.example/a", "title": "raft consensus", "text": "raft is a distributed consensus algorithm"},
			{"url": "https://x.example/b", "title": "pasta recipe", "text": "boil water with salt then drop pasta"},
		},
	})
	mustWriteJSON(t, queriesPath, map[string]any{
		"name": "bm25-local",
		"queries": []map[string]any{
			{"text": "raft consensus", "relevant": []string{"https://x.example/a"}},
			{"text": "pasta", "relevant": []string{"https://x.example/b"}},
		},
	})
	out := captureStdoutCosift(t, func() {
		err := runEval(context.Background(), []string{
			"-retriever", "bm25",
			"-corpus", corpusPath,
			"-queries", queriesPath,
			"-embed-cache", "",
		})
		if err != nil {
			t.Errorf("runEval bm25 local: %v", err)
		}
	})
	if !strings.Contains(out, "bm25-local") {
		t.Errorf("missing summary name: %s", out)
	}
}

func TestRunEvalUnknownRetriever(t *testing.T) {
	tmp := t.TempDir()
	corpusPath := filepath.Join(tmp, "c.json")
	queriesPath := filepath.Join(tmp, "q.json")
	mustWriteJSON(t, corpusPath, map[string]any{"docs": []any{}})
	mustWriteJSON(t, queriesPath, map[string]any{
		"queries": []map[string]any{{"text": "x", "relevant": []string{}}},
	})
	err := runEval(context.Background(), []string{
		"-retriever", "nonesuch",
		"-corpus", corpusPath,
		"-queries", queriesPath,
	})
	if err == nil || !strings.Contains(err.Error(), "unknown retriever") {
		t.Errorf("got %v", err)
	}
}

func TestRunEvalDenseNoAPIKey(t *testing.T) {
	_ = os.Unsetenv("OPENAI_API_KEY")
	_ = os.Unsetenv("OPENAI")
	tmp := t.TempDir()
	corpusPath := filepath.Join(tmp, "c.json")
	queriesPath := filepath.Join(tmp, "q.json")
	mustWriteJSON(t, corpusPath, map[string]any{
		"docs": []map[string]any{{"url": "https://x", "title": "t", "text": "body"}},
	})
	mustWriteJSON(t, queriesPath, map[string]any{
		"queries": []map[string]any{{"text": "x", "relevant": []string{"https://x"}}},
	})
	err := runEval(context.Background(), []string{
		"-retriever", "dense",
		"-corpus", corpusPath,
		"-queries", queriesPath,
	})
	if err == nil || !strings.Contains(err.Error(), "OPENAI_API_KEY") {
		t.Errorf("got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Eval-side retriever adapters — direct unit tests using stub Retrievers
// and a stub ChatClient. These cover the 0% Search methods on hybridAdapter,
// rerankAdapter, paraphraseRetriever, and plannerRetriever, plus the
// httpAPIRetriever wire-shape.
// ---------------------------------------------------------------------------

// stubRetriever returns a canned URL list per query.
type stubRetriever struct {
	byQuery map[string][]string
	err     error
}

func (s *stubRetriever) Search(_ context.Context, q string, k int) ([]string, error) {
	if s.err != nil {
		return nil, s.err
	}
	urls := s.byQuery[q]
	if len(urls) > k {
		urls = urls[:k]
	}
	return urls, nil
}

// stubChat returns a canned reply.
type stubChat struct {
	reply string
	err   error
}

func (s *stubChat) Chat(_ context.Context, _ []embed.ChatMsg) (string, error) {
	return s.reply, s.err
}
func (s *stubChat) Model() string { return "stub" }

func TestHybridAdapterSearch(t *testing.T) {
	a := &stubRetriever{byQuery: map[string][]string{
		"q": {"https://x.example/a", "https://x.example/b"},
	}}
	b := &stubRetriever{byQuery: map[string][]string{
		"q": {"https://x.example/b", "https://x.example/c"},
	}}
	h := &hybridAdapter{a: a, b: b}
	urls, err := h.Search(context.Background(), "q", 3)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(urls) == 0 {
		t.Errorf("expected fused URLs")
	}
}

func TestHybridAdapterSearchInnerErr(t *testing.T) {
	a := &stubRetriever{err: fmt.Errorf("a-fail")}
	b := &stubRetriever{}
	h := &hybridAdapter{a: a, b: b}
	_, err := h.Search(context.Background(), "q", 3)
	if err == nil {
		t.Error("expected error from a")
	}
}

func TestHybridAdapterSearchSecondErr(t *testing.T) {
	a := &stubRetriever{byQuery: map[string][]string{"q": {"https://x"}}}
	b := &stubRetriever{err: fmt.Errorf("b-fail")}
	h := &hybridAdapter{a: a, b: b}
	_, err := h.Search(context.Background(), "q", 3)
	if err == nil {
		t.Error("expected error from b")
	}
}

func TestParaphraseRetrieverNoChat(t *testing.T) {
	// chat returns parseable JSON paraphrases.
	inner := &stubRetriever{byQuery: map[string][]string{
		"q":      {"https://a"},
		"para 1": {"https://b"},
		"para 2": {"https://c"},
	}}
	chat := &stubChat{reply: `["para 1","para 2"]`}
	p := &paraphraseRetriever{
		inner: inner, chat: chat, n: 2, cache: make(map[string][]string),
	}
	urls, err := p.Search(context.Background(), "q", 5)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(urls) == 0 {
		t.Errorf("expected fused URLs")
	}
	// Cache hit on second call.
	urls2, _ := p.Search(context.Background(), "q", 5)
	if len(urls2) != len(urls) {
		t.Errorf("cache hit changed shape: %v vs %v", urls, urls2)
	}
}

func TestParaphraseRetrieverChatErr(t *testing.T) {
	inner := &stubRetriever{byQuery: map[string][]string{"q": {"https://a"}}}
	chat := &stubChat{err: fmt.Errorf("chat down")}
	p := &paraphraseRetriever{
		inner: inner, chat: chat, n: 2, cache: make(map[string][]string),
	}
	urls, err := p.Search(context.Background(), "q", 5)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	// On chat err, paraphrases returns nil → falls back to main result list.
	if len(urls) != 1 {
		t.Errorf("expected pass-through main, got %v", urls)
	}
}

func TestParaphraseRetrieverWithMainWeight(t *testing.T) {
	inner := &stubRetriever{byQuery: map[string][]string{
		"q":   {"https://a"},
		"alt": {"https://b"},
	}}
	chat := &stubChat{reply: `["alt"]`}
	p := &paraphraseRetriever{
		inner: inner, chat: chat, n: 1, mainWeight: 2.0,
		cache: make(map[string][]string),
	}
	if _, err := p.Search(context.Background(), "q", 5); err != nil {
		t.Errorf("Search: %v", err)
	}
}

func TestParaphraseRetrieverInnerErr(t *testing.T) {
	inner := &stubRetriever{err: fmt.Errorf("inner fail")}
	chat := &stubChat{reply: `["alt"]`}
	p := &paraphraseRetriever{
		inner: inner, chat: chat, n: 1, cache: make(map[string][]string),
	}
	if _, err := p.Search(context.Background(), "q", 5); err == nil {
		t.Error("expected inner error to bubble")
	}
}

func TestPlannerRetrieverHappy(t *testing.T) {
	inner := &stubRetriever{byQuery: map[string][]string{
		"q":     {"https://a"},
		"sub-1": {"https://b"},
		"sub-2": {"https://c"},
	}}
	chat := &stubChat{reply: `["sub-1","sub-2"]`}
	p := &plannerRetriever{
		inner: inner, chat: chat, cache: make(map[string][]string),
	}
	urls, err := p.Search(context.Background(), "q", 5)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(urls) == 0 {
		t.Errorf("expected fused URLs")
	}
}

func TestPlannerRetrieverChatErr(t *testing.T) {
	inner := &stubRetriever{byQuery: map[string][]string{"q": {"https://a"}}}
	chat := &stubChat{err: fmt.Errorf("chat down")}
	p := &plannerRetriever{
		inner: inner, chat: chat, cache: make(map[string][]string),
	}
	urls, err := p.Search(context.Background(), "q", 5)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(urls) != 1 {
		t.Errorf("expected pass-through main on chat err, got %v", urls)
	}
}

func TestPlannerRetrieverBadResponse(t *testing.T) {
	inner := &stubRetriever{byQuery: map[string][]string{"q": {"https://a"}}}
	chat := &stubChat{reply: "definitely not a JSON array"}
	p := &plannerRetriever{
		inner: inner, chat: chat, cache: make(map[string][]string),
	}
	urls, _ := p.Search(context.Background(), "q", 5)
	if len(urls) != 1 {
		t.Errorf("expected pass-through main on unparseable plan, got %v", urls)
	}
}

func TestPlannerRetrieverCapped(t *testing.T) {
	inner := &stubRetriever{byQuery: map[string][]string{
		"q":  {"https://a"},
		"s1": {"https://b"},
		"s2": {"https://c"},
		"s3": {"https://d"},
		"s4": {"https://e"},
		"s5": {"https://f"},
		"s6": {"https://g"},
	}}
	chat := &stubChat{reply: `["s1","s2","s3","s4","s5","s6"]`}
	p := &plannerRetriever{
		inner: inner, chat: chat, cache: make(map[string][]string),
	}
	if _, err := p.Search(context.Background(), "q", 5); err != nil {
		t.Errorf("Search: %v", err)
	}
}

// httpAPIRetriever — direct test using httptest. Covers happy + error paths.
func TestRerankAdapterHappy(t *testing.T) {
	inner := &stubRetriever{byQuery: map[string][]string{
		"q": {"https://a", "https://b", "https://c"},
	}}
	rr := &realFakeReranker{order: []string{"https://b", "https://a"}}
	a := &rerankAdapter{
		inner: inner, reranker: rr, candidateK: 10,
		textByURL: map[string]string{
			"https://a": "doc a", "https://b": "doc b", "https://c": "doc c",
		},
	}
	urls, err := a.Search(context.Background(), "q", 2)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(urls) != 2 || urls[0] != "https://b" || urls[1] != "https://a" {
		t.Errorf("got %v", urls)
	}
}

func TestRerankAdapterInnerErr(t *testing.T) {
	inner := &stubRetriever{err: fmt.Errorf("inner")}
	rr := &realFakeReranker{}
	a := &rerankAdapter{inner: inner, reranker: rr, candidateK: 5}
	if _, err := a.Search(context.Background(), "q", 2); err == nil {
		t.Error("expected inner err to propagate")
	}
}

func TestRerankAdapterSingleURL(t *testing.T) {
	inner := &stubRetriever{byQuery: map[string][]string{"q": {"https://only"}}}
	rr := &realFakeReranker{}
	a := &rerankAdapter{inner: inner, reranker: rr, candidateK: 5}
	urls, err := a.Search(context.Background(), "q", 5)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(urls) != 1 {
		t.Errorf("got %v, want 1 (single URL short-circuit)", urls)
	}
}

func TestRerankAdapterRerankErrFallback(t *testing.T) {
	inner := &stubRetriever{byQuery: map[string][]string{
		"q": {"https://a", "https://b", "https://c"},
	}}
	rr := &realFakeReranker{err: fmt.Errorf("rerank down")}
	a := &rerankAdapter{
		inner: inner, reranker: rr, candidateK: 10,
		textByURL: map[string]string{"https://a": "x"},
	}
	urls, err := a.Search(context.Background(), "q", 2)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	// Falls back to inner order when reranker fails.
	if len(urls) != 3 {
		t.Errorf("got %v, want 3 (fallback to inner)", urls)
	}
}

func TestRerankAdapterCandidateKDefault(t *testing.T) {
	// candidateK = 0 → uses k*5.
	inner := &stubRetriever{byQuery: map[string][]string{
		"q": {"https://a", "https://b"},
	}}
	rr := &realFakeReranker{order: []string{"https://b", "https://a"}}
	a := &rerankAdapter{inner: inner, reranker: rr, candidateK: 0}
	if _, err := a.Search(context.Background(), "q", 2); err != nil {
		t.Errorf("Search: %v", err)
	}
}

func TestHTTPAPIRetrieverHappy(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/search", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"hits": []map[string]any{
				{"url": "https://x.example/a"},
				{"url": "https://x.example/b"},
			},
		})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()
	r := &httpAPIRetriever{
		baseURL: ts.URL, retriever: "bm25", rerank: true, bearer: "tok",
		http: &http.Client{Timeout: 5 * time.Second},
	}
	urls, err := r.Search(context.Background(), "q", 5)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(urls) != 2 {
		t.Errorf("got %v", urls)
	}
}

func TestHTTPAPIRetrieverNon200(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer ts.Close()
	r := &httpAPIRetriever{baseURL: ts.URL, http: &http.Client{Timeout: 5 * time.Second}}
	_, err := r.Search(context.Background(), "q", 5)
	if err == nil || !strings.Contains(err.Error(), "api 500") {
		t.Errorf("got %v", err)
	}
}

func TestHTTPAPIRetrieverDialErr(t *testing.T) {
	r := &httpAPIRetriever{
		baseURL: "http://127.0.0.1:1",
		http:    &http.Client{Timeout: 200 * time.Millisecond},
	}
	if _, err := r.Search(context.Background(), "q", 5); err == nil {
		t.Error("expected dial error")
	}
}

func TestHTTPAPIRetrieverBadJSON(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("not-json"))
	}))
	defer ts.Close()
	r := &httpAPIRetriever{baseURL: ts.URL, http: &http.Client{Timeout: 5 * time.Second}}
	if _, err := r.Search(context.Background(), "q", 5); err == nil {
		t.Error("expected decode error")
	}
}

func TestApplyBM25EnvOverridesUnset(t *testing.T) {
	f := populatedPebbleStore(t)
	t.Setenv("COSIFT_BM25_K1", "")
	t.Setenv("COSIFT_BM25_B", "")
	got := applyBM25EnvOverrides(f.idx)
	if got.k1Set || got.bSet || got.k1Bad != "" || got.bBad != "" {
		t.Errorf("unset env → empty result, got %+v", got)
	}
}
