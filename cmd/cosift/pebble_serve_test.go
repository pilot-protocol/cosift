package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/index"
	"github.com/pilot-protocol/cosift/internal/store"
)

// TestPebbleServeEndToEnd — populate a Pebble store, launch the
// pebble-serve handler in-process against a free port, hit /healthz +
// /stats + /search + /contents, assert the responses are coherent.
// The /search assertion is the load-bearing one: it proves
// the Pebble backend serves real BM25 results through an HTTP layer.
func TestPebbleServeEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}
	// time-decay default-on changes retriever labels for these
	// assertions ("bm25" → "bm25+decay:180d"). Test contract is about
	// retriever-selection logic, not decay; disable decay for this fixture.
	t.Setenv("COSIFT_DEFAULT_DECAY_DAYS", "0")

	dir := filepath.Join(t.TempDir(), "pebble")
	ps, err := store.OpenPebble(dir)
	if err != nil {
		t.Fatalf("OpenPebble: %v", err)
	}
	idx := index.NewPebbleBM25(ps)
	ctx := context.Background()

	corpus := []struct{ url, title, text string }{
		{"https://x/raft", "Raft consensus", "Raft is a distributed consensus algorithm."},
		{"https://x/paxos", "Paxos algorithm", "Paxos is the classical distributed consensus algorithm."},
		{"https://x/cooking", "Cooking pasta", "Boil water, salt, drop pasta, drain when al dente."},
	}
	for _, d := range corpus {
		id, err := ps.UpsertDocument(ctx, &store.Document{
			URL: d.url, Title: d.title, Text: d.text, FetchedAt: time.Now(),
		})
		if err != nil {
			t.Fatalf("upsert: %v", err)
		}
		if err := idx.IndexDocument(ctx, id, d.title, d.text); err != nil {
			t.Fatalf("index: %v", err)
		}
	}
	ps.Close()

	// Launch pebble-serve on a free port in a goroutine.
	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	serveCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		cfg := &config.Config{
			Server: config.Server{Addr: addr},
		}
		done <- runPebbleServe(serveCtx, cfg, []string{"-dir", dir, "-addr", addr})
	}()

	if !waitForPort(addr, 4*time.Second) {
		t.Fatalf("server didn't come up on %s within 4s", addr)
	}
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Logf("server shutdown took >3s")
		}
	}()

	base := "http://" + addr

	// /healthz
	if resp := mustGet(t, base+"/healthz"); resp["status"] != "ok" {
		t.Errorf("healthz: %v", resp)
	}

	// /stats
	stats := mustGet(t, base+"/stats")
	if int(stats["documents"].(float64)) != len(corpus) {
		t.Errorf("stats documents: want %d, got %v", len(corpus), stats["documents"])
	}
	if stats["backend"] != "pebble" {
		t.Errorf("stats backend: want pebble, got %v", stats["backend"])
	}
	// assert's BM25 config fields land in the response.
	// Catches regressions in /stats payload assembly + the K1/B getters.
	if _, ok := stats["bm25_k1"].(float64); !ok {
		t.Errorf("stats: missing bm25_k1, got %v", stats["bm25_k1"])
	}
	if _, ok := stats["bm25_b"].(float64); !ok {
		t.Errorf("stats: missing bm25_b, got %v", stats["bm25_b"])
	}
	// retrievers list lets clients introspect capability without
	// probing each ?retriever= value. No HNSW graph in the fixture, so it
	// should be exactly bm25 + bm25-mlt.
	rArr, ok := stats["retrievers"].([]any)
	if !ok || len(rArr) < 2 {
		t.Errorf("stats: missing retrievers array, got %v", stats["retrievers"])
	} else {
		gotR := map[string]bool{}
		for _, v := range rArr {
			gotR[v.(string)] = true
		}
		if !gotR["bm25"] || !gotR["bm25-mlt"] {
			t.Errorf("stats retrievers: want bm25 + bm25-mlt, got %v", rArr)
		}
		if gotR["dense"] || gotR["hybrid"] {
			t.Errorf("stats retrievers: dense/hybrid leaked into list without a loaded HNSW graph: %v", rArr)
		}
	}

	// /search — raft query
	got := mustGet(t, base+"/search?q=raft+consensus&k=3")
	hits, ok := got["hits"].([]any)
	if !ok || len(hits) == 0 {
		t.Fatalf("search /search?q=raft: no hits in %+v", got)
	}
	topURL := hits[0].(map[string]any)["url"].(string)
	if topURL != "https://x/raft" {
		t.Errorf("top hit for raft query: want https://x/raft, got %s", topURL)
	}
	// total_candidates reports the BM25 pool size before
	// filter. Must be > 0 when hits > 0 (the candidate pool fed the hits).
	if tc, _ := got["total_candidates"].(float64); tc <= 0 {
		t.Errorf("/search: total_candidates should be > 0, got %v", got["total_candidates"])
	}
	// retriever label identifies the pipeline.
	// Bare BM25 should be exactly "bm25"; rerank/expand decorate this.
	if rt, _ := got["retriever"].(string); rt != "bm25" {
		t.Errorf("/search retriever: want 'bm25', got %q", rt)
	}

	// /contents
	contents := mustGet(t, base+"/contents?url="+url.QueryEscape("https://x/raft"))
	if contents["title"] != "Raft consensus" {
		t.Errorf("contents title: %v", contents["title"])
	}
	if !strings.Contains(contents["text"].(string), "Raft") {
		t.Errorf("contents text: %v", contents["text"])
	}

	// /find_similar — text mode. Exercises the text-mode branch
	// the compile bug lived in. Catches regression class:
	// out-of-scope variables inside the topN==0 / empty-result short-circuit.
	fs := mustGet(t, base+"/find_similar?text="+url.QueryEscape("distributed consensus replicated log"))
	fsHits, ok := fs["hits"].([]any)
	if !ok {
		t.Fatalf("find_similar text-mode: want hits array, got %T (%v)", fs["hits"], fs)
	}
	if len(fsHits) == 0 {
		t.Errorf("find_similar text-mode: expected at least one hit on consensus terms, got 0")
	}

	// /find_similar — URL mode. The source URL must NOT appear in the
	// result set. Catches the source-URL-exclusion check
	// (`if h.URL == src.URL { continue }`).
	fu := mustGet(t, base+"/find_similar?url="+url.QueryEscape("https://x/raft")+"&k=5")
	fuHits, _ := fu["hits"].([]any)
	for _, h := range fuHits {
		hm, _ := h.(map[string]any)
		if u, _ := hm["url"].(string); u == "https://x/raft" {
			t.Errorf("find_similar url-mode: source URL leaked into results: %v", fuHits)
			break
		}
	}

	// /find_similar?retriever=hybrid without a loaded HNSW graph
	// must silently fall through to BM25-MLT and emit a warning. Parallel
	// to the /search?retriever=dense fallback contract — locks in
	// the Iter behavior so a future refactor can't regress it
	// to a 5xx or a dropped warning.
	hr := mustGet(t, base+"/find_similar?url="+url.QueryEscape("https://x/raft")+"&retriever=hybrid&k=5")
	if rt, _ := hr["retriever"].(string); rt != "bm25-mlt" {
		t.Errorf("/find_similar?retriever=hybrid without graph: want fallback 'bm25-mlt', got %q", rt)
	}
	if hits, _ := hr["hits"].([]any); len(hits) == 0 {
		t.Errorf("/find_similar?retriever=hybrid without graph: BM25-MLT fallback should still return hits, got 0")
	}
	hrwarn, _ := hr["warnings"].([]any)
	if len(hrwarn) == 0 {
		t.Errorf("/find_similar?retriever=hybrid without graph: want a warning, got none")
	} else {
		first, _ := hrwarn[0].(string)
		if !strings.Contains(first, "retriever=hybrid") {
			t.Errorf("/find_similar?retriever=hybrid: warning didn't mention the value: %s", first)
		}
		// warning text says "BM25-MLT" on /find_similar (the
		// actual fallback retriever), not the generic "BM25" used for
		// /search/answer/research. Catches accidental regression of the
		// per-endpoint suffix.
		if !strings.Contains(first, "BM25-MLT") {
			t.Errorf("/find_similar warning should mention BM25-MLT fallback (matches retriever label), got: %s", first)
		}
	}

	// /find_similar?url=X&mmr=0.5 without HNSW: must warn that
	// the graph is missing (no diversification possible). Must NOT warn
	// about embedder — URL-mode MMR uses the source's graph vector.
	mfu := mustGet(t, base+"/find_similar?url="+url.QueryEscape("https://x/raft")+"&mmr=0.5")
	mfwarn, _ := mfu["warnings"].([]any)
	sawGraphMissing := false
	for _, w := range mfwarn {
		ws, _ := w.(string)
		if strings.Contains(ws, "mmr requires HNSW") {
			sawGraphMissing = true
		}
		if strings.Contains(ws, "mmr requires an embedder") {
			t.Errorf("/find_similar?url=X&mmr=0.5: URL-mode carve-out failed — got misleading embedder warning: %s", ws)
		}
	}
	if !sawGraphMissing {
		t.Errorf("/find_similar?url=X&mmr=0.5 without graph: want HNSW-missing warning, got %v", mfwarn)
	}

	// /find_similar?retriever=dense&url=X without graph: also falls through.
	// specifically: the URL-mode carve-out suppresses the
	// 'no embedder' warning, so the only warning we should see is the
	// graph-missing one — NOT a misleading 'no embedder' duplicate.
	dr := mustGet(t, base+"/find_similar?url="+url.QueryEscape("https://x/raft")+"&retriever=dense")
	if rt, _ := dr["retriever"].(string); rt != "bm25-mlt" {
		t.Errorf("/find_similar?retriever=dense without graph: want fallback 'bm25-mlt', got %q", rt)
	}
	drwarn, _ := dr["warnings"].([]any)
	for _, w := range drwarn {
		ws, _ := w.(string)
		if strings.Contains(ws, "no embedder configured") {
			t.Errorf("/find_similar?retriever=dense&url=X: URL-mode warning carve-out failed; got misleading embedder warning: %s", ws)
		}
	}

	// /verify — counter drift check. 503 on drift; we expect OK.
	vresp, err := http.Get(base + "/verify")
	if err != nil {
		t.Fatalf("/verify: %v", err)
	}
	vresp.Body.Close()
	if vresp.StatusCode != 200 {
		t.Errorf("/verify: want 200, got %d", vresp.StatusCode)
	}

	// POST /search — JSON body equivalent of GET ?q=&k=. The
	// re-encode-to-URL.Values handoff makes this a thin pass-through; this
	// assertion guards against a regression that breaks the wrapper.
	postBody := strings.NewReader(`{"q":"raft consensus","k":3}`)
	pres, err := http.Post(base+"/search", "application/json", postBody)
	if err != nil {
		t.Fatalf("POST /search: %v", err)
	}
	pbody, _ := io.ReadAll(pres.Body)
	pres.Body.Close()
	if pres.StatusCode != 200 {
		t.Fatalf("POST /search: HTTP %d: %s", pres.StatusCode, pbody)
	}
	var pj map[string]any
	if err := json.Unmarshal(pbody, &pj); err != nil {
		t.Fatalf("POST /search decode: %v", err)
	}
	if hits, _ := pj["hits"].([]any); len(hits) == 0 {
		t.Errorf("POST /search: expected hits, got %s", pbody)
	}

	// /metrics — Prometheus exposition format. Just confirm
	// 200 + Content-Type + at least one expected metric name.
	mresp, err := http.Get(base + "/metrics")
	if err != nil {
		t.Fatalf("/metrics: %v", err)
	}
	mbody, _ := io.ReadAll(mresp.Body)
	mresp.Body.Close()
	if mresp.StatusCode != 200 {
		t.Errorf("/metrics: want 200, got %d", mresp.StatusCode)
	}
	if !strings.Contains(string(mbody), "cosift_indexed_docs") {
		t.Errorf("/metrics: missing cosift_indexed_docs in body: %s", mbody)
	}

	// /search?expand=true without a chat client should:
	//   - normalize the expand field to "hyde"
	//   - emit a warning about no chat client configured
	// Since no chat is configured in this test scaffold, effective_query
	// stays empty (omitted) and the warning fires.
	eresp := mustGet(t, base+"/search?q=raft&expand=true")
	if exp, _ := eresp["expand"].(string); exp != "hyde" {
		t.Errorf("/search?expand=true: want expand normalized to 'hyde', got %q", exp)
	}
	ewarn, _ := eresp["warnings"].([]any)
	if len(ewarn) == 0 {
		t.Errorf("/search?expand=true without chat: want a warning, got none")
	}

	// /search?retriever=dense without a loaded HNSW graph should:
	//   - silently fall through to BM25 (retriever stays "bm25", hits still
	//     come back) — made this transparent via the retrieve helper
	//   - emit a warning so operators don't think dense fired
	// The test fixture never sets COSIFT_LOAD_HNSW=true and has no embedder,
	// so this exercises the silent-fallback path the label preserves.
	rresp := mustGet(t, base+"/search?q=raft&retriever=dense")
	if rt, _ := rresp["retriever"].(string); rt != "bm25" {
		t.Errorf("/search?retriever=dense without graph: want fallback to 'bm25', got %q", rt)
	}
	if rhits, _ := rresp["hits"].([]any); len(rhits) == 0 {
		t.Errorf("/search?retriever=dense without graph: BM25 fallback should still return hits, got 0")
	}
	rwarn, _ := rresp["warnings"].([]any)
	if len(rwarn) == 0 {
		t.Errorf("/search?retriever=dense without graph: want a warning, got none")
	} else {
		first, _ := rwarn[0].(string)
		if !strings.Contains(first, "retriever=dense") {
			t.Errorf("/search?retriever=dense: warning didn't mention the value: %s", first)
		}
	}

	// /search?mmr=0.5 without HNSW must silently fall through
	// (no MMR diversification) and emit a warning. Search results still
	// return — MMR is a re-ranker, never a gate.
	mres := mustGet(t, base+"/search?q=raft&mmr=0.5")
	if mhits, _ := mres["hits"].([]any); len(mhits) == 0 {
		t.Errorf("/search?mmr=0.5 without HNSW: should still return hits, got 0")
	}
	if rt, _ := mres["retriever"].(string); strings.Contains(rt, "mmr:") {
		t.Errorf("/search?mmr=0.5 without HNSW: retriever label should NOT contain mmr suffix, got %q", rt)
	}
	mwarn, _ := mres["warnings"].([]any)
	sawMMRWarn := false
	for _, w := range mwarn {
		if s, _ := w.(string); strings.Contains(s, "mmr requires HNSW") {
			sawMMRWarn = true
			break
		}
	}
	if !sawMMRWarn {
		t.Errorf("/search?mmr=0.5 without HNSW: want warning mentioning HNSW, got %v", mwarn)
	}

	// /search?mmr=nope: unparseable values warn instead of silently
	// being ignored. Mirrors the Iter sort=/k= warning pattern.
	mbres := mustGet(t, base+"/search?q=raft&mmr=nope")
	sawBadMMR := false
	for _, w := range mbres["warnings"].([]any) {
		if s, _ := w.(string); strings.Contains(s, "mmr=nope") {
			sawBadMMR = true
			break
		}
	}
	if !sawBadMMR {
		t.Errorf("/search?mmr=nope: want warning mentioning the bad value, got %v", mbres["warnings"])
	}

	// /search with a malformed sort value must surface a warning
	// in the response. Covers the Iter warnings machinery.
	wresp := mustGet(t, base+"/search?q=raft&sort=newest")
	warnings, ok := wresp["warnings"].([]any)
	if !ok || len(warnings) == 0 {
		t.Errorf("/search?sort=newest: expected warnings array, got %v", wresp["warnings"])
	} else {
		first, _ := warnings[0].(string)
		if !strings.Contains(first, "sort=newest") {
			t.Errorf("/search?sort=newest: warning didn't mention the value: %s", first)
		}
	}

	// /search?include_text=true must inline doc.Text on each hit.
	// made the flag work uniformly across endpoints; this guards
	// /search specifically since it has the most params in flight.
	itresp := mustGet(t, base+"/search?q=raft&include_text=true&k=3")
	ithits, _ := itresp["hits"].([]any)
	if len(ithits) == 0 {
		t.Errorf("/search?include_text=true: no hits, can't verify text inlining")
	} else {
		first, _ := ithits[0].(map[string]any)
		if txt, _ := first["text"].(string); txt == "" {
			t.Errorf("/search?include_text=true: top hit missing text field: %+v", first)
		}
	}

	// /search with include_domains. The dot-boundary matcher
	// excludes hosts outside the allowlist. The corpus has 'x' as the only
	// host, so include_domains=x should keep everything and =other.tld
	// should drop everything.
	dresp := mustGet(t, base+"/search?q=consensus&k=5&include_domains=other.tld")
	dhits, _ := dresp["hits"].([]any)
	if len(dhits) != 0 {
		t.Errorf("/search?include_domains=other.tld: want 0 hits, got %d", len(dhits))
	}
	dresp2 := mustGet(t, base+"/search?q=consensus&k=5&include_domains=x")
	dhits2, _ := dresp2["hits"].([]any)
	if len(dhits2) == 0 {
		t.Errorf("/search?include_domains=x: want hits from corpus, got 0")
	}

	// POST /contents batch. Up to 100 URLs; each result has
	// found+title+text or found:false+error.
	batchBody := strings.NewReader(`{"urls":["https://x/raft","https://x/missing"]}`)
	cres, err := http.Post(base+"/contents", "application/json", batchBody)
	if err != nil {
		t.Fatalf("POST /contents: %v", err)
	}
	cbody, _ := io.ReadAll(cres.Body)
	cres.Body.Close()
	if cres.StatusCode != 200 {
		t.Fatalf("POST /contents: HTTP %d: %s", cres.StatusCode, cbody)
	}
	var cj map[string]any
	if err := json.Unmarshal(cbody, &cj); err != nil {
		t.Fatalf("POST /contents decode: %v", err)
	}
	results, _ := cj["results"].([]any)
	if len(results) != 2 {
		t.Fatalf("POST /contents: want 2 results, got %d: %s", len(results), cbody)
	}
	r0, _ := results[0].(map[string]any)
	r1, _ := results[1].(map[string]any)
	if found0, _ := r0["found"].(bool); !found0 {
		t.Errorf("POST /contents: first url should be found, got %+v", r0)
	}
	if found1, _ := r1["found"].(bool); found1 {
		t.Errorf("POST /contents: missing URL should report found:false, got %+v", r1)
	}
}

// mustGet GETs the URL and JSON-decodes the response. Fails the test on
// any HTTP or decode error.
func mustGet(t *testing.T, urlStr string) map[string]any {
	t.Helper()
	resp, err := http.Get(urlStr)
	if err != nil {
		t.Fatalf("GET %s: %v", urlStr, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s: %v", urlStr, err)
	}
	if resp.StatusCode >= 400 {
		t.Fatalf("GET %s: HTTP %d: %s", urlStr, resp.StatusCode, body)
	}
	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("decode %s: %v (body=%s)", urlStr, err, body)
	}
	return out
}

func waitForPort(addr string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + addr + "/healthz")
		if err == nil {
			resp.Body.Close()
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}
