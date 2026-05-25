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

	"github.com/calinteodor/cosift/internal/config"
	"github.com/calinteodor/cosift/internal/index"
	"github.com/calinteodor/cosift/internal/store"
)

// TestPebbleServeEndToEnd — populate a Pebble store, launch the
// pebble-serve handler in-process against a free port, hit /healthz +
// /stats + /search + /contents, assert the responses are coherent.
// Iter 205. The /search assertion is the load-bearing one: it proves
// the Pebble backend serves real BM25 results through an HTTP layer.
func TestPebbleServeEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}

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
	// Iter 346: assert iter-280's BM25 config fields land in the response.
	// Catches regressions in /stats payload assembly + the iter-279 K1/B getters.
	if _, ok := stats["bm25_k1"].(float64); !ok {
		t.Errorf("stats: missing bm25_k1, got %v", stats["bm25_k1"])
	}
	if _, ok := stats["bm25_b"].(float64); !ok {
		t.Errorf("stats: missing bm25_b, got %v", stats["bm25_b"])
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

	// /contents
	contents := mustGet(t, base+"/contents?url="+url.QueryEscape("https://x/raft"))
	if contents["title"] != "Raft consensus" {
		t.Errorf("contents title: %v", contents["title"])
	}
	if !strings.Contains(contents["text"].(string), "Raft") {
		t.Errorf("contents text: %v", contents["text"])
	}

	// /find_similar — text mode (iter 298). Exercises the text-mode branch
	// the iter-322 compile bug lived in. Catches regression class:
	// out-of-scope variables inside the topN==0 / empty-result short-circuit.
	fs := mustGet(t, base+"/find_similar?text="+url.QueryEscape("distributed consensus replicated log"))
	fsHits, ok := fs["hits"].([]any)
	if !ok {
		t.Fatalf("find_similar text-mode: want hits array, got %T (%v)", fs["hits"], fs)
	}
	if len(fsHits) == 0 {
		t.Errorf("find_similar text-mode: expected at least one hit on consensus terms, got 0")
	}

	// /verify — counter drift check (iter 230). 503 on drift; we expect OK.
	vresp, err := http.Get(base + "/verify")
	if err != nil {
		t.Fatalf("/verify: %v", err)
	}
	vresp.Body.Close()
	if vresp.StatusCode != 200 {
		t.Errorf("/verify: want 200, got %d", vresp.StatusCode)
	}

	// POST /search (iter 277) — JSON body equivalent of GET ?q=&k=. The
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

	// /metrics (iter 231) — Prometheus exposition format. Just confirm
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

	// /search with a malformed sort value (iter 310) must surface a warning
	// in the response. Covers the iter-292/309/310/311/313 warnings machinery.
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

	// /search?include_text=true (iter 237) must inline doc.Text on each hit.
	// Iter 247 made the flag work uniformly across endpoints; this guards
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

	// /search with include_domains (iter 232). The dot-boundary matcher
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

	// POST /contents batch (iter 254/255). Up to 100 URLs; each result has
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
