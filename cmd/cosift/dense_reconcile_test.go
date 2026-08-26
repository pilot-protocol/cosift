package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// divergeFixture soft-deletes the docs behind the given URLs, leaving their
// HNSW nodes live — the store/graph divergence purge-domain creates.
func divergeFixture(t *testing.T, f *populatedFixture, urls []string) {
	t.Helper()
	ctx := context.Background()
	ids := map[string]int64{}
	if err := f.ps.IterDocMeta(ctx, func(docID int64, url, _ string) error {
		ids[url] = docID
		return nil
	}); err != nil {
		t.Fatalf("IterDocMeta: %v", err)
	}
	for _, u := range urls {
		id, ok := ids[u]
		if !ok {
			t.Fatalf("no docID for %s", u)
		}
		if ok, err := f.ps.SoftDeleteDocument(ctx, id, u); err != nil || !ok {
			t.Fatalf("soft delete %s: ok=%v err=%v", u, ok, err)
		}
	}
}

// denseCandidateOrder returns the raw dense candidate URLs via the
// resolution-bypass path (enrich=false&decay=0).
func denseCandidateOrder(t *testing.T, srv *pebbleHTTP, k int) []string {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet,
		"/search?q=consensus&retriever=dense&k=6&enrich=false&rerank=false&decay=0", nil)
	rec := httptest.NewRecorder()
	srv.handleSearch(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("probe status = %d, body=%s", rec.Code, rec.Body.String())
	}
	var resp searchResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("probe decode: %v", err)
	}
	urls := make([]string, 0, len(resp.Hits))
	for _, h := range resp.Hits {
		urls = append(urls, h.URL)
	}
	if len(urls) < k {
		t.Fatalf("probe returned %d candidates, need >= %d", len(urls), k)
	}
	return urls
}

func doSearch(t *testing.T, srv *pebbleHTTP, query string) searchResponse {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, query, nil)
	rec := httptest.NewRecorder()
	srv.handleSearch(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	var resp searchResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return resp
}

// Dense /search under divergence: drops are counted, surfaced in the
// response, and the over-fetch slack keeps len(hits) == k. Runs the real
// enrichment path (no COSIFT_DEFAULT_DECAY_DAYS=0).
func TestSearchDenseDivergenceDropsAndSlack(t *testing.T) {
	mock := openaiTestServer(t)
	f := populatedPebbleStore(t)
	srv := f.makeServer(mock)

	order := denseCandidateOrder(t, srv, 4)
	deleted := order[:2]
	divergeFixture(t, f, deleted)

	resp := doSearch(t, srv, "/search?q=consensus&retriever=dense&k=4&rerank=false")
	if len(resp.Hits) != 4 {
		t.Fatalf("slack should refill to k=4, got %d hits", len(resp.Hits))
	}
	for _, h := range resp.Hits {
		for _, d := range deleted {
			if h.URL == d {
				t.Fatalf("deleted URL %s surfaced in dense hits", d)
			}
		}
	}
	if resp.DenseDrops != 2 {
		t.Fatalf("dense_drops = %d, want 2", resp.DenseDrops)
	}
	if got := srv.denseResolutionDrops.Load(); got != 2 {
		t.Fatalf("denseResolutionDrops = %d, want 2", got)
	}
}

// Hybrid under divergence keeps the "hybrid >= bm25-only" hit-count
// invariant that was false in prod.
func TestSearchHybridDivergenceInvariant(t *testing.T) {
	mock := openaiTestServer(t)
	f := populatedPebbleStore(t)
	srv := f.makeServer(mock)

	order := denseCandidateOrder(t, srv, 4)
	deleted := order[:2]
	divergeFixture(t, f, deleted)

	hybrid := doSearch(t, srv, "/search?q=consensus&retriever=hybrid&k=4&rerank=false")
	bm25 := doSearch(t, srv, "/search?q=consensus&retriever=bm25&k=4&rerank=false")
	if len(hybrid.Hits) < len(bm25.Hits) {
		t.Fatalf("hybrid returned %d hits < bm25's %d", len(hybrid.Hits), len(bm25.Hits))
	}
	if len(hybrid.Hits) != 4 {
		t.Fatalf("hybrid should refill to k=4, got %d", len(hybrid.Hits))
	}
	for _, h := range hybrid.Hits {
		for _, d := range deleted {
			if h.URL == d {
				t.Fatalf("deleted URL %s surfaced in hybrid hits", d)
			}
		}
	}
}

// The enrich=false&decay=0 bypass returns orphans as phantom hits and does
// NOT count them as drops — pins the probe path's current contract.
func TestSearchDenseBypassPhantomHits(t *testing.T) {
	mock := openaiTestServer(t)
	f := populatedPebbleStore(t)
	srv := f.makeServer(mock)

	order := denseCandidateOrder(t, srv, 2)
	deleted := order[:1]
	divergeFixture(t, f, deleted)

	resp := doSearch(t, srv,
		"/search?q=consensus&retriever=dense&k=6&enrich=false&rerank=false&decay=0")
	found := false
	for _, h := range resp.Hits {
		if h.URL == deleted[0] {
			found = true
		}
	}
	if !found {
		t.Fatalf("bypass path should return the orphan as a phantom hit")
	}
	if resp.DenseDrops != 0 || srv.denseResolutionDrops.Load() != 0 {
		t.Fatalf("bypass path must not count drops (got resp=%d counter=%d)",
			resp.DenseDrops, srv.denseResolutionDrops.Load())
	}
}

// /find_similar (URL mode, no embedder needed) counts divergence drops and
// excludes deleted URLs.
func TestFindSimilarDivergenceDrops(t *testing.T) {
	t.Setenv("COSIFT_DEFAULT_DECAY_DAYS", "0")
	f := populatedPebbleStore(t)
	srv := f.makeServer(nil)

	deleted := []string{f.docs[1]}
	divergeFixture(t, f, deleted)

	req := httptest.NewRequest(http.MethodGet,
		"/find_similar?url=https%3A%2F%2Fx.example%2Fraft&retriever=dense&k=5", nil)
	rec := httptest.NewRecorder()
	srv.handleFindSimilar(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	for _, d := range deleted {
		if strings.Contains(body, d) {
			t.Fatalf("deleted URL %s surfaced in find_similar", d)
		}
	}
	if srv.denseResolutionDrops.Load() == 0 {
		t.Fatalf("find_similar drop not counted")
	}
}

// Full load path: persisted graph + soft-deleted docs → loadHNSWInto
// reconciles before publishing; kill switch skips it.
func TestLoadHNSWIntoReconcilesOrphans(t *testing.T) {
	f := populatedPebbleStore(t)
	if err := f.hnsw.Persist(context.Background(), f.ps); err != nil {
		t.Fatalf("persist: %v", err)
	}
	deleted := []string{f.docs[0], f.docs[3]}
	divergeFixture(t, f, deleted)

	srv := &pebbleHTTP{}
	srv.loadHNSWInto(context.Background(), f.ps, f.dim, f.hnsw.Len())
	g := srv.hnsw()
	if g == nil {
		t.Fatalf("graph nil after load")
	}
	if got := srv.reconciledOrphans.Load(); got != int64(len(deleted)) {
		t.Fatalf("reconciled_orphans = %d, want %d", got, len(deleted))
	}
	snap := srv.hnswLoadSnapshot()
	if snap["reconciled_orphans"] != int64(len(deleted)) {
		t.Fatalf("snapshot reconciled_orphans = %v", snap["reconciled_orphans"])
	}
	for _, d := range deleted {
		if v, ok := g.LookupVectorByURL(d); ok && len(v) > 0 {
			t.Fatalf("orphan %s still live after reconciling load", d)
		}
	}
	if v, ok := g.LookupVectorByURL(f.docs[1]); !ok || len(v) == 0 {
		t.Fatalf("live doc lost its vec in reconcile")
	}

	t.Setenv("COSIFT_RECONCILE_ON_LOAD", "false")
	srv2 := &pebbleHTTP{}
	srv2.loadHNSWInto(context.Background(), f.ps, f.dim, f.hnsw.Len())
	if srv2.reconciledOrphans.Load() != 0 {
		t.Fatalf("kill switch ignored")
	}
	if v, ok := srv2.hnsw().LookupVectorByURL(deleted[0]); !ok || len(v) == 0 {
		t.Fatalf("kill switch should leave orphans live")
	}
}

// Compact hardening: persist survives a canceled request context, and
// force_persist=1 retries the persist when removed == 0.
func TestHNSWCompactPersistHardening(t *testing.T) {
	f := populatedPebbleStore(t)
	srv := f.makeServer(nil)

	compact := func(q string, ctx context.Context) map[string]any {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, "/admin/hnsw-compact"+q, nil)
		if ctx != nil {
			req = req.WithContext(ctx)
		}
		rec := httptest.NewRecorder()
		srv.handleHNSWCompact(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("compact %q status = %d, body=%s", q, rec.Code, rec.Body.String())
		}
		var resp map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode: %v", err)
		}
		return resp
	}

	f.hnsw.MarkURLPassagesInvalid(f.docs[5])
	cctx, cancel := context.WithCancel(context.Background())
	cancel()
	resp := compact("", cctx)
	if resp["persisted"] != true {
		t.Fatalf("persist did not survive canceled request ctx: %v", resp)
	}
	if resp["removed"].(float64) != 1 {
		t.Fatalf("removed = %v, want 1", resp["removed"])
	}

	resp = compact("", nil)
	if resp["removed"].(float64) != 0 || resp["persisted"] != false {
		t.Fatalf("no-op compact should skip persist: %v", resp)
	}

	resp = compact("?force_persist=1", nil)
	if resp["persisted"] != true || resp["forced"] != true {
		t.Fatalf("force_persist should re-persist: %v", resp)
	}

	f.hnsw.MarkURLPassagesInvalid(f.docs[4])
	resp = compact("?skip_persist=1", nil)
	if resp["removed"].(float64) != 1 || resp["persisted"] != false {
		t.Fatalf("skip_persist changed behavior: %v", resp)
	}
}
