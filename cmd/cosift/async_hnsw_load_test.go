package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/index"
	"github.com/pilot-protocol/cosift/internal/store"
)

// LoadHNSWProgress must reconstruct the full graph and fire the progress
// callback with (loaded, total) settling at the node count.
func TestLoadHNSWProgressReports(t *testing.T) {
	f := populatedPebbleStore(t)
	if err := f.hnsw.Persist(context.Background(), f.ps); err != nil {
		t.Fatalf("persist: %v", err)
	}
	want := uint64(f.hnsw.Len())

	var calls int
	var lastLoaded, lastTotal uint64
	g, ok, err := index.LoadHNSWProgress(context.Background(), f.ps, func(loaded, total uint64) {
		calls++
		lastLoaded, lastTotal = loaded, total
	})
	if err != nil || !ok {
		t.Fatalf("LoadHNSWProgress: ok=%v err=%v", ok, err)
	}
	if uint64(g.Len()) != want {
		t.Fatalf("loaded %d nodes, want %d", g.Len(), want)
	}
	if calls == 0 {
		t.Fatalf("progress callback never fired")
	}
	if lastLoaded != want || lastTotal != want {
		t.Fatalf("final progress = %d/%d, want %d/%d", lastLoaded, lastTotal, want, want)
	}
}

// A corrupt persisted node blob is skipped (left as a zombie) without failing
// the whole load.
func TestLoadHNSWProgressSkipsCorruptNode(t *testing.T) {
	f := populatedPebbleStore(t)
	if err := f.hnsw.Persist(context.Background(), f.ps); err != nil {
		t.Fatalf("persist: %v", err)
	}
	// Overwrite one node's blob with garbage too short to decode.
	if err := f.ps.PutVectorNode(context.Background(), 1, []byte{0x00, 0x01, 0x02}); err != nil {
		t.Fatalf("corrupt node: %v", err)
	}
	g, ok, err := index.LoadHNSW(context.Background(), f.ps)
	if err != nil || !ok {
		t.Fatalf("load with one corrupt node: ok=%v err=%v", ok, err)
	}
	if g.Len() != f.hnsw.Len() {
		t.Fatalf("node count changed: got %d, want %d", g.Len(), f.hnsw.Len())
	}
}

// A cancelled context aborts the load without publishing a partial graph.
func TestLoadHNSWProgressAborts(t *testing.T) {
	f := populatedPebbleStore(t)
	if err := f.hnsw.Persist(context.Background(), f.ps); err != nil {
		t.Fatalf("persist: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	g, ok, err := index.LoadHNSWProgress(ctx, f.ps, nil)
	if err == nil || ok || g != nil {
		t.Fatalf("cancelled load: want (nil,false,err), got (%v,%v,%v)", g, ok, err)
	}
}

// loadHNSWInto publishes the graph atomically and reports ready at 100%.
func TestLoadHNSWIntoPublishesReady(t *testing.T) {
	f := populatedPebbleStore(t)
	if err := f.hnsw.Persist(context.Background(), f.ps); err != nil {
		t.Fatalf("persist: %v", err)
	}
	want := f.hnsw.Len()

	srv := &pebbleHTTP{}
	if srv.hnsw() != nil {
		t.Fatalf("graph should be nil before load")
	}
	if snap := srv.hnswLoadSnapshot(); snap["state"] != "none" {
		t.Fatalf("pre-load state = %v, want none", snap["state"])
	}

	srv.loadHNSWInto(context.Background(), f.ps, f.dim, want)

	g := srv.hnsw()
	if g == nil {
		t.Fatalf("graph nil after load")
	}
	if g.Len() != want {
		t.Fatalf("published graph has %d nodes, want %d", g.Len(), want)
	}
	snap := srv.hnswLoadSnapshot()
	if snap["state"] != "ready" {
		t.Fatalf("post-load state = %v, want ready", snap["state"])
	}
	if pct, _ := snap["pct"].(float64); pct != 100 {
		t.Fatalf("post-load pct = %v, want 100", snap["pct"])
	}
}

// /healthz answers 200 with no graph loaded — the property that lets the
// listener bind before the background HNSW load finishes.
func TestHealthzIndependentOfHNSWLoad(t *testing.T) {
	f := populatedPebbleStore(t)
	if err := f.hnsw.Persist(context.Background(), f.ps); err != nil {
		t.Fatalf("persist: %v", err)
	}
	srv := &pebbleHTTP{store: f.ps, hasVectors: true, vectorDim: f.dim, vectorNodes: f.hnsw.Len()}

	rec := httptest.NewRecorder()
	srv.handleHealthz(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/healthz before load = %d, want 200", rec.Code)
	}
	if srv.hnsw() != nil {
		t.Fatalf("graph should still be nil before load")
	}

	srv.loadHNSWInto(context.Background(), f.ps, f.dim, f.hnsw.Len())
	if srv.hnsw() == nil {
		t.Fatalf("dense unavailable after load completed")
	}
}

// The efSearch override and PQ-disabled branches apply cleanly on load.
func TestLoadHNSWIntoEfSearchPQDisabled(t *testing.T) {
	f := populatedPebbleStore(t)
	if err := f.hnsw.Persist(context.Background(), f.ps); err != nil {
		t.Fatalf("persist: %v", err)
	}
	t.Setenv("COSIFT_HNSW_EF_SEARCH", "200")
	t.Setenv("COSIFT_DISABLE_PQ", "true")

	srv := &pebbleHTTP{}
	srv.loadHNSWInto(context.Background(), f.ps, f.dim, f.hnsw.Len())
	if srv.hnsw() == nil || srv.hnswLoadState.Load() != 2 {
		t.Fatalf("expected ready graph, got state=%d", srv.hnswLoadState.Load())
	}
}

// A store with no persisted graph resolves to the error state without publishing.
func TestLoadHNSWIntoNoGraph(t *testing.T) {
	f := populatedPebbleStore(t) // deliberately NOT persisted → no HNSW meta on disk
	srv := &pebbleHTTP{}
	srv.loadHNSWInto(context.Background(), f.ps, f.dim, 0)
	if srv.hnsw() != nil {
		t.Fatalf("no graph should be published")
	}
	if srv.hnswLoadState.Load() != 3 {
		t.Fatalf("state = %d, want 3 (error)", srv.hnswLoadState.Load())
	}
}

// A cancelled context aborts loadHNSWInto without publishing a graph.
func TestLoadHNSWIntoAborts(t *testing.T) {
	f := populatedPebbleStore(t)
	if err := f.hnsw.Persist(context.Background(), f.ps); err != nil {
		t.Fatalf("persist: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	srv := &pebbleHTTP{}
	srv.loadHNSWInto(ctx, f.ps, f.dim, f.hnsw.Len())
	if srv.hnsw() != nil {
		t.Fatalf("aborted load must not publish a graph")
	}
}

// While loading, the snapshot reports percent and a rate-based ETA.
func TestHNSWLoadSnapshotLoading(t *testing.T) {
	srv := &pebbleHTTP{}
	srv.hnswLoadState.Store(1)
	srv.hnswLoadStart.Store(time.Now().Add(-10 * time.Second).UnixNano())
	srv.hnswTotal.Store(1000)
	srv.hnswLoaded.Store(250)

	snap := srv.hnswLoadSnapshot()
	if snap["state"] != "loading" {
		t.Fatalf("state = %v, want loading", snap["state"])
	}
	if pct, _ := snap["pct"].(float64); pct != 25 {
		t.Fatalf("pct = %v, want 25", snap["pct"])
	}
	if _, ok := snap["eta_s"].(float64); !ok {
		t.Fatalf("expected an eta_s while loading")
	}
}

// End-to-end: with COSIFT_LOAD_HNSW=true the listener binds while the graph
// loads in the background, and /stats reports the load settling to ready.
func TestPebbleServeAsyncHNSWWarmup(t *testing.T) {
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
	g := index.NewHNSW(8)
	for _, d := range []struct{ url, title, text string }{
		{"https://x/a", "Alpha", "alpha document about consensus"},
		{"https://x/b", "Beta", "beta document about replication"},
		{"https://x/c", "Gamma", "gamma document about routing"},
	} {
		id, err := ps.UpsertDocument(ctx, &store.Document{URL: d.url, Title: d.title, Text: d.text, FetchedAt: time.Now()})
		if err != nil {
			t.Fatalf("upsert: %v", err)
		}
		if err := idx.IndexDocument(ctx, id, d.title, d.text); err != nil {
			t.Fatalf("index: %v", err)
		}
		g.Add(d.url, d.title, deterministicVec(d.title, 8))
	}
	if err := g.Persist(ctx, ps); err != nil {
		t.Fatalf("persist hnsw: %v", err)
	}
	ps.Close()

	t.Setenv("COSIFT_LOAD_HNSW", "true")
	// A mock embedder + an (empty) seeds file exercises the in-serve crawler
	// wiring, which starts only after the async load publishes the graph.
	mock := openaiTestServer(t)
	seedsFile := filepath.Join(t.TempDir(), "seeds.txt")
	if err := os.WriteFile(seedsFile, []byte("# no seeds\n"), 0o644); err != nil {
		t.Fatalf("write seeds: %v", err)
	}
	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	serveCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		cfg := &config.Config{
			Server:     config.Server{Addr: addr},
			Embeddings: config.Embeddings{Model: "test-embed", URL: mock.URL() + "/v1", Dim: 8},
		}
		done <- runPebbleServe(serveCtx, cfg, []string{
			"-dir", dir, "-addr", addr,
			"-crawl-seeds-file", seedsFile, "-crawl-checkpoint", "200ms",
		})
	}()

	if !waitForPort(addr, 5*time.Second) {
		t.Fatalf("server didn't bind within 5s (async load must not block bind)")
	}
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Logf("shutdown took >3s")
		}
	}()

	base := "http://" + addr
	if resp := mustGet(t, base+"/healthz"); resp["status"] != "ok" {
		t.Errorf("healthz: %v", resp)
	}

	// The 3-node graph loads near-instantly; poll /stats until it's ready.
	deadline := time.Now().Add(5 * time.Second)
	ready := false
	for time.Now().Before(deadline) {
		st := mustGet(t, base+"/stats")
		if hl, ok := st["hnsw_load"].(map[string]any); ok && hl["state"] == "ready" {
			ready = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !ready {
		t.Fatalf("hnsw_load never reached ready state")
	}
	if st := mustGet(t, base+"/stats"); st["hnsw_loaded"] != true {
		t.Errorf("hnsw_loaded = %v, want true", st["hnsw_loaded"])
	}
	// Let the in-serve crawler start and its checkpoint goroutine log the
	// baseline so shutdown (deferred cancel) exercises the final HNSW persist.
	time.Sleep(400 * time.Millisecond)
}
