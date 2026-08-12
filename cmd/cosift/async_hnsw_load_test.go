package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pilot-protocol/cosift/internal/index"
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
