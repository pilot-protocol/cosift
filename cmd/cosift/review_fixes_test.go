package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/index"
	"github.com/pilot-protocol/cosift/internal/store"
)

// A stale slot that dwarfs the active one is a graph whose meta was
// overwritten (older binary on an HSW2 store); load must keep it.
func TestLoadKeepsStaleSlotLargerThanActive(t *testing.T) {
	f := populatedPebbleStore(t)
	ctx := context.Background()
	big := f.hnsw
	for i := 0; i < 400; i++ {
		u := "https://x.example/big/" + string(rune('a'+i%26)) + string(rune('a'+i/26))
		big.Add(u, "big", deterministicVec(u, f.dim))
	}
	if err := big.Persist(ctx, f.ps); err != nil {
		t.Fatal(err)
	}
	if err := big.PersistSwap(ctx, f.ps, nil); err != nil {
		t.Fatal(err)
	}
	if err := f.ps.ClearVectorSlot(ctx, store.VectorSlotA); err != nil {
		t.Fatal(err)
	}
	// An old binary that cannot read HSW2 starts empty and checkpoints a tiny
	// graph into slot A with an HSW1 meta.
	tiny := index.NewHNSW(f.dim)
	tiny.Add("https://x.example/tiny", "t", deterministicVec("tiny", f.dim))
	if err := tiny.Persist(ctx, f.ps); err != nil {
		t.Fatal(err)
	}

	srv := &pebbleHTTP{store: f.ps, hasVectors: true, vectorDim: f.dim, vectorNodes: 1}
	srv.loadHNSWInto(ctx, f.ps, f.dim, 1)
	if g := srv.hnsw(); g == nil || g.Len() != 1 {
		t.Fatalf("expected the tiny graph to load, got %v", g)
	}
	if empty, _ := f.ps.VectorSlotEmpty(ctx, store.VectorSlotB); empty {
		t.Fatal("stale slot B holding the real graph was cleared")
	}
	if !srv.staleSlotKept.Load() {
		t.Fatal("staleSlotKept not flagged")
	}

	// A genuinely stale slot (crash after swap, comparable size) is cleared.
	srv2 := &pebbleHTTP{store: f.ps, hasVectors: true, vectorDim: f.dim}
	if err := big.PersistSwap(ctx, f.ps, nil); err != nil { // back into slot A, B left populated
		t.Fatal(err)
	}
	srv2.reclaimStaleSlot(ctx, f.ps, store.VectorSlotA)
	if empty, _ := f.ps.VectorSlotEmpty(ctx, store.VectorSlotB); !empty {
		t.Fatal("comparable stale slot not cleared")
	}
	if srv2.staleSlotKept.Load() {
		t.Fatal("comparable stale slot wrongly flagged")
	}
}

// /stats reports graph status through the non-blocking TryStatus path.
func TestStatsGraphStatusShape(t *testing.T) {
	f := populatedPebbleStore(t)
	srv := f.makeServer(nil)
	raw, err := srv.buildStatsBody(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var stats map[string]any
	_ = json.Unmarshal(raw, &stats)
	if stats["hnsw_busy"] != false || stats["hnsw_slot"] == nil || stats["pq"] == nil || stats["hnsw_stale_slot_kept"] != false {
		t.Fatalf("stats graph status: busy=%v slot=%v pq=%v stale_kept=%v", stats["hnsw_busy"], stats["hnsw_slot"], stats["pq"], stats["hnsw_stale_slot_kept"])
	}
}

// The compact ETA is derived from the persist phase alone.
func TestCompactETAUsesPersistPhase(t *testing.T) {
	j := &compactJob{running: true, started: time.Now().Add(-10 * time.Minute), persistStart: time.Now().Add(-10 * time.Second)}
	j.progress = index.CompactProgress{Phase: "persist", Written: 500, Total: 1000}
	eta, _ := j.snapshot()["eta_s"].(float64)
	if eta < 8 || eta > 13 {
		t.Fatalf("eta_s = %v, want ~10 (persist-phase rate)", eta)
	}
}

// A fresh empty graph must not be created over a store that still holds one.
func TestFreshGraphRefusedOverPersistedStore(t *testing.T) {
	if err := (&pebbleHTTP{hasVectors: true}).freshGraphAllowed(); err == nil {
		t.Fatal("expected refusal when the store has vector data")
	}
	if err := (&pebbleHTTP{}).freshGraphAllowed(); err != nil {
		t.Fatalf("empty store should allow a fresh graph: %v", err)
	}
}

// The checkpoint persists from the graph's own count and refuses after an
// unpersisted compact until a full persist runs.
func TestHNSWCheckpointPaths(t *testing.T) {
	f := populatedPebbleStore(t)
	ctx := context.Background()
	g := f.hnsw
	if err := g.Persist(ctx, f.ps); err != nil {
		t.Fatal(err)
	}
	if hnswCheckpoint(g, f.ps, "noop", false) {
		t.Fatal("nothing to persist should return false")
	}
	g.Add("https://x.example/ckpt-1", "n", deterministicVec("ckpt-1", f.dim))
	if !hnswCheckpoint(g, f.ps, "tick", false) {
		t.Fatal("checkpoint with new nodes should persist")
	}
	if r, _, _ := index.LoadHNSW(ctx, f.ps); r == nil || r.Len() != g.Len() {
		t.Fatalf("reload after checkpoint: %v", r)
	}
	g.MarkURLPassagesInvalid(f.docs[2])
	if g.Compact() == 0 {
		t.Fatal("fixture: compact should remove the invalidated node")
	}
	g.Add("https://x.example/ckpt-2", "n", deterministicVec("ckpt-2", f.dim))
	if hnswCheckpoint(g, f.ps, "diverged", false) || hnswCheckpoint(g, f.ps, "diverged-wait", true) {
		t.Fatal("checkpoint must refuse while the layout is diverged")
	}
	if err := g.PersistSwap(ctx, f.ps, nil); err != nil {
		t.Fatal(err)
	}
	g.Add("https://x.example/ckpt-3", "n", deterministicVec("ckpt-3", f.dim))
	if !hnswCheckpoint(g, f.ps, "final", true) {
		t.Fatal("blocking checkpoint after the swap should persist")
	}
	if r, _, _ := index.LoadHNSW(ctx, f.ps); r == nil || r.Len() != g.Len() || r.Slot() != g.Slot() {
		t.Fatalf("reload after swap+checkpoint: %v", r)
	}
}

// startInProcessCrawl must not create a fresh graph over a populated store.
func TestStartInProcessCrawlRefusesFreshGraph(t *testing.T) {
	f := populatedPebbleStore(t)
	srv := f.makeServer(openaiTestServer(t))
	srv.hnswAt.Store(nil)
	seeds := filepath.Join(t.TempDir(), "seeds.txt")
	if err := os.WriteFile(seeds, []byte("https://example.com/\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	err := srv.startInProcessCrawl(context.Background(), f.ps, seeds, time.Minute, &config.Config{}, &wg)
	if err == nil || !strings.Contains(err.Error(), "refusing") {
		t.Fatalf("expected refusal, got %v", err)
	}
	if srv.hnsw() != nil {
		t.Fatal("a fresh graph was created over the populated store")
	}
}

func TestPQStatsInfoBranches(t *testing.T) {
	info := pqStatsInfo(index.PQStatus{Enabled: true, Dim: 8, M: 2, K: 4, NodesTotal: 5, NodesValid: 0, NodesWithCode: 2})
	if info["coverage_pct"] != 40.0 || info["zombie_nodes"] != 5 || info["m"] != 2 {
		t.Fatalf("pq info: %v", info)
	}
	info = pqStatsInfo(index.PQStatus{NodesTotal: 10, NodesValid: 8, NodesWithCode: 4})
	if info["coverage_pct"] != 50.0 || info["zombie_nodes"] != 2 {
		t.Fatalf("pq info (disabled): %v", info)
	}
	if _, ok := info["dim"]; ok {
		t.Fatal("dim must be absent when PQ is disabled")
	}
}
