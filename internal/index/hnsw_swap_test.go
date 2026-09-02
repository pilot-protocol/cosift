package index

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/pilot-protocol/cosift/internal/store"
)

func openTestStore(t *testing.T) *store.PebbleStore {
	t.Helper()
	ps, err := store.OpenPebble(filepath.Join(t.TempDir(), "pebble"))
	if err != nil {
		t.Fatalf("OpenPebble: %v", err)
	}
	t.Cleanup(func() { ps.Close() })
	return ps
}

func mustLoad(t *testing.T, ps *store.PebbleStore) *HNSW {
	t.Helper()
	g, ok, err := LoadHNSW(context.Background(), ps)
	if err != nil || !ok {
		t.Fatalf("load: ok=%v err=%v", ok, err)
	}
	return g
}

// sameGraph compares every node's url/vec-liveness/neighbor lists.
func sameGraph(t *testing.T, want, got *HNSW, where string) {
	t.Helper()
	want.mu.RLock()
	defer want.mu.RUnlock()
	got.mu.RLock()
	defer got.mu.RUnlock()
	if len(want.nodes) != len(got.nodes) {
		t.Fatalf("%s: len %d != %d", where, len(got.nodes), len(want.nodes))
	}
	if want.entryPoint != got.entryPoint || want.maxLevel != got.maxLevel {
		t.Fatalf("%s: meta ep %d/%d level %d/%d", where, got.entryPoint, want.entryPoint, got.maxLevel, want.maxLevel)
	}
	for i := range want.nodes {
		w, g := &want.nodes[i], &got.nodes[i]
		if w.url != g.url || (len(w.vec) > 0) != (len(g.vec) > 0) {
			t.Fatalf("%s: node %d url/liveness differ", where, i)
		}
		for l := range w.neighbors {
			wl, gl := w.neighbors[l], g.neighbors[l]
			if len(wl) == 0 && len(gl) == 0 {
				continue
			}
			if !reflect.DeepEqual(wl, gl) {
				t.Fatalf("%s: node %d layer %d neighbors differ\n want %v\n got  %v", where, i, l, wl, gl)
			}
		}
	}
}

func countSlot(t *testing.T, ps *store.PebbleStore, slot byte) int {
	t.Helper()
	n := 0
	if err := ps.IterateVectorNodes(context.Background(), slot, func(uint64, []byte) bool { n++; return true }); err != nil {
		t.Fatal(err)
	}
	return n
}

// Back-links added to already-persisted nodes land in the next incremental
// checkpoint.
func TestHNSWIncrementalPersistsBackLinks(t *testing.T) {
	ps := openTestStore(t)
	ctx := context.Background()
	h := buildTestHNSW(200, 8, 3, 5)
	if err := h.Persist(ctx, ps); err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(9))
	for i := 200; i < 260; i++ {
		v := make([]float32, 8)
		for j := range v {
			v[j] = float32(rng.NormFloat64())
		}
		h.AddPassage(fmt.Sprintf("https://x/%d", i), "late", 0, 1, v)
	}
	if len(h.dirty) == 0 {
		t.Fatal("inserts produced no dirty back-links")
	}
	if err := h.PersistFrom(ctx, ps, 200); err != nil {
		t.Fatal(err)
	}
	if len(h.dirty) != 0 {
		t.Fatalf("dirty set not drained: %d", len(h.dirty))
	}
	sameGraph(t, h, mustLoad(t, ps), "after incremental")
}

// Invalidations persist incrementally, including the fromIdx == len case
// that used to be a meta-only refresh.
func TestHNSWIncrementalPersistsInvalidations(t *testing.T) {
	ps := openTestStore(t)
	ctx := context.Background()
	h := buildTestHNSW(100, 8, 3, 5)
	if err := h.Persist(ctx, ps); err != nil {
		t.Fatal(err)
	}
	h.MarkURLPassagesInvalid("https://x/7")
	h.ReconcileURLs(func(u string) bool { return u != "https://x/8" })
	if err := h.PersistFrom(ctx, ps, 100); err != nil {
		t.Fatal(err)
	}
	g := mustLoad(t, ps)
	sameGraph(t, h, g, "after invalidation checkpoint")
	if g.valid != 98 || len(g.byURL["https://x/7"]) != 0 || len(g.byURL["https://x/8"]) != 0 {
		t.Fatalf("reloaded valid=%d byURL7=%v", g.valid, g.byURL["https://x/7"])
	}
	checkURLIndex(t, g, "reload")
}

func TestHNSWPersistErrorKeepsDirty(t *testing.T) {
	ps := openTestStore(t)
	ctx := context.Background()
	h := buildTestHNSW(60, 8, 3, 5)
	if err := h.Persist(ctx, ps); err != nil {
		t.Fatal(err)
	}
	h.MarkURLPassagesInvalid("https://x/3")
	dead := true
	if err := h.PersistFrom(&failAfterFlagCtx{Context: ctx, fail: &dead}, ps, 60); err == nil {
		t.Fatal("want persist error")
	}
	if _, ok := h.dirty[int32(3)]; !ok {
		t.Fatal("dirty entry lost on persist failure")
	}
	if err := h.PersistFrom(ctx, ps, 60); err != nil {
		t.Fatal(err)
	}
	g := mustLoad(t, ps)
	if len(g.nodes[3].vec) != 0 {
		t.Fatal("invalidation not persisted after retry")
	}
}

func TestHNSWPersistSwapRoundTrip(t *testing.T) {
	ps := openTestStore(t)
	ctx := context.Background()
	h := buildTestHNSW(300, 8, 3, 5)
	if err := h.Persist(ctx, ps); err != nil {
		t.Fatal(err)
	}
	if meta, _, _ := ps.GetVectorMeta(ctx); len(meta) != 20 {
		t.Fatalf("slot A meta must stay HSW1 (20 B), got %d", len(meta))
	}
	h.MarkURLPassagesInvalid("https://x/1")
	h.Compact()

	var last PersistProgress
	if err := h.PersistSwap(ctx, ps, func(p PersistProgress) { last = p }); err != nil {
		t.Fatal(err)
	}
	if h.Slot() != store.VectorSlotB || last.Written != 299 || last.Total != 299 {
		t.Fatalf("slot=%#x progress=%+v", h.Slot(), last)
	}
	if meta, _, _ := ps.GetVectorMeta(ctx); len(meta) != 21 || meta[20] != store.VectorSlotB {
		t.Fatalf("meta after swap: %v", meta)
	}
	if countSlot(t, ps, store.VectorSlotA) != 300 {
		t.Fatal("old slot must survive until the caller clears it")
	}
	if err := ps.ClearVectorSlot(ctx, store.VectorSlotA); err != nil {
		t.Fatal(err)
	}
	g := mustLoad(t, ps)
	if g.Slot() != store.VectorSlotB {
		t.Fatalf("loaded slot %#x", g.Slot())
	}
	sameGraph(t, h, g, "after swap")

	// Incremental checkpoints now land in the new slot.
	h.AddPassage("https://x/new", "n", 0, 1, []float32{1, 0, 0, 0, 0, 0, 0, 0})
	if err := h.PersistFrom(ctx, ps, 299); err != nil {
		t.Fatal(err)
	}
	if countSlot(t, ps, store.VectorSlotB) != 300 || countSlot(t, ps, store.VectorSlotA) != 0 {
		t.Fatalf("slots after incremental: A=%d B=%d", countSlot(t, ps, store.VectorSlotA), countSlot(t, ps, store.VectorSlotB))
	}
	sameGraph(t, h, mustLoad(t, ps), "after incremental into B")

	// Swapping back lands in slot A with an HSW1 meta again.
	if err := h.PersistSwap(ctx, ps, nil); err != nil {
		t.Fatal(err)
	}
	if meta, _, _ := ps.GetVectorMeta(ctx); len(meta) != 20 || h.Slot() != store.VectorSlotA {
		t.Fatalf("swap back: meta len %d slot %#x", len(meta), h.Slot())
	}
	sameGraph(t, h, mustLoad(t, ps), "after swap back")
}

// A swap that dies mid-write leaves meta on the old slot and the old graph
// loadable; the next swap clears the partial target and succeeds.
func TestHNSWPersistSwapFailureKeepsOldSlot(t *testing.T) {
	ps := openTestStore(t)
	ctx := context.Background()
	h := buildTestHNSW(300, 16, 3, 5)
	if err := h.Persist(ctx, ps); err != nil {
		t.Fatal(err)
	}
	before := mustLoad(t, ps)

	h.MarkURLPassagesInvalid("https://x/2")
	h.Compact()
	savedWindow := persistWindowBytes
	persistWindowBytes = 4 * 1024
	failNow := false
	persistFlushed = func(int, int) { failNow = true }
	defer func() {
		persistWindowBytes = savedWindow
		persistFlushed = nil
	}()
	if err := h.PersistSwap(&failAfterFlagCtx{Context: ctx, fail: &failNow}, ps, nil); err == nil {
		t.Fatal("want swap error")
	}
	persistFlushed = nil
	if h.Slot() != store.VectorSlotA {
		t.Fatalf("slot flipped on failure: %#x", h.Slot())
	}
	if countSlot(t, ps, store.VectorSlotB) == 0 {
		t.Fatal("fixture: expected partial garbage in slot B")
	}
	sameGraph(t, before, mustLoad(t, ps), "old graph after failed swap")

	if err := h.PersistSwap(ctx, ps, nil); err != nil {
		t.Fatal(err)
	}
	if countSlot(t, ps, store.VectorSlotB) != 299 {
		t.Fatalf("slot B after retry: %d", countSlot(t, ps, store.VectorSlotB))
	}
	sameGraph(t, h, mustLoad(t, ps), "after retry")
}

func TestHNSWMetaFormats(t *testing.T) {
	v1 := encodeHNSWMeta(8, 2, 5, 100, store.VectorSlotA)
	if len(v1) != 20 || string(v1[:4]) != hnswMetaMagic {
		t.Fatalf("slot A meta: %v", v1)
	}
	m, err := decodeHNSWMeta(v1)
	if err != nil || m.slot != store.VectorSlotA || m.nodeCount != 100 {
		t.Fatalf("decode v1: %+v %v", m, err)
	}
	v2 := encodeHNSWMeta(8, 2, 5, 100, store.VectorSlotB)
	if len(v2) != 21 || string(v2[:4]) != hnswMetaMagicV2 {
		t.Fatalf("slot B meta: %v", v2)
	}
	m, err = decodeHNSWMeta(v2)
	if err != nil || m.slot != store.VectorSlotB || m.entryPoint != 5 {
		t.Fatalf("decode v2: %+v %v", m, err)
	}
	bad := append([]byte{}, v2...)
	bad[20] = 0x07
	if _, err := decodeHNSWMeta(bad); err == nil {
		t.Fatal("bad slot accepted")
	}
	if _, err := decodeHNSWMeta(v2[:20]); err == nil {
		t.Fatal("HSW2 magic with 20 bytes accepted")
	}
}

func TestHNSWTryPersistBusy(t *testing.T) {
	ps := openTestStore(t)
	h := buildTestHNSW(10, 8, 3, 5)
	h.persistMu.Lock()
	ok, err := h.TryPersistFrom(context.Background(), ps, 0)
	h.persistMu.Unlock()
	if ok || err != nil {
		t.Fatalf("busy: ok=%v err=%v", ok, err)
	}
	ok, err = h.TryPersistFrom(context.Background(), ps, 0)
	if !ok || err != nil {
		t.Fatalf("idle: ok=%v err=%v", ok, err)
	}
	if mustLoad(t, ps).Len() != 10 {
		t.Fatal("persist did not land")
	}
}

// Writers keep making progress while a windowed persist runs, and a final
// incremental checkpoint reconciles disk with memory.
func TestHNSWPersistConcurrentAdds(t *testing.T) {
	ps := openTestStore(t)
	ctx := context.Background()
	h := buildTestHNSW(400, 8, 3, 5)
	savedWindow := persistWindowBytes
	persistWindowBytes = 8 * 1024
	defer func() { persistWindowBytes = savedWindow }()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		rng := rand.New(rand.NewSource(21))
		for i := 0; i < 200; i++ {
			v := make([]float32, 8)
			for j := range v {
				v[j] = float32(rng.NormFloat64())
			}
			h.AddPassage(fmt.Sprintf("https://c/%d", i), "c", 0, 1, v)
			if i%50 == 0 {
				h.MarkURLPassagesInvalid(fmt.Sprintf("https://x/%d", i))
			}
		}
	}()
	if err := h.Persist(ctx, ps); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	written := countSlot(t, ps, store.VectorSlotA)
	if err := h.PersistFrom(ctx, ps, written); err != nil {
		t.Fatal(err)
	}
	g := mustLoad(t, ps)
	sameGraph(t, h, g, "after concurrent adds")
	checkURLIndex(t, g, "reload")
}
