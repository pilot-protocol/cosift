package index

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/store"
)

// Compact renumbers ids; an incremental persist must refuse until a full one.
func TestHNSWCompactWithoutPersistRefusesIncremental(t *testing.T) {
	ps := openTestStore(t)
	ctx := context.Background()
	h := buildTestHNSW(300, 8, 3, 5)
	if err := h.Persist(ctx, ps); err != nil {
		t.Fatal(err)
	}
	before := mustLoad(t, ps)
	h.MarkURLPassagesInvalid("https://x/7")
	if h.Compact() != 1 || !h.LayoutDiverged() {
		t.Fatal("compact should renumber and flag divergence")
	}
	h.AddPassage("https://x/new", "n", 0, 1, []float32{1, 0, 0, 0, 0, 0, 0, 0})
	err := h.PersistFrom(ctx, ps, h.PersistedCount())
	if !errors.Is(err, ErrLayoutDiverged) {
		t.Fatalf("want ErrLayoutDiverged, got %v", err)
	}
	if ok, err := h.TryPersistFrom(ctx, ps, h.PersistedCount()); !ok || !errors.Is(err, ErrLayoutDiverged) {
		t.Fatalf("try: ok=%v err=%v", ok, err)
	}
	sameGraph(t, before, mustLoad(t, ps), "disk untouched")
	if err := h.PersistSwap(ctx, ps, nil); err != nil {
		t.Fatal(err)
	}
	if h.LayoutDiverged() || h.PersistedCount() != h.Len() {
		t.Fatalf("after swap: diverged=%v persisted=%d len=%d", h.LayoutDiverged(), h.PersistedCount(), h.Len())
	}
	sameGraph(t, h, mustLoad(t, ps), "after swap")
	h.AddPassage("https://x/new2", "n", 0, 1, []float32{0, 1, 0, 0, 0, 0, 0, 0})
	if err := h.PersistFrom(ctx, ps, h.PersistedCount()); err != nil {
		t.Fatal(err)
	}
	sameGraph(t, h, mustLoad(t, ps), "incremental after swap")
}

// A compact that removes nothing must not drop pending back-link dirties.
func TestHNSWZeroRemovalCompactKeepsDirty(t *testing.T) {
	ps := openTestStore(t)
	ctx := context.Background()
	h := buildTestHNSW(200, 8, 3, 5)
	if err := h.Persist(ctx, ps); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 30; i++ {
		v := make([]float32, 8)
		v[i%8] = 1
		h.AddPassage("https://x/late", "n", i, 1, v)
	}
	dirty := h.DirtyCount()
	if dirty == 0 {
		t.Fatal("fixture: inserts should dirty back-linked nodes")
	}
	if h.Compact() != 0 || h.DirtyCount() != dirty || h.LayoutDiverged() {
		t.Fatalf("zero-removal compact changed state: dirty %d→%d diverged=%v", dirty, h.DirtyCount(), h.LayoutDiverged())
	}
	if err := h.PersistFrom(ctx, ps, h.PersistedCount()); err != nil {
		t.Fatal(err)
	}
	sameGraph(t, h, mustLoad(t, ps), "after zero-removal compact")
}

// A corrupt entry-point blob must not leave every search empty.
func TestHNSWLoadRelocatesDeadEntryPoint(t *testing.T) {
	ps := openTestStore(t)
	ctx := context.Background()
	h := buildTestHNSW(300, 8, 3, 5)
	if err := h.Persist(ctx, ps); err != nil {
		t.Fatal(err)
	}
	if err := ps.PutVectorNode(ctx, store.VectorSlotA, uint64(h.entryPoint), []byte{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	g := mustLoad(t, ps)
	if g.zombieIdx(g.entryPoint) {
		t.Fatalf("entry point %d still dead after load", g.entryPoint)
	}
	q := make([]float32, 8)
	q[0] = 1
	if hits := g.Search(ctx, q, 10); len(hits) != 10 {
		t.Fatalf("search after corrupt entry: %d hits", len(hits))
	}
}

// Live nodes with an empty URL stay in the index across a reload.
func TestHNSWLoadIndexesEmptyURL(t *testing.T) {
	ps := openTestStore(t)
	ctx := context.Background()
	h := buildTestHNSW(50, 8, 3, 5)
	h.AddPassage("", "untitled", 0, 1, []float32{1, 0, 0, 0, 0, 0, 0, 0})
	if err := h.Persist(ctx, ps); err != nil {
		t.Fatal(err)
	}
	g := mustLoad(t, ps)
	checkURLIndex(t, g, "load with empty url")
	if g.valid != h.valid {
		t.Fatalf("valid %d != %d", g.valid, h.valid)
	}
}

// TryStatus must not queue behind the graph write lock (compact phase).
func TestHNSWTryStatusUnderWriteLock(t *testing.T) {
	h := buildTestHNSW(50, 8, 3, 5)
	if _, slot, ok := h.TryStatus(); !ok || slot != store.VectorSlotA {
		t.Fatalf("unlocked TryStatus: ok=%v slot=%#x", ok, slot)
	}
	h.mu.Lock()
	done := make(chan bool, 1)
	go func() { _, _, ok := h.TryStatus(); done <- ok }()
	select {
	case ok := <-done:
		if ok {
			t.Fatal("TryStatus reported ok under the write lock")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("TryStatus blocked behind the write lock")
	}
	h.mu.Unlock()
}

// CompactPersist: no-op without zombies, swap + cleanup normally, cleanup
// skipped when serve is stopping.
func TestHNSWCompactPersistPhases(t *testing.T) {
	ps := openTestStore(t)
	ctx := context.Background()
	h := buildTestHNSW(120, 8, 3, 5)
	if err := h.Persist(ctx, ps); err != nil {
		t.Fatal(err)
	}
	res, err := h.CompactPersist(ctx, ps, false, nil, nil)
	if err != nil || res.Persisted || res.Removed != 0 {
		t.Fatalf("no-op compact: %+v %v", res, err)
	}

	h.MarkURLPassagesInvalid("https://x/3")
	stop := make(chan struct{})
	close(stop)
	var phases []string
	res, err = h.CompactPersist(ctx, ps, false, stop, func(p CompactProgress) { phases = append(phases, p.Phase) })
	if err != nil || !res.Persisted || !res.CleanupSkipped || res.Removed != 1 {
		t.Fatalf("stopped compact: %+v %v", res, err)
	}
	if h.Slot() != store.VectorSlotB || countSlot(t, ps, store.VectorSlotA) == 0 {
		t.Fatalf("cleanup must be skipped on stop: slot=%#x A=%d", h.Slot(), countSlot(t, ps, store.VectorSlotA))
	}
	for _, p := range phases {
		if p == "cleanup" {
			t.Fatal("cleanup phase reported despite stop")
		}
	}
	sameGraph(t, h, mustLoad(t, ps), "after stopped compact")

	h.MarkURLPassagesInvalid("https://x/4")
	res, err = h.CompactPersist(ctx, ps, false, nil, nil)
	if err != nil || !res.Persisted || res.CleanupSkipped {
		t.Fatalf("full compact: %+v %v", res, err)
	}
	if h.Slot() != store.VectorSlotA || countSlot(t, ps, store.VectorSlotB) != 0 {
		t.Fatalf("cleanup should clear the old slot: slot=%#x B=%d", h.Slot(), countSlot(t, ps, store.VectorSlotB))
	}
	sameGraph(t, h, mustLoad(t, ps), "after full compact")
}
