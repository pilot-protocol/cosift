package index

import (
	"context"
	"math"
	"math/rand"
	"testing"
)

// TestHNSWReconcileURLs — nodes whose URL fails the live predicate are
// invalidated in one pass; live URLs untouched; second run is a no-op.
func TestHNSWReconcileURLs(t *testing.T) {
	h := NewHNSW(4)
	h.AddPassage("https://purged", "spam", 0, 50, []float32{1, 0, 0, 0})
	h.AddPassage("https://purged", "spam", 50, 50, []float32{0, 1, 0, 0})
	h.Add("https://kept", "real doc", []float32{0, 0, 1, 0})

	live := func(url string) bool { return url == "https://kept" }

	invalidated, scanned := h.ReconcileURLs(live)
	if invalidated != 2 || scanned != 3 {
		t.Fatalf("expected (2,3), got (%d,%d)", invalidated, scanned)
	}
	if v, ok := h.LookupVectorByURL("https://kept"); !ok || len(v) == 0 {
		t.Fatalf("live URL lost its vec")
	}
	if v, ok := h.LookupVectorByURL("https://purged"); ok && len(v) > 0 {
		t.Fatalf("purged URL still has a vec")
	}

	hits := h.Search(context.Background(), []float32{1, 0, 0, 0}, 10)
	for _, hit := range hits {
		if hit.URL == "https://purged" {
			t.Errorf("purged URL surfaced in search: %+v", hit)
		}
	}

	invalidated, scanned = h.ReconcileURLs(live)
	if invalidated != 0 || scanned != 3 {
		t.Fatalf("second run should be a no-op, got (%d,%d)", invalidated, scanned)
	}

	if inv, _ := h.ReconcileURLs(nil); inv != 0 {
		t.Fatalf("nil predicate must not invalidate, got %d", inv)
	}
}

// TestHNSWReconcileNilsPQCodes — reconcile must clear a node's PQ code along
// with its vec, and a stale code alone must never resurrect a zombie in
// search (distanceToNode checks vec before the PQ branch).
func TestHNSWReconcileNilsPQCodes(t *testing.T) {
	const dim, n = 8, 64
	rng := rand.New(rand.NewSource(11))
	h := NewHNSW(dim)
	vecs := make([][]float32, 0, n)
	for i := 0; i < n; i++ {
		v := make([]float32, dim)
		for j := range v {
			v[j] = float32(rng.NormFloat64())
		}
		vecs = append(vecs, v)
		url := "https://live/x"
		if i == 0 {
			url = "https://purged/x"
		}
		h.Add(url, "t", v)
	}
	cb, err := TrainPQCodebook(vecs, dim, 2, 4, 5, rand.New(rand.NewSource(3)))
	if err != nil {
		t.Fatalf("train: %v", err)
	}
	h.UsePQ(cb, nil)
	ids, codes, err := h.EncodeMissing()
	if err != nil || len(ids) != n {
		t.Fatalf("encode: err=%v coded=%d", err, len(ids))
	}
	_ = codes
	if !h.HasPQ() {
		t.Fatal("HasPQ false after encode")
	}

	inv, _ := h.ReconcileURLs(func(url string) bool { return url != "https://purged/x" })
	if inv != 1 {
		t.Fatalf("expected 1 invalidation, got %d", inv)
	}
	if h.codes[0] != nil {
		t.Fatalf("reconcile left the PQ code on the invalidated node")
	}

	// Regression for the resurrect path: with a code re-attached to the
	// vec-less slot (the pre-fix load-order hazard), distanceToNode must
	// still treat the node as unreachable — the vec check precedes the PQ
	// branch. Asserted on distanceToNode directly because Search's result
	// filter would mask the traversal-side bug.
	h.codes[0] = make([]uint16, cb.M)
	table, err := cb.QueryTable(vecs[0])
	if err != nil {
		t.Fatalf("query table: %v", err)
	}
	if d := h.distanceToNode(vecs[0], table, 0); d != math.MaxFloat64 {
		t.Fatalf("zombie with stale PQ code got a finite distance: %v", d)
	}
	if d := h.distanceToNode(vecs[0], table, 1); d == math.MaxFloat64 {
		t.Fatalf("live coded node unexpectedly unreachable")
	}
}
