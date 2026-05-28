package index

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

// TestHNSWMatchesBruteForceOnSmallSet — correctness check. HNSW is an
// approximate kNN; on a small synthetic corpus where brute-force is cheap,
// it must return results that overlap heavily with the exact top-k.
//
// locks in the recall floor for the new HNSW implementation.
// At standard params (M=16, efConstruction=200, efSearch=50), recall@10
// against ground truth should be ≥0.9 on random vectors; we set the gate
// at 0.85 to tolerate occasional run-to-run variance from the random
// level draws.
func TestHNSWMatchesBruteForceOnSmallSet(t *testing.T) {
	const (
		n      = 1000
		dim    = 64
		nQuery = 30
		k      = 10
	)
	rng := rand.New(rand.NewSource(42))

	brute := NewVectorIndex(dim)
	hnsw := NewHNSW(dim)
	// Use the same RNG seed for the level draws so the test is deterministic
	// across go versions (which can re-seed Go's global rand differently).
	hnsw.rng = rand.New(rand.NewSource(7))

	vecs := make([][]float32, n)
	for i := 0; i < n; i++ {
		v := make([]float32, dim)
		for j := range v {
			v[j] = float32(rng.NormFloat64())
		}
		vecs[i] = v
		url := fmt.Sprintf("https://x/%d", i)
		brute.Add(url, "t", v)
		hnsw.Add(url, "t", v)
	}

	if hnsw.Len() != n {
		t.Fatalf("hnsw.Len: want %d, got %d", n, hnsw.Len())
	}

	var totalRecall float64
	for q := 0; q < nQuery; q++ {
		queryV := make([]float32, dim)
		for j := range queryV {
			queryV[j] = float32(rng.NormFloat64())
		}
		gold := brute.Search(context.Background(), queryV, k)
		approx := hnsw.Search(context.Background(), queryV, k)
		goldURLs := map[string]bool{}
		for _, g := range gold {
			goldURLs[g.URL] = true
		}
		hits := 0
		for _, a := range approx {
			if goldURLs[a.URL] {
				hits++
			}
		}
		totalRecall += float64(hits) / float64(k)
	}
	meanRecall := totalRecall / float64(nQuery)
	if meanRecall < 0.85 {
		t.Errorf("HNSW recall@%d vs brute-force ground truth: got %.3f, want ≥0.85", k, meanRecall)
	}
	t.Logf("HNSW recall@%d over %d queries: %.3f", k, nQuery, meanRecall)
}

// TestHNSWSearchEmpty returns nil cleanly on an empty index.
func TestHNSWSearchEmpty(t *testing.T) {
	h := NewHNSW(8)
	q := make([]float32, 8)
	hits := h.Search(context.Background(), q, 5)
	if len(hits) != 0 {
		t.Errorf("empty index: want nil/empty, got %+v", hits)
	}
}

// TestHNSWSearchSingle — index with exactly one vector returns it.
// TestHNSWLookupVectorByURL locks down: /find_similar?retriever=dense
// reads the source's persisted vector by URL so it can skip the embed RPC.
// Must return a unit-normalized vector (HNSW stores normalized passages),
// must NOT alias the graph's internal slice, and must report not-found.
func TestHNSWLookupVectorByURL(t *testing.T) {
	h := NewHNSW(4)
	h.Add("https://a", "doc a", []float32{1, 0, 0, 0})
	h.Add("https://b", "doc b", []float32{0, 1, 0, 0})

	vec, ok := h.LookupVectorByURL("https://a")
	if !ok {
		t.Fatalf("LookupVectorByURL(\"https://a\"): want ok=true")
	}
	if len(vec) != 4 {
		t.Fatalf("vec len: want 4, got %d", len(vec))
	}
	if vec[0] < 0.99 {
		t.Errorf("vec[0]: want ~1.0 (unit-normalized), got %.3f", vec[0])
	}
	// Mutating the returned slice must not poison subsequent searches.
	vec[0] = 999
	hits := h.Search(context.Background(), []float32{1, 0, 0, 0}, 1)
	if len(hits) == 0 || hits[0].URL != "https://a" || hits[0].Score < 0.99 {
		t.Errorf("graph state mutated by caller: hits=%+v", hits)
	}

	if _, ok := h.LookupVectorByURL("https://missing"); ok {
		t.Errorf("LookupVectorByURL(\"https://missing\"): want ok=false")
	}
}

func TestHNSWSearchSingle(t *testing.T) {
	h := NewHNSW(4)
	h.Add("https://only", "the only", []float32{1, 0, 0, 0})
	hits := h.Search(context.Background(), []float32{1, 0, 0, 0}, 5)
	if len(hits) != 1 {
		t.Fatalf("want 1 hit, got %d", len(hits))
	}
	if hits[0].URL != "https://only" {
		t.Errorf("URL: got %s", hits[0].URL)
	}
	if hits[0].Score < 0.99 {
		t.Errorf("identical-vector score should be ~1.0, got %.3f", hits[0].Score)
	}
}

// TestHNSWDedupByURL — same URL with multiple passages, max-passage
// aggregation returns the URL exactly once with its best score.
func TestHNSWDedupByURL(t *testing.T) {
	h := NewHNSW(4)
	h.AddPassage("https://multi", "doc with two passages", 0, 50, []float32{1, 0, 0, 0})
	h.AddPassage("https://multi", "doc with two passages", 50, 50, []float32{0, 0.5, 0.5, 0.5})
	h.Add("https://other", "different doc", []float32{0, 1, 0, 0})

	hits := h.Search(context.Background(), []float32{1, 0, 0, 0}, 10)
	seen := map[string]int{}
	for _, h := range hits {
		seen[h.URL]++
	}
	if seen["https://multi"] != 1 {
		t.Errorf("multi-passage URL should appear exactly once; got %d times", seen["https://multi"])
	}
	// And its score should be the higher of the two passages (cos=1 with the
	// (1,0,0,0) passage), not the lower.
	for _, h := range hits {
		if h.URL == "https://multi" && h.Score < 0.99 {
			t.Errorf("max-passage aggregation should pick the (1,0,0,0) passage; got score %.3f", h.Score)
		}
	}
}

// TestHNSWMarkURLPassagesInvalid — zombie reclaim: prior generations of a
// re-crawled URL's passages must be zeroed (vec=nil) so they fall through
// the "len(vec)==0" guard in Search/searchLayer. Other URLs' nodes must
// not be touched.
func TestHNSWMarkURLPassagesInvalid(t *testing.T) {
	h := NewHNSW(4)
	// Old generation of https://reused — 2 passages with different vecs.
	h.AddPassage("https://reused", "old title", 0, 50, []float32{1, 0, 0, 0})
	h.AddPassage("https://reused", "old title", 50, 50, []float32{0, 1, 0, 0})
	// An unrelated URL that must survive intact.
	h.Add("https://kept", "different doc", []float32{0, 0, 1, 0})

	n := h.MarkURLPassagesInvalid("https://reused")
	if n != 2 {
		t.Fatalf("expected 2 invalidations, got %d", n)
	}
	// The kept URL still has its vec.
	if v, ok := h.LookupVectorByURL("https://kept"); !ok || len(v) == 0 {
		t.Fatalf("https://kept should still have a vec after invalidating a different URL")
	}
	// The reused URL's nodes are zombies — LookupVectorByURL returns the
	// first match's vec, which is now nil-or-empty.
	if v, ok := h.LookupVectorByURL("https://reused"); ok && len(v) > 0 {
		t.Fatalf("https://reused vec should be empty after invalidate; got len=%d", len(v))
	}
	// Search must skip the zombies and find https://kept.
	hits := h.Search(context.Background(), []float32{0, 0, 1, 0}, 10)
	foundKept := false
	for _, hit := range hits {
		if hit.URL == "https://reused" {
			t.Errorf("zombie URL appeared in search results: %+v", hit)
		}
		if hit.URL == "https://kept" {
			foundKept = true
		}
	}
	if !foundKept {
		t.Errorf("non-zombie URL https://kept missing from search results: %+v", hits)
	}

	// Re-adding a fresh passage for the URL should work normally; new node
	// participates in search, with the higher score winning per existing
	// dedup-by-URL logic.
	h.AddPassage("https://reused", "new title", 0, 50, []float32{0, 0, 1, 0})
	hits = h.Search(context.Background(), []float32{0, 0, 1, 0}, 10)
	foundReused := false
	for _, hit := range hits {
		if hit.URL == "https://reused" {
			foundReused = true
		}
	}
	if !foundReused {
		t.Errorf("re-added passage for https://reused not found in search: %+v", hits)
	}
}

// TestHNSWGraphInvariants — the constructed graph must respect the
// neighbor cap and contain bidirectional edges. Sanity check on the
// adjacency-list bookkeeping.
func TestHNSWGraphInvariants(t *testing.T) {
	h := NewHNSW(8)
	h.rng = rand.New(rand.NewSource(11))
	for i := 0; i < 200; i++ {
		v := make([]float32, 8)
		for j := range v {
			v[j] = rand.Float32()
		}
		h.Add(fmt.Sprintf("u%d", i), "t", v)
	}
	for i, n := range h.nodes {
		for lvl, nbs := range n.neighbors {
			mCap := h.M
			if lvl == 0 {
				mCap = h.Mmax0
			}
			if len(nbs) > mCap {
				t.Errorf("node %d layer %d: neighbor count %d > cap %d", i, lvl, len(nbs), mCap)
			}
			// Distinctness within layer.
			seen := map[int]bool{}
			for _, nb := range nbs {
				if seen[nb] {
					t.Errorf("node %d layer %d: duplicate neighbor %d", i, lvl, nb)
				}
				seen[nb] = true
				if nb == i {
					t.Errorf("node %d layer %d: self-loop", i, lvl)
				}
			}
		}
	}
	// Spot-check: a sample node's neighbor at layer 0 should also have us in
	// ITS neighbor list at layer 0 (bidirectional). Not all edges have to be
	// bidirectional after pruning, but on average most should be.
	bidir := 0
	checked := 0
	for i := 0; i < 50 && i < len(h.nodes); i++ {
		for _, nb := range h.nodes[i].neighbors[0] {
			checked++
			for _, back := range h.nodes[nb].neighbors[0] {
				if back == i {
					bidir++
					break
				}
			}
		}
	}
	if checked > 0 {
		ratio := float64(bidir) / float64(checked)
		t.Logf("bidirectional edge ratio (layer 0, first 50 nodes): %.2f", ratio)
		if ratio < 0.5 {
			t.Errorf("expected ≥50%% bidirectional edges at layer 0; got %.2f", ratio)
		}
	}
}

// BenchmarkHNSWSearch — read-side latency benchmark vs brute-force baseline.
// Run with `go test -bench BenchmarkHNSW -benchmem`.
func BenchmarkHNSWSearch(b *testing.B) {
	const (
		n   = 5000
		dim = 128
	)
	h := NewHNSW(dim)
	rng := rand.New(rand.NewSource(99))
	for i := 0; i < n; i++ {
		v := make([]float32, dim)
		for j := range v {
			v[j] = float32(rng.NormFloat64())
		}
		h.Add(fmt.Sprintf("u%d", i), "t", v)
	}
	queries := make([][]float32, b.N)
	for i := range queries {
		q := make([]float32, dim)
		for j := range q {
			q[j] = float32(rng.NormFloat64())
		}
		queries[i] = q
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = h.Search(context.Background(), queries[i], 10)
	}
}

// Sanity guard: assert the candidate heaps order correctly.
func TestHNSWHeapOrderings(t *testing.T) {
	mn := &candMinHeap{}
	mx := &candMaxHeap{}
	for _, d := range []float32{3, 1, 4, 1, 5, 9, 2, 6} {
		*mn = append(*mn, candEntry{dist: d})
		*mx = append(*mx, candEntry{dist: d})
	}
	sort.Sort(mn)
	if (*mn)[0].dist != 1 {
		t.Errorf("min-heap-after-sort front: got %v", (*mn)[0].dist)
	}
	sort.Sort(mx)
	if (*mx)[0].dist != 9 {
		t.Errorf("max-heap-after-sort front: got %v", (*mx)[0].dist)
	}
}
