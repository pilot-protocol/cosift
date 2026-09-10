package index

import (
	"context"
	"math/rand"
	"testing"
)

// Zombie transit is budgeted per layer search: a dead component must not be
// drained (unbounded transit walks the whole graph once zombies percolate).
func TestHNSWZombieTransitBounded(t *testing.T) {
	const n, dim, k = 1000, 16, 10
	h := buildTestHNSW(n, dim, 3, 5)
	zr := rand.New(rand.NewSource(5))
	for i := range h.nodes {
		if zr.Float64() < 0.15 && i != h.entryPoint {
			h.MarkURLPassagesInvalid(h.nodes[i].url)
		}
	}
	maxExpanded, maxVisited := 0, 0
	searchStatsHook = func(visited, expanded int) {
		maxVisited = max(maxVisited, visited)
		maxExpanded = max(maxExpanded, expanded)
	}
	defer func() { searchStatsHook = nil }()

	rng := rand.New(rand.NewSource(11))
	recall := 0.0
	const nq = 20
	for qi := 0; qi < nq; qi++ {
		q := make([]float32, dim)
		for j := range q {
			q[j] = float32(rng.NormFloat64())
		}
		gt := map[string]bool{}
		for _, g := range h.BruteForceTopK(q, k) {
			gt[g.URL] = true
		}
		hits := 0
		for _, a := range h.Search(context.Background(), q, k) {
			if gt[a.URL] {
				hits++
			}
		}
		recall += float64(hits) / float64(len(gt))
	}
	recall /= nq
	t.Logf("15%% zombies: max zombie expansions per layer search=%d (budget %d), max visited=%d, recall=%.3f", maxExpanded, h.efSearch, maxVisited, recall)
	if maxExpanded > h.efSearch {
		t.Fatalf("zombie transit unbounded: %d expansions in one layer search (budget %d)", maxExpanded, h.efSearch)
	}
	if recall < 0.9 {
		t.Fatalf("recall with zombies %.3f", recall)
	}
}
