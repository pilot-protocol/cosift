package index

import (
	"context"
	"math/rand"
	"testing"
)

// Scattered zombies must not turn a search into a walk of the whole graph.
func TestHNSWZombieTransitBounded(t *testing.T) {
	const n, dim, k = 3000, 16, 10
	h := buildTestHNSW(n, dim, 3, 5)
	rng := rand.New(rand.NewSource(11))
	queries := make([][]float32, 20)
	for i := range queries {
		q := make([]float32, dim)
		for j := range q {
			q[j] = float32(rng.NormFloat64())
		}
		queries[i] = q
	}
	visited := 0
	searchVisitedHook = func(v int) { visited += v }
	defer func() { searchVisitedHook = nil }()
	run := func() (int, float64) {
		visited = 0
		recall := 0.0
		for _, q := range queries {
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
		return visited / len(queries), recall / float64(len(queries))
	}
	cleanVisited, cleanRecall := run()

	zr := rand.New(rand.NewSource(5))
	for i := range h.nodes {
		if zr.Float64() < 0.10 && i != h.entryPoint {
			h.MarkURLPassagesInvalid(h.nodes[i].url)
		}
	}
	zVisited, zRecall := run()
	t.Logf("clean visited=%d recall=%.3f; 10%% zombies visited=%d recall=%.3f", cleanVisited, cleanRecall, zVisited, zRecall)
	if float64(zVisited) > 2.5*float64(cleanVisited) {
		t.Fatalf("zombie transit unbounded: visited %d vs clean %d", zVisited, cleanVisited)
	}
	if zRecall < 0.9 || zRecall < cleanRecall-0.05 {
		t.Fatalf("recall with zombies %.3f (clean %.3f)", zRecall, cleanRecall)
	}
}
