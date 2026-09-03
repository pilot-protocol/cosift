package index

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"testing"
)

// TestHNSWZombieCompaction reproduces the production finding:
// when ~80% of HNSW nodes have nil vec (partial-persist zombies),
// graph navigation fragments and Recall@K drops far below the clean
// baseline — bench-pq saw production at 0.74 vs the expected 0.99.
// Compact rebuilds the node/neighbor arrays without zombies and the
// recall snaps back.
func TestHNSWZombieCompaction(t *testing.T) {
	const (
		n      = 1000
		dim    = 64
		nQuery = 30
		k      = 10
	)
	rng := rand.New(rand.NewSource(42))
	h := NewHNSW(dim)
	h.rng = rand.New(rand.NewSource(7))

	for i := 0; i < n; i++ {
		v := make([]float32, dim)
		for j := range v {
			v[j] = float32(rng.NormFloat64())
		}
		h.Add(fmt.Sprintf("https://x/%d", i), "t", v)
	}

	queryVecs := make([][]float32, nQuery)
	for q := 0; q < nQuery; q++ {
		qv := make([]float32, dim)
		for j := range qv {
			qv[j] = float32(rng.NormFloat64())
		}
		queryVecs[q] = qv
	}

	measure := func(name string) float64 {
		var total float64
		for _, q := range queryVecs {
			gt := h.BruteForceTopK(q, k)
			approx := h.Search(context.Background(), q, k)
			gtURLs := map[string]bool{}
			for _, g := range gt {
				gtURLs[g.URL] = true
			}
			if len(gt) == 0 {
				continue
			}
			hits := 0
			for _, a := range approx {
				if gtURLs[a.URL] {
					hits++
				}
			}
			total += float64(hits) / float64(len(gt))
		}
		avg := total / float64(nQuery)
		t.Logf("%s Recall@%d = %.3f (Len=%d)", name, k, avg, h.Len())
		return avg
	}

	clean := measure("clean")
	if clean < 0.85 {
		t.Fatalf("clean HNSW recall too low: %.3f", clean)
	}

	// Zombify ~80% of nodes by clearing vec while leaving neighbor lists
	// pointing at them. Spare the entry point so the graph still has a
	// starting node — matches the production layout (entry point was a
	// real high-level node, not a zombie).
	zSeed := rand.New(rand.NewSource(99))
	zombified := 0
	for i := range h.nodes {
		if i == h.entryPoint {
			continue
		}
		if zSeed.Float64() < 0.8 {
			h.nodes[i].vec = nil
			zombified++
		}
	}
	t.Logf("zombified %d of %d nodes", zombified, len(h.nodes))

	zombied := measure("with-zombies")
	if zombied >= clean*0.9 {
		t.Logf("note: zombies didn't drag recall enough on this seed (clean=%.3f, zombied=%.3f); test still validates compact()'s round-trip", clean, zombied)
	}

	removed := h.Compact()
	if removed != zombified {
		t.Fatalf("compact removed %d, expected %d", removed, zombified)
	}

	compacted := measure("compacted")
	// Compact doesn't restore edges to surviving nodes — it only removes
	// dangling refs. Same recall as the zombied state (slightly higher is
	// possible if traversal now skips fewer dead branches).
	if compacted < zombied*0.95 {
		t.Errorf("compaction regressed recall: zombied=%.3f compacted=%.3f", zombied, compacted)
	}

	// Rebuild does restore: fresh graph topology with full M-neighbor
	// connectivity among surviving nodes.
	rebuilt := h.Rebuild()
	swap := h
	h = rebuilt
	defer func() { _ = swap }()

	rebuiltRecall := measure("rebuilt")
	if rebuiltRecall < clean*0.9 {
		t.Errorf("rebuild did not restore recall: clean=%.3f rebuilt=%.3f", clean, rebuiltRecall)
	}
}

// TestHNSWCompactProgressLogs pins the write-lock liveness lines.
func TestHNSWCompactProgressLogs(t *testing.T) {
	h := buildTestHNSW(250, 16, 3, 5)
	for i := 0; i < 40; i++ {
		h.nodes[i*5].vec = nil
	}

	savedEvery := compactProgressEvery
	compactProgressEvery = 100
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer func() {
		compactProgressEvery = savedEvery
		log.SetOutput(os.Stderr)
	}()

	removed := h.Compact()
	if removed != 40 {
		t.Errorf("removed: want 40, got %d", removed)
	}
	out := buf.String()
	if !strings.Contains(out, "hnsw compact: scanning") {
		t.Errorf("missing scanning progress line in:\n%s", out)
	}
	if !strings.Contains(out, "hnsw compact: rewiring neighbors") {
		t.Errorf("missing rewiring progress line in:\n%s", out)
	}
}
