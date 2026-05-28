package index

import (
	"math"
	"math/rand"
	"testing"
)

// TestPQTrainAndEncodeRoundTrip — train a codebook on N=2000 random
// 64-d vectors, encode them, and verify reconstruction error stays
// below a sane threshold. Locks down the happy path.
func TestPQTrainAndEncodeRoundTrip(t *testing.T) {
	dim := 64
	M := 8  // 8 subspaces of 8 dims each
	K := 32 // 32 centroids per subspace — small for speed
	N := 2000
	rng := rand.New(rand.NewSource(7))
	train := make([][]float32, N)
	for i := range train {
		v := make([]float32, dim)
		for j := range v {
			v[j] = float32(rng.NormFloat64())
		}
		train[i] = v
	}
	cb, err := TrainPQCodebook(train, dim, M, K, 25, rng)
	if err != nil {
		t.Fatalf("train: %v", err)
	}
	if cb.Dim != dim || cb.M != M || cb.K != K || cb.SubDim != dim/M {
		t.Errorf("codebook shape: %+v", cb)
	}
	if len(cb.Centroids) != M {
		t.Fatalf("M centroid groups: want %d, got %d", M, len(cb.Centroids))
	}
	// Encode + score the training set against itself.
	var totalErr, baseline float64
	for _, v := range train[:200] {
		code, err := cb.Encode(v)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		if len(code) != M {
			t.Fatalf("code length: want %d, got %d", M, len(code))
		}
		// Reconstruct via concatenated centroids.
		recon := make([]float32, dim)
		for sub := 0; sub < M; sub++ {
			copy(recon[sub*cb.SubDim:(sub+1)*cb.SubDim], cb.Centroids[sub][code[sub]])
		}
		totalErr += sqDist(v, recon)
		baseline += sqDist(v, make([]float32, dim)) // dist from origin
	}
	// PQ-32 with K=32 over normal random data should achieve much better
	// reconstruction than the trivial "zeros" baseline.
	if totalErr >= baseline*0.5 {
		t.Errorf("PQ reconstruction not much better than zero-vec baseline: pq=%.2f baseline=%.2f", totalErr, baseline)
	}
}

// TestPQDistanceMatchesDirect — encode a vector, then verify that PQ
// distance computation against a query produces values monotonic with
// direct L2 distance. Doesn't have to be exact (PQ is lossy), just
// correctly ranked for nearest-neighbor lookup.
func TestPQDistanceMatchesDirect(t *testing.T) {
	dim := 32
	M := 4
	K := 32
	N := 500
	rng := rand.New(rand.NewSource(13))
	train := make([][]float32, N)
	for i := range train {
		v := make([]float32, dim)
		for j := range v {
			v[j] = float32(rng.NormFloat64())
		}
		train[i] = v
	}
	cb, err := TrainPQCodebook(train, dim, M, K, 25, rng)
	if err != nil {
		t.Fatalf("train: %v", err)
	}
	query := train[0]
	table, err := cb.QueryTable(query)
	if err != nil {
		t.Fatalf("query table: %v", err)
	}
	// Score 50 candidates by both PQ distance and direct L2.
	type scored struct {
		idx      int
		pqDist   float32
		directSq float64
	}
	out := make([]scored, 0, 50)
	for i := 0; i < 50; i++ {
		code, err := cb.Encode(train[i])
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, scored{
			idx:      i,
			pqDist:   PQDistance(table, code, M, K),
			directSq: sqDist(query, train[i]),
		})
	}
	// Spearman rank check: top-5 by direct distance and by PQ distance
	// should overlap heavily. With ~50% recall PQ is unusably bad for
	// retrieval — we want at least 3/5 in common.
	directTop5 := topK(out, func(a, b scored) bool { return a.directSq < b.directSq }, 5)
	pqTop5 := topK(out, func(a, b scored) bool { return a.pqDist < b.pqDist }, 5)
	overlap := 0
	for _, a := range directTop5 {
		for _, b := range pqTop5 {
			if a.idx == b.idx {
				overlap++
			}
		}
	}
	if overlap < 3 {
		t.Errorf("PQ vs direct top-5 overlap = %d/5 (too low)", overlap)
	}
	// query against itself should have ~0 PQ distance.
	selfCode, _ := cb.Encode(query)
	selfPQ := PQDistance(table, selfCode, M, K)
	maxObserved := float32(0)
	for _, o := range out {
		if o.pqDist > maxObserved {
			maxObserved = o.pqDist
		}
	}
	if selfPQ >= maxObserved*0.5 {
		t.Errorf("self-PQ-distance (%v) should be much smaller than the max observed (%v)", selfPQ, maxObserved)
	}
	// Sanity on table size.
	if len(table) != M*K {
		t.Errorf("table size: want %d, got %d", M*K, len(table))
	}
	if math.IsNaN(float64(selfPQ)) {
		t.Errorf("self PQ is NaN")
	}
}

// TestPQTrainRejectsBadInputs.
func TestPQTrainRejectsBadInputs(t *testing.T) {
	cases := []struct {
		name string
		dim  int
		M    int
		K    int
		data [][]float32
	}{
		{"empty", 4, 2, 2, nil},
		{"dim 0", 0, 2, 2, [][]float32{{1, 2}}},
		{"M 0", 4, 0, 2, [][]float32{{1, 2, 3, 4}}},
		{"dim not div by M", 5, 2, 2, [][]float32{{1, 2, 3, 4, 5}}},
		{"K too large", 4, 2, 100000, [][]float32{{1, 2, 3, 4}}},
	}
	for _, c := range cases {
		if _, err := TrainPQCodebook(c.data, c.dim, c.M, c.K, 5, nil); err == nil {
			t.Errorf("%s: expected error, got nil", c.name)
		}
	}
}

// topK is a tiny generic max-heap-style helper.
func topK[T any](xs []T, less func(a, b T) bool, k int) []T {
	out := make([]T, 0, k)
	for _, x := range xs {
		inserted := false
		for i := range out {
			if less(x, out[i]) {
				out = append(out[:i], append([]T{x}, out[i:]...)...)
				inserted = true
				break
			}
		}
		if !inserted {
			out = append(out, x)
		}
		if len(out) > k {
			out = out[:k]
		}
	}
	return out
}
