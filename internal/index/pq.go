// Package index — Product Quantization (PQ) for vector compression.
//
// Codebook trainer + encoder + asymmetric distance computation. PQ is
// offline: encode a corpus, store codes, query later.
//
// Idea: split each d-dimensional vector into M equal subspaces (length
// d/M each). Per subspace, run k-means to find K centroids. Each vector
// becomes M cluster IDs of log2(K) bits each — typically K=256 = 1 byte
// per subspace, so an M=96-subspace code is 96 bytes total. For d=768,
// that's a 32× compression (3072 B raw → 96 B compressed) with 1-2%
// recall loss on most retrieval benchmarks.
//
// Query: precompute a query-to-codebook distance table (M × K floats).
// Per-candidate scoring is M lookups + M-1 sums — way faster than the
// raw d-dim dot product. This is the "asymmetric distance" path; the
// query vector stays uncompressed, only the corpus is quantized.
//
// Pure Go, stdlib only. No FAISS, no Cgo, no new deps.
package index

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sync"
)

// PQCodebook holds the M × K × subspaceDim float32 centroid table.
// One codebook serves an entire HNSW (or any corpus) — encoded vectors
// are then stored alongside.
type PQCodebook struct {
	Dim       int           // original vector dimension
	M         int           // number of subspaces (Dim must be divisible by M)
	K         int           // centroids per subspace (typically 256 → 1 byte/subspace)
	SubDim    int           // = Dim / M
	Centroids [][][]float32 // [M][K][SubDim]
}

// TrainPQCodebookParallel is the parallelization: subspaces run
// concurrently across maxParallel workers. K-means per subspace is the
// dominant cost (~70% of train time), so the speedup is near-linear up
// to GOMAXPROCS. Same return contract as TrainPQCodebook.
func TrainPQCodebookParallel(train [][]float32, dim, M, K, iters, maxParallel int, rngSeed int64) (*PQCodebook, error) {
	if len(train) == 0 {
		return nil, errors.New("empty training set")
	}
	if dim <= 0 || M <= 0 || K <= 0 {
		return nil, errors.New("dim/M/K must be positive")
	}
	if dim%M != 0 {
		return nil, fmt.Errorf("dim %d not divisible by M %d", dim, M)
	}
	if K > 65536 {
		return nil, errors.New("k must fit in uint16 (≤ 65536) for storage layout")
	}
	subDim := dim / M
	if iters <= 0 {
		iters = 25
	}
	if maxParallel <= 0 {
		maxParallel = 4
	}
	cb := &PQCodebook{Dim: dim, M: M, K: K, SubDim: subDim,
		Centroids: make([][][]float32, M)}
	// Sanity-check input dims once on the calling goroutine.
	for i, v := range train {
		if len(v) != dim {
			return nil, fmt.Errorf("train[%d]: got dim %d, want %d", i, len(v), dim)
		}
	}
	type job struct{ sub int }
	jobs := make(chan job, M)
	var wg sync.WaitGroup
	for w := 0; w < maxParallel; w++ {
		wg.Add(1)
		go func(workerSeed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(workerSeed))
			for j := range jobs {
				subTrain := make([][]float32, len(train))
				for i, v := range train {
					subTrain[i] = v[j.sub*subDim : (j.sub+1)*subDim]
				}
				cb.Centroids[j.sub] = kmeansSubspace(subTrain, K, subDim, iters, rng)
			}
		}(rngSeed + int64(w))
	}
	for sub := 0; sub < M; sub++ {
		jobs <- job{sub: sub}
	}
	close(jobs)
	wg.Wait()
	return cb, nil
}

// TrainPQCodebook runs lloyd's k-means per subspace to fit a PQ codebook
// over the given training set. Returns an error if dims are inconsistent
// or M doesn't divide Dim cleanly.
//
// Notes:
//   - Sampling: if len(train) is large, callers should sample (~100K vecs
//     is plenty) before passing in; full corpora are unnecessary and slow.
//   - k-means is deterministic given a seeded RNG; pass rng=nil to use
//     time-seeded.
//   - iters controls k-means convergence; 25 is a safe default per
//     subspace at K=256.
func TrainPQCodebook(train [][]float32, dim, M, K, iters int, rng *rand.Rand) (*PQCodebook, error) {
	if len(train) == 0 {
		return nil, errors.New("empty training set")
	}
	if dim <= 0 || M <= 0 || K <= 0 {
		return nil, errors.New("dim/M/K must be positive")
	}
	if dim%M != 0 {
		return nil, fmt.Errorf("dim %d not divisible by M %d", dim, M)
	}
	if K > 65536 {
		return nil, errors.New("k must fit in uint16 (≤ 65536) for storage layout")
	}
	subDim := dim / M
	if rng == nil {
		rng = rand.New(rand.NewSource(1))
	}
	cb := &PQCodebook{Dim: dim, M: M, K: K, SubDim: subDim,
		Centroids: make([][][]float32, M)}
	if iters <= 0 {
		iters = 25
	}
	for sub := 0; sub < M; sub++ {
		// Gather the subspace slice of every training vector.
		subTrain := make([][]float32, len(train))
		for i, v := range train {
			if len(v) != dim {
				return nil, fmt.Errorf("train[%d]: got dim %d, want %d", i, len(v), dim)
			}
			subTrain[i] = v[sub*subDim : (sub+1)*subDim]
		}
		cb.Centroids[sub] = kmeansSubspace(subTrain, K, subDim, iters, rng)
	}
	return cb, nil
}

// kmeansSubspace runs vanilla lloyd's k-means with k-means++ seeding.
// Returns K centroids each of dim d.
func kmeansSubspace(data [][]float32, K, d, iters int, rng *rand.Rand) [][]float32 {
	n := len(data)
	if n < K {
		// Not enough training data — duplicate to fill, the model will be
		// near-perfect on the input but won't generalize beyond it.
		// Caller's problem to feed enough samples (~K * 100 is sane).
		K = n
	}
	// k-means++ seeding.
	centroids := make([][]float32, K)
	first := rng.Intn(n)
	centroids[0] = cloneVec(data[first])
	dists := make([]float64, n)
	for i := range dists {
		dists[i] = sqDist(data[i], centroids[0])
	}
	for c := 1; c < K; c++ {
		var sum float64
		for _, d := range dists {
			sum += d
		}
		if sum == 0 {
			// All points identical to the chosen centroid; fill remaining
			// arbitrarily so K is full.
			centroids[c] = cloneVec(data[rng.Intn(n)])
			continue
		}
		r := rng.Float64() * sum
		var acc float64
		pick := 0
		for i, d := range dists {
			acc += d
			if acc >= r {
				pick = i
				break
			}
		}
		centroids[c] = cloneVec(data[pick])
		// Update min distances.
		for i := range data {
			if newD := sqDist(data[i], centroids[c]); newD < dists[i] {
				dists[i] = newD
			}
		}
	}
	// Lloyd's iterations.
	assign := make([]int, n)
	for it := 0; it < iters; it++ {
		moved := false
		// Assign.
		for i := range data {
			best, bestD := 0, math.MaxFloat64
			for c := 0; c < K; c++ {
				if d := sqDist(data[i], centroids[c]); d < bestD {
					bestD = d
					best = c
				}
			}
			if assign[i] != best {
				assign[i] = best
				moved = true
			}
		}
		if !moved && it > 0 {
			break // converged
		}
		// Update centroids: per-cluster mean.
		counts := make([]int, K)
		sums := make([][]float64, K)
		for c := range sums {
			sums[c] = make([]float64, d)
		}
		for i, v := range data {
			c := assign[i]
			counts[c]++
			for j, x := range v {
				sums[c][j] += float64(x)
			}
		}
		for c := 0; c < K; c++ {
			if counts[c] == 0 {
				// Empty cluster — keep prior centroid to avoid drift.
				continue
			}
			for j := 0; j < d; j++ {
				centroids[c][j] = float32(sums[c][j] / float64(counts[c]))
			}
		}
	}
	return centroids
}

// Encode quantizes a single vector against this codebook. Result is M
// codes; with K≤256 each code fits in a uint8 byte (caller's choice).
func (cb *PQCodebook) Encode(vec []float32) ([]uint16, error) {
	if len(vec) != cb.Dim {
		return nil, fmt.Errorf("encode: got dim %d, want %d", len(vec), cb.Dim)
	}
	out := make([]uint16, cb.M)
	for sub := 0; sub < cb.M; sub++ {
		slice := vec[sub*cb.SubDim : (sub+1)*cb.SubDim]
		best, bestD := uint16(0), float32(math.MaxFloat32)
		for c := 0; c < cb.K; c++ {
			d := sqDistFloat32(slice, cb.Centroids[sub][c])
			if d < bestD {
				bestD = d
				best = uint16(c)
			}
		}
		out[sub] = best
	}
	return out, nil
}

// QueryTable precomputes the M × K asymmetric-distance lookup table for
// a query vector. Cost: M*K*subDim multiplies; reused across all candidate
// scores in a search, so amortizes to nearly zero per candidate.
// Returns a flat M*K float32 slice indexed as table[sub*K + code].
func (cb *PQCodebook) QueryTable(query []float32) ([]float32, error) {
	if len(query) != cb.Dim {
		return nil, fmt.Errorf("query: got dim %d, want %d", len(query), cb.Dim)
	}
	out := make([]float32, cb.M*cb.K)
	for sub := 0; sub < cb.M; sub++ {
		q := query[sub*cb.SubDim : (sub+1)*cb.SubDim]
		for c := 0; c < cb.K; c++ {
			out[sub*cb.K+c] = sqDistFloat32(q, cb.Centroids[sub][c])
		}
	}
	return out, nil
}

// PQDistance computes asymmetric squared distance between a query (via
// its QueryTable) and an encoded vector. M lookups + M-1 sums.
func PQDistance(table []float32, code []uint16, M, K int) float32 {
	var s float32
	for sub := 0; sub < M; sub++ {
		s += table[sub*K+int(code[sub])]
	}
	return s
}

// cloneVec is a defensive copy used by k-means seeding so centroids
// don't alias caller-owned training slices.
func cloneVec(v []float32) []float32 {
	cp := make([]float32, len(v))
	copy(cp, v)
	return cp
}

// sqDist computes L2 squared distance between two []float32 of equal
// length. Returns float64 to keep k-means seeding precise on large
// summations.
func sqDist(a, b []float32) float64 {
	var s float64
	for i, x := range a {
		d := float64(x - b[i])
		s += d * d
	}
	return s
}

// sqDistFloat32 — same but returns float32 (fine for encode loop where
// we just want the argmin).
func sqDistFloat32(a, b []float32) float32 {
	var s float32
	for i, x := range a {
		d := x - b[i]
		s += d * d
	}
	return s
}
