package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"path/filepath"
	"sort"
	"time"

	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/index"
)

// runBenchPQ samples N vectors from the persisted HNSW graph, runs each as
// a query against (a) brute-force exact top-k and (b) the production HNSW
// search path (which uses PQ when a codebook is loaded), and reports the
// resulting Recall@k + latency. Lets operators measure whether the approx
// search path is still frontier-grade after corpus growth or PQ retraining.
// Iter 427.
func runBenchPQ(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("bench-pq", flag.ExitOnError)
	dir := fs.String("dir", "", "PebbleStore directory (defaults to <cfg.DataDir>/pebble)")
	n := fs.Int("n", 100, "number of sampled queries")
	k := fs.Int("k", 10, "top-K per query")
	seed := fs.Int64("seed", 1, "sample seed (deterministic; bump to vary the sample)")
	noPQ := fs.Bool("no-pq", false, "skip loading PQ codes — bench HNSW graph alone")
	efSearch := fs.Int("ef-search", 0, "override HNSW efSearch (0 = use default/built-in)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *n <= 0 || *k <= 0 {
		return fmt.Errorf("bench-pq: -n and -k must be positive")
	}

	d := *dir
	if d == "" {
		d = filepath.Join(cfg.DataDir, "pebble")
	}
	ps, err := openPebbleOrFriendlyErr(d)
	if err != nil {
		return err
	}
	defer ps.Close()

	t0 := time.Now()
	g, ok, err := index.LoadHNSW(ctx, ps)
	if err != nil {
		return fmt.Errorf("bench-pq: load HNSW: %w", err)
	}
	if !ok {
		return fmt.Errorf("bench-pq: no HNSW graph in %s", d)
	}
	loadDur := time.Since(t0)
	if *efSearch > 0 {
		g.SetEfSearch(*efSearch)
		fmt.Printf("bench-pq: efSearch override = %d\n", *efSearch)
	}

	pqLabel := "off (raw vectors)"
	if !*noPQ {
		if cb, cbOK, _ := index.LoadPQCodebook(ctx, ps); cbOK {
			codes := make([][]uint16, g.Len())
			_ = ps.IteratePQCodes(ctx, func(nodeID uint64, blob []byte) bool {
				if int(nodeID) >= len(codes) {
					return true
				}
				c, err := cb.DecodeCodeBlob(blob)
				if err != nil || len(c) != cb.M {
					return true
				}
				codes[int(nodeID)] = c
				return true
			})
			g.UsePQ(cb, codes)
			pqLabel = fmt.Sprintf("on (M=%d K=%d)", cb.M, cb.K)
		}
	}

	queries := g.SampleVectors(*n, *seed)
	if len(queries) == 0 {
		return fmt.Errorf("bench-pq: HNSW has no valid vectors to sample")
	}
	if len(queries) < *n {
		fmt.Printf("note: only %d valid vectors available; bench will use %d queries\n", len(queries), len(queries))
	}

	fmt.Printf("bench-pq: dir=%s loaded_in=%s pq=%s queries=%d k=%d\n",
		d, loadDur.Round(time.Millisecond), pqLabel, len(queries), *k)

	var (
		bruteTotal  time.Duration
		approxTotal time.Duration
		recalls     = make([]float64, 0, len(queries))
	)
	for i, q := range queries {
		t1 := time.Now()
		gt := g.BruteForceTopK(q, *k)
		bruteTotal += time.Since(t1)

		t2 := time.Now()
		approx := g.Search(ctx, q, *k)
		approxTotal += time.Since(t2)

		recalls = append(recalls, recallAtK(gt, approx))
		if (i+1)%50 == 0 {
			fmt.Printf("  ...%d/%d queries\n", i+1, len(queries))
		}
	}

	mean, p50, p95, stddev := summary(recalls)
	avgBrute := bruteTotal / time.Duration(len(queries))
	avgApprox := approxTotal / time.Duration(len(queries))
	speedup := float64(avgBrute) / float64(avgApprox)

	fmt.Println()
	fmt.Printf("Recall@%d:\n", *k)
	fmt.Printf("  mean    %.4f\n", mean)
	fmt.Printf("  p50     %.4f\n", p50)
	fmt.Printf("  p95     %.4f\n", p95)
	fmt.Printf("  stddev  %.4f\n", stddev)
	fmt.Println()
	fmt.Printf("Latency (avg over %d queries):\n", len(queries))
	fmt.Printf("  brute-force  %s\n", avgBrute.Round(time.Microsecond))
	fmt.Printf("  hnsw+approx  %s\n", avgApprox.Round(time.Microsecond))
	fmt.Printf("  speedup      %.1fx\n", speedup)
	return nil
}

// recallAtK computes |truth ∩ pred| / k by URL match, capped at k.
// Both inputs come from the same doc-level aggregation, so each URL appears
// at most once per side.
func recallAtK(truth, pred []index.VectorHit) float64 {
	if len(truth) == 0 {
		return 0
	}
	denom := len(truth)
	tset := make(map[string]struct{}, len(truth))
	for _, h := range truth {
		tset[h.URL] = struct{}{}
	}
	hit := 0
	for _, h := range pred {
		if _, ok := tset[h.URL]; ok {
			hit++
		}
	}
	return float64(hit) / float64(denom)
}

// summary returns mean, p50, p95, and population stddev of xs.
func summary(xs []float64) (mean, p50, p95, stddev float64) {
	if len(xs) == 0 {
		return
	}
	sum := 0.0
	for _, v := range xs {
		sum += v
	}
	mean = sum / float64(len(xs))
	sq := 0.0
	for _, v := range xs {
		d := v - mean
		sq += d * d
	}
	stddev = math.Sqrt(sq / float64(len(xs)))
	sorted := make([]float64, len(xs))
	copy(sorted, xs)
	sort.Float64s(sorted)
	pick := func(p float64) float64 {
		i := int(float64(len(sorted)) * p)
		if i >= len(sorted) {
			i = len(sorted) - 1
		}
		return sorted[i]
	}
	return mean, pick(0.50), pick(0.95), stddev
}
