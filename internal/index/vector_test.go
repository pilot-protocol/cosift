package index

import (
	"context"
	"math"
	"testing"
)

func TestVectorIndexBasic(t *testing.T) {
	vi := NewVectorIndex(3)
	vi.Add("A", "alpha", []float32{1, 0, 0})
	vi.Add("B", "beta", []float32{0, 1, 0})
	vi.Add("C", "gamma", []float32{0, 0, 1})

	hits := vi.Search(context.Background(), []float32{0.9, 0.1, 0}, 3)
	if len(hits) != 3 {
		t.Fatalf("hits: got %d want 3", len(hits))
	}
	if hits[0].URL != "A" {
		t.Errorf("top hit: got %s want A", hits[0].URL)
	}
	// Cosine of normalized [0.9,0.1,0] and [1,0,0] = 0.9/sqrt(0.82) ≈ 0.994
	if math.Abs(hits[0].Score-0.994) > 0.01 {
		t.Errorf("top score: got %v want ~0.994", hits[0].Score)
	}
}

func TestVectorIndexDimMismatchIgnored(t *testing.T) {
	vi := NewVectorIndex(3)
	vi.Add("A", "ok", []float32{1, 0, 0})
	vi.Add("B", "bad", []float32{1, 0}) // wrong dim — silently dropped
	if vi.Len() != 1 {
		t.Errorf("dim-mismatched add should be skipped: len = %d", vi.Len())
	}
}

func TestVectorIndexK(t *testing.T) {
	vi := NewVectorIndex(2)
	vi.Add("A", "a", []float32{1, 0})
	vi.Add("B", "b", []float32{0, 1})
	hits := vi.Search(context.Background(), []float32{1, 0}, 1)
	if len(hits) != 1 || hits[0].URL != "A" {
		t.Errorf("k=1: %+v", hits)
	}
}

func TestRRFBasic(t *testing.T) {
	// Two retrievers agree on A at top → A wins.
	out := RRF([][]string{
		{"A", "B", "C"},
		{"A", "C", "D"},
	}, 3, 60)
	if out[0] != "A" {
		t.Errorf("top: got %s want A", out[0])
	}
}

func TestRRFDisagreeingTopOnes(t *testing.T) {
	// L1: A,B,C / L2: B,A,C — both A and B see rank-1 + rank-2; should be close.
	out := RRF([][]string{
		{"A", "B", "C"},
		{"B", "A", "C"},
	}, 3, 60)
	if len(out) != 3 {
		t.Fatalf("len out: got %d want 3", len(out))
	}
	// A and B should be tied or near-tied → both above C.
	if out[2] != "C" {
		t.Errorf("expected C at bottom, got %+v", out)
	}
}

func TestRRFK(t *testing.T) {
	out := RRF([][]string{{"A", "B", "C"}}, 2, 60)
	if len(out) != 2 {
		t.Errorf("topK=2: got %d", len(out))
	}
}

func TestVectorIndexDedupesByURL(t *testing.T) {
	// Same URL appears in 3 passages, each pointing in a slightly different
	// direction. Vectors are normalized internally, so we need *direction*
	// differences (not just magnitude) to get distinct cosines.
	vi := NewVectorIndex(3)
	vi.AddPassage("https://example/a", "A", 0, 100, []float32{1, 0.5, 0})    // cos ≈ 0.894
	vi.AddPassage("https://example/a", "A", 100, 100, []float32{1, 0, 0})    // cos = 1.0  ← best
	vi.AddPassage("https://example/a", "A", 200, 100, []float32{1, 0.3, 0})  // cos ≈ 0.958
	vi.AddPassage("https://example/b", "B", 0, 50, []float32{0, 1, 0})

	hits := vi.Search(context.Background(), []float32{1, 0, 0}, 10)
	if len(hits) != 2 {
		t.Fatalf("expected 2 unique URLs after dedup, got %d (hits=%+v)", len(hits), hits)
	}
	if hits[0].URL != "https://example/a" {
		t.Errorf("top URL: got %q want example/a", hits[0].URL)
	}
	if hits[0].Offset != 100 || hits[0].Length != 100 {
		t.Errorf("expected the best passage's span (100,100), got (%d,%d)", hits[0].Offset, hits[0].Length)
	}
}

func TestRRFDownweightsLateRanks(t *testing.T) {
	// X is rank-1 in only one list; Y is rank-3 in both.
	// 1/(60+1) ≈ 0.0164 (X) vs 2/(60+3) ≈ 0.0317 (Y) — Y should win.
	out := RRF([][]string{
		{"X", "B", "Y"},
		{"A", "C", "Y"},
	}, 3, 60)
	if out[0] != "Y" {
		t.Errorf("expected Y first (agreement at rank 3 beats one rank-1), got %+v", out)
	}
}

// Iter 158: MMR re-ranking. Corpus has 3 near-duplicates of the same vector
// and 1 distinct vector. Pure-relevance search picks the 3 near-duplicates
// first; MMR with lambda<1 promotes the distinct one to surface diverse content.
func TestSearchMMRPromotesDiversity(t *testing.T) {
	vi := NewVectorIndex(3)
	// Three near-duplicates aligned with the query (cos ~= 1.0, 0.99, 0.98).
	vi.AddPassage("https://x/dup1", "Dup1", 0, 10, []float32{1, 0, 0})
	vi.AddPassage("https://x/dup2", "Dup2", 0, 10, []float32{0.99, 0.1, 0})
	vi.AddPassage("https://x/dup3", "Dup3", 0, 10, []float32{0.98, 0.2, 0})
	// Distinct orthogonal vector (cos ~= 0 with query, ~= 0 with dups).
	vi.AddPassage("https://x/distinct", "Distinct", 0, 10, []float32{0, 0, 1})

	// Pure relevance (Search): the 3 duplicates dominate top-3, distinct is last.
	rel := vi.Search(context.Background(), []float32{1, 0, 0}, 4)
	if len(rel) != 4 || rel[0].URL != "https://x/dup1" {
		t.Fatalf("relevance baseline: top should be dup1, got %+v", rel)
	}
	if rel[3].URL != "https://x/distinct" {
		t.Errorf("relevance baseline: distinct should be last (low cosine), got %+v", rel)
	}

	// MMR with lambda=0.5: distinct should move up — once dup1 is selected,
	// dup2/dup3 are heavily penalized (high sim to dup1) while distinct (sim
	// ≈ 0 to dup1) wins the relevance/diversity tradeoff.
	mmr := vi.SearchMMR(context.Background(), []float32{1, 0, 0}, 3, 0.5, 0)
	if len(mmr) != 3 {
		t.Fatalf("MMR k=3: got %d hits", len(mmr))
	}
	if mmr[0].URL != "https://x/dup1" {
		t.Errorf("MMR first pick must be top-relevance dup1, got %s", mmr[0].URL)
	}
	// dup1 is at position 0. Position 1 should be distinct (diversity wins).
	if mmr[1].URL != "https://x/distinct" {
		t.Errorf("MMR second pick should be distinct (diversity beats near-dup); got %s", mmr[1].URL)
	}
}

func TestSearchMMRLambdaOneEqualsSearch(t *testing.T) {
	// lambda=1.0 → pure relevance → identical ranking to Search.
	vi := NewVectorIndex(2)
	vi.Add("https://x/a", "A", []float32{1, 0})
	vi.Add("https://x/b", "B", []float32{0.9, 0.1})
	vi.Add("https://x/c", "C", []float32{0.5, 0.5})

	rel := vi.Search(context.Background(), []float32{1, 0}, 3)
	mmr := vi.SearchMMR(context.Background(), []float32{1, 0}, 3, 1.0, 0)
	if len(rel) != len(mmr) {
		t.Fatalf("len mismatch: relevance=%d mmr=%d", len(rel), len(mmr))
	}
	for i := range rel {
		if rel[i].URL != mmr[i].URL {
			t.Errorf("lambda=1.0 must match Search: pos %d got %s want %s", i, mmr[i].URL, rel[i].URL)
		}
	}
}

func TestSearchMMREdgeCases(t *testing.T) {
	vi := NewVectorIndex(2)
	vi.Add("https://x/a", "A", []float32{1, 0})

	// k=0 → empty.
	if got := vi.SearchMMR(context.Background(), []float32{1, 0}, 0, 0.7, 0); len(got) != 0 {
		t.Errorf("k=0 should be empty, got %d", len(got))
	}
	// k > corpus size → return everything.
	if got := vi.SearchMMR(context.Background(), []float32{1, 0}, 100, 0.7, 0); len(got) != 1 {
		t.Errorf("k > corpus: got %d want 1", len(got))
	}
	// lambda out of bounds (negative) → clamps to 0.0 internally; should not panic.
	if got := vi.SearchMMR(context.Background(), []float32{1, 0}, 1, -0.5, 0); len(got) != 1 {
		t.Errorf("negative lambda should clamp + return; got %d", len(got))
	}
	// lambda > 1 → clamps to 1.0; should not panic.
	if got := vi.SearchMMR(context.Background(), []float32{1, 0}, 1, 2.0, 0); len(got) != 1 {
		t.Errorf("lambda > 1 should clamp + return; got %d", len(got))
	}
}

// Iter-135 RRFWeighted tests.

func TestRRFWeightedEqualWeightsMatchesRRF(t *testing.T) {
	// Equal weights of any positive constant must produce the same ranking
	// as standard RRF (the constant cancels in pairwise comparison).
	lists := [][]string{
		{"A", "B", "C"},
		{"B", "A", "D"},
		{"D", "C", "B"},
	}
	plain := RRF(lists, 4, 60)
	weighted := RRFWeighted(lists, []float64{1.0, 1.0, 1.0}, 4, 60)
	if len(plain) != len(weighted) {
		t.Fatalf("len mismatch: plain=%d weighted=%d", len(plain), len(weighted))
	}
	for i, u := range plain {
		if weighted[i] != u {
			t.Errorf("rank %d: plain=%s weighted=%s — equal-weights must match RRF", i, u, weighted[i])
		}
	}
	// And again with weights={2,2,2} — should also match RRF.
	weighted2 := RRFWeighted(lists, []float64{2.0, 2.0, 2.0}, 4, 60)
	for i, u := range plain {
		if weighted2[i] != u {
			t.Errorf("rank %d: any equal-constant weights must match RRF, got %s vs %s", i, u, weighted2[i])
		}
	}
}

func TestRRFWeightedShiftsOrderingByWeight(t *testing.T) {
	// Two retrievers disagree on top-1: L1 picks X, L2 picks Y. Both put the
	// other at rank-2. Standard RRF ties them. With L1 weighted 3× heavier, X
	// must win.
	lists := [][]string{
		{"X", "Y", "Z"}, // retriever 1
		{"Y", "X", "W"}, // retriever 2
	}
	tied := RRF(lists, 4, 60)
	// Sanity: under equal weights, X and Y have identical scores; tie-break is
	// map-iteration-order dependent, so don't assert exact order — just that
	// both are in the top-2.
	top2 := map[string]bool{tied[0]: true, tied[1]: true}
	if !top2["X"] || !top2["Y"] {
		t.Fatalf("equal-weight RRF should put X and Y in top-2, got %+v", tied)
	}

	// Now heavily weight retriever 1 — X must be strictly above Y.
	weighted := RRFWeighted(lists, []float64{3.0, 1.0}, 4, 60)
	xIdx, yIdx := -1, -1
	for i, u := range weighted {
		if u == "X" {
			xIdx = i
		}
		if u == "Y" {
			yIdx = i
		}
	}
	if xIdx < 0 || yIdx < 0 {
		t.Fatalf("X and Y must both be present, got %+v", weighted)
	}
	if xIdx >= yIdx {
		t.Errorf("with weight 3:1 favoring L1's X over L2's Y, X must outrank Y; got X@%d Y@%d (%+v)", xIdx, yIdx, weighted)
	}
}

func TestRRFWeightedNilWeightsFallsBackToEqual(t *testing.T) {
	// Iter 137 fix: avoid tied scores so map-iteration order across two RRF
	// calls doesn't make the comparison flaky. Lists `[[A,B,C],[A,C]]` give:
	//   A: 1/61 + 1/61 ≈ 0.0328
	//   C: 1/63 + 1/62 ≈ 0.0320
	//   B: 1/62        ≈ 0.0161
	// All distinct → both RRF and RRFWeighted produce [A, C, B] deterministically.
	lists := [][]string{{"A", "B", "C"}, {"A", "C"}}
	nilOut := RRFWeighted(lists, nil, 3, 60)
	plain := RRF(lists, 3, 60)
	if len(nilOut) != len(plain) {
		t.Fatalf("len mismatch: nil=%d plain=%d", len(nilOut), len(plain))
	}
	for i := range plain {
		if nilOut[i] != plain[i] {
			t.Errorf("nil weights must match RRF: rank %d got %s want %s", i, nilOut[i], plain[i])
		}
	}
	if plain[0] != "A" || plain[1] != "C" || plain[2] != "B" {
		t.Fatalf("sanity: expected [A,C,B], got %+v", plain)
	}
}

func TestRRFWeightedDefensiveFallback(t *testing.T) {
	// Length mismatch and non-positive weight must both fall back to equal-weight,
	// not silently drop a retriever. The safer-on-conflict default established
	// in iter-126 (-y vs -dry-run) applies here too.
	//
	// Iter 137 fix: use the same flake-proof corpus as
	// TestRRFWeightedNilWeightsFallsBackToEqual (A=0.0328, C=0.0320, B=0.0161
	// — all distinct).
	lists := [][]string{{"A", "B", "C"}, {"A", "C"}}
	plain := RRF(lists, 3, 60)
	if plain[0] != "A" || plain[1] != "C" || plain[2] != "B" {
		t.Fatalf("sanity: expected [A,C,B], got %+v", plain)
	}

	cases := []struct {
		name    string
		weights []float64
	}{
		{"length-mismatch-short", []float64{1.0}},
		{"length-mismatch-long", []float64{1.0, 1.0, 1.0}},
		{"zero-weight", []float64{1.0, 0.0}},
		{"negative-weight", []float64{1.0, -2.0}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := RRFWeighted(lists, tc.weights, 3, 60)
			if len(out) != len(plain) {
				t.Fatalf("%s: len mismatch", tc.name)
			}
			for i := range plain {
				if out[i] != plain[i] {
					t.Errorf("%s: rank %d got %s want %s (must fall back to equal-weight)", tc.name, i, out[i], plain[i])
				}
			}
		})
	}
}
