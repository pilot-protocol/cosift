package eval

import (
	"math"
	"testing"
)

const eps = 1e-6

func approxEqual(a, b float64) bool {
	return math.Abs(a-b) < eps
}

func TestRecallAtK(t *testing.T) {
	rel := []string{"A", "B", "C"}
	got := []string{"X", "A", "Y", "B", "Z"}

	if r := recallAtK(got, rel, 1); !approxEqual(r, 0) {
		t.Errorf("recall@1: got %v want 0", r)
	}
	if r := recallAtK(got, rel, 2); !approxEqual(r, 1.0/3.0) {
		t.Errorf("recall@2: got %v want 0.333", r)
	}
	if r := recallAtK(got, rel, 4); !approxEqual(r, 2.0/3.0) {
		t.Errorf("recall@4: got %v want 0.667", r)
	}
	if r := recallAtK(got, rel, 10); !approxEqual(r, 2.0/3.0) {
		t.Errorf("recall@10: got %v want 0.667 (only 2 of 3 retrieved)", r)
	}
}

func TestRecallAtKEmpty(t *testing.T) {
	if r := recallAtK([]string{"A"}, []string{}, 5); r != 0 {
		t.Errorf("empty relevant: got %v want 0", r)
	}
	if r := recallAtK([]string{}, []string{"A"}, 5); r != 0 {
		t.Errorf("empty got: got %v want 0", r)
	}
}

func TestMRR(t *testing.T) {
	rel := []string{"A"}
	if m := mrrAtK([]string{"A", "B", "C"}, rel, 10); !approxEqual(m, 1.0) {
		t.Errorf("MRR top-1: got %v want 1.0", m)
	}
	if m := mrrAtK([]string{"X", "A", "B"}, rel, 10); !approxEqual(m, 0.5) {
		t.Errorf("MRR pos 2: got %v want 0.5", m)
	}
	if m := mrrAtK([]string{"X", "Y", "Z"}, rel, 10); !approxEqual(m, 0) {
		t.Errorf("MRR no hit: got %v want 0", m)
	}
}

func TestNDCG(t *testing.T) {
	rel := []string{"A", "B"}

	// Perfect ranking: A, B at top → DCG = 1/log2(2) + 1/log2(3); IDCG same.
	if n := ndcgAtK([]string{"A", "B", "X"}, rel, 10); !approxEqual(n, 1.0) {
		t.Errorf("nDCG perfect: got %v want 1.0", n)
	}

	// Reversed but still both in top-2: nDCG = 1.0 because order within ideal is symmetric for binary relevance? No — order matters.
	// DCG([A,B]) = 1/log2(2) + 1/log2(3) = 1 + 0.6309
	// DCG([B,A]) = same = 1 + 0.6309 (both relevant, same gains)
	// IDCG = 1 + 0.6309
	// So nDCG = 1.0. Good — for binary judgments and 2 relevant at top-2, order doesn't matter.
	if n := ndcgAtK([]string{"B", "A", "X"}, rel, 10); !approxEqual(n, 1.0) {
		t.Errorf("nDCG reversed within top-2: got %v want 1.0", n)
	}

	// Hit at 3 only: DCG = 1/log2(4) = 0.5; IDCG = 1 + 0.6309 = 1.6309
	if n := ndcgAtK([]string{"X", "Y", "A"}, []string{"A"}, 10); !approxEqual(n, 0.5) {
		t.Errorf("nDCG single hit at pos 3: got %v want 0.5", n)
	}
}

func TestMeanOf(t *testing.T) {
	rows := []PerQuery{
		{Metrics: Metrics{Recall10: 1.0, NDCG10: 1.0}},
		{Metrics: Metrics{Recall10: 0.5, NDCG10: 0.25}},
		{Metrics: Metrics{Recall10: 0.0, NDCG10: 0.0}},
	}
	m := meanOf(rows)
	if !approxEqual(m.Recall10, 0.5) {
		t.Errorf("mean recall@10: got %v want 0.5", m.Recall10)
	}
	if !approxEqual(m.NDCG10, 0.4166666) {
		t.Errorf("mean nDCG@10: got %v want ~0.417", m.NDCG10)
	}
}
