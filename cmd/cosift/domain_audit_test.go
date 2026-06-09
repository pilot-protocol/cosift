package main

import "testing"

// TestClassifyThresholds locks in the production-tuned thresholds used
// to recommend per-host action on the GH200 corpus. Specifically:
//
//   - A suspect-TLD-only signal (.cfd / .sbs at score 0.2) must
//     classify as "block" — without this the threshold lands on the
//     wrong side and 6.6M spam hosts stay surfaced under "down-weight."
//   - The hard floor between "down-weight" and "block" is 0.25, set
//     deliberately above the 0.2 suspect-TLD baseline so legitimate
//     borderline sites can still be tuned.
func TestClassifyThresholds(t *testing.T) {
	cases := []struct {
		score float64
		want  string
	}{
		{0.95, "trusted"},
		{0.80, "trusted"},
		{0.75, "trusted"},
		{0.74, "keep"},
		{0.50, "keep"},
		{0.40, "keep"},
		{0.39, "down-weight"},
		{0.30, "down-weight"},
		{0.25, "down-weight"},
		{0.24, "block"},
		{0.20, "block"}, // suspect-TLD-only
		{0.00, "block"},
	}
	for _, c := range cases {
		if got := classify(c.score); got != c.want {
			t.Errorf("classify(%.2f) = %q, want %q", c.score, got, c.want)
		}
	}
}
