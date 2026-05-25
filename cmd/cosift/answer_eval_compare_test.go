package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// helper: synthesize a saved report JSON file from a compact spec.
func writeReport(t *testing.T, path, synth, judge, queries string, planner, paraphrase []int) {
	t.Helper()
	type sr struct {
		Strategy     string `json:"strategy"`
		Coverage     int    `json:"coverage"`
		Grounding    int    `json:"grounding"`
		JudgeComment string `json:"judge_comment"`
	}
	type qr struct {
		Query   string `json:"query"`
		Reports []sr   `json:"reports"`
	}
	// planner/paraphrase slices: [cov, grnd, cov, grnd, ...] pairs, one pair per query.
	if len(planner)%2 != 0 || len(paraphrase)%2 != 0 || len(planner) != len(paraphrase) {
		t.Fatalf("bad spec: planner=%d paraphrase=%d (must be even and equal)", len(planner), len(paraphrase))
	}
	n := len(planner) / 2
	reports := make([]qr, n)
	for i := 0; i < n; i++ {
		reports[i] = qr{
			Query: "q" + string(rune('A'+i)),
			Reports: []sr{
				{Strategy: "planner", Coverage: planner[2*i], Grounding: planner[2*i+1]},
				{Strategy: "paraphrase", Coverage: paraphrase[2*i], Grounding: paraphrase[2*i+1]},
			},
		}
	}
	// Compute summary inline so loader doesn't need to recompute.
	type summaryEntry struct {
		N             int     `json:"n"`
		MeanCoverage  float64 `json:"mean_coverage"`
		MeanGrounding float64 `json:"mean_grounding"`
		Combined      float64 `json:"combined"`
	}
	summary := map[string]summaryEntry{}
	for _, strat := range []string{"planner", "paraphrase"} {
		var covSum, grdSum int
		for _, q := range reports {
			for _, sr := range q.Reports {
				if sr.Strategy == strat {
					covSum += sr.Coverage
					grdSum += sr.Grounding
				}
			}
		}
		s := summaryEntry{N: n}
		if n > 0 {
			s.MeanCoverage = float64(covSum) / float64(n)
			s.MeanGrounding = float64(grdSum) / float64(n)
			s.Combined = s.MeanCoverage + s.MeanGrounding
		}
		summary[strat] = s
	}
	doc := map[string]any{
		"synth_model": synth,
		"judge_model": judge,
		"queries":     queries,
		"reports":     reports,
		"summary":     summary,
		"when":        "2026-05-23T00:00:00Z",
	}
	buf, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func TestLoadAnswerEvalReport(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "a.json")
	writeReport(t, p, "synth-a", "judge-a", "queries.json",
		[]int{5, 5, 4, 3}, // planner: q1 cov=5 grnd=5, q2 cov=4 grnd=3
		[]int{3, 4, 5, 5}, // paraphrase: q1 cov=3 grnd=4, q2 cov=5 grnd=5
	)
	r, err := loadAnswerEvalReport(p)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if r.SynthModel != "synth-a" {
		t.Errorf("synth: %q", r.SynthModel)
	}
	p1, ok := r.Summary["planner"]
	if !ok || p1.N != 2 {
		t.Fatalf("planner summary missing or wrong N: %+v", p1)
	}
	wantCov := 4.5 // (5+4)/2
	wantGrd := 4.0 // (5+3)/2
	if p1.MeanCoverage != wantCov || p1.MeanGrounding != wantGrd {
		t.Errorf("planner mean: cov=%.2f grnd=%.2f want %.2f/%.2f", p1.MeanCoverage, p1.MeanGrounding, wantCov, wantGrd)
	}
	if p1.Combined != wantCov+wantGrd {
		t.Errorf("planner combined: %.2f", p1.Combined)
	}
}

// Old reports (iter 56) had an empty {} summary. Loader must reconstruct.
func TestLoadAnswerEvalRecomputeSummary(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "old.json")
	// Write a report manually with no summary block (mimicking iter-56).
	doc := map[string]any{
		"synth_model": "old",
		"judge_model": "old",
		"reports": []map[string]any{
			{
				"query": "q1",
				"reports": []map[string]any{
					{"strategy": "planner", "coverage": 5, "grounding": 4},
					{"strategy": "paraphrase", "coverage": 3, "grounding": 5},
				},
			},
		},
		// no "summary" field
	}
	b, _ := json.Marshal(doc)
	if err := os.WriteFile(p, b, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	r, err := loadAnswerEvalReport(p)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if r.Summary["planner"].N != 1 || r.Summary["planner"].MeanCoverage != 5 {
		t.Errorf("recompute failed: planner=%+v", r.Summary["planner"])
	}
	if r.Summary["paraphrase"].N != 1 || r.Summary["paraphrase"].MeanGrounding != 5 {
		t.Errorf("recompute failed: paraphrase=%+v", r.Summary["paraphrase"])
	}
}

func TestRecomputeSummaryEmptyReports(t *testing.T) {
	r := savedAnswerEvalReport{} // no reports at all
	got := recomputeSummary(r)
	if got["planner"].N != 0 || got["paraphrase"].N != 0 {
		t.Errorf("empty reports should give zero summaries: %+v", got)
	}
	if got["planner"].Combined != 0 {
		t.Errorf("zero summary combined: %v", got["planner"].Combined)
	}
}

func TestAbsHelper(t *testing.T) {
	for _, c := range []struct{ in, want int }{
		{0, 0}, {5, 5}, {-5, 5}, {-1, 1},
	} {
		if got := abs(c.in); got != c.want {
			t.Errorf("abs(%d) = %d want %d", c.in, got, c.want)
		}
	}
}

func TestTruncateHelper(t *testing.T) {
	for _, c := range []struct {
		in     string
		max    int
		want   string
	}{
		{"abc", 10, "abc"},
		{"abcde", 5, "abcde"},
		{"abcdefghij", 5, "abcd…"},
		{"a very long query text that exceeds the cap", 10, "a very lo…"},
	} {
		if got := truncate(c.in, c.max); got != c.want {
			t.Errorf("truncate(%q, %d) = %q want %q", c.in, c.max, got, c.want)
		}
	}
}
