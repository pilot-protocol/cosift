package eval

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// fakeRetriever returns a fixed response per query.
type fakeRetriever struct {
	bestOf map[string][]string
	fail   map[string]error
}

func (f *fakeRetriever) Search(_ context.Context, q string, _ int) ([]string, error) {
	if err, ok := f.fail[q]; ok {
		return nil, err
	}
	return f.bestOf[q], nil
}

func TestLoadQuerySet(t *testing.T) {
	qs := QuerySet{
		Name: "fixture",
		Queries: []Query{
			{Text: "alpha", Relevant: []string{"u1", "u2"}},
		},
	}
	b, _ := json.Marshal(qs)
	dir := t.TempDir()
	p := filepath.Join(dir, "qs.json")
	if err := os.WriteFile(p, b, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := LoadQuerySet(p)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got.Name != "fixture" || len(got.Queries) != 1 || got.Queries[0].Text != "alpha" {
		t.Errorf("bad roundtrip: %+v", got)
	}
}

func TestLoadQuerySetMissingFile(t *testing.T) {
	if _, err := LoadQuerySet(filepath.Join(t.TempDir(), "nope.json")); err == nil {
		t.Errorf("missing file: expected error")
	}
}

func TestLoadQuerySetBadJSON(t *testing.T) {
	p := filepath.Join(t.TempDir(), "bad.json")
	_ = os.WriteFile(p, []byte("{garbage"), 0o644)
	if _, err := LoadQuerySet(p); err == nil {
		t.Errorf("bad JSON: expected parse error")
	}
}

func TestLoadCorpus(t *testing.T) {
	c := Corpus{Docs: []CorpusDoc{{URL: "u", Title: "t", Text: "body"}}}
	b, _ := json.Marshal(c)
	p := filepath.Join(t.TempDir(), "c.json")
	if err := os.WriteFile(p, b, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := LoadCorpus(p)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(got.Docs) != 1 || got.Docs[0].URL != "u" {
		t.Errorf("bad roundtrip: %+v", got)
	}
}

func TestLoadCorpusMissingFile(t *testing.T) {
	if _, err := LoadCorpus(filepath.Join(t.TempDir(), "nope.json")); err == nil {
		t.Errorf("missing file: expected error")
	}
}

func TestLoadCorpusBadJSON(t *testing.T) {
	p := filepath.Join(t.TempDir(), "bad.json")
	_ = os.WriteFile(p, []byte("not json"), 0o644)
	if _, err := LoadCorpus(p); err == nil {
		t.Errorf("bad JSON: expected parse error")
	}
}

func TestRunNoParaphrases(t *testing.T) {
	qs := &QuerySet{
		Name: "basic",
		Queries: []Query{
			{Text: "q1", Relevant: []string{"u1"}},
			{Text: "q2", Relevant: []string{"u2", "u3"}},
		},
	}
	r := &fakeRetriever{bestOf: map[string][]string{
		"q1": {"u1", "noise"},
		"q2": {"u2", "noise", "u3"},
	}}
	sum, err := Run(context.Background(), qs, r)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if sum.NumQueries != 2 || len(sum.PerQuery) != 2 {
		t.Fatalf("bad summary: %+v", sum)
	}
	// q1: u1 at pos 0 → R@1 = 1.0
	if sum.PerQuery[0].Metrics.Recall1 != 1.0 {
		t.Errorf("q1 R@1: got %v want 1.0", sum.PerQuery[0].Metrics.Recall1)
	}
	if sum.Name != "basic" {
		t.Errorf("name: %s", sum.Name)
	}
}

func TestRunRetrieverError(t *testing.T) {
	qs := &QuerySet{
		Queries: []Query{{Text: "q1", Relevant: []string{"u"}}},
	}
	r := &fakeRetriever{fail: map[string]error{"q1": errors.New("boom")}}
	if _, err := Run(context.Background(), qs, r); err == nil {
		t.Errorf("expected error")
	}
}

func TestRunWithParaphrases(t *testing.T) {
	qs := &QuerySet{
		Queries: []Query{
			{
				Text:        "main",
				Paraphrases: []string{"p1", "p2"},
				Relevant:    []string{"target"},
			},
		},
	}
	// Main misses but paraphrase finds target — fusion should rescue.
	r := &fakeRetriever{bestOf: map[string][]string{
		"main": {"noise1", "noise2"},
		"p1":   {"target", "noise3"},
		"p2":   {"noise4", "target"},
	}}
	sum, err := Run(context.Background(), qs, r)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if sum.PerQuery[0].Metrics.Recall10 != 1.0 {
		t.Errorf("paraphrase fusion R@10: got %v want 1.0", sum.PerQuery[0].Metrics.Recall10)
	}
}

func TestRunMainErrorAborts(t *testing.T) {
	qs := &QuerySet{
		Queries: []Query{
			{Text: "main", Paraphrases: []string{"p1"}, Relevant: []string{"x"}},
		},
	}
	r := &fakeRetriever{fail: map[string]error{"main": errors.New("nope")}}
	if _, err := Run(context.Background(), qs, r); err == nil {
		t.Errorf("expected main error to propagate")
	}
}

func TestRunParaphraseErrorIsTolerated(t *testing.T) {
	qs := &QuerySet{
		Queries: []Query{
			{Text: "main", Paraphrases: []string{"p1"}, Relevant: []string{"u"}},
		},
	}
	r := &fakeRetriever{
		bestOf: map[string][]string{"main": {"u"}},
		fail:   map[string]error{"p1": errors.New("transient")},
	}
	sum, err := Run(context.Background(), qs, r)
	if err != nil {
		t.Fatalf("paraphrase error should not abort: %v", err)
	}
	if sum.PerQuery[0].Metrics.Recall10 == 0 {
		t.Errorf("main-only RRF should still hit: %+v", sum.PerQuery[0].Metrics)
	}
}

func TestRRFFuse(t *testing.T) {
	lists := [][]string{
		{"a", "b", "c"},
		{"b", "a", "d"},
	}
	out := rrfFuse(lists, 3, 60)
	if len(out) != 3 {
		t.Fatalf("len: got %d want 3", len(out))
	}
	// Both a and b score 1/61 + 1/62 ≈ 0.0325; their order is implementation
	// dependent because of map iteration. Just assert they're both in top-2.
	top2 := map[string]bool{out[0]: true, out[1]: true}
	if !top2["a"] || !top2["b"] {
		t.Errorf("expected a and b in top-2, got %v", out)
	}
}

func TestRRFFuseDefaultK(t *testing.T) {
	// rrfK<=0 should fall back to 60 internally.
	out := rrfFuse([][]string{{"x", "y"}}, 0, 0)
	if len(out) != 2 {
		t.Errorf("k<=0 should default to len(pairs); got %d", len(out))
	}
}

func TestScore(t *testing.T) {
	m := score([]string{"a", "b", "c"}, []string{"a", "c"})
	if m.Recall1 != 0.5 {
		t.Errorf("R@1: got %v want 0.5", m.Recall1)
	}
	if m.MRR10 != 1.0 {
		t.Errorf("MRR: got %v want 1.0", m.MRR10)
	}
}

func TestPrintTable(t *testing.T) {
	sum := &Summary{
		Name: "set",
		When: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		PerQuery: []PerQuery{
			{Query: "short", Metrics: Metrics{Recall10: 1.0, NDCG10: 1.0}},
			{Query: "this is a fairly long query string that should be truncated when printed in the table", Metrics: Metrics{Recall10: 0.5, NDCG10: 0.25}},
		},
		Mean: Metrics{Recall10: 0.75},
	}
	out := PrintTable(sum)
	if !strings.Contains(out, "MEAN") {
		t.Errorf("output should contain MEAN row, got: %s", out)
	}
	if !strings.Contains(out, "set") {
		t.Errorf("output should contain set name, got: %s", out)
	}
	if !strings.Contains(out, "..") {
		t.Errorf("long query should be truncated with ..")
	}
}

func TestSaveLoadSummaryRoundtrip(t *testing.T) {
	sum := &Summary{
		Name:       "roundtrip",
		When:       time.Now().UTC(),
		NumQueries: 1,
		PerQuery:   []PerQuery{{Query: "q", Got: []string{"a"}, Relevant: []string{"a"}, Metrics: Metrics{Recall1: 1}}},
		Mean:       Metrics{Recall1: 1},
	}
	p := filepath.Join(t.TempDir(), "sum.json")
	if err := SaveSummary(sum, p); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, err := LoadSummary(p)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got.Name != sum.Name || got.NumQueries != 1 {
		t.Errorf("bad roundtrip: %+v", got)
	}
	if got.Mean.Recall1 != 1 {
		t.Errorf("mean lost: %+v", got.Mean)
	}
}

func TestLoadSummaryMissingFile(t *testing.T) {
	if _, err := LoadSummary(filepath.Join(t.TempDir(), "nope.json")); err == nil {
		t.Errorf("missing file: expected error")
	}
}

func TestLoadSummaryBadJSON(t *testing.T) {
	p := filepath.Join(t.TempDir(), "bad.json")
	_ = os.WriteFile(p, []byte("bad"), 0o644)
	if _, err := LoadSummary(p); err == nil {
		t.Errorf("bad JSON: expected error")
	}
}

func TestSaveSummaryWriteError(t *testing.T) {
	// Write into a path whose parent does not exist.
	bad := filepath.Join(t.TempDir(), "missing-dir", "x", "y.json")
	if err := SaveSummary(&Summary{Name: "x"}, bad); err == nil {
		t.Errorf("expected write error to nonexistent dir")
	}
}

func TestDiff(t *testing.T) {
	b := &Summary{Mean: Metrics{Recall1: 0.5, Recall3: 0.6, Recall10: 0.7, MRR10: 0.5, NDCG10: 0.6}}
	c := &Summary{Mean: Metrics{Recall1: 0.7, Recall3: 0.5, Recall10: 0.7, MRR10: 0.6, NDCG10: 0.5}}
	out := Diff(b, c)
	if !strings.Contains(out, "+0.200") {
		t.Errorf("R@1 delta should be +0.200, got: %s", out)
	}
	if !strings.Contains(out, "-0.100") {
		t.Errorf("R@3 delta should be -0.100, got: %s", out)
	}
	if !strings.Contains(out, "baseline") || !strings.Contains(out, "current") {
		t.Errorf("output should have headers, got: %s", out)
	}
}
