package main

// Iter 353: unit tests for the small pure helpers introduced during the
// path-2 rework. These functions are called from inside the HTTP handler
// and CLI consumer paths; the E2E test exercises them transitively, but
// behavior contracts are clearer when locked down at the function level.

import (
	"testing"

	"github.com/calinteodor/cosift/internal/index"
	"github.com/calinteodor/cosift/internal/server"
)

func TestNormalizeExpandMode(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"", ""},
		{"true", "hyde"},
		{"hyde", "hyde"},
		{"paraphrase", "paraphrase"},
		{"HYDE", ""},     // case-sensitive
		{"false", ""},    // not a recognized strategy
		{"unknown", ""},  // typo
		{"hybrid", ""},   // SQLite-side retriever value, not an expansion
	}
	for _, c := range cases {
		got := normalizeExpandMode(c.in)
		if got != c.want {
			t.Errorf("normalizeExpandMode(%q): want %q, got %q", c.in, c.want, got)
		}
	}
}

func TestSourceIDOf(t *testing.T) {
	cases := []struct {
		name string
		i    int
		src  server.AnswerSource
		want int
	}{
		{"explicit id preserved", 0, server.AnswerSource{ID: 7}, 7},
		{"explicit id beats position", 4, server.AnswerSource{ID: 2}, 2},
		{"zero id falls back to position+1", 0, server.AnswerSource{}, 1},
		{"zero id at index 2 → 3", 2, server.AnswerSource{}, 3},
	}
	for _, c := range cases {
		got := sourceIDOf(c.i, c.src)
		if got != c.want {
			t.Errorf("sourceIDOf(%d, %+v): want %d, got %d", c.i, c.src, c.want, got)
		}
	}
}

func TestRrfFuse(t *testing.T) {
	// Iter 354: lock down rrfFuse contract (iter 272). RRF score per URL =
	// sum over lists of 1/(k+rank+1). Construct a fixture with no ties so
	// the ranking is deterministic — sort.Slice doesn't guarantee a stable
	// order for equal-keyed elements.
	listA := []index.Hit{
		{URL: "https://a", Title: "A"},
		{URL: "https://b", Title: "B"},
		{URL: "https://c", Title: "C"},
	}
	listB := []index.Hit{
		{URL: "https://a", Title: "A"},
		{URL: "https://b", Title: "B"},
	}
	got := rrfFuse([][]index.Hit{listA, listB}, 60)

	// a should rank first (rank 0 in both lists).
	if len(got) == 0 || got[0].URL != "https://a" {
		t.Fatalf("rrfFuse: expected https://a as top hit, got %+v", got)
	}
	// b should be second (rank 1 in both lists).
	if len(got) < 2 || got[1].URL != "https://b" {
		t.Errorf("rrfFuse: expected https://b as second, got %+v", got)
	}
	// c should be third (only appears in A at rank 2).
	if len(got) < 3 || got[2].URL != "https://c" {
		t.Errorf("rrfFuse: expected https://c as third, got %+v", got)
	}
	// Score is set to the fused RRF score, not the original Hit.Score.
	if got[0].Score <= 0 {
		t.Errorf("rrfFuse: top hit should have positive fused score, got %v", got[0].Score)
	}
	// Empty input → empty output.
	if out := rrfFuse(nil, 60); len(out) != 0 {
		t.Errorf("rrfFuse(nil): want empty, got %+v", out)
	}
	// fuseK<=0 falls back to default (60). Doesn't crash; same ranking.
	got0 := rrfFuse([][]index.Hit{listA, listB}, 0)
	if len(got0) == 0 || got0[0].URL != "https://a" {
		t.Errorf("rrfFuse(fuseK=0): expected https://a top, got %+v", got0)
	}

	// Iter 378: hybrid-fallback near-miss. /search?retriever=hybrid runs
	// BM25 and dense; if one returns an empty list (e.g. graph loaded but
	// query embeds to a vec with no neighbors), rrfFuse must preserve the
	// other list's ordering — not crash, not drop everything.
	emptyDense := [][]index.Hit{listA, {}}
	gotEmpty := rrfFuse(emptyDense, 60)
	if len(gotEmpty) != 3 || gotEmpty[0].URL != "https://a" || gotEmpty[1].URL != "https://b" || gotEmpty[2].URL != "https://c" {
		t.Errorf("rrfFuse with one empty list should preserve other's ranking, got %+v", gotEmpty)
	}
	// And the dual: empty BM25, dense intact (the URL-mode dense case).
	emptyBM := [][]index.Hit{{}, listB}
	gotEmpty2 := rrfFuse(emptyBM, 60)
	if len(gotEmpty2) != 2 || gotEmpty2[0].URL != "https://a" || gotEmpty2[1].URL != "https://b" {
		t.Errorf("rrfFuse with empty BM25 list should preserve dense ranking, got %+v", gotEmpty2)
	}

	// Single-list input: behaves as a pass-through of the input order. Same
	// invariant the iter-373 hybrid fallback relies on when only one
	// retriever fires.
	single := rrfFuse([][]index.Hit{listA}, 60)
	if len(single) != 3 || single[0].URL != "https://a" || single[2].URL != "https://c" {
		t.Errorf("rrfFuse single list should preserve order, got %+v", single)
	}
}

func TestParseSubQueries(t *testing.T) {
	// Iter 355: lock down the planner-output parser (iter 243). The chat
	// client returns a JSON array, sometimes wrapped in markdown fences,
	// sometimes prefixed by chatty prose. Falls back to [fallback] when
	// the array can't be located.
	cases := []struct {
		name, raw, fallback string
		want                []string
	}{
		{"bare array", `["a","b"]`, "fb", []string{"a", "b"}},
		{"fenced json", "```json\n[\"a\",\"b\"]\n```", "fb", []string{"a", "b"}},
		{"fenced plain", "```\n[\"a\"]\n```", "fb", []string{"a"}},
		{"chatty prefix", `Sure! Here is the plan: ["a","b","c"]`, "fb", []string{"a", "b", "c"}},
		{"trailing whitespace", `["a"]   `, "fb", []string{"a"}},
		{"empty raw → fallback", ``, "fb-empty", []string{"fb-empty"}},
		{"no array → fallback", `not a json array`, "fb-missing", []string{"fb-missing"}},
		{"malformed array → fallback", `[unclosed`, "fb-bad", []string{"fb-bad"}},
	}
	for _, c := range cases {
		got := parseSubQueries(c.raw, c.fallback)
		if len(got) != len(c.want) {
			t.Errorf("%s: want %d items, got %d (%v)", c.name, len(c.want), len(got), got)
			continue
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Errorf("%s: item %d: want %q, got %q", c.name, i, c.want[i], got[i])
			}
		}
	}
}

func TestPeekWarnings(t *testing.T) {
	cases := []struct {
		name string
		body string
		want []string
	}{
		{"no field", `{"hits":[]}`, nil},
		{"empty array", `{"warnings":[]}`, nil},
		{"single warning", `{"warnings":["expand=foo unknown"]}`, []string{"expand=foo unknown"}},
		{"two warnings", `{"warnings":["a","b"]}`, []string{"a", "b"}},
		{"malformed JSON tolerated", `not json`, nil},
	}
	for _, c := range cases {
		got := peekWarnings([]byte(c.body))
		if len(got) != len(c.want) {
			t.Errorf("%s: want %d warnings, got %d (%v)", c.name, len(c.want), len(got), got)
			continue
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Errorf("%s: warning %d: want %q, got %q", c.name, i, c.want[i], got[i])
			}
		}
	}
}
