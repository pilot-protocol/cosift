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
	// sum over lists of 1/(k+rank+1). A URL appearing first in list A and
	// last in list B should outscore a URL appearing only in the middle
	// of list A.
	listA := []index.Hit{
		{URL: "https://a", Title: "A"},
		{URL: "https://b", Title: "B"},
		{URL: "https://c", Title: "C"},
	}
	listB := []index.Hit{
		{URL: "https://b", Title: "B"},
		{URL: "https://a", Title: "A"},
	}
	got := rrfFuse([][]index.Hit{listA, listB}, 60)

	// a should rank first (first in A, second in B).
	if len(got) == 0 || got[0].URL != "https://a" {
		t.Fatalf("rrfFuse: expected https://a as top hit, got %+v", got)
	}
	// b should be second (second in A, first in B).
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
