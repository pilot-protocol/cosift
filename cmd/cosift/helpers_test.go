package main

// Iter 353: unit tests for the small pure helpers introduced during the
// path-2 rework. These functions are called from inside the HTTP handler
// and CLI consumer paths; the E2E test exercises them transitively, but
// behavior contracts are clearer when locked down at the function level.

import (
	"testing"

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
