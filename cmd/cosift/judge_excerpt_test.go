package main

import "testing"

// Regression: with no reranker, the judge scored empty excerpts and dropped every candidate.
func TestJudgeExcerpt(t *testing.T) {
	cases := []struct {
		name       string
		rerankText string
		title      string
		excerpt    string
		want       string
	}{
		{"reranker ran", "reranked body", "Title", "retrieval excerpt", "reranked body"},
		{"no reranker falls back", "", "Title", "retrieval excerpt", "Title\nretrieval excerpt"},
		{"no reranker, empty title", "", "", "retrieval excerpt", "\nretrieval excerpt"},
	}
	for _, c := range cases {
		if got := judgeExcerpt(c.rerankText, c.title, c.excerpt); got != c.want {
			t.Errorf("%s: judgeExcerpt(%q, %q, %q) = %q, want %q",
				c.name, c.rerankText, c.title, c.excerpt, got, c.want)
		}
	}
}
