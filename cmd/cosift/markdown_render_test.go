package main

import (
	"strings"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/server"
)

func TestRenderAnswerMarkdownWithStrategy(t *testing.T) {
	pub := time.Date(2026, 5, 22, 0, 0, 0, 0, time.UTC)
	out := renderAnswerMarkdown(
		"what is bm25",
		"planner",
		[]string{"formula", "vs tf-idf"},
		"BM25 is a ranking function [1].",
		[]server.AnswerSource{
			{ID: 1, URL: "https://x/bm25", Title: "BM25", Domain: "x", PublishedAt: &pub},
		},
	)
	for _, want := range []string{
		"# what is bm25\n\n",
		"> Strategy: `planner` (plan: formula | vs tf-idf)\n\n",
		"BM25 is a ranking function [1].\n",
		"## Sources\n\n",
		"1. [BM25](https://x/bm25) — x, 2026-05-22\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing exact substring %q in render:\n%s", want, out)
		}
	}
}

func TestRenderAnswerMarkdownNoStrategy(t *testing.T) {
	out := renderAnswerMarkdown(
		"what is bm25",
		"",
		nil,
		"BM25 is a ranking function.",
		nil,
	)
	if strings.Contains(out, "> Strategy:") {
		t.Errorf("empty strategy should omit blockquote: %q", out)
	}
	if strings.Contains(out, "## Sources") {
		t.Errorf("empty sources should omit Sources header: %q", out)
	}
}

func TestRenderAnswerMarkdownPartialTrailingMetadata(t *testing.T) {
	// Source with domain only (no published_at) → trailing has just domain.
	// Source with no domain or date → no em-dash trailer.
	out := renderAnswerMarkdown(
		"q", "", nil, "A.",
		[]server.AnswerSource{
			{ID: 1, URL: "https://x/a", Title: "A", Domain: "x"},
			{ID: 2, URL: "https://y/b", Title: "B"},
		},
	)
	if !strings.Contains(out, "1. [A](https://x/a) — x\n") {
		t.Errorf("source 1 should have `— x` trailer (domain only): %q", out)
	}
	if !strings.Contains(out, "2. [B](https://y/b)\n") {
		t.Errorf("source 2 (no metadata) should have no em-dash trailer: %q", out)
	}
	if strings.Contains(out, "2. [B](https://y/b) —") {
		t.Errorf("source 2 should not have an empty `—` separator: %q", out)
	}
}

func TestRenderAnswerMarkdownAnswerLineEnding(t *testing.T) {
	// Answer prose without trailing newline should get one added so the
	// `## Sources` header sits on its own line.
	out := renderAnswerMarkdown("q", "", nil, "Answer text", []server.AnswerSource{
		{ID: 1, URL: "https://x", Title: "T"},
	})
	if !strings.Contains(out, "Answer text\n\n## Sources") {
		t.Errorf("expected newline between answer and Sources header: %q", out)
	}

	// Same with trailing newline already present.
	out2 := renderAnswerMarkdown("q", "", nil, "Answer text\n", []server.AnswerSource{
		{ID: 1, URL: "https://x", Title: "T"},
	})
	if strings.Contains(out2, "\n\n\n## Sources") {
		t.Errorf("trailing newline shouldn't be doubled: %q", out2)
	}
}

func TestRenderRankedMarkdownBasic(t *testing.T) {
	pub := time.Date(2026, 5, 22, 0, 0, 0, 0, time.UTC)
	out := renderRankedMarkdown("Results: bm25", []server.SearchHit{
		{URL: "https://x/a", Title: "BM25 paper", Score: 0.873, Source: "bm25", Domain: "x", PublishedAt: &pub, Excerpt: "BM25 explains ranking."},
		{URL: "https://y/b", Title: "TF-IDF", Score: 0.542, Source: "bm25"},
	})
	for _, want := range []string{
		"# Results: bm25\n\n",
		"## 1. [BM25 paper](https://x/a)\n\n",
		"_Score: 0.873 · x · 2026-05-22_\n",
		"> BM25 explains ranking.\n",
		"## 2. [TF-IDF](https://y/b)\n\n",
		"_Score: 0.542_\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in render:\n%s", want, out)
		}
	}
	// Second hit has no domain/date — metadata line should be score only.
	if strings.Contains(out, "_Score: 0.542 · _") || strings.Contains(out, "_Score: 0.542 ·_") {
		t.Errorf("hit 2 metadata shouldn't have empty separator: %q", out)
	}
}

func TestRenderRankedMarkdownFindSimilarTitle(t *testing.T) {
	// same renderer handles /find_similar with a different title.
	out := renderRankedMarkdown("Similar to: https://x/seed", []server.SearchHit{
		{URL: "https://x/a", Title: "Neighbor A", Score: 0.91, Source: "dense"},
	})
	if !strings.HasPrefix(out, "# Similar to: https://x/seed") {
		t.Errorf("title should be passed through: %q", out)
	}
	if !strings.Contains(out, "## 1. [Neighbor A](https://x/a)") {
		t.Errorf("missing hit: %q", out)
	}
}

func TestRenderRankedMarkdownEmptyHits(t *testing.T) {
	out := renderRankedMarkdown("Results: nothing matches", nil)
	if !strings.Contains(out, "# Results: nothing matches") {
		t.Errorf("missing query heading: %q", out)
	}
	if !strings.Contains(out, "_No results._") {
		t.Errorf("missing empty-results marker: %q", out)
	}
}

func TestRenderRankedMarkdownHighlightWinsOverExcerpt(t *testing.T) {
	// When both highlight and excerpt are present, highlight wins because it's
	// the matched passage (more relevant than the prefix excerpt).
	out := renderRankedMarkdown("Results: q", []server.SearchHit{
		{URL: "https://x/a", Title: "A", Score: 0.5,
			Highlight: &server.Highlight{Text: "the matched span"},
			Excerpt:   "the prefix excerpt"},
	})
	if !strings.Contains(out, "> the matched span") {
		t.Errorf("highlight should win: %q", out)
	}
	if strings.Contains(out, "the prefix excerpt") {
		t.Errorf("excerpt should be suppressed when highlight present: %q", out)
	}
}

func TestValidateFormat(t *testing.T) {
	for _, ok := range []string{"text", "md", "markdown"} {
		if err := validateFormat(ok); err != nil {
			t.Errorf("validateFormat(%q) should accept: %v", ok, err)
		}
	}
	for _, bad := range []string{"", "yaml", "json", "MD", "Markdown"} {
		if err := validateFormat(bad); err == nil {
			t.Errorf("validateFormat(%q) should reject", bad)
		}
	}
}
