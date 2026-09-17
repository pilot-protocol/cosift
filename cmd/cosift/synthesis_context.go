package main

import (
	"strings"
	"unicode/utf8"
)

// Display snippets are too short to be the only synthesis evidence: important
// instructions often occur after an article's introduction. Bound source text
// to 10 KB each and 30 KB across the selected sources, independent of their
// display excerpts. Retrieval/reranking order and prompt fencing are unchanged.
func synthesisContext(text string, sourceCount int) string {
	limit := min(10000, 30000/max(1, sourceCount))
	if len(text) <= limit {
		return text
	}
	if limit < 3 {
		return ""
	}
	cut := limit - 3
	for cut > 0 && !utf8.RuneStart(text[cut]) {
		cut--
	}
	return strings.TrimSpace(text[:cut]) + "…"
}
