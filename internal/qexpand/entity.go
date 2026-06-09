// Package qexpand performs rule-based query rewrites that target the
// failure modes observed in the 50-question eval — entity-fact lookups
// where the question form ("who created the Linux kernel?") buries the
// biographical content under term-density matches for "linux kernel".
//
// The fix is conceptually simple: strip the question form, keep the
// entity, and add canonical-attribute words ("creator inventor founder
// author"). BM25 then ranks pages that mention those words together
// with the entity above pages that just mention the entity in passing.
//
// Rule-based (not LLM-based) so it adds no latency or cost. The
// alternative would be HyDE / paraphrase via the chat client; both
// already exist and cost a chat call per query. Entity-expansion is
// the cheap layer that fires only when a question pattern is detected
// and reuses BM25 only.
//
// Caller fuses results from the original + expanded queries via RRF.
// Returns nil when no pattern matches — caller falls back to bare BM25.
package qexpand

import (
	"regexp"
	"strings"
)

// RewriteEntity inspects q for entity-question patterns and returns
// alternative phrasings to run through BM25 alongside the original.
// Empty result = no rewrite applied.
//
// Patterns are checked in order; the first match wins. The function is
// idempotent and case-insensitive; the returned strings are
// space-normalized but otherwise verbatim of the input entity span.
func RewriteEntity(q string) []string {
	q = strings.TrimSpace(q)
	if q == "" {
		return nil
	}
	lower := strings.ToLower(q)
	for _, p := range patterns {
		if m := p.re.FindStringSubmatchIndex(lower); m != nil {
			entity := strings.TrimSpace(lower[m[2]:m[3]])
			entity = trimTrailingPunct(entity)
			if entity == "" {
				continue
			}
			out := make([]string, 0, len(p.attrs))
			for _, attr := range p.attrs {
				out = append(out, entity+" "+attr)
			}
			return out
		}
	}
	return nil
}

type pattern struct {
	re    *regexp.Regexp
	attrs []string
}

// patterns are ordered most-specific → least-specific so a "who wrote
// X" query doesn't match the broader "who is X" pattern. The capturing
// group is the entity span; attrs are the keyword tails appended to it.
//
// Calibrated against the 50-question eval's fact-category failures:
// "Who created Linux?", "Who wrote 1984?", "Who invented Bitcoin?",
// "How tall is Mount Everest?", "What is the capital of Australia?",
// "What is the longest river in the world?".
var patterns = []pattern{
	{
		re:    regexp.MustCompile(`^who (?:created|invented|founded|built|developed|designed|made|started) (?:the )?(.+?)\??$`),
		attrs: []string{"creator inventor founder", "history origin", "developer designer"},
	},
	{
		re:    regexp.MustCompile(`^who (?:wrote|authored|composed) (?:the )?(.+?)\??$`),
		attrs: []string{"author writer", "book novel publication"},
	},
	{
		re:    regexp.MustCompile(`^who (?:discovered) (.+?)\??$`),
		attrs: []string{"discoverer", "discovery history"},
	},
	{
		re:    regexp.MustCompile(`^who is (?:the )?(.+?)\??$`),
		attrs: []string{"biography", "about"},
	},
	{
		re:    regexp.MustCompile(`^when (?:was|did|were) (?:the )?(.+?)(?: (?:created|invented|founded|born|happen|happened|occur|land|landed))?\??$`),
		attrs: []string{"date year", "history origin"},
	},
	{
		re:    regexp.MustCompile(`^where is (?:the )?(.+?)\??$`),
		attrs: []string{"location address country"},
	},
	{
		re:    regexp.MustCompile(`^how tall is (?:the )?(.+?)\??$`),
		attrs: []string{"height meters feet", "tall"},
	},
	{
		re:    regexp.MustCompile(`^how (?:big|large|long|wide|deep|high) is (?:the )?(.+?)\??$`),
		attrs: []string{"size dimensions", "length width"},
	},
	{
		re:    regexp.MustCompile(`^what is the capital of (.+?)\??$`),
		attrs: []string{"capital city"},
	},
	{
		re:    regexp.MustCompile(`^what is the (?:largest|biggest|longest|tallest|shortest) (.+?)\??$`),
		attrs: []string{"superlative record"},
	},
	{
		re:    regexp.MustCompile(`^what is the population of (.+?)\??$`),
		attrs: []string{"population inhabitants census"},
	},
}

// trimTrailingPunct strips a single trailing '?', '.', '!' or whitespace
// — patterns generally allow optional '?' but we keep this as a safety
// net for entities the regex couldn't tokenize cleanly.
func trimTrailingPunct(s string) string {
	s = strings.TrimRight(s, " \t.!?")
	return s
}
