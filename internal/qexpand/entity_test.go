package qexpand

import (
	"strings"
	"testing"
)

// TestRewriteEntityCovers50EvalFailures locks in the patterns that map
// each of the failed-fact questions to at least one alternative
// phrasing biased toward biographical / canonical-source content.
func TestRewriteEntityCovers50EvalFailures(t *testing.T) {
	cases := map[string][]string{
		"Who created the Linux kernel?":           {"creator", "history", "developer"},
		"Who created Python?":                     {"creator", "history", "developer"},
		"Who invented Bitcoin?":                   {"creator", "history", "developer"},
		"Who wrote the novel 1984?":               {"author", "book"},
		"How tall is Mount Everest?":              {"height", "tall"},
		"How tall is the Eiffel Tower?":           {"height", "tall"},
		"What is the largest country by area?":    {"superlative"},
		"What is the longest river in the world?": {"superlative"},
		"What is the capital of Australia?":       {"capital city"},
		"Who is Marie Curie?":                     {"biography"},
		"When did the Apollo 11 mission land?":    {"date", "history"},
		"Where is the Eiffel Tower?":              {"location"},
		"What is the population of Tokyo?":        {"population"},
	}
	for q, expectFragments := range cases {
		got := RewriteEntity(q)
		if len(got) == 0 {
			t.Errorf("%q: no rewrites produced", q)
			continue
		}
		// each fragment should appear in at least one rewrite tail
		joined := strings.ToLower(strings.Join(got, " | "))
		for _, frag := range expectFragments {
			if !strings.Contains(joined, frag) {
				t.Errorf("%q: missing attribute fragment %q in rewrites %v", q, frag, got)
			}
		}
	}
}

// TestRewriteEntityIgnoresNonQuestions verifies the rewriter doesn't
// fire on plain queries like "raft consensus algorithm" or "kubernetes
// statefulset" — those are not question forms and shouldn't be
// rewritten.
func TestRewriteEntityIgnoresNonQuestions(t *testing.T) {
	cases := []string{
		"raft consensus algorithm",
		"kubernetes statefulset persistent volume claim",
		"rust ownership borrow checker",
		"docker multi-stage build",
		"hyperloglog probabilistic counting",
		"",
	}
	for _, q := range cases {
		if got := RewriteEntity(q); len(got) != 0 {
			t.Errorf("%q: should produce no rewrites, got %v", q, got)
		}
	}
}

func TestRewriteEntityEntityExtraction(t *testing.T) {
	cases := map[string]string{
		"Who created the Linux kernel":     "linux kernel",
		"who invented Bitcoin?":            "bitcoin",
		"How tall is the Eiffel Tower":     "eiffel tower",
		"What is the capital of Australia": "australia",
	}
	for q, want := range cases {
		got := RewriteEntity(q)
		if len(got) == 0 {
			t.Fatalf("%q: no rewrites", q)
		}
		if !strings.HasPrefix(got[0], want+" ") {
			t.Errorf("%q: first rewrite %q does not start with entity %q", q, got[0], want)
		}
	}
}
