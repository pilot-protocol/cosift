package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/index"
	"github.com/pilot-protocol/cosift/internal/store"
)

// Iter 159 — unit tests for selectPRFTerms.

func TestSelectPRFTermsBasic(t *testing.T) {
	// 4 docs about programming languages. "concurrency" appears in 3,
	// "memory" in 2, "syntax" in 1. Query is "language". Expected expansion:
	// concurrency, memory (skip syntax because doc-freq < 2).
	texts := map[string]string{
		"https://x/go":     "concurrency primitives in the language are first-class",
		"https://x/rust":   "memory safety without garbage collection concurrency",
		"https://x/erlang": "concurrency model based on actors and messaging",
		"https://x/c":      "manual memory management and direct hardware access syntax",
	}
	got := selectPRFTerms(texts, "language", 5)
	// Want at least "concurrency" and "memory" in the output, "syntax" excluded.
	want := map[string]bool{"concurrency": true, "memory": true}
	gotSet := make(map[string]bool)
	for _, t := range got {
		gotSet[t] = true
	}
	for term := range want {
		if !gotSet[term] {
			t.Errorf("expected %q in expansion, got %+v", term, got)
		}
	}
	if gotSet["syntax"] {
		t.Errorf("syntax appears in only 1 doc — should be excluded; got %+v", got)
	}
	if gotSet["language"] {
		t.Errorf("query term must be excluded from expansion; got %+v", got)
	}
}

func TestSelectPRFTermsEmpty(t *testing.T) {
	if got := selectPRFTerms(nil, "anything", 5); got != nil {
		t.Errorf("nil texts: expected nil, got %+v", got)
	}
	if got := selectPRFTerms(map[string]string{}, "anything", 5); got != nil {
		t.Errorf("empty texts: expected nil, got %+v", got)
	}
	if got := selectPRFTerms(map[string]string{"u": "alpha beta"}, "alpha", 0); got != nil {
		t.Errorf("n=0: expected nil, got %+v", got)
	}
}

func TestSelectPRFTermsCapsAtN(t *testing.T) {
	// 5 distinct terms (post-stopword), each appearing in 2+ docs.
	texts := map[string]string{
		"a": "concurrency memory syntax compiler runtime",
		"b": "concurrency memory syntax compiler runtime",
		"c": "concurrency memory syntax compiler runtime",
	}
	got := selectPRFTerms(texts, "language", 3)
	if len(got) != 3 {
		t.Errorf("n=3 cap: got %d terms", len(got))
	}
}

func TestSelectPRFTermsDeterministic(t *testing.T) {
	// Equal-count terms should sort alphabetically (iter-137 flake lesson:
	// never rely on map-iteration order for ranking).
	texts := map[string]string{
		"a": "banana apple cherry",
		"b": "banana apple cherry",
	}
	got := selectPRFTerms(texts, "q", 3)
	if len(got) != 3 {
		t.Fatalf("expected 3 terms, got %d", len(got))
	}
	// Sorted by count desc (all 3 have count=2), then term asc.
	want := []string{"apple", "banana", "cherry"}
	gotCopy := make([]string, len(got))
	copy(gotCopy, got)
	if !sort.StringsAreSorted(gotCopy) {
		t.Errorf("equal-count terms should sort alphabetically, got %+v", got)
	}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("rank %d: got %q want %q", i, got[i], w)
		}
	}
}

// Iter 170 — unit tests for applyPRFIfRequested.
// Coverage: param parsing (off / on), retriever gating (bm25 / hybrid / dense),
// hit-count gate (≥3 floor), source-tag-suffix presence.

func newPRFTestServer(t *testing.T) (*Server, *httptest.Server, []string) {
	t.Helper()
	s, err := store.OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	idx := index.NewBM25(s)
	// Corpus engineered to mirror iter 159's recall test: 3 docs share a
	// distinctive vocabulary term "goroutines"; a 4th doc uses only
	// "goroutines" without the query term "concurrency".
	docs := []struct{ url, title, text string }{
		{"https://x/dup1", "Dup1", "concurrency content via goroutines"},
		{"https://x/dup2", "Dup2", "concurrency model with goroutines"},
		{"https://x/dup3", "Dup3", "concurrency-heavy goroutines code"},
		{"https://x/miss", "Miss", "goroutines are lightweight threads"},
	}
	urls := make([]string, len(docs))
	for i, d := range docs {
		id, _ := s.UpsertDocument(context.Background(), &store.Document{
			URL: d.url, Title: d.title, Text: d.text, Source: "test", FetchedAt: time.Now(),
		})
		_ = idx.IndexDocument(context.Background(), id, d.title, d.text)
		urls[i] = d.url
	}
	srv := New(s)
	httpSrv := httptest.NewServer(srv.Handler())
	t.Cleanup(httpSrv.Close)
	return srv, httpSrv, urls
}

func TestApplyPRFGatesByParam(t *testing.T) {
	srv, _, _ := newPRFTestServer(t)
	hits, _, _, _ := srv.runSearch(context.Background(), "bm25", "concurrency", 10)

	// No ?prf — return unchanged, empty suffix.
	r, _ := http.NewRequest("GET", "http://localhost/?", nil)
	out, _, tag := srv.applyPRFIfRequested(context.Background(), r, "bm25", "concurrency", hits, nil, 10)
	if tag != "" {
		t.Errorf("absent prf param: expected empty suffix, got %q", tag)
	}
	if len(out) != len(hits) {
		t.Errorf("absent prf param: expected hits unchanged, got len %d (was %d)", len(out), len(hits))
	}

	// ?prf=false — still off.
	r, _ = http.NewRequest("GET", "http://localhost/?prf=false", nil)
	_, _, tag = srv.applyPRFIfRequested(context.Background(), r, "bm25", "concurrency", hits, nil, 10)
	if tag != "" {
		t.Errorf("prf=false: expected empty suffix, got %q", tag)
	}
}

func TestApplyPRFGatesByRetriever(t *testing.T) {
	srv, _, _ := newPRFTestServer(t)
	hits, _, _, _ := srv.runSearch(context.Background(), "bm25", "concurrency", 10)

	r, _ := http.NewRequest("GET", "http://localhost/?prf=true", nil)

	// Dense retriever: PRF must be a no-op.
	out, _, tag := srv.applyPRFIfRequested(context.Background(), r, "dense", "concurrency", hits, nil, 10)
	if tag != "" {
		t.Errorf("dense retriever: PRF should no-op, got tag %q", tag)
	}
	if len(out) != len(hits) {
		t.Errorf("dense retriever: hits should be unchanged")
	}

	// Empty retriever name: also no-op.
	_, _, tag = srv.applyPRFIfRequested(context.Background(), r, "", "concurrency", hits, nil, 10)
	if tag != "" {
		t.Errorf("empty retriever: PRF should no-op, got %q", tag)
	}
}

func TestApplyPRFGatesByHitCount(t *testing.T) {
	srv, _, _ := newPRFTestServer(t)
	hits, _, _, _ := srv.runSearch(context.Background(), "bm25", "concurrency", 10)

	r, _ := http.NewRequest("GET", "http://localhost/?prf=true", nil)

	// <3 hits: no-op (doc-frequency floor in selectPRFTerms produces no
	// expansion).
	short := hits
	if len(short) > 2 {
		short = short[:2]
	}
	out, _, tag := srv.applyPRFIfRequested(context.Background(), r, "bm25", "concurrency", short, nil, 10)
	if tag != "" {
		t.Errorf("<3 hits: PRF should no-op, got tag %q", tag)
	}
	if len(out) != len(short) {
		t.Errorf("<3 hits: hits should be unchanged")
	}
}

func TestApplyPRFHappyPathBM25(t *testing.T) {
	srv, _, _ := newPRFTestServer(t)
	hits, _, _, _ := srv.runSearch(context.Background(), "bm25", "concurrency", 10)

	r, _ := http.NewRequest("GET", "http://localhost/?prf=true", nil)
	out, _, tag := srv.applyPRFIfRequested(context.Background(), r, "bm25", "concurrency", hits, nil, 10)
	if !strings.HasPrefix(tag, "+prf(") {
		t.Errorf("happy path: expected +prf( suffix, got %q", tag)
	}
	if len(out) == 0 {
		t.Errorf("happy path: expected non-empty hits")
	}
	// Re-search should have surfaced the goroutines-only "miss" doc.
	found := false
	for _, h := range out {
		if h.URL == "https://x/miss" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("PRF expansion should have surfaced miss doc; got %+v", out)
	}
}

// Iter 172 — applyPRFToResearchPassages unit tests.

func TestApplyPRFToResearchPassagesGates(t *testing.T) {
	srv, _, _ := newPRFTestServer(t)

	passages := []researchPassage{
		{url: "https://x/a", title: "A", text: "alpha content"},
		{url: "https://x/b", title: "B", text: "beta content"},
	}

	// No ?prf — return unchanged.
	r, _ := http.NewRequest("GET", "http://localhost/?", nil)
	got, tag := srv.applyPRFToResearchPassages(context.Background(), r, "q", passages)
	if tag != "" {
		t.Errorf("absent prf: expected empty suffix, got %q", tag)
	}
	if len(got) != len(passages) {
		t.Errorf("absent prf: passages should be unchanged")
	}

	// <3 passages: no-op (matches iter-159 doc-freq ≥ 2 floor).
	r, _ = http.NewRequest("GET", "http://localhost/?prf=true", nil)
	got, tag = srv.applyPRFToResearchPassages(context.Background(), r, "q", passages)
	if tag != "" {
		t.Errorf("<3 passages: expected empty suffix, got %q", tag)
	}
}

func TestApplyPRFToResearchPassagesHappyPath(t *testing.T) {
	srv, _, _ := newPRFTestServer(t)

	// Use the same 4-doc corpus from newPRFTestServer: 3 dups + 1 "miss".
	// The post-fusion passages would normally come from gatherResearchPassages;
	// here we hand-build them from the 3 dup docs to match what /research
	// would have produced for query "concurrency".
	passages := []researchPassage{
		{url: "https://x/dup1", title: "Dup1", text: "concurrency content via goroutines"},
		{url: "https://x/dup2", title: "Dup2", text: "concurrency model with goroutines"},
		{url: "https://x/dup3", title: "Dup3", text: "concurrency-heavy goroutines code"},
	}

	r, _ := http.NewRequest("GET", "http://localhost/?prf=true", nil)
	got, tag := srv.applyPRFToResearchPassages(context.Background(), r, "concurrency", passages)
	if !strings.HasPrefix(tag, "+prf(") {
		t.Errorf("happy path: expected +prf( suffix, got %q", tag)
	}
	// PRF should mine "goroutines" from the top-3, do BM25 on "concurrency
	// goroutines", and surface the goroutines-only "miss" doc.
	urls := make(map[string]bool)
	for _, p := range got {
		urls[p.url] = true
	}
	if !urls["https://x/miss"] {
		t.Errorf("PRF augment should surface goroutines-only doc; got URLs %+v", urls)
	}
	// Original passages must remain in the augmented list.
	for _, want := range []string{"https://x/dup1", "https://x/dup2", "https://x/dup3"} {
		if !urls[want] {
			t.Errorf("original passage %q dropped from augmented list", want)
		}
	}
}

// Smoke test: /research?prf=true + streaming variant must accept the param
// without panic. /research requires chat — wire a polyChat stub.
func TestPRFWiringOnResearchSmoke(t *testing.T) {
	srv, _, _ := newPRFTestServer(t)
	chat := &polyChat{
		planReply:  `["concurrency facet"]`,
		synthReply: "synthesized [1]",
	}
	srv = srv.WithChat(chat)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	cases := []string{
		"/research?q=concurrency&strategy=planner&prf=true",
		"/research?q=concurrency&strategy=planner&stream=true&prf=true",
	}
	for _, path := range cases {
		t.Run(path, func(t *testing.T) {
			resp, err := http.Get(httpSrv.URL + path)
			if err != nil {
				t.Fatalf("GET: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != 200 {
				t.Errorf("status %d: expected 200", resp.StatusCode)
			}
		})
	}
}

// Smoke tests: /answer + streaming /answer must accept ?prf=true and return
// 200 without panic. Behavior is tested in iter 159 (algorithm) + the
// applyPRFIfRequested unit tests above. This test catches the wiring
// contract per the iter-169 enumeration discipline.
func TestPRFWiringOnAnswerSmoke(t *testing.T) {
	srv, _, _ := newPRFTestServer(t)
	// /answer requires a chat client; wire a stub that returns a canned answer.
	chat := &polyChat{synthReply: "answered with [1]"}
	srv = srv.WithChat(chat)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	cases := []string{
		"/answer?q=concurrency&k=3&prf=true",
		"/answer?q=concurrency&k=3&stream=true&prf=true",
	}
	for _, path := range cases {
		t.Run(path, func(t *testing.T) {
			resp, err := http.Get(httpSrv.URL + path)
			if err != nil {
				t.Fatalf("GET: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != 200 {
				t.Errorf("status %d: expected 200", resp.StatusCode)
			}
		})
	}
}
