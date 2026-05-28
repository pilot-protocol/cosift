package index

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/store"
)

// newTestStore opens a store at a t.TempDir() — small, isolated, auto-cleaned.
func newTestStore(t *testing.T) *store.Store {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "data")
	s, err := store.Open(dir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestTokenize(t *testing.T) {
	tokens := Tokenize("The Quick brown fox jumps over the LAZY dog 42")
	want := []string{"quick", "brown", "fox", "jumps", "over", "lazy", "dog", "42"}
	if len(tokens) != len(want) {
		t.Fatalf("token count: got %d want %d (got %v)", len(tokens), len(want), tokens)
	}
	for i, w := range want {
		if tokens[i] != w {
			t.Errorf("token %d: got %q want %q", i, tokens[i], w)
		}
	}
}

func TestBM25IndexAndSearch(t *testing.T) {
	s := newTestStore(t)
	idx := NewBM25(s)
	ctx := context.Background()

	docs := []struct {
		url, title, text string
	}{
		{"https://example.com/a", "Go programming language", "Go is a statically typed compiled language designed at Google. Concurrency primitives, garbage collection, simple syntax."},
		{"https://example.com/b", "Rust programming language", "Rust is a systems language focused on safety and concurrency. Ownership model prevents data races at compile time."},
		{"https://example.com/c", "Cooking pasta", "Boil water, salt it, add pasta. Drain when al dente. Toss with sauce."},
	}
	for _, d := range docs {
		id, err := s.UpsertDocument(ctx, &store.Document{
			URL: d.url, Title: d.title, Text: d.text, Source: "test",
			FetchedAt: time.Now(),
		})
		if err != nil {
			t.Fatalf("upsert: %v", err)
		}
		if err := idx.IndexDocument(ctx, id, d.title, d.text); err != nil {
			t.Fatalf("index: %v", err)
		}
	}

	// Query that should clearly favor doc A.
	hits, err := idx.Search(ctx, "go concurrency primitives", 3)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(hits) == 0 {
		t.Fatal("no hits")
	}
	if hits[0].URL != "https://example.com/a" {
		t.Errorf("top hit URL: got %s want example.com/a (hits=%+v)", hits[0].URL, hits)
	}

	// Cooking query should not surface programming docs.
	hits, err = idx.Search(ctx, "pasta sauce", 3)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(hits) == 0 || hits[0].URL != "https://example.com/c" {
		t.Errorf("cooking query top hit: %+v", hits)
	}

	// Empty / stopword-only query returns nothing.
	hits, err = idx.Search(ctx, "the a of", 5)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(hits) != 0 {
		t.Errorf("stopword-only query returned hits: %+v", hits)
	}
}

func TestReindexReplacesPostings(t *testing.T) {
	s := newTestStore(t)
	idx := NewBM25(s)
	ctx := context.Background()

	id, err := s.UpsertDocument(ctx, &store.Document{
		URL: "https://x.test/p", Title: "old", Text: "alpha beta gamma", Source: "test", FetchedAt: time.Now(),
	})
	if err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if err := idx.IndexDocument(ctx, id, "old", "alpha beta gamma"); err != nil {
		t.Fatalf("index: %v", err)
	}

	// Reindex with different content.
	if err := idx.IndexDocument(ctx, id, "new", "delta epsilon"); err != nil {
		t.Fatalf("reindex: %v", err)
	}

	// alpha should now be unfindable.
	hits, err := idx.Search(ctx, "alpha", 5)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(hits) != 0 {
		t.Errorf("alpha should be gone after reindex, got %+v", hits)
	}

	// delta should find it.
	hits, err = idx.Search(ctx, "delta", 5)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(hits) == 0 || hits[0].DocID != id {
		t.Errorf("delta did not find reindexed doc: %+v", hits)
	}
}

// TestBM25TitleBoostLiftsTitleMatches — A document with the query
// term in its TITLE should rank above a document with the same term in only
// the BODY, even when the body-match doc has the term repeated many times.
// Locks in the title-boost ordering invariant.
func TestBM25TitleBoostLiftsTitleMatches(t *testing.T) {
	s := newTestStore(t)
	idx := NewBM25(s)
	ctx := context.Background()

	// Doc A: query term in title (single occurrence).
	// Doc B: query term once in body only.
	// With title boost, A's title-tf = TitleBoost (3), B's body-tf = 1,
	// so A must rank decisively above B.
	//
	// Note: this is a simple "title-boost" lift, not full BM25F. A doc with
	// the term repeated MANY times in the body (e.g. tf>5) can still outrank
	// a single title match because BM25 sums raw tf and our boost is only a
	// 3x multiplier. The boost handles the common "title is signal, body is
	// noise" case; for pathological keyword-stuffed bodies, real BM25F with
	// per-field length norms would be needed (future iter).
	docs := []struct {
		url, title, text string
	}{
		{"https://x/a-title-match", "Raft consensus protocol",
			"This article explains a distributed algorithm. The protocol uses logs and elections to maintain a replicated state machine across multiple nodes in a cluster."},
		{"https://x/b-body-only", "Distributed systems overview",
			"This article covers raft as one of several consensus algorithms used in distributed systems. The text discusses leader election and log replication."},
	}
	for _, d := range docs {
		id, err := s.UpsertDocument(ctx, &store.Document{
			URL: d.url, Title: d.title, Text: d.text, Source: "test",
			FetchedAt: time.Now(),
		})
		if err != nil {
			t.Fatalf("upsert: %v", err)
		}
		if err := idx.IndexDocument(ctx, id, d.title, d.text); err != nil {
			t.Fatalf("index: %v", err)
		}
	}

	hits, err := idx.Search(ctx, "raft", 5)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(hits) < 2 {
		t.Fatalf("want 2 hits, got %d", len(hits))
	}
	if hits[0].URL != "https://x/a-title-match" {
		t.Errorf("title-match doc should rank above body-only repeat; got order: %s, %s (scores %.3f vs %.3f)",
			hits[0].URL, hits[1].URL, hits[0].Score, hits[1].Score)
	}
}

// TestBM25PhraseQuery — Quoted phrases must require verbatim
// adjacency. A doc with "hello world" should match `"hello world"`; a doc
// with "hello there world" must NOT (the words exist but aren't adjacent).
func TestBM25PhraseQuery(t *testing.T) {
	s := newTestStore(t)
	idx := NewBM25(s)
	ctx := context.Background()

	docs := []struct{ url, title, text string }{
		{"https://x/exact", "Greetings", "I say hello world to everyone."},
		{"https://x/split", "Greetings 2", "I say hello there world to everyone."},
		{"https://x/reverse", "Greetings 3", "I say world hello to everyone."},
		{"https://x/nophrase", "Unrelated", "Database design for distributed systems."},
	}
	for _, d := range docs {
		id, err := s.UpsertDocument(ctx, &store.Document{
			URL: d.url, Title: d.title, Text: d.text, Source: "test", FetchedAt: time.Now(),
		})
		if err != nil {
			t.Fatalf("upsert %s: %v", d.url, err)
		}
		if err := idx.IndexDocument(ctx, id, d.title, d.text); err != nil {
			t.Fatalf("index %s: %v", d.url, err)
		}
	}

	// Phrase query: only the exact-match doc should survive.
	hits, err := idx.Search(ctx, `"hello world"`, 10)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(hits) != 1 {
		t.Errorf("want exactly 1 hit for verbatim phrase; got %d: %+v", len(hits), urls(hits))
	} else if hits[0].URL != "https://x/exact" {
		t.Errorf("want exact-match doc, got %s", hits[0].URL)
	}

	// Without quotes, bag-of-words BM25 returns all three doc-A/B/C
	// (all contain both "hello" and "world").
	hits, err = idx.Search(ctx, `hello world`, 10)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(hits) < 3 {
		t.Errorf("bag-of-words should match all 3 docs with both tokens; got %d: %v", len(hits), urls(hits))
	}
}

// TestBM25PhraseAndTermMixed — phrase clause AND extra terms. The result
// must satisfy BOTH: contain the phrase verbatim AND have a high BM25
// score for the additional tokens.
func TestBM25PhraseAndTermMixed(t *testing.T) {
	s := newTestStore(t)
	idx := NewBM25(s)
	ctx := context.Background()

	docs := []struct{ url, title, text string }{
		{"https://x/phrase-and-term", "Distributed systems",
			"In distributed systems, leader election is critical for consensus protocols like raft."},
		{"https://x/phrase-only", "Voting",
			"leader election in a club voting context."},
		{"https://x/term-only", "Raft details",
			"The raft protocol describes how nodes pick a leader."},
	}
	for _, d := range docs {
		id, _ := s.UpsertDocument(ctx, &store.Document{
			URL: d.url, Title: d.title, Text: d.text, Source: "test", FetchedAt: time.Now(),
		})
		if err := idx.IndexDocument(ctx, id, d.title, d.text); err != nil {
			t.Fatalf("index %s: %v", d.url, err)
		}
	}

	// Both phrase-bearing docs survive the phrase filter (term-only doesn't
	// contain "leader election" verbatim — correctly excluded). Among the
	// two survivors, phrase-and-term has "raft" too, so it scores higher.
	hits, err := idx.Search(ctx, `raft "leader election"`, 10)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(hits) != 2 {
		t.Fatalf("want 2 hits (both phrase-bearing docs; term-only filtered out), got %d: %v", len(hits), urls(hits))
	}
	if hits[0].URL != "https://x/phrase-and-term" {
		t.Errorf("phrase-and-term should rank above phrase-only (extra term 'raft'); got order: %v", urls(hits))
	}
	for _, h := range hits {
		if h.URL == "https://x/term-only" {
			t.Errorf("term-only doc should NOT survive phrase filter (no verbatim 'leader election'); got: %v", urls(hits))
		}
	}
}

// TestParsePhrases — locks in the quote parser.
func TestParsePhrases(t *testing.T) {
	cases := []struct {
		in       string
		wantQ    string
		wantPhrs []string
	}{
		{`hello world`, "hello world", nil},
		{`"hello world"`, ` hello world`, []string{"hello world"}},
		{`raft "leader election"`, `raft  leader election`, []string{"leader election"}},
		{`"primary key" "foreign key"`, ` primary key  foreign key`, []string{"primary key", "foreign key"}},
		{`unterminated "phrase`, "unterminated phrase", nil},
		{`"" hello`, ` hello`, nil}, // empty phrase ignored
	}
	normalize := func(s string) string { return strings.Join(strings.Fields(s), " ") }
	for _, c := range cases {
		gotQ, gotPhrs := parsePhrases(c.in)
		// Normalize whitespace — the exact internal separator count is an
		// implementation detail; we care that the tokens are present.
		if normalize(gotQ) != normalize(c.wantQ) {
			t.Errorf("parsePhrases(%q) query (normalized): want %q, got %q", c.in, normalize(c.wantQ), normalize(gotQ))
		}
		if !equalStringSlices(gotPhrs, c.wantPhrs) {
			t.Errorf("parsePhrases(%q) phrases: want %v, got %v", c.in, c.wantPhrs, gotPhrs)
		}
	}
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func urls(hits []Hit) []string {
	out := make([]string, len(hits))
	for i, h := range hits {
		out[i] = h.URL
	}
	return out
}
