package index

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/calinteodor/cosift/internal/store"
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
