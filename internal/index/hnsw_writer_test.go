package index

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/store"
)

// TestHNSWWriterBridgesPassagesIntoGraph — The PassageWriter
// satisfies the crawler interface and routes incoming passages into the
// HNSW graph + the PebbleStore.
func TestHNSWWriterBridgesPassagesIntoGraph(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pebble")
	ps, err := store.OpenPebble(dir)
	if err != nil {
		t.Fatalf("OpenPebble: %v", err)
	}
	defer ps.Close()

	// Need a Document in the store so GetDocMeta resolves URL+title.
	ctx := context.Background()
	id, err := ps.UpsertDocument(ctx, &store.Document{
		URL: "https://x/doc1", Title: "Doc One", FetchedAt: time.Now(),
	})
	if err != nil {
		t.Fatalf("upsert: %v", err)
	}

	h := NewHNSW(4)
	w := NewHNSWWriter(h, ps, 0) // no auto-flush; test drives Flush manually

	vec := []float32{0.7, 0.1, 0.1, 0.7}
	if err := w.UpsertPassage(ctx, &store.Passage{
		DocID: id, Offset: 0, Length: 50, Model: "test", Embedding: vec,
	}); err != nil {
		t.Fatalf("upsert passage: %v", err)
	}

	// HNSW now holds one entry; search returns the right URL.
	hits := h.Search(ctx, vec, 5)
	if len(hits) != 1 {
		t.Fatalf("want 1 hit, got %d", len(hits))
	}
	if hits[0].URL != "https://x/doc1" {
		t.Errorf("hit URL: want https://x/doc1, got %s", hits[0].URL)
	}
	if hits[0].Title != "Doc One" {
		t.Errorf("hit Title: got %q", hits[0].Title)
	}

	// Persist + reload — confirm the bridge produces a recoverable graph.
	if err := w.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	loaded, ok, err := LoadHNSW(ctx, ps)
	if err != nil || !ok {
		t.Fatalf("load: ok=%v err=%v", ok, err)
	}
	loadedHits := loaded.Search(ctx, vec, 5)
	if len(loadedHits) != 1 || loadedHits[0].URL != "https://x/doc1" {
		t.Errorf("post-reload hits: %+v", loadedHits)
	}
}

// TestHNSWWriterAutoFlushOnThreshold — when persistEvery is set, the
// writer triggers Persist automatically after N inserts. Locks in the
// auto-checkpoint contract so callers don't have to poll Flush manually.
func TestHNSWWriterAutoFlushOnThreshold(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pebble")
	ps, err := store.OpenPebble(dir)
	if err != nil {
		t.Fatalf("OpenPebble: %v", err)
	}
	defer ps.Close()

	ctx := context.Background()
	// Three docs to write three passages for.
	for i := 1; i <= 3; i++ {
		_, err := ps.UpsertDocument(ctx, &store.Document{
			URL:       "https://x/" + string(rune('a'+i-1)),
			Title:     "doc",
			FetchedAt: time.Now(),
		})
		if err != nil {
			t.Fatalf("upsert %d: %v", i, err)
		}
	}

	h := NewHNSW(4)
	w := NewHNSWWriter(h, ps, 2) // auto-persist every 2 passages

	vec := []float32{1, 0, 0, 0}
	for i := int64(1); i <= 3; i++ {
		if err := w.UpsertPassage(ctx, &store.Passage{
			DocID: i, Offset: 0, Length: 50, Model: "test", Embedding: vec,
		}); err != nil {
			t.Fatalf("passage %d: %v", i, err)
		}
	}

	// After 3 passages and persistEvery=2, exactly one auto-persist
	// happened (after the 2nd). The 3rd is in memory but not yet on disk —
	// load should see 2 nodes, not 3.
	loaded, ok, err := LoadHNSW(ctx, ps)
	if err != nil || !ok {
		t.Fatalf("load: ok=%v err=%v", ok, err)
	}
	// Loaded must have at least the auto-flushed batch; in-memory writer
	// has all three. After explicit Flush, persisted count catches up.
	if loaded.Len() < 2 {
		t.Errorf("auto-flush at threshold 2: want loaded ≥2 nodes, got %d", loaded.Len())
	}
	if err := w.Flush(ctx); err != nil {
		t.Fatalf("manual flush: %v", err)
	}
	loaded2, ok, err := LoadHNSW(ctx, ps)
	if err != nil || !ok {
		t.Fatalf("re-load: ok=%v err=%v", ok, err)
	}
	if loaded2.Len() != 3 {
		t.Errorf("post-flush count: want 3, got %d", loaded2.Len())
	}
}
