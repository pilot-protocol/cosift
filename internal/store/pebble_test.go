package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func newPebbleStore(t *testing.T) *PebbleStore {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "pebble")
	p, err := OpenPebble(dir)
	if err != nil {
		t.Fatalf("OpenPebble: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

// TestPebbleUpsertAndGet — basic round trip. New URL gets fresh ID,
// repeat upsert reuses the ID, GetDocByURL roundtrips the payload.
func TestPebbleUpsertAndGet(t *testing.T) {
	p := newPebbleStore(t)
	ctx := context.Background()
	d := &Document{
		URL: "https://example.com/a", Domain: "example.com",
		Title: "Hello", Text: "body of a", FetchedAt: time.Now(),
	}
	id1, err := p.UpsertDocument(ctx, d)
	if err != nil {
		t.Fatalf("upsert 1: %v", err)
	}
	if id1 <= 0 {
		t.Errorf("first ID should be positive, got %d", id1)
	}

	got, err := p.GetDocByURL(ctx, "https://example.com/a")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.ID != id1 || got.URL != d.URL || got.Title != "Hello" {
		t.Errorf("round-trip mismatch: %+v", got)
	}

	// Repeat upsert with new content — same ID.
	d2 := &Document{
		URL: "https://example.com/a", Domain: "example.com",
		Title: "Hello v2", Text: "updated", FetchedAt: time.Now(),
	}
	id2, err := p.UpsertDocument(ctx, d2)
	if err != nil {
		t.Fatalf("upsert 2: %v", err)
	}
	if id2 != id1 {
		t.Errorf("repeat upsert should reuse ID: id1=%d id2=%d", id1, id2)
	}
	got2, _ := p.GetDocByURL(ctx, "https://example.com/a")
	if got2.Title != "Hello v2" {
		t.Errorf("title should be updated, got %q", got2.Title)
	}
}

// TestPebbleMultipleDocs — distinct URLs get distinct monotonic IDs.
func TestPebbleMultipleDocs(t *testing.T) {
	p := newPebbleStore(t)
	ctx := context.Background()
	ids := make([]int64, 5)
	for i := range ids {
		d := &Document{
			URL: "https://example.com/" + string(rune('a'+i)),
			Title: "T", Text: "x", FetchedAt: time.Now(),
		}
		id, err := p.UpsertDocument(ctx, d)
		if err != nil {
			t.Fatalf("upsert %d: %v", i, err)
		}
		ids[i] = id
	}
	// IDs must be distinct and monotonic.
	seen := map[int64]bool{}
	for i, id := range ids {
		if seen[id] {
			t.Errorf("duplicate ID at %d: %d", i, id)
		}
		seen[id] = true
		if i > 0 && id <= ids[i-1] {
			t.Errorf("IDs not monotonic at %d: %d <= %d", i, id, ids[i-1])
		}
	}
}

// TestPebbleGetMissing — non-existent URL returns ErrNotFound, not an error.
func TestPebbleGetMissing(t *testing.T) {
	p := newPebbleStore(t)
	_, err := p.GetDocByURL(context.Background(), "https://nonexistent/")
	if err != ErrNotFound {
		t.Errorf("missing URL: want ErrNotFound, got %v", err)
	}
}

// TestPebblePersistenceAcrossOpen — write, close, reopen, read. The
// next-ID counter must survive so subsequent IDs don't collide with
// pre-restart ones.
func TestPebblePersistenceAcrossOpen(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pebble")
	p1, err := OpenPebble(dir)
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}
	ctx := context.Background()
	id1, err := p1.UpsertDocument(ctx, &Document{
		URL: "https://a", Title: "t", FetchedAt: time.Now(),
	})
	if err != nil {
		t.Fatalf("upsert 1: %v", err)
	}
	if err := p1.Close(); err != nil {
		t.Fatalf("close 1: %v", err)
	}

	p2, err := OpenPebble(dir)
	if err != nil {
		t.Fatalf("open 2: %v", err)
	}
	defer p2.Close()
	got, err := p2.GetDocByURL(ctx, "https://a")
	if err != nil {
		t.Fatalf("reopened get: %v", err)
	}
	if got.ID != id1 {
		t.Errorf("post-reopen ID: want %d, got %d", id1, got.ID)
	}

	// Fresh insert must get an ID > id1.
	id2, _ := p2.UpsertDocument(ctx, &Document{
		URL: "https://b", Title: "t", FetchedAt: time.Now(),
	})
	if id2 <= id1 {
		t.Errorf("post-reopen new ID should be > %d, got %d", id1, id2)
	}
}

// TestPebbleStats — Stats.Documents reflects the upsert count.
func TestPebbleStats(t *testing.T) {
	p := newPebbleStore(t)
	ctx := context.Background()
	for i := 0; i < 7; i++ {
		_, _ = p.UpsertDocument(ctx, &Document{
			URL: "https://x/" + string(rune('a'+i)),
			Title: "t", FetchedAt: time.Now(),
		})
	}
	s, err := p.Stats(ctx)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if s.Documents != 7 {
		t.Errorf("documents: want 7, got %d", s.Documents)
	}
}
