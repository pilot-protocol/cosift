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

// trivialTokenize splits on whitespace + lowercases. Adequate for tests;
// the real BM25 tokenizer with stopwords is in package index.
func trivialTokenize(s string) []string {
	var out []string
	cur := []rune{}
	flush := func() {
		if len(cur) > 0 {
			out = append(out, string(cur))
			cur = cur[:0]
		}
	}
	for _, r := range s {
		if r == ' ' || r == '\t' || r == '\n' || r == ',' || r == '.' {
			flush()
		} else {
			if r >= 'A' && r <= 'Z' {
				r = r + 32
			}
			cur = append(cur, r)
		}
	}
	flush()
	return out
}

// TestPebbleIndexAndPostings — IndexDocument writes term metadata + postings
// + doc_len; GetTermInfo / IteratePostings / GetDocLen read them back.
func TestPebbleIndexAndPostings(t *testing.T) {
	p := newPebbleStore(t)
	ctx := context.Background()

	// Two docs sharing tokens.
	if err := p.IndexDocument(ctx, 1, "Hello World", "hello again", trivialTokenize, 3); err != nil {
		t.Fatalf("index doc 1: %v", err)
	}
	if err := p.IndexDocument(ctx, 2, "Goodbye World", "world peace", trivialTokenize, 3); err != nil {
		t.Fatalf("index doc 2: %v", err)
	}

	// "hello": doc_freq=1 (only doc 1)
	info, ok, err := p.GetTermInfo(ctx, "hello")
	if err != nil || !ok {
		t.Fatalf("term 'hello': ok=%v err=%v", ok, err)
	}
	if info.DocFreq != 1 {
		t.Errorf("hello doc_freq: want 1, got %d", info.DocFreq)
	}

	// "world": doc_freq=2 (both docs have it)
	info, ok, _ = p.GetTermInfo(ctx, "world")
	if !ok {
		t.Fatalf("term 'world' missing")
	}
	if info.DocFreq != 2 {
		t.Errorf("world doc_freq: want 2, got %d", info.DocFreq)
	}

	// Iterate postings for "world" — both docs, with title-boosted tf for the
	// docs whose title carries the term.
	type p2 struct {
		docID int64
		tf    int64
	}
	var got []p2
	if err := p.IteratePostings(ctx, info.ID, func(e PostingEntry) bool {
		got = append(got, p2{e.DocID, e.TF})
		return true
	}); err != nil {
		t.Fatalf("iterate: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("postings for 'world': want 2, got %d: %v", len(got), got)
	}
	// Both docs have "world" in title (boost=3) + maybe body — doc 1: title+nothing → 3
	// doc 2: title + body "world peace" → 3+1 = 4
	for _, e := range got {
		if e.docID == 1 && e.tf != 3 {
			t.Errorf("doc 1 'world' tf: want 3, got %d", e.tf)
		}
		if e.docID == 2 && e.tf != 4 {
			t.Errorf("doc 2 'world' tf: want 4 (title-boost 3 + body 1), got %d", e.tf)
		}
	}

	// doc_len: doc 1 has 4 tokens ("hello world hello again"); doc 2 has 4 too.
	dl1, ok, _ := p.GetDocLen(ctx, 1)
	if !ok || dl1 != 4 {
		t.Errorf("doc 1 doc_len: want 4, got %d (ok=%v)", dl1, ok)
	}
	dl2, ok, _ := p.GetDocLen(ctx, 2)
	if !ok || dl2 != 4 {
		t.Errorf("doc 2 doc_len: want 4, got %d", dl2)
	}
}

// TestPebbleReindexSameDocIsIdempotent — calling IndexDocument again with
// the same docID must not double-count doc_freq.
func TestPebbleReindexSameDocIsIdempotent(t *testing.T) {
	p := newPebbleStore(t)
	ctx := context.Background()
	if err := p.IndexDocument(ctx, 1, "Hello", "world", trivialTokenize, 1); err != nil {
		t.Fatalf("first index: %v", err)
	}
	if err := p.IndexDocument(ctx, 1, "Hello", "world", trivialTokenize, 1); err != nil {
		t.Fatalf("re-index: %v", err)
	}
	info, ok, _ := p.GetTermInfo(ctx, "hello")
	if !ok || info.DocFreq != 1 {
		t.Errorf("re-index should not bump doc_freq: want 1, got %d", info.DocFreq)
	}
}

// TestPebbleIteratePostingsEmpty — non-existent term ID yields no entries.
func TestPebbleIteratePostingsEmpty(t *testing.T) {
	p := newPebbleStore(t)
	called := false
	err := p.IteratePostings(context.Background(), 9999, func(e PostingEntry) bool {
		called = true
		return true
	})
	if err != nil {
		t.Errorf("iterate empty: %v", err)
	}
	if called {
		t.Errorf("callback should not fire on empty prefix")
	}
}

// TestPebblePostingsPersistAcrossReopen — close + reopen preserves postings.
func TestPebblePostingsPersistAcrossReopen(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pebble")
	p, err := OpenPebble(dir)
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}
	ctx := context.Background()
	if err := p.IndexDocument(ctx, 1, "Title", "body of stuff", trivialTokenize, 1); err != nil {
		t.Fatalf("index: %v", err)
	}
	info, _, _ := p.GetTermInfo(ctx, "body")
	originalID := info.ID
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	p2, err := OpenPebble(dir)
	if err != nil {
		t.Fatalf("open 2: %v", err)
	}
	defer p2.Close()
	info2, ok, _ := p2.GetTermInfo(ctx, "body")
	if !ok {
		t.Fatalf("term 'body' lost on reopen")
	}
	if info2.ID != originalID {
		t.Errorf("term ID changed on reopen: %d → %d", originalID, info2.ID)
	}

	// A new IndexDocument call must assign a NEW term ID for unseen terms,
	// not collide with the existing ones.
	if err := p2.IndexDocument(ctx, 2, "Title", "unseen vocabulary", trivialTokenize, 1); err != nil {
		t.Fatalf("post-reopen index: %v", err)
	}
	infoNew, ok, _ := p2.GetTermInfo(ctx, "unseen")
	if !ok || infoNew.ID == originalID {
		t.Errorf("new term ID should differ from existing: existing=%d new=%d", originalID, infoNew.ID)
	}
}
