package store

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble"
)

type cancelAfterCalls struct {
	context.Context
	left int
}

func (c *cancelAfterCalls) Err() error {
	if c.left <= 0 {
		return context.Canceled
	}
	c.left--
	return nil
}

func cursorSaved(t *testing.T, p *PebbleStore) bool {
	t.Helper()
	_, closer, err := p.db.Get(metaKey(metaContentSHACursor))
	if err == nil {
		closer.Close()
		return true
	}
	if !errors.Is(err, pebble.ErrNotFound) {
		t.Fatal(err)
	}
	return false
}

func TestHasLiveContentIgnoresMalformedIndexKeys(t *testing.T) {
	p := newPebbleStore(t)
	sha := shaOf("indexed text")
	if err := p.db.Set(append(contentSHAKey(sha, 0)[:1+len(sha)], 'x'), nil, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	mustLive(t, p, "indexed text", 0, false)
	if _, err := p.UpsertDocument(context.Background(), contentDoc("https://example.com/a", "indexed text")); err != nil {
		t.Fatal(err)
	}
	mustLive(t, p, "indexed text", 0, true)
}

func TestHasLiveContentSurfacesACorruptDocument(t *testing.T) {
	p := newPebbleStore(t)
	sha := shaOf("vouched text")
	if err := p.db.Set(contentSHAKey(sha, 99), nil, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := p.db.Set(docKey(99), []byte("not a document"), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	live, err := p.HasLiveContent(context.Background(), sha, 0)
	if err == nil || !strings.Contains(err.Error(), "decode doc 99") || live {
		t.Fatalf("corrupt document: live=%v err=%v", live, err)
	}
}

func TestContentIndexCompleteHonoursCancellation(t *testing.T) {
	p := newPebbleStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.ContentIndexComplete(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("ContentIndexComplete on a cancelled context: %v", err)
	}
	res, err := p.BackfillContentSHA(ctx, false, nil)
	if !errors.Is(err, context.Canceled) || res.AlreadyComplete || res.Scanned != 0 {
		t.Fatalf("backfill on a cancelled context: %+v %v", res, err)
	}
}

func TestBackfillContentSHACancelledBeforeAnyDocumentSavesNoCursor(t *testing.T) {
	p := newPebbleStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	for _, text := range []string{"a", "b"} {
		if _, err := p.UpsertDocument(ctx, contentDoc("https://example.com/"+text, text)); err != nil {
			t.Fatal(err)
		}
	}
	clearContentIndex(t, p)
	cancel()
	res, err := p.BackfillContentSHA(ctx, true, nil)
	if !errors.Is(err, context.Canceled) || res.Scanned != 0 || cursorSaved(t, p) {
		t.Fatalf("cancelled rescan: %+v %v cursor=%v", res, err, cursorSaved(t, p))
	}
	if done, _ := p.ContentIndexComplete(context.Background()); done {
		t.Fatal("cancelled rescan marked the index complete")
	}
}

func TestBackfillContentSHACommitsTheOpenBatchWhenCancelled(t *testing.T) {
	p := newPebbleStore(t)
	texts := []string{"a", "b", "c", "d", "e"}
	for _, text := range texts {
		if _, err := p.UpsertDocument(context.Background(), contentDoc("https://example.com/"+text, text)); err != nil {
			t.Fatal(err)
		}
	}
	clearContentIndex(t, p)

	first, err := p.BackfillContentSHA(&cancelAfterCalls{Context: context.Background(), left: 3}, false, nil)
	if !errors.Is(err, context.Canceled) || first.Scanned != 2 || first.Indexed != 2 || !cursorSaved(t, p) {
		t.Fatalf("interrupted pass: %+v %v cursor=%v", first, err, cursorSaved(t, p))
	}
	if done, _ := p.ContentIndexComplete(context.Background()); done {
		t.Fatal("interrupted pass marked the index complete")
	}
	mustLive(t, p, "b", 0, true)
	mustLive(t, p, "c", 0, false)

	second, err := p.BackfillContentSHA(context.Background(), false, nil)
	if err != nil || second.ResumedFrom != first.LastID || second.Scanned != 3 {
		t.Fatalf("resume: %+v after %+v: %v", second, first, err)
	}
	for _, text := range texts {
		mustLive(t, p, text, 0, true)
	}
}

func TestBackfillContentSHAIgnoresForeignKeysInTheDocumentFamily(t *testing.T) {
	p := newPebbleStore(t)
	ctx := context.Background()
	id, err := p.UpsertDocument(ctx, contentDoc("https://example.com/a", "kept"))
	if err != nil {
		t.Fatal(err)
	}
	if err := p.db.Set(append(docKey(id+1), 'x'), []byte("not a document"), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	clearContentIndex(t, p)
	res, err := p.BackfillContentSHA(ctx, false, nil)
	if err != nil || res.Scanned != 1 || res.Indexed != 1 {
		t.Fatalf("backfill %+v %v", res, err)
	}
	mustLive(t, p, "kept", 0, true)
}

func TestBackfillContentSHAFailsOnACorruptDocumentWithoutCompleting(t *testing.T) {
	p := newPebbleStore(t)
	ctx := context.Background()
	if _, err := p.UpsertDocument(ctx, contentDoc("https://example.com/a", "fine")); err != nil {
		t.Fatal(err)
	}
	if err := p.db.Set(docKey(2), []byte("not a document"), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	clearContentIndex(t, p)
	res, err := p.BackfillContentSHA(ctx, false, nil)
	if err == nil || !strings.Contains(err.Error(), "decode doc 2") || res.Scanned != 1 {
		t.Fatalf("corrupt document: %+v %v", res, err)
	}
	if done, _ := p.ContentIndexComplete(ctx); done {
		t.Fatal("a failed pass marked the index complete")
	}
}

func TestBackfillContentSHAIgnoresAMalformedCursor(t *testing.T) {
	p := newPebbleStore(t)
	ctx := context.Background()
	if _, err := p.UpsertDocument(ctx, contentDoc("https://example.com/a", "one")); err != nil {
		t.Fatal(err)
	}
	clearContentIndex(t, p)
	if err := p.db.Set(metaKey(metaContentSHACursor), []byte{1, 2, 3}, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	res, err := p.BackfillContentSHA(ctx, false, nil)
	if err != nil || res.ResumedFrom != 0 || res.Scanned != 1 {
		t.Fatalf("malformed cursor: %+v %v", res, err)
	}
}

func TestBackfillContentSHAReportsProgressAfterEachBatch(t *testing.T) {
	old := contentSHABatch
	contentSHABatch = 2
	t.Cleanup(func() { contentSHABatch = old })
	p := newPebbleStore(t)
	ctx := context.Background()
	for _, text := range []string{"a", "b", "c", "d", "e"} {
		if _, err := p.UpsertDocument(ctx, contentDoc("https://example.com/"+text, text)); err != nil {
			t.Fatal(err)
		}
	}
	clearContentIndex(t, p)
	var scanned []int64
	res, err := p.BackfillContentSHA(ctx, false, func(r ContentSHABackfill) { scanned = append(scanned, r.Scanned) })
	if err != nil || res.Scanned != 5 {
		t.Fatalf("backfill %+v %v", res, err)
	}
	if len(scanned) != 3 || scanned[0] != 2 || scanned[1] != 4 || scanned[2] != 5 {
		t.Fatalf("progress reports %v, want [2 4 5]", scanned)
	}
}
