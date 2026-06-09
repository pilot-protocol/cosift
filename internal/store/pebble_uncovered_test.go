package store

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPebbleMetrics(t *testing.T) {
	p := newPebbleStore(t)
	m := p.Metrics()
	if m == nil {
		t.Errorf("Metrics returned nil")
	}
}

func TestPebbleCheckpoint(t *testing.T) {
	p := newPebbleStore(t)
	ctx := context.Background()
	_, _ = p.UpsertDocument(ctx, &Document{URL: "https://x/y", FetchedAt: time.Now()})
	dest := filepath.Join(t.TempDir(), "ckpt")
	if err := p.Checkpoint(dest); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	// Sanity-check the checkpoint directory exists and contains something.
	p2, err := OpenPebble(dest)
	if err != nil {
		t.Fatalf("OpenPebble(ckpt): %v", err)
	}
	defer p2.Close()
	got, err := p2.GetDocByURL(ctx, "https://x/y")
	if err != nil {
		t.Fatalf("read from checkpoint: %v", err)
	}
	if got.URL != "https://x/y" {
		t.Errorf("checkpoint roundtrip mismatch: %+v", got)
	}
}

func TestPebbleVectorMetaRoundtrip(t *testing.T) {
	p := newPebbleStore(t)
	ctx := context.Background()

	// Initial miss → ok=false, no error.
	_, ok, err := p.GetVectorMeta(ctx)
	if err != nil {
		t.Fatalf("initial GetVectorMeta: %v", err)
	}
	if ok {
		t.Errorf("expected miss on fresh store")
	}

	blob := []byte{0x01, 0x02, 0x03, 0x04, 0x05}
	if err := p.PutVectorMeta(ctx, blob); err != nil {
		t.Fatalf("PutVectorMeta: %v", err)
	}
	got, ok, err := p.GetVectorMeta(ctx)
	if err != nil {
		t.Fatalf("GetVectorMeta after put: %v", err)
	}
	if !ok {
		t.Errorf("expected hit")
	}
	if string(got) != string(blob) {
		t.Errorf("blob roundtrip: got %v want %v", got, blob)
	}
}

func TestPebbleVectorNodeRoundtrip(t *testing.T) {
	p := newPebbleStore(t)
	ctx := context.Background()

	if err := p.PutVectorNode(ctx, 42, []byte("nodepayload")); err != nil {
		t.Fatalf("PutVectorNode: %v", err)
	}

	// Iterate and confirm we see it.
	found := false
	if err := p.IterateVectorNodes(ctx, func(id uint64, blob []byte) bool {
		if id == 42 && string(blob) == "nodepayload" {
			found = true
		}
		return true
	}); err != nil {
		t.Fatalf("IterateVectorNodes: %v", err)
	}
	if !found {
		t.Errorf("expected to find node 42")
	}
}

func TestPebbleVectorNodesBatch(t *testing.T) {
	p := newPebbleStore(t)
	ctx := context.Background()

	// Empty input is a no-op.
	if err := p.PutVectorNodesBatch(ctx, nil); err != nil {
		t.Errorf("empty batch: %v", err)
	}

	entries := []VectorNodeEntry{
		{ID: 1, Blob: []byte("one")},
		{ID: 2, Blob: []byte("two")},
		{ID: 3, Blob: []byte("three")},
	}
	if err := p.PutVectorNodesBatch(ctx, entries); err != nil {
		t.Fatalf("PutVectorNodesBatch: %v", err)
	}

	seen := map[uint64]string{}
	_ = p.IterateVectorNodes(ctx, func(id uint64, blob []byte) bool {
		seen[id] = string(blob)
		return true
	})
	if len(seen) != 3 || seen[1] != "one" || seen[2] != "two" || seen[3] != "three" {
		t.Errorf("batch roundtrip: %+v", seen)
	}
}

func TestPebbleClearVectorFamily(t *testing.T) {
	p := newPebbleStore(t)
	ctx := context.Background()
	_ = p.PutVectorMeta(ctx, []byte("meta"))
	_ = p.PutVectorNode(ctx, 1, []byte("one"))

	if err := p.ClearVectorFamily(ctx); err != nil {
		t.Fatalf("ClearVectorFamily: %v", err)
	}
	if _, ok, _ := p.GetVectorMeta(ctx); ok {
		t.Errorf("VectorMeta should be cleared")
	}
	count := 0
	_ = p.IterateVectorNodes(ctx, func(_ uint64, _ []byte) bool {
		count++
		return true
	})
	if count != 0 {
		t.Errorf("expected 0 vector nodes after clear, got %d", count)
	}
}

func TestPebblePQCodebookRoundtrip(t *testing.T) {
	p := newPebbleStore(t)
	ctx := context.Background()

	_, ok, err := p.GetPQCodebook(ctx)
	if err != nil {
		t.Fatalf("initial GetPQCodebook: %v", err)
	}
	if ok {
		t.Errorf("expected miss on fresh store")
	}

	blob := []byte("the-codebook-blob")
	if err := p.PutPQCodebook(ctx, blob); err != nil {
		t.Fatalf("PutPQCodebook: %v", err)
	}
	got, ok, err := p.GetPQCodebook(ctx)
	if err != nil {
		t.Fatalf("GetPQCodebook after put: %v", err)
	}
	if !ok || string(got) != string(blob) {
		t.Errorf("roundtrip: ok=%v got=%v", ok, string(got))
	}
}

func TestPebblePQCodesBatch(t *testing.T) {
	p := newPebbleStore(t)
	ctx := context.Background()

	if err := p.PutPQCodesBatch(ctx, nil); err != nil {
		t.Errorf("empty batch: %v", err)
	}

	entries := []PQCodeEntry{
		{ID: 10, Blob: []byte("code-10")},
		{ID: 20, Blob: []byte("code-20")},
	}
	if err := p.PutPQCodesBatch(ctx, entries); err != nil {
		t.Fatalf("PutPQCodesBatch: %v", err)
	}
	seen := map[uint64]string{}
	_ = p.IteratePQCodes(ctx, func(id uint64, blob []byte) bool {
		seen[id] = string(blob)
		return true
	})
	if len(seen) != 2 || seen[10] != "code-10" || seen[20] != "code-20" {
		t.Errorf("PQ batch roundtrip: %+v", seen)
	}
}

func TestPebbleClearPQFamily(t *testing.T) {
	p := newPebbleStore(t)
	ctx := context.Background()
	_ = p.PutPQCodebook(ctx, []byte("cb"))
	_ = p.PutPQCodesBatch(ctx, []PQCodeEntry{{ID: 1, Blob: []byte("c1")}})

	if err := p.ClearPQFamily(ctx); err != nil {
		t.Fatalf("ClearPQFamily: %v", err)
	}
	if _, ok, _ := p.GetPQCodebook(ctx); ok {
		t.Errorf("codebook should be cleared")
	}
	count := 0
	_ = p.IteratePQCodes(ctx, func(_ uint64, _ []byte) bool {
		count++
		return true
	})
	if count != 0 {
		t.Errorf("expected 0 PQ codes after clear, got %d", count)
	}
}

func TestPebbleIteratePQCodesStops(t *testing.T) {
	p := newPebbleStore(t)
	ctx := context.Background()
	_ = p.PutPQCodesBatch(ctx, []PQCodeEntry{
		{ID: 1, Blob: []byte("a")},
		{ID: 2, Blob: []byte("b")},
		{ID: 3, Blob: []byte("c")},
	})
	count := 0
	_ = p.IteratePQCodes(ctx, func(_ uint64, _ []byte) bool {
		count++
		return count < 2 // stop after second
	})
	if count != 2 {
		t.Errorf("iterator stop didn't honor false return: got %d", count)
	}
}

func TestPebbleUpsertDocumentBatch(t *testing.T) {
	p := newPebbleStore(t)
	ctx := context.Background()

	// Empty input.
	ids, err := p.UpsertDocumentBatch(ctx, nil)
	if err != nil || ids != nil {
		t.Errorf("empty batch: got ids=%v err=%v", ids, err)
	}

	docs := []*Document{
		{URL: "https://a.com/1", Domain: "a.com", Title: "A1", Text: "body1", FetchedAt: time.Now()},
		{URL: "https://a.com/2", Domain: "a.com", Title: "A2", Text: "body2", FetchedAt: time.Now()},
		{URL: "https://b.com/1", Domain: "b.com", Title: "B1", Text: "body3", FetchedAt: time.Now()},
	}
	ids, err = p.UpsertDocumentBatch(ctx, docs)
	if err != nil {
		t.Fatalf("UpsertDocumentBatch: %v", err)
	}
	if len(ids) != 3 {
		t.Fatalf("ids: got %d want 3", len(ids))
	}
	for i, id := range ids {
		if id <= 0 {
			t.Errorf("ids[%d] <= 0", i)
		}
	}

	// Re-upsert returns same IDs.
	ids2, err := p.UpsertDocumentBatch(ctx, docs)
	if err != nil {
		t.Fatalf("re-upsert: %v", err)
	}
	for i, id := range ids2 {
		if id != ids[i] {
			t.Errorf("re-upsert ids[%d]: got %d want %d", i, id, ids[i])
		}
	}
}

func TestPebbleUpsertDocumentBatchEmptyURLRejected(t *testing.T) {
	p := newPebbleStore(t)
	docs := []*Document{
		{URL: "https://ok", FetchedAt: time.Now()},
		{URL: "", FetchedAt: time.Now()}, // bad
	}
	if _, err := p.UpsertDocumentBatch(context.Background(), docs); err == nil {
		t.Errorf("expected error for empty URL in batch")
	}
}

func TestPebbleIterDocsLite(t *testing.T) {
	p := newPebbleStore(t)
	ctx := context.Background()
	urls := []string{"https://a/1", "https://a/2", "https://b/1"}
	for _, u := range urls {
		if _, err := p.UpsertDocument(ctx, &Document{URL: u, FetchedAt: time.Now()}); err != nil {
			t.Fatalf("upsert %s: %v", u, err)
		}
	}

	seen := map[string]bool{}
	if err := p.IterDocsLite(ctx, func(_ int64, url string) error {
		seen[url] = true
		return nil
	}); err != nil {
		t.Fatalf("IterDocsLite: %v", err)
	}
	for _, u := range urls {
		if !seen[u] {
			t.Errorf("missed url %s in iter", u)
		}
	}
}

func TestPebblePurgeFrontierByHost(t *testing.T) {
	p := newPebbleStore(t)
	ctx := context.Background()

	// Empty host is a no-op.
	if n, err := p.PurgeFrontierByHost(ctx, ""); err != nil || n != 0 {
		t.Errorf("empty host: n=%d err=%v", n, err)
	}

	_ = p.PushFrontier(ctx, "https://a.com/1", 0, 1)
	_ = p.PushFrontier(ctx, "https://a.com/2", 0, 1)
	_ = p.PushFrontier(ctx, "https://b.com/1", 0, 1)

	n, err := p.PurgeFrontierByHost(ctx, "a.com")
	if err != nil {
		t.Fatalf("PurgeFrontierByHost: %v", err)
	}
	if n != 2 {
		t.Errorf("purged: got %d want 2", n)
	}

	// b.com should still be present.
	hosts, _ := p.CountQueuedPerHost(ctx, []string{"a.com", "b.com"})
	if hosts["a.com"] != 0 {
		t.Errorf("a.com still has %d queued", hosts["a.com"])
	}
	if hosts["b.com"] != 1 {
		t.Errorf("b.com lost: %d", hosts["b.com"])
	}
}

func TestPebbleCorpusStats(t *testing.T) {
	p := newPebbleStore(t)
	ctx := context.Background()

	// Empty store.
	sum, count, err := p.CorpusStats(ctx)
	if err != nil {
		t.Fatalf("CorpusStats empty: %v", err)
	}
	if sum != 0 || count != 0 {
		t.Errorf("empty corpus: sum=%d count=%d", sum, count)
	}

	// Index something.
	id, _ := p.UpsertDocument(ctx, &Document{URL: "u", FetchedAt: time.Now()})
	tokenize := func(s string) []string { return strings.Fields(s) }
	if err := p.IndexDocument(ctx, id, "title here", "the quick brown fox", tokenize, 2); err != nil {
		t.Fatalf("IndexDocument: %v", err)
	}
	sum, count, err = p.CorpusStats(ctx)
	if err != nil {
		t.Fatalf("CorpusStats: %v", err)
	}
	// 2 title tokens + 4 body tokens = 6 (titleBoost only affects tf, not doc_len).
	if sum != 6 || count != 1 {
		t.Errorf("sum=%d count=%d want sum=6 count=1", sum, count)
	}
}

// TestPebbleCorpusStatsLockFreePath verifies the atomic-mirror path:
// after a single IndexDocument commit, subsequent CorpusStats calls
// must serve from atomics without taking p.mu. We assert by manually
// holding p.mu and confirming CorpusStats still returns immediately.
func TestPebbleCorpusStatsLockFreePath(t *testing.T) {
	p := newPebbleStore(t)
	ctx := context.Background()
	tokenize := func(s string) []string { return strings.Fields(s) }

	id, _ := p.UpsertDocument(ctx, &Document{URL: "u", FetchedAt: time.Now()})
	if err := p.IndexDocument(ctx, id, "", "a b c d", tokenize, 1); err != nil {
		t.Fatalf("IndexDocument: %v", err)
	}

	// Mirror must be populated post-commit.
	if !p.corpusStatsLoaded.Load() {
		t.Fatalf("atomic mirror not seeded after IndexDocument")
	}

	// Hold the mutex; CorpusStats must NOT block on it.
	p.mu.Lock()
	defer p.mu.Unlock()

	done := make(chan struct{})
	var sum, count int64
	var err error
	go func() {
		sum, count, err = p.CorpusStats(ctx)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("CorpusStats blocked on p.mu — fast path didn't kick in")
	}
	if err != nil {
		t.Fatalf("CorpusStats: %v", err)
	}
	if sum != 4 || count != 1 {
		t.Errorf("sum=%d count=%d want sum=4 count=1", sum, count)
	}
}

func TestPebbleSumDocLengths(t *testing.T) {
	p := newPebbleStore(t)
	ctx := context.Background()
	tokenize := func(s string) []string { return strings.Fields(s) }

	id1, _ := p.UpsertDocument(ctx, &Document{URL: "u1", FetchedAt: time.Now()})
	id2, _ := p.UpsertDocument(ctx, &Document{URL: "u2", FetchedAt: time.Now()})
	_ = p.IndexDocument(ctx, id1, "", "a b c", tokenize, 1)
	_ = p.IndexDocument(ctx, id2, "", "d e", tokenize, 1)

	total, count, err := p.SumDocLengths(ctx)
	if err != nil {
		t.Fatalf("SumDocLengths: %v", err)
	}
	if total != 5 || count != 2 {
		t.Errorf("total=%d count=%d want total=5 count=2", total, count)
	}
}

func TestPebbleListDomains(t *testing.T) {
	p := newPebbleStore(t)
	ctx := context.Background()
	_, _ = p.UpsertDocument(ctx, &Document{URL: "https://a.com/1", Domain: "a.com", FetchedAt: time.Now()})
	_, _ = p.UpsertDocument(ctx, &Document{URL: "https://a.com/2", Domain: "a.com", FetchedAt: time.Now()})
	_, _ = p.UpsertDocument(ctx, &Document{URL: "https://b.org/1", Domain: "b.org", FetchedAt: time.Now()})

	list, total, err := p.ListDomains(ctx, "", 0, 10)
	if err != nil {
		t.Fatalf("ListDomains: %v", err)
	}
	if total != 2 {
		t.Errorf("total: got %d want 2", total)
	}
	if len(list) != 2 {
		t.Fatalf("list len: %d", len(list))
	}
	// Sorted desc by count → a.com first.
	if list[0].Host != "a.com" || list[0].Count != 2 {
		t.Errorf("top: %+v", list[0])
	}

	// Substring filter.
	filt, total, err := p.ListDomains(ctx, "b.", 0, 10)
	if err != nil {
		t.Fatalf("ListDomains filtered: %v", err)
	}
	if total != 1 || len(filt) != 1 || filt[0].Host != "b.org" {
		t.Errorf("filter: total=%d list=%+v", total, filt)
	}

	// Pagination beyond total.
	page, total, err := p.ListDomains(ctx, "", 100, 10)
	if err != nil {
		t.Fatalf("ListDomains pagination: %v", err)
	}
	if len(page) != 0 || total != 2 {
		t.Errorf("offset past end: got len=%d total=%d", len(page), total)
	}

	// Default limit when limit<=0.
	_, _, err = p.ListDomains(ctx, "", 0, 0)
	if err != nil {
		t.Errorf("default limit: %v", err)
	}

	// Negative offset → clamped to 0.
	all, _, err := p.ListDomains(ctx, "", -5, 10)
	if err != nil {
		t.Errorf("negative offset: %v", err)
	}
	if len(all) != 2 {
		t.Errorf("negative offset should return all, got %d", len(all))
	}
}

func TestPebbleTopDomains(t *testing.T) {
	p := newPebbleStore(t)
	ctx := context.Background()
	_, _ = p.UpsertDocument(ctx, &Document{URL: "https://a.com/1", Domain: "a.com", FetchedAt: time.Now()})
	_, _ = p.UpsertDocument(ctx, &Document{URL: "https://a.com/2", Domain: "a.com", FetchedAt: time.Now()})
	_, _ = p.UpsertDocument(ctx, &Document{URL: "https://b.org/1", Domain: "b.org", FetchedAt: time.Now()})

	top, err := p.TopDomains(ctx, 5)
	if err != nil {
		t.Fatalf("TopDomains: %v", err)
	}
	if len(top) != 2 {
		t.Fatalf("len: got %d want 2", len(top))
	}
	if top[0].Host != "a.com" || top[0].Count != 2 {
		t.Errorf("top: %+v", top[0])
	}

	// Default topN.
	if _, err := p.TopDomains(ctx, 0); err != nil {
		t.Errorf("default topN: %v", err)
	}
}

func TestPebbleTopQueuedHosts(t *testing.T) {
	p := newPebbleStore(t)
	ctx := context.Background()
	_ = p.PushFrontier(ctx, "https://a.com/1", 0, 1)
	_ = p.PushFrontier(ctx, "https://a.com/2", 0, 1)
	_ = p.PushFrontier(ctx, "https://b.com/1", 0, 1)

	top, err := p.TopQueuedHosts(ctx, 5)
	if err != nil {
		t.Fatalf("TopQueuedHosts: %v", err)
	}
	if len(top) != 2 {
		t.Fatalf("len: got %d want 2", len(top))
	}
	if top[0].Host != "a.com" || top[0].Count != 2 {
		t.Errorf("top: %+v", top[0])
	}

	// Default topN.
	if _, err := p.TopQueuedHosts(ctx, 0); err != nil {
		t.Errorf("default topN: %v", err)
	}
}

func TestPebbleGetDocMetaMiss(t *testing.T) {
	p := newPebbleStore(t)
	_, _, ok, err := p.GetDocMeta(context.Background(), 9999)
	if err != nil {
		t.Errorf("miss: %v", err)
	}
	if ok {
		t.Errorf("expected miss")
	}
}

func TestPebbleGetDocMetaHit(t *testing.T) {
	p := newPebbleStore(t)
	ctx := context.Background()
	id, _ := p.UpsertDocument(ctx, &Document{URL: "u", Title: "T", FetchedAt: time.Now()})
	url, title, ok, err := p.GetDocMeta(ctx, id)
	if err != nil {
		t.Fatalf("GetDocMeta: %v", err)
	}
	if !ok {
		t.Errorf("expected hit")
	}
	if url != "u" || title != "T" {
		t.Errorf("meta: url=%q title=%q", url, title)
	}
}
