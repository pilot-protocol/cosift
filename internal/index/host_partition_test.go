package index

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/store"
)

// indexHostDocs upserts + indexes docs across two hosts. When the
// COSIFT_HOST_PARTITION env is set by the caller before this runs, the 'P'
// family is populated on index; otherwise the caller backfills.
func indexHostDocs(t *testing.T, ps *store.PebbleStore, idx *PebbleBM25) {
	t.Helper()
	ctx := context.Background()
	docs := []struct {
		url, domain, title, text string
	}{
		{"https://site-a.com/install", "site-a.com", "Install guide", "install the daemon and run the cli tool to register"},
		{"https://site-a.com/trust", "site-a.com", "Trust model", "trust model overlay network peers handshake encryption"},
		{"https://site-a.com/cooking", "site-a.com", "Pasta recipe", "boil water salt pasta drain sauce al dente"},
		{"https://other-b.com/install", "other-b.com", "Install other", "install the daemon and run the cli tool to register agents"},
		{"https://other-b.com/trust", "other-b.com", "Trust other", "trust model overlay network peers handshake encryption setup"},
	}
	for _, d := range docs {
		id, err := ps.UpsertDocument(ctx, &store.Document{
			URL: d.url, Domain: d.domain, Title: d.title, Text: d.text, FetchedAt: time.Now(),
		})
		if err != nil {
			t.Fatalf("upsert: %v", err)
		}
		if err := idx.IndexDocument(ctx, id, d.title, d.text); err != nil {
			t.Fatalf("index: %v", err)
		}
	}
}

// TestSearchInHostScopesToHost: with the partition written at index time,
// SearchInHost returns ONLY docs from the requested host, and its result set
// equals Search filtered to that host.
func TestSearchInHostScopesToHost(t *testing.T) {
	t.Setenv("COSIFT_HOST_PARTITION", "1")
	ps, idx := newPebbleBM25(t)
	ctx := context.Background()
	indexHostDocs(t, ps, idx)

	hits, err := idx.SearchInHost(ctx, "install daemon cli register", "site-a.com", 10)
	if err != nil {
		t.Fatalf("SearchInHost: %v", err)
	}
	if len(hits) == 0 {
		t.Fatal("no hits from SearchInHost")
	}
	for _, h := range hits {
		if !contains(h.URL, "site-a.com") {
			t.Errorf("SearchInHost leaked a non-site-a host: %s", h.URL)
		}
	}
	// Top hit should be the install page, not the trust or cooking page.
	if !contains(hits[0].URL, "/install") {
		t.Errorf("top hit: got %s, want a /install page", hits[0].URL)
	}
}

// TestSearchInHostEmptyPartition: a host with no partition returns the
// sentinel so callers can fall back.
func TestSearchInHostEmptyPartition(t *testing.T) {
	t.Setenv("COSIFT_HOST_PARTITION", "1")
	ps, idx := newPebbleBM25(t)
	indexHostDocs(t, ps, idx)
	_, err := idx.SearchInHost(context.Background(), "anything", "never-indexed.example", 10)
	if !errors.Is(err, ErrHostPartitionEmpty) {
		t.Fatalf("want ErrHostPartitionEmpty, got %v", err)
	}
}

// TestBackfillHostPostings: index WITHOUT the write flag (no 'P' family),
// then backfill, then SearchInHost works and matches the at-index-time path.
func TestBackfillHostPostings(t *testing.T) {
	ps, idx := newPebbleBM25(t) // flag NOT set → no partition on index
	ctx := context.Background()
	indexHostDocs(t, ps, idx)

	// Before backfill: partition is empty.
	if _, err := idx.SearchInHost(ctx, "install", "site-a.com", 10); !errors.Is(err, ErrHostPartitionEmpty) {
		t.Fatalf("pre-backfill want ErrHostPartitionEmpty, got %v", err)
	}

	written, err := ps.BackfillHostPostings(ctx, "", nil)
	if err != nil {
		t.Fatalf("backfill: %v", err)
	}
	if written == 0 {
		t.Fatal("backfill wrote 0 postings")
	}

	hits, err := idx.SearchInHost(ctx, "install daemon cli register", "site-a.com", 10)
	if err != nil {
		t.Fatalf("post-backfill SearchInHost: %v", err)
	}
	if len(hits) == 0 {
		t.Fatal("post-backfill: no hits")
	}
	for _, h := range hits {
		if !contains(h.URL, "site-a.com") {
			t.Errorf("post-backfill leaked host: %s", h.URL)
		}
	}
}

// TestIndexDocumentBulkSkipsHostPartition: even with the partition write flag
// ON, docs indexed via IndexDocumentBulk must NOT land in the 'P' family, so
// SearchInHost can't find them (they fall back to the boost path). This is the
// WET-ingest write-amplification fix.
func TestIndexDocumentBulkSkipsHostPartition(t *testing.T) {
	t.Setenv("COSIFT_HOST_PARTITION", "1")
	ps, idx := newPebbleBM25(t)
	ctx := context.Background()

	// Normal index → partitioned.
	id1, err := ps.UpsertDocument(ctx, &store.Document{
		URL: "https://normal.com/p", Domain: "normal.com", Title: "Normal", Text: "alpha beta gamma", FetchedAt: time.Now(),
	})
	if err != nil {
		t.Fatalf("upsert normal: %v", err)
	}
	if err := idx.IndexDocument(ctx, id1, "Normal", "alpha beta gamma"); err != nil {
		t.Fatalf("index normal: %v", err)
	}
	// Bulk index → must NOT be partitioned.
	id2, err := ps.UpsertDocument(ctx, &store.Document{
		URL: "https://bulk.com/p", Domain: "bulk.com", Title: "Bulk", Text: "alpha beta gamma", FetchedAt: time.Now(),
	})
	if err != nil {
		t.Fatalf("upsert bulk: %v", err)
	}
	if err := idx.IndexDocumentBulk(ctx, id2, "Bulk", "alpha beta gamma"); err != nil {
		t.Fatalf("index bulk: %v", err)
	}

	// normal.com has a partition (found via SearchInHost).
	hits, err := idx.SearchInHost(ctx, "alpha beta gamma", "normal.com", 10)
	if err != nil {
		t.Fatalf("SearchInHost normal: %v", err)
	}
	if len(hits) == 0 {
		t.Error("normal.com should be in the host partition but SearchInHost found nothing")
	}
	// bulk.com has NO partition → SearchInHost returns the empty sentinel.
	_, err = idx.SearchInHost(ctx, "alpha beta gamma", "bulk.com", 10)
	if !errors.Is(err, ErrHostPartitionEmpty) {
		t.Errorf("bulk.com should NOT be partitioned; want ErrHostPartitionEmpty, got %v", err)
	}
}

func contains(s, sub string) bool { return strings.Contains(s, sub) }
