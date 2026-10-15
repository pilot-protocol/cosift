package main

import (
	"context"
	"strings"
	"testing"

	"github.com/pilot-protocol/cosift/internal/crawler"
	"github.com/pilot-protocol/cosift/internal/store"
)

func newReceiptServer(db *store.PebbleStore, fetch func(context.Context, string, *crawler.LocalArtifact, string) (crawler.ContributionReceipt, error)) *pebbleHTTP {
	return &pebbleHTTP{store: db, crawlCommunityFetch: fetch}
}

func openReceiptStore(t *testing.T) *store.PebbleStore {
	t.Helper()
	db, err := store.OpenPebble(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestCommunityReceiptDoesNotOverrideTheCrawlerVerdict(t *testing.T) {
	db := openReceiptStore(t)
	completeContentIndex(t, db)
	s := newReceiptServer(db, func(context.Context, string, *crawler.LocalArtifact, string) (crawler.ContributionReceipt, error) {
		return crawler.ContributionReceipt{Indexed: true, Novel: false, ContentHash: strings.Repeat("d", 64)}, nil
	})
	r, err := s.fetchCommunityReceipt(context.Background(), "variant-of-indexed-job", "https://example.com/guide?ref=1", nil, strings.Repeat("a", 64))
	if err != nil || !r.Indexed || r.Novel {
		t.Fatalf("a URL absent from the store made already-indexed content novel: %+v %v", r, err)
	}
}
