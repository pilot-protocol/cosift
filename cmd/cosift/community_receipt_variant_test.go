package main

import (
	"context"
	"crypto/sha256"
	"testing"

	"github.com/pilot-protocol/cosift/internal/crawler"
	"github.com/pilot-protocol/cosift/internal/store"
)

func TestCommunityReceiptReplayRejudgesLegacyNoveltyAgainstBulkVariants(t *testing.T) {
	db := openReceiptStore(t)
	completeContentIndex(t, db)
	ctx := context.Background()
	text := "Legacy article body"
	sum := sha256.Sum256([]byte(text))
	mine := "https://example.com/legacy?ref=1"
	if _, err := db.UpsertDocument(ctx, &store.Document{URL: mine, Title: "Guide", Text: text, ContentSHA: sum[:]}); err != nil {
		t.Fatal(err)
	}
	legacyReceipt(t, db, "legacy-variant-job-1", mine, text)
	s := newReceiptServer(db, func(context.Context, string, *crawler.LocalArtifact, string) (crawler.ContributionReceipt, error) {
		t.Fatal("a journaled receipt must not re-crawl")
		return crawler.ContributionReceipt{}, nil
	})
	approved := crawler.ApprovedContentHash("Guide", text)
	if r, err := s.fetchCommunityReceipt(ctx, "legacy-variant-job-1", mine, nil, approved); err != nil || !r.Novel {
		t.Fatalf("sole holder of the content must stay novel: %+v %v", r, err)
	}
	other := "other text, extracted differently"
	otherSum := sha256.Sum256([]byte(other))
	if _, err := db.UpsertDocument(ctx, &store.Document{URL: "https://example.com/legacy", Title: "Guide", Text: other, Source: "wet-import", ContentSHA: otherSum[:]}); err != nil {
		t.Fatal(err)
	}
	if r, err := s.fetchCommunityReceipt(ctx, "legacy-variant-job-1", mine, nil, approved); err != nil || r.Novel {
		t.Fatalf("a bulk-imported page at a variant URL must make the legacy receipt non-novel: %+v %v", r, err)
	}
}
