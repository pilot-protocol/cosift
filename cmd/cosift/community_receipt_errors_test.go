package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"

	"github.com/pilot-protocol/cosift/internal/crawler"
	"github.com/pilot-protocol/cosift/internal/store"
)

func TestCommunityReceiptKeepsTheJournalWhenTheContextEndsAfterIndexing(t *testing.T) {
	db := openReceiptStore(t)
	completeContentIndex(t, db)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	s := newReceiptServer(db, func(ctx context.Context, raw string, _ *crawler.LocalArtifact, _ string) (crawler.ContributionReceipt, error) {
		calls++
		novel := calls == 1
		_ = crawler.ReportNovelty(ctx, crawler.NoveltyDecision{FinalURL: raw, Novel: novel})
		cancel()
		return crawler.ContributionReceipt{Indexed: true, Novel: novel, ContentHash: strings.Repeat("a", 64)}, nil
	})
	approved := strings.Repeat("a", 64)
	if _, err := s.fetchCommunityReceipt(ctx, "cancelled-after-index-1", "https://example.com/late", nil, approved); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected the cancellation, got %v", err)
	}
	r, err := s.fetchCommunityReceipt(context.Background(), "cancelled-after-index-1", "https://example.com/late", nil, approved)
	if err != nil || !r.Novel || !r.Indexed {
		t.Fatalf("retry lost the recorded verdict: %+v %v", r, err)
	}
	if calls != 2 {
		t.Fatalf("fetch calls = %d, want 2 (the first attempt never journaled a receipt)", calls)
	}
}

func TestCommunityReceiptReplayFailsOnACancelledContext(t *testing.T) {
	s := newReceiptServer(openReceiptStore(t), nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	record := communityReceiptRecord{Receipt: &crawler.ContributionReceipt{Indexed: true, Novel: true}}
	if _, err := s.replayCommunityReceipt(ctx, record, "https://example.com/a"); !errors.Is(err, context.Canceled) {
		t.Fatalf("replay on a cancelled context: %v", err)
	}
}

func TestCommunityReceiptReplayFailsWhenTheLegacyURLCannotBeJudged(t *testing.T) {
	db := openReceiptStore(t)
	completeContentIndex(t, db)
	s := newReceiptServer(db, nil)
	record := communityReceiptRecord{Receipt: &crawler.ContributionReceipt{Indexed: true, Novel: true, ContentHash: strings.Repeat("a", 64)}}
	if r, err := s.replayCommunityReceipt(context.Background(), record, "://not a url"); err == nil || r.Novel {
		t.Fatalf("an unjudgeable legacy receipt stayed novel: %+v %v", r, err)
	}
}

func TestContentUnclaimed(t *testing.T) {
	db := openReceiptStore(t)
	completeContentIndex(t, db)
	ctx := context.Background()
	s := newReceiptServer(db, nil)
	text := "unclaimed body"
	sum := sha256.Sum256([]byte(text))
	if _, err := db.UpsertDocument(ctx, &store.Document{URL: "https://example.com/mine", Title: "Guide", Text: text, ContentSHA: sum[:]}); err != nil {
		t.Fatal(err)
	}
	receipt := func(final, hash string) crawler.ContributionReceipt {
		return crawler.ContributionReceipt{FinalURL: final, ContentHash: hash}
	}
	good := hex.EncodeToString(sum[:])
	cancelled, cancel := context.WithCancel(ctx)
	cancel()

	for name, tc := range map[string]struct {
		ctx     context.Context
		raw     string
		receipt crawler.ContributionReceipt
		want    bool
		wantErr bool
	}{
		"sole holder":                   {ctx, "https://example.com/mine", receipt("", good), true, false},
		"the final URL wins over raw":   {ctx, "https://example.com/other", receipt("https://example.com/mine", good), true, false},
		"unstored document":             {ctx, "https://example.com/missing", receipt("", good), false, false},
		"content hash is not hex":       {ctx, "https://example.com/mine", receipt("", "not-hex"), false, false},
		"raw URL that cannot be judged": {ctx, "://not a url", receipt("", good), false, true},
		"cancelled context":             {cancelled, "https://example.com/mine", receipt("", good), false, true},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := s.contentUnclaimed(tc.ctx, tc.raw, tc.receipt)
			if got != tc.want || (err != nil) != tc.wantErr {
				t.Fatalf("contentUnclaimed = %v, %v; want %v, err=%v", got, err, tc.want, tc.wantErr)
			}
		})
	}
}
