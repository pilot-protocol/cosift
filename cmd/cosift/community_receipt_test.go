package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/cockroachdb/pebble"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/crawler"
	"github.com/pilot-protocol/cosift/internal/store"
)

func TestCommunityReceiptSurvivesLostResponseAndRestart(t *testing.T) {
	dir := t.TempDir()
	db, err := store.OpenPebble(dir)
	if err != nil {
		t.Fatal(err)
	}
	completeContentIndex(t, db)
	calls := 0
	setup := func() *pebbleHTTP {
		s := &pebbleHTTP{store: db, cluster: config.Cluster{PeerAuthToken: "secret"}, crawlCommunityFetch: func(ctx context.Context, raw string, _ *crawler.LocalArtifact, _ string) (crawler.ContributionReceipt, error) {
			calls++
			// Simulate a recrawl: only the first delivery can report novelty.
			_ = crawler.ReportNovelty(ctx, crawler.NoveltyDecision{FinalURL: raw, Novel: calls == 1})
			return crawler.ContributionReceipt{Indexed: true, Novel: calls == 1, ContentHash: strings.Repeat("a", 64)}, nil
		}}
		s.crawlCommunityReady.Store(true)
		return s
	}
	invoke := func(s *pebbleHTTP, raw string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", "/admin/community-enqueue", strings.NewReader(`{"submission_id":"stable-submission-123","url":"`+raw+`","approved_content_hash":"`+strings.Repeat("a", 64)+`"}`))
		r.Header.Set("Authorization", "Bearer secret")
		w := httptest.NewRecorder()
		s.handleCommunityEnqueue(w, r)
		return w
	}
	s := setup()
	first := invoke(s, "https://example.com/guide")
	if first.Code != 200 {
		t.Fatal(first.Body.String())
	}
	// Drop that HTTP response, then restart the backend and retry the same job.
	db.Close()
	db, err = store.OpenPebble(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	retry := invoke(setup(), "https://example.com/guide")
	var receipt crawler.ContributionReceipt
	json.Unmarshal(retry.Body.Bytes(), &receipt)
	if retry.Code != 200 || !receipt.Novel || calls != 1 {
		t.Fatalf("lost receipt: code=%d novel=%v indexing_calls=%d", retry.Code, receipt.Novel, calls)
	}
	if conflict := invoke(setup(), "https://example.com/different"); conflict.Code != 409 {
		t.Fatalf("id reused with different payload: %d", conflict.Code)
	}
}

func TestCommunityReceiptResumesInterruptedIndexing(t *testing.T) {
	dir := t.TempDir()
	db, err := store.OpenPebble(dir)
	if err != nil {
		t.Fatal(err)
	}
	completeContentIndex(t, db)
	ctx := context.Background()
	s := &pebbleHTTP{store: db, crawlCommunityFetch: func(ctx context.Context, raw string, a *crawler.LocalArtifact, approvedHash string) (crawler.ContributionReceipt, error) {
		if err := crawler.ReportNovelty(ctx, crawler.NoveltyDecision{FinalURL: raw, Novel: true}); err != nil {
			t.Fatal(err)
		}
		_, err := db.UpsertDocument(ctx, &store.Document{URL: raw, Title: "Guide", Text: "Useful content"})
		if err != nil {
			t.Fatal(err)
		}
		return crawler.ContributionReceipt{}, context.DeadlineExceeded
	}}
	if _, err := s.fetchCommunityReceipt(ctx, "interrupted-job-123", "https://example.com/guide", nil, strings.Repeat("a", 64)); err == nil {
		t.Fatal("expected interruption")
	}
	db.Close()
	db, err = store.OpenPebble(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s = &pebbleHTTP{store: db, crawlCommunityFetch: func(context.Context, string, *crawler.LocalArtifact, string) (crawler.ContributionReceipt, error) {
		return crawler.ContributionReceipt{Indexed: true, Novel: false, ContentHash: strings.Repeat("b", 64)}, nil
	}}
	receipt, err := s.fetchCommunityReceipt(ctx, "interrupted-job-123", "https://example.com/guide", nil, strings.Repeat("a", 64))
	if err != nil || !receipt.Novel {
		t.Fatalf("lost original eligibility: %+v %v", receipt, err)
	}
	// Tracking parameters must not turn an existing document into a paid contribution.
	receipt, err = s.fetchCommunityReceipt(ctx, "different-job-123", "https://example.com/guide?utm_source=credits", nil, strings.Repeat("a", 64))
	if err != nil || receipt.Novel {
		t.Fatalf("existing canonical document rewarded: %+v %v", receipt, err)
	}
}

func TestCommunityReceiptApprovalBindingAndLegacyUpgrade(t *testing.T) {
	db, err := store.OpenPebble(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	completeContentIndex(t, db)
	ctx := context.Background()
	raw := "https://example.com/approved"
	title, text := "Guide", "Safe approved content"
	if _, err := db.UpsertDocument(ctx, &store.Document{URL: raw, Title: title, Text: text}); err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(struct {
		URL      string
		Artifact *crawler.LocalArtifact
	}{raw, nil})
	ph := sha256.Sum256(payload)
	th := sha256.Sum256([]byte(text))
	record := communityReceiptRecord{PayloadHash: hex.EncodeToString(ph[:]), Eligible: true, Receipt: &crawler.ContributionReceipt{Indexed: true, Novel: true, ContentHash: hex.EncodeToString(th[:])}}
	encoded, _ := json.Marshal(record)
	key := []byte("mcommunity_receipt:legacy-approval-job")
	if err := db.DB().Set(key, encoded, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	calls := 0
	s := &pebbleHTTP{store: db, crawlCommunityFetch: func(context.Context, string, *crawler.LocalArtifact, string) (crawler.ContributionReceipt, error) {
		calls++
		return crawler.ContributionReceipt{}, nil
	}}
	if _, err := s.fetchCommunityReceipt(ctx, "legacy-approval-job", raw, nil, crawler.ApprovedContentHash(title, "Different")); !errors.Is(err, crawler.ErrContributionRejected) {
		t.Fatalf("legacy receipt bypassed approval: %v", err)
	}
	approved := crawler.ApprovedContentHash(title, text)
	receipt, err := s.fetchCommunityReceipt(ctx, "legacy-approval-job", raw, nil, approved)
	if err != nil || !receipt.Novel || calls != 0 {
		t.Fatalf("legacy unchanged replay failed: %+v %v calls=%d", receipt, err, calls)
	}
	encoded, closer, err := db.DB().Get(key)
	if err != nil {
		t.Fatal(err)
	}
	defer closer.Close()
	var upgraded communityReceiptRecord
	if err := json.Unmarshal(encoded, &upgraded); err != nil || upgraded.ApprovedContentHash != approved {
		t.Fatal("legacy receipt approval not persisted")
	}
	if _, err := s.fetchCommunityReceipt(ctx, "legacy-approval-job", raw, nil, crawler.ApprovedContentHash(title, "Different")); !errors.Is(err, crawler.ErrContributionRejected) {
		t.Fatalf("bound receipt bypassed approval: %v", err)
	}
}

func TestCommunityReceiptNoveltyNeedsCompleteIndex(t *testing.T) {
	db := openReceiptStore(t)
	ctx := context.Background()
	s := newReceiptServer(db, func(ctx context.Context, raw string, _ *crawler.LocalArtifact, _ string) (crawler.ContributionReceipt, error) {
		_ = crawler.ReportNovelty(ctx, crawler.NoveltyDecision{FinalURL: raw, Novel: true})
		return crawler.ContributionReceipt{Indexed: true, Novel: true, ContentHash: strings.Repeat("a", 64)}, nil
	})
	approved := strings.Repeat("a", 64)
	before, err := s.fetchCommunityReceipt(ctx, "incomplete-index-job-1", "https://example.com/one", nil, approved)
	if err != nil || before.Novel || before.ContentIndexComplete {
		t.Fatalf("incomplete index: %+v %v", before, err)
	}
	completeContentIndex(t, db)
	after, err := s.fetchCommunityReceipt(ctx, "complete-index-job-2", "https://example.com/two", nil, approved)
	if err != nil || !after.Novel || !after.ContentIndexComplete {
		t.Fatalf("complete index: %+v %v", after, err)
	}
	replay, err := s.fetchCommunityReceipt(ctx, "incomplete-index-job-1", "https://example.com/one", nil, approved)
	if err != nil || replay.Novel || !replay.ContentIndexComplete {
		t.Fatalf("replay must report the current index state without upgrading novelty: %+v %v", replay, err)
	}
}

func TestCommunityReceiptContentVerdictOverridesURLEligibility(t *testing.T) {
	db := openReceiptStore(t)
	completeContentIndex(t, db)
	s := newReceiptServer(db, func(ctx context.Context, raw string, _ *crawler.LocalArtifact, _ string) (crawler.ContributionReceipt, error) {
		_ = crawler.ReportNovelty(ctx, crawler.NoveltyDecision{FinalURL: raw, Novel: false})
		return crawler.ContributionReceipt{Indexed: true, Novel: true, ContentHash: strings.Repeat("b", 64)}, nil
	})
	r, err := s.fetchCommunityReceipt(context.Background(), "duplicate-content-job-1", "https://example.com/mirror", nil, strings.Repeat("a", 64))
	if err != nil || r.Novel {
		t.Fatalf("content already in the corpus was paid: %+v %v", r, err)
	}
}

func TestCommunityReceiptReplaysTheVerdictTakenBeforeTheWrite(t *testing.T) {
	db := openReceiptStore(t)
	completeContentIndex(t, db)
	ctx := context.Background()
	attempt := 0
	s := newReceiptServer(db, func(ctx context.Context, raw string, _ *crawler.LocalArtifact, _ string) (crawler.ContributionReceipt, error) {
		attempt++
		if attempt == 1 {
			if err := crawler.ReportNovelty(ctx, crawler.NoveltyDecision{FinalURL: raw, Novel: true}); err != nil {
				t.Fatal(err)
			}
			return crawler.ContributionReceipt{}, context.DeadlineExceeded
		}
		// The page is indexed by now, so a fresh verdict would say "not novel".
		_ = crawler.ReportNovelty(ctx, crawler.NoveltyDecision{FinalURL: raw, Novel: false})
		return crawler.ContributionReceipt{Indexed: true, ContentHash: strings.Repeat("c", 64)}, nil
	})
	approved := strings.Repeat("a", 64)
	if _, err := s.fetchCommunityReceipt(ctx, "interrupted-verdict-job", "https://example.com/slow", nil, approved); err == nil {
		t.Fatal("expected the first attempt to fail")
	}
	r, err := s.fetchCommunityReceipt(ctx, "interrupted-verdict-job", "https://example.com/slow", nil, approved)
	if err != nil || !r.Novel {
		t.Fatalf("replay lost the recorded verdict: %+v %v", r, err)
	}
}

func legacyReceipt(t *testing.T, db *store.PebbleStore, id, raw, text string) {
	t.Helper()
	payload, _ := json.Marshal(struct {
		URL      string
		Artifact *crawler.LocalArtifact
	}{raw, nil})
	ph := sha256.Sum256(payload)
	th := sha256.Sum256([]byte(text))
	record := communityReceiptRecord{
		PayloadHash: hex.EncodeToString(ph[:]), ApprovedContentHash: crawler.ApprovedContentHash("Guide", text), Eligible: true,
		Receipt: &crawler.ContributionReceipt{Indexed: true, Novel: true, ContentHash: hex.EncodeToString(th[:])},
	}
	encoded, _ := json.Marshal(record)
	if err := db.DB().Set([]byte("mcommunity_receipt:"+id), encoded, pebble.Sync); err != nil {
		t.Fatal(err)
	}
}

func TestCommunityReceiptReplayRejudgesLegacyNovelty(t *testing.T) {
	db := openReceiptStore(t)
	ctx := context.Background()
	text := "Legacy article body"
	sum := sha256.Sum256([]byte(text))
	mine := "https://example.com/legacy"
	if _, err := db.UpsertDocument(ctx, &store.Document{URL: mine, Title: "Guide", Text: text, ContentSHA: sum[:]}); err != nil {
		t.Fatal(err)
	}
	legacyReceipt(t, db, "legacy-novel-job-1", mine, text)
	s := newReceiptServer(db, func(context.Context, string, *crawler.LocalArtifact, string) (crawler.ContributionReceipt, error) {
		t.Fatal("a journaled receipt must not re-crawl")
		return crawler.ContributionReceipt{}, nil
	})
	approved := crawler.ApprovedContentHash("Guide", text)
	if r, err := s.fetchCommunityReceipt(ctx, "legacy-novel-job-1", mine, nil, approved); err != nil || r.Novel || r.ContentIndexComplete {
		t.Fatalf("legacy receipt on an incomplete index: %+v %v", r, err)
	}
	completeContentIndex(t, db)
	if r, err := s.fetchCommunityReceipt(ctx, "legacy-novel-job-1", mine, nil, approved); err != nil || !r.Novel || !r.ContentIndexComplete {
		t.Fatalf("sole holder of the content must stay novel: %+v %v", r, err)
	}
	if _, err := db.UpsertDocument(ctx, &store.Document{URL: "https://mirror.example/legacy", Title: "Guide", Text: text, ContentSHA: sum[:]}); err != nil {
		t.Fatal(err)
	}
	if r, err := s.fetchCommunityReceipt(ctx, "legacy-novel-job-1", mine, nil, approved); err != nil || r.Novel {
		t.Fatalf("a copy elsewhere must make the legacy receipt non-novel: %+v %v", r, err)
	}
}

func TestCommunityEnqueueReceiptJSONCarriesTheIndexContract(t *testing.T) {
	db := openReceiptStore(t)
	completeContentIndex(t, db)
	s := newReceiptServer(db, func(ctx context.Context, raw string, _ *crawler.LocalArtifact, _ string) (crawler.ContributionReceipt, error) {
		_ = crawler.ReportNovelty(ctx, crawler.NoveltyDecision{FinalURL: raw, Novel: true})
		return crawler.ContributionReceipt{Indexed: true, Novel: true, ContentHash: strings.Repeat("e", 64), FinalURL: raw}, nil
	})
	s.cluster = config.Cluster{PeerAuthToken: "secret"}
	s.crawlCommunityReady.Store(true)
	r := httptest.NewRequest("POST", "/admin/community-enqueue", strings.NewReader(`{"submission_id":"json-contract-job-1","url":"https://example.com/json","approved_content_hash":"`+strings.Repeat("a", 64)+`"}`))
	r.Header.Set("Authorization", "Bearer secret")
	w := httptest.NewRecorder()
	s.handleCommunityEnqueue(w, r)
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil || w.Code != 200 {
		t.Fatalf("code %d body %s", w.Code, w.Body.String())
	}
	if body["novel"] != true || body["content_index_complete"] != true || body["final_url"] != "https://example.com/json" || body["indexed"] != true {
		t.Fatalf("receipt JSON: %s", w.Body.String())
	}
}
