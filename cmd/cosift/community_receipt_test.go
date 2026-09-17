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
	calls := 0
	setup := func() *pebbleHTTP {
		s := &pebbleHTTP{store: db, cluster: config.Cluster{PeerAuthToken: "secret"}, crawlCommunityFetch: func(context.Context, string, *crawler.LocalArtifact, string) (crawler.ContributionReceipt, error) {
			calls++
			// Simulate a recrawl: only the first delivery can report novelty.
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
	ctx := context.Background()
	s := &pebbleHTTP{store: db, crawlCommunityFetch: func(ctx context.Context, raw string, a *crawler.LocalArtifact, approvedHash string) (crawler.ContributionReceipt, error) {
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
