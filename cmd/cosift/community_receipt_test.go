package main

import (
	"context"
	"encoding/json"
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
		s := &pebbleHTTP{store: db, cluster: config.Cluster{PeerAuthToken: "secret"}, crawlCommunityFetch: func(context.Context, string, *crawler.LocalArtifact) (crawler.ContributionReceipt, error) {
			calls++
			// Simulate a recrawl: only the first delivery can report novelty.
			return crawler.ContributionReceipt{Indexed: true, Novel: calls == 1, ContentHash: strings.Repeat("a", 64)}, nil
		}}
		s.crawlCommunityReady.Store(true)
		return s
	}
	invoke := func(s *pebbleHTTP, raw string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", "/admin/community-enqueue", strings.NewReader(`{"submission_id":"stable-submission-123","url":"`+raw+`"}`))
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
	s := &pebbleHTTP{store: db, crawlCommunityFetch: func(ctx context.Context, raw string, a *crawler.LocalArtifact) (crawler.ContributionReceipt, error) {
		_, err := db.UpsertDocument(ctx, &store.Document{URL: raw, Title: "Guide", Text: "Useful content"})
		if err != nil {
			t.Fatal(err)
		}
		return crawler.ContributionReceipt{}, context.DeadlineExceeded
	}}
	if _, err := s.fetchCommunityReceipt(ctx, "interrupted-job-123", "https://example.com/guide", nil); err == nil {
		t.Fatal("expected interruption")
	}
	db.Close()
	db, err = store.OpenPebble(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s = &pebbleHTTP{store: db, crawlCommunityFetch: func(context.Context, string, *crawler.LocalArtifact) (crawler.ContributionReceipt, error) {
		return crawler.ContributionReceipt{Indexed: true, Novel: false, ContentHash: strings.Repeat("b", 64)}, nil
	}}
	receipt, err := s.fetchCommunityReceipt(ctx, "interrupted-job-123", "https://example.com/guide", nil)
	if err != nil || !receipt.Novel {
		t.Fatalf("lost original eligibility: %+v %v", receipt, err)
	}
	// Tracking parameters must not turn an existing document into a paid contribution.
	receipt, err = s.fetchCommunityReceipt(ctx, "different-job-123", "https://example.com/guide?utm_source=credits", nil)
	if err != nil || receipt.Novel {
		t.Fatalf("existing canonical document rewarded: %+v %v", receipt, err)
	}
}
