package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// The lane field must be optional and backward compatible: an omitted/empty
// lane keeps the historical default (Seed → discovered), never the
// parseLaneName("") default (submitted).
func TestHandleCrawlEnqueueLane(t *testing.T) {
	var seedCalls, laneCalls int
	var gotLane byte
	s := &pebbleHTTP{
		crawlSeed:     func(string) error { seedCalls++; return nil },
		crawlSeedLane: func(_ string, lane byte) error { laneCalls++; gotLane = lane; return nil },
	}

	post := func(body string) int {
		req := httptest.NewRequest(http.MethodPost, "/admin/crawl-enqueue", strings.NewReader(body))
		rec := httptest.NewRecorder()
		s.handleCrawlEnqueue(rec, req)
		return rec.Code
	}

	if code := post(`{"url":"https://example.com/a"}`); code != http.StatusOK {
		t.Fatalf("no lane: code %d", code)
	}
	if seedCalls != 1 || laneCalls != 0 {
		t.Errorf("empty lane must use crawlSeed: seed=%d lane=%d", seedCalls, laneCalls)
	}

	if code := post(`{"url":"https://example.com/b","lane":"bulk"}`); code != http.StatusOK {
		t.Fatalf("bulk lane: code %d", code)
	}
	if laneCalls != 1 || gotLane != 3 {
		t.Errorf("lane=bulk: laneCalls=%d gotLane=%d, want 1/3", laneCalls, gotLane)
	}

	if code := post(`{"url":"https://example.com/c","lane":"refresh"}`); code != http.StatusOK {
		t.Fatalf("refresh lane: code %d", code)
	}
	if gotLane != 1 {
		t.Errorf("lane=refresh: gotLane=%d, want 1", gotLane)
	}
}
