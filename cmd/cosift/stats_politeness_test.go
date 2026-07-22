package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestStatsAndMetricsExposePolitenessCounters(t *testing.T) {
	f := populatedPebbleStore(t)
	srv := f.makeServer(nil)
	srv.crawlActive = true
	srv.crawlPoliteness = func() (int64, int64) { return 7, 3 }

	body, err := srv.buildStatsBody(context.Background())
	if err != nil {
		t.Fatalf("buildStatsBody: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got := out["crawl_dropped_disallowed"]; got != float64(7) {
		t.Errorf("crawl_dropped_disallowed: got %v want 7", got)
	}
	if got := out["crawl_rate_limited_deferrals"]; got != float64(3) {
		t.Errorf("crawl_rate_limited_deferrals: got %v want 3", got)
	}

	rec := httptest.NewRecorder()
	srv.handleMetrics(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics: code %d", rec.Code)
	}
	metrics := rec.Body.String()
	for _, want := range []string{
		"cosift_crawl_dropped_disallowed_total 7",
		"cosift_crawl_rate_limited_deferrals_total 3",
	} {
		if !strings.Contains(metrics, want) {
			t.Errorf("/metrics missing %q", want)
		}
	}
}

func TestStatsOmitsPolitenessWithoutHook(t *testing.T) {
	f := populatedPebbleStore(t)
	srv := f.makeServer(nil)
	srv.crawlActive = true

	body, err := srv.buildStatsBody(context.Background())
	if err != nil {
		t.Fatalf("buildStatsBody: %v", err)
	}
	if strings.Contains(string(body), "crawl_dropped_disallowed") {
		t.Error("politeness keys present without a crawlPoliteness hook")
	}
}
