package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pilot-protocol/cosift/internal/config"
)

// --- truncateForEmbedLite ---

func TestTruncateForEmbedLiteShort(t *testing.T) {
	if got := truncateForEmbedLite("hello", 100); got != "hello" {
		t.Errorf("got %q", got)
	}
}

func TestTruncateForEmbedLiteTruncates(t *testing.T) {
	// tokenCap=2 → maxBytes=8.
	s := "0123456789ABCDEF"
	got := truncateForEmbedLite(s, 2)
	if len(got) != 8 {
		t.Errorf("got len %d, want 8", len(got))
	}
	if got != "01234567" {
		t.Errorf("got %q", got)
	}
}

// --- responseRecorder ---

func TestNewResponseRecorderDefaults(t *testing.T) {
	r := newResponseRecorder()
	if r.code != 200 {
		t.Errorf("default code = %d, want 200", r.code)
	}
	if r.hdr == nil {
		t.Error("hdr is nil")
	}
}

func TestResponseRecorderWriteAndHeader(t *testing.T) {
	r := newResponseRecorder()
	r.Header().Set("X-Test", "yes")
	if got, _ := r.Write([]byte("hello")); got != 5 {
		t.Errorf("write returned %d", got)
	}
	if r.body.String() != "hello" {
		t.Errorf("body = %q", r.body.String())
	}
	if r.hdr.Get("X-Test") != "yes" {
		t.Errorf("header lost")
	}
}

func TestResponseRecorderWriteHeader(t *testing.T) {
	r := newResponseRecorder()
	r.WriteHeader(http.StatusCreated)
	if r.code != http.StatusCreated {
		t.Errorf("code = %d", r.code)
	}
}

// --- auth-gated handlers: unauthorized path ---

func TestHandleCheckpointUnauthorized(t *testing.T) {
	s := &pebbleHTTP{cluster: config.Cluster{PeerAuthToken: "secret"}}
	req := httptest.NewRequest(http.MethodPost, "/admin/checkpoint", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	rec := httptest.NewRecorder()
	s.handleCheckpoint(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("code = %d, want 401", rec.Code)
	}
}

func TestHandlePQEncodeUnauthorized(t *testing.T) {
	s := &pebbleHTTP{cluster: config.Cluster{PeerAuthToken: "secret"}}
	req := httptest.NewRequest(http.MethodPost, "/admin/pq-encode", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	rec := httptest.NewRecorder()
	s.handlePQEncode(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("code = %d, want 401", rec.Code)
	}
}

func TestHandlePQEncodeNoHNSW(t *testing.T) {
	// Auth disabled (empty token) and no hnsw → 400.
	s := &pebbleHTTP{}
	req := httptest.NewRequest(http.MethodPost, "/admin/pq-encode", nil)
	rec := httptest.NewRecorder()
	s.handlePQEncode(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("code = %d, want 400", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "HNSW") {
		t.Errorf("missing HNSW hint: %s", rec.Body.String())
	}
}

func TestHandlePQTrainUnauthorized(t *testing.T) {
	s := &pebbleHTTP{cluster: config.Cluster{PeerAuthToken: "secret"}}
	req := httptest.NewRequest(http.MethodPost, "/admin/pq-train",
		bytes.NewReader([]byte(`{}`)))
	req.Header.Set("Authorization", "Bearer wrong-token")
	rec := httptest.NewRecorder()
	s.handlePQTrain(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("code = %d, want 401", rec.Code)
	}
}

func TestHandleEmbedBackfillUnauthorized(t *testing.T) {
	s := &pebbleHTTP{cluster: config.Cluster{PeerAuthToken: "secret"}}
	req := httptest.NewRequest(http.MethodPost, "/admin/embed-backfill",
		bytes.NewReader([]byte(`{}`)))
	req.Header.Set("Authorization", "Bearer wrong-token")
	rec := httptest.NewRecorder()
	s.handleEmbedBackfill(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("code = %d, want 401", rec.Code)
	}
}

func TestHandleEmbedBackfillMissingDeps(t *testing.T) {
	s := &pebbleHTTP{}
	req := httptest.NewRequest(http.MethodPost, "/admin/embed-backfill",
		bytes.NewReader([]byte(`{}`)))
	rec := httptest.NewRecorder()
	s.handleEmbedBackfill(rec, req)
	if rec.Code != http.StatusNotImplemented {
		t.Errorf("code = %d, want 501", rec.Code)
	}
}

func TestHandleHNSWCompactUnauthorized(t *testing.T) {
	s := &pebbleHTTP{cluster: config.Cluster{PeerAuthToken: "secret"}}
	req := httptest.NewRequest(http.MethodPost, "/admin/hnsw-compact", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	rec := httptest.NewRecorder()
	s.handleHNSWCompact(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("code = %d, want 401", rec.Code)
	}
}

func TestHandleFrontierPurgeHostUnauthorized(t *testing.T) {
	s := &pebbleHTTP{cluster: config.Cluster{PeerAuthToken: "secret"}}
	req := httptest.NewRequest(http.MethodPost, "/admin/frontier-purge-host", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	rec := httptest.NewRecorder()
	s.handleFrontierPurgeHost(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("code = %d, want 401", rec.Code)
	}
}

func TestHandleSitemapImportUnauthorized(t *testing.T) {
	s := &pebbleHTTP{cluster: config.Cluster{PeerAuthToken: "secret"}}
	req := httptest.NewRequest(http.MethodPost, "/admin/sitemap-import", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	rec := httptest.NewRecorder()
	s.handleSitemapImport(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("code = %d, want 401", rec.Code)
	}
}

func TestHandleSitePackUnauthorized(t *testing.T) {
	s := &pebbleHTTP{cluster: config.Cluster{PeerAuthToken: "secret"}}
	req := httptest.NewRequest(http.MethodPost, "/admin/site-pack",
		bytes.NewReader([]byte(`{"host":"example.com"}`)))
	req.Header.Set("Authorization", "Bearer wrong-token")
	rec := httptest.NewRecorder()
	s.handleSitePack(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("code = %d, want 401", rec.Code)
	}
}

func TestHandleCrawlEnqueueUnauthorized(t *testing.T) {
	s := &pebbleHTTP{cluster: config.Cluster{PeerAuthToken: "secret"}}
	req := httptest.NewRequest(http.MethodPost, "/admin/crawl-enqueue",
		bytes.NewReader([]byte(`{"url":"https://example.com"}`)))
	req.Header.Set("Authorization", "Bearer wrong-token")
	rec := httptest.NewRecorder()
	s.handleCrawlEnqueue(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("code = %d, want 401", rec.Code)
	}
}

func TestHandleCrawlNowUnauthorized(t *testing.T) {
	s := &pebbleHTTP{cluster: config.Cluster{PeerAuthToken: "secret"}}
	req := httptest.NewRequest(http.MethodPost, "/admin/crawl-now", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	rec := httptest.NewRecorder()
	s.handleCrawlNow(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("code = %d, want 401", rec.Code)
	}
}

func TestHandleEvalQuickUnauthorized(t *testing.T) {
	s := &pebbleHTTP{cluster: config.Cluster{PeerAuthToken: "secret"}}
	req := httptest.NewRequest(http.MethodPost, "/admin/eval-quick", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	rec := httptest.NewRecorder()
	s.handleEvalQuick(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("code = %d, want 401", rec.Code)
	}
}

func TestHandleWETImportUnauthorized(t *testing.T) {
	s := &pebbleHTTP{cluster: config.Cluster{PeerAuthToken: "secret"}}
	req := httptest.NewRequest(http.MethodPost, "/admin/wet-import", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	rec := httptest.NewRecorder()
	s.handleWETImport(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("code = %d, want 401", rec.Code)
	}
}

func TestHandleRSSImportUnauthorized(t *testing.T) {
	s := &pebbleHTTP{cluster: config.Cluster{PeerAuthToken: "secret"}}
	req := httptest.NewRequest(http.MethodPost, "/admin/rss-import", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	rec := httptest.NewRecorder()
	s.handleRSSImport(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("code = %d, want 401", rec.Code)
	}
}
