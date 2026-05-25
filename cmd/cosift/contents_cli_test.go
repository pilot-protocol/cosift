package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/calinteodor/cosift/internal/config"
	"github.com/calinteodor/cosift/internal/server"
)

// newContentsStub returns an httptest server that handles both GET /contents
// (single) and POST /contents (batch). Captures the last request method,
// query, and decoded body for assertions.
func newContentsStub(t *testing.T, single server.ContentsResponse, batch server.ContentsBatchResponse) (*httptest.Server, func() (method string, query string, batchReq *server.ContentsBatchRequest)) {
	t.Helper()
	var (
		lastMethod string
		lastQuery  string
		lastBatch  *server.ContentsBatchRequest
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/contents" {
			http.NotFound(w, r)
			return
		}
		lastMethod = r.Method
		lastQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			_ = json.NewEncoder(w).Encode(single)
		case http.MethodPost:
			var req server.ContentsBatchRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			lastBatch = &req
			_ = json.NewEncoder(w).Encode(batch)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, func() (string, string, *server.ContentsBatchRequest) {
		return lastMethod, lastQuery, lastBatch
	}
}

func captureContentsStdout(t *testing.T, f func()) string {
	t.Helper()
	orig := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout = w
	done := make(chan []byte)
	go func() {
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, r)
		done <- buf.Bytes()
	}()
	f()
	_ = w.Close()
	os.Stdout = orig
	return string(<-done)
}

func TestRunContentsCLISingleGET(t *testing.T) {
	fetched := time.Date(2026, 5, 20, 12, 0, 0, 0, time.UTC)
	single := server.ContentsResponse{
		URL: "https://x/a", Title: "Article A", Text: "Body of A.",
		Cached: true, FetchedAt: fetched, Lang: "en",
	}
	srv, capture := newContentsStub(t, single, server.ContentsBatchResponse{})
	cfg := config.Default()
	out := captureContentsStdout(t, func() {
		if err := runContentsCLI(context.Background(), cfg,
			[]string{"-server", srv.URL, "https://x/a"}); err != nil {
			t.Fatalf("runContentsCLI: %v", err)
		}
	})
	method, query, batch := capture()
	if method != http.MethodGet {
		t.Errorf("want GET, got %s", method)
	}
	if !strings.Contains(query, "url=https") {
		t.Errorf("query missing url param: %q", query)
	}
	if batch != nil {
		t.Errorf("single mode should not POST batch: %+v", batch)
	}
	for _, want := range []string{"https://x/a", "Article A", "Body of A.", "Cached:     true", "Lang:       en"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in output: %q", want, out)
		}
	}
}

func TestRunContentsCLIBatchPOSTFromPositionalArgs(t *testing.T) {
	batch := server.ContentsBatchResponse{
		Results: []server.ContentsBatchItem{
			{URL: "https://x/a", Found: true, Title: "A", Text: "Body of A.", Cached: true},
			{URL: "https://x/b", Found: false, Error: "not in index"},
		},
		Took: "5ms",
	}
	srv, capture := newContentsStub(t, server.ContentsResponse{}, batch)
	cfg := config.Default()
	out := captureContentsStdout(t, func() {
		if err := runContentsCLI(context.Background(), cfg,
			[]string{"-server", srv.URL, "https://x/a", "https://x/b"}); err != nil {
			t.Fatalf("runContentsCLI: %v", err)
		}
	})
	method, _, br := capture()
	if method != http.MethodPost {
		t.Errorf("multi-URL should POST, got %s", method)
	}
	if br == nil || len(br.URLs) != 2 {
		t.Fatalf("batch request not captured / wrong shape: %+v", br)
	}
	if br.URLs[0] != "https://x/a" || br.URLs[1] != "https://x/b" {
		t.Errorf("URLs not forwarded in order: %+v", br.URLs)
	}
	for _, want := range []string{"https://x/a", "https://x/b", "Body of A.", "not in index"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in output: %q", want, out)
		}
	}
}

func TestRunContentsCLIBatchPOSTFromFile(t *testing.T) {
	// -file with one URL still POSTs as batch (matches multi-URL behavior;
	// avoids surprising single-vs-batch toggle based on file contents).
	dir := t.TempDir()
	urlsPath := filepath.Join(dir, "urls.txt")
	if err := os.WriteFile(urlsPath, []byte("# comment\nhttps://x/a\n\nhttps://x/b\n"), 0644); err != nil {
		t.Fatalf("write urls file: %v", err)
	}
	batch := server.ContentsBatchResponse{
		Results: []server.ContentsBatchItem{
			{URL: "https://x/a", Found: true, Title: "A", Text: "Body of A."},
			{URL: "https://x/b", Found: true, Title: "B", Text: "Body of B."},
		},
	}
	srv, capture := newContentsStub(t, server.ContentsResponse{}, batch)
	cfg := config.Default()
	_ = captureContentsStdout(t, func() {
		if err := runContentsCLI(context.Background(), cfg,
			[]string{"-server", srv.URL, "-file", urlsPath}); err != nil {
			t.Fatalf("runContentsCLI: %v", err)
		}
	})
	method, _, br := capture()
	if method != http.MethodPost {
		t.Errorf("-file should POST batch, got %s", method)
	}
	if br == nil || len(br.URLs) != 2 {
		t.Fatalf("expected 2 URLs from file (blanks + comments dropped): %+v", br)
	}
	if br.URLs[0] != "https://x/a" || br.URLs[1] != "https://x/b" {
		t.Errorf("URLs from file wrong: %+v", br.URLs)
	}
}

func TestRunContentsCLINoURLs(t *testing.T) {
	// Empty args after flag parsing → clear error.
	cfg := config.Default()
	err := runContentsCLI(context.Background(), cfg, []string{"-server", "http://localhost:1"})
	if err == nil {
		t.Fatal("expected error when no URLs given")
	}
	if !strings.Contains(err.Error(), "URL") {
		t.Errorf("error should mention URL requirement: %v", err)
	}
}

func TestRunContentsCLITextOnlySingle(t *testing.T) {
	single := server.ContentsResponse{URL: "https://x/a", Title: "A", Text: "Just the body."}
	srv, _ := newContentsStub(t, single, server.ContentsBatchResponse{})
	cfg := config.Default()
	out := captureContentsStdout(t, func() {
		if err := runContentsCLI(context.Background(), cfg,
			[]string{"-server", srv.URL, "-text", "https://x/a"}); err != nil {
			t.Fatalf("runContentsCLI: %v", err)
		}
	})
	// `-text` strips metadata — no Title:/URL:/Cached: labels.
	for _, banned := range []string{"Title:", "URL:", "Cached:"} {
		if strings.Contains(out, banned) {
			t.Errorf("-text should strip %q, but it's present in: %q", banned, out)
		}
	}
	if !strings.Contains(out, "Just the body.") {
		t.Errorf("missing text body: %q", out)
	}
}

func TestRunContentsCLITextOnlyBatchSeparator(t *testing.T) {
	batch := server.ContentsBatchResponse{
		Results: []server.ContentsBatchItem{
			{URL: "https://x/a", Found: true, Text: "First."},
			{URL: "https://x/b", Found: true, Text: "Second."},
		},
	}
	srv, _ := newContentsStub(t, server.ContentsResponse{}, batch)
	cfg := config.Default()
	out := captureContentsStdout(t, func() {
		if err := runContentsCLI(context.Background(), cfg,
			[]string{"-server", srv.URL, "-text", "https://x/a", "https://x/b"}); err != nil {
			t.Fatalf("runContentsCLI: %v", err)
		}
	})
	if !strings.Contains(out, "First.") || !strings.Contains(out, "Second.") {
		t.Errorf("missing text bodies: %q", out)
	}
	if !strings.Contains(out, "---") {
		t.Errorf("batch -text should print --- separator between docs: %q", out)
	}
}

func TestRunContentsCLIJSONPassthroughSingle(t *testing.T) {
	single := server.ContentsResponse{URL: "https://x/a", Title: "A", Text: "Body."}
	srv, _ := newContentsStub(t, single, server.ContentsBatchResponse{})
	cfg := config.Default()
	out := captureContentsStdout(t, func() {
		if err := runContentsCLI(context.Background(), cfg,
			[]string{"-server", srv.URL, "-json", "https://x/a"}); err != nil {
			t.Fatalf("runContentsCLI: %v", err)
		}
	})
	var decoded server.ContentsResponse
	if err := json.Unmarshal([]byte(out), &decoded); err != nil {
		t.Fatalf("decode output: %v\nout=%q", err, out)
	}
	if decoded.Title != "A" {
		t.Errorf("roundtrip mismatch: %+v", decoded)
	}
}

func TestRunContentsCLIServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)
	cfg := config.Default()
	err := runContentsCLI(context.Background(), cfg, []string{"-server", srv.URL, "https://x/missing"})
	if err == nil {
		t.Fatal("expected error from 404 response")
	}
	if !strings.Contains(err.Error(), "404") {
		t.Errorf("error should surface status: %v", err)
	}
}
