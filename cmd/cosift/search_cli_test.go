package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/server"
)

// newSearchStubServer returns an httptest server that records the last /search
// URL and replies with the canned response. Lets the CLI tests verify both the
// wire-level request shape (filters/flags map correctly) and the output side.
func newSearchStubServer(t *testing.T, resp server.SearchResponse) (*httptest.Server, *url.URL) {
	t.Helper()
	var last *url.URL
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/search" {
			http.NotFound(w, r)
			return
		}
		u := *r.URL
		last = &u
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(srv.Close)
	// last is updated by the handler; tests dereference it after runSearchCLI.
	// Return a pointer-to-pointer dance via closure isn't worth it; tests grab
	// last from the closure indirectly by reading srv.URL + the handler state.
	// Instead, expose last via a separate accessor pattern below.
	_ = last
	return srv, parseURL(t, srv.URL)
}

// newSearchStubWithCapture is the same as newSearchStubServer but exposes the
// captured request URL through a callback rather than a return value, since
// Go closures over locals work cleanly that way.
func newSearchStubWithCapture(t *testing.T, resp server.SearchResponse) (*httptest.Server, func() *url.URL) {
	t.Helper()
	var captured *url.URL
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/search" {
			http.NotFound(w, r)
			return
		}
		u := *r.URL
		captured = &u
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(srv.Close)
	return srv, func() *url.URL { return captured }
}

func parseURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse %q: %v", raw, err)
	}
	return u
}

// captureSearchStdout mirrors the helper in query_test.go (private to package main).
func captureSearchStdout(t *testing.T, f func()) string {
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

func TestRunSearchCLIJSONPassthrough(t *testing.T) {
	published := time.Date(2026, 5, 20, 0, 0, 0, 0, time.UTC)
	stubResp := server.SearchResponse{
		Query: "concurrent",
		K:     3,
		Hits: []server.SearchHit{
			{URL: "https://x/go", Title: "Go", Score: 1.5, Source: "bm25", Domain: "x", PublishedAt: &published},
			{URL: "https://x/rust", Title: "Rust", Score: 1.2, Source: "bm25"},
		},
		Took: "12ms",
	}
	srv, _ := newSearchStubWithCapture(t, stubResp)

	cfg := config.Default()
	out := captureSearchStdout(t, func() {
		if err := runSearchCLI(context.Background(), cfg, "concurrent",
			[]string{"-server", srv.URL, "-json"}); err != nil {
			t.Fatalf("runSearchCLI: %v", err)
		}
	})
	// JSON passthrough — output should decode back to the same shape.
	var decoded server.SearchResponse
	if err := json.Unmarshal([]byte(out), &decoded); err != nil {
		t.Fatalf("decode output: %v\nout=%q", err, out)
	}
	if decoded.Query != "concurrent" {
		t.Errorf("query mismatch: %q", decoded.Query)
	}
	if len(decoded.Hits) != 2 {
		t.Errorf("want 2 hits, got %d", len(decoded.Hits))
	}
	if decoded.Hits[0].URL != "https://x/go" {
		t.Errorf("top hit URL: %q", decoded.Hits[0].URL)
	}
}

func TestRunSearchCLIHumanOutput(t *testing.T) {
	stubResp := server.SearchResponse{
		Query: "go",
		K:     1,
		Hits: []server.SearchHit{
			{URL: "https://x/go", Title: "Go concurrent", Score: 1.5, Source: "bm25", Domain: "x", Excerpt: "Go has goroutines"},
		},
	}
	srv, _ := newSearchStubWithCapture(t, stubResp)

	cfg := config.Default()
	out := captureSearchStdout(t, func() {
		if err := runSearchCLI(context.Background(), cfg, "go",
			[]string{"-server", srv.URL}); err != nil {
			t.Fatalf("runSearchCLI: %v", err)
		}
	})
	// Human output must NOT be JSON.
	if strings.HasPrefix(strings.TrimSpace(out), "[") || strings.HasPrefix(strings.TrimSpace(out), "{") {
		t.Errorf("human output started with JSON delimiter: %q", out)
	}
	if !strings.Contains(out, "https://x/go") {
		t.Errorf("missing URL: %q", out)
	}
	if !strings.Contains(out, "Go concurrent") {
		t.Errorf("missing title: %q", out)
	}
	if !strings.Contains(out, "Go has goroutines") {
		t.Errorf("missing excerpt: %q", out)
	}
}

func TestRunSearchCLIFlagsMapToQueryParams(t *testing.T) {
	stubResp := server.SearchResponse{Query: "x", K: 5}
	srv, captured := newSearchStubWithCapture(t, stubResp)

	cfg := config.Default()
	_ = captureSearchStdout(t, func() {
		if err := runSearchCLI(context.Background(), cfg, "concurrent goroutines",
			[]string{
				"-server", srv.URL,
				"-k", "5",
				"-retriever", "hybrid",
				"-rerank",
				"-expand",
				"-since", "2026-01-01",
				"-until", "2026-12-31",
				"-include-domains", "example.com,test.org",
				"-exclude-domains", "spam.io",
				"-sort", "date_desc",
				"-json",
			}); err != nil {
			t.Fatalf("runSearchCLI: %v", err)
		}
	})
	got := captured()
	if got == nil {
		t.Fatal("handler never recorded a request")
	}
	q := got.Query()
	cases := map[string]string{
		"q":               "concurrent goroutines",
		"k":               "5",
		"retriever":       "hybrid",
		"rerank":          "true",
		"expand":          "true",
		"since":           "2026-01-01",
		"until":           "2026-12-31",
		"include_domains": "example.com,test.org",
		"exclude_domains": "spam.io",
		"sort":            "date_desc",
	}
	for k, want := range cases {
		if q.Get(k) != want {
			t.Errorf("query[%s] = %q, want %q", k, q.Get(k), want)
		}
	}
}

func TestRunSearchCLIServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "bad request", http.StatusBadRequest)
	}))
	t.Cleanup(srv.Close)

	cfg := config.Default()
	err := runSearchCLI(context.Background(), cfg, "x", []string{"-server", srv.URL})
	if err == nil {
		t.Fatal("expected error from 400 response, got nil")
	}
	if !strings.Contains(err.Error(), "400") {
		t.Errorf("error should mention status: %v", err)
	}
}

func TestRunSearchCLIKValidation(t *testing.T) {
	cfg := config.Default()
	// k=0 and k=1000 fail before any HTTP call.
	if err := runSearchCLI(context.Background(), cfg, "x", []string{"-k", "0"}); err == nil {
		t.Errorf("k=0 should error")
	}
	if err := runSearchCLI(context.Background(), cfg, "x", []string{"-k", "1000"}); err == nil {
		t.Errorf("k=1000 should error")
	}
}

func TestRunSearchCLIMarkdownFormat(t *testing.T) {
	pub := time.Date(2026, 5, 22, 0, 0, 0, 0, time.UTC)
	stub := server.SearchResponse{
		Query: "bm25",
		K:     2,
		Hits: []server.SearchHit{
			{URL: "https://x/a", Title: "BM25", Score: 0.873, Source: "bm25", Domain: "x", PublishedAt: &pub, Excerpt: "BM25 ranks docs."},
			{URL: "https://y/b", Title: "TF-IDF", Score: 0.542, Source: "bm25"},
		},
	}
	srv, _ := newSearchStubWithCapture(t, stub)
	cfg := config.Default()
	for _, alias := range []string{"md", "markdown"} {
		t.Run(alias, func(t *testing.T) {
			out := captureSearchStdout(t, func() {
				if err := runSearchCLI(context.Background(), cfg, "bm25",
					[]string{"-server", srv.URL, "-format", alias}); err != nil {
					t.Fatalf("runSearchCLI: %v", err)
				}
			})
			if !strings.HasPrefix(out, "# Results: bm25") {
				t.Errorf("markdown should start with `# Results:`: %q", out)
			}
			for _, want := range []string{
				"## 1. [BM25](https://x/a)",
				"_Score: 0.873 · x · 2026-05-22_",
				"> BM25 ranks docs.",
				"## 2. [TF-IDF](https://y/b)",
				"_Score: 0.542_",
			} {
				if !strings.Contains(out, want) {
					t.Errorf("missing %q in output: %q", want, out)
				}
			}
		})
	}
}

func TestRunSearchCLIFormatValidation(t *testing.T) {
	cfg := config.Default()
	if err := runSearchCLI(context.Background(), cfg, "q", []string{"-format", "yaml"}); err == nil {
		t.Errorf("invalid format value should error")
	}
}

func TestRunSearchCLIEmptyHits(t *testing.T) {
	srv, _ := newSearchStubWithCapture(t, server.SearchResponse{Query: "miss", K: 10, Hits: nil})
	cfg := config.Default()
	out := captureSearchStdout(t, func() {
		if err := runSearchCLI(context.Background(), cfg, "miss",
			[]string{"-server", srv.URL}); err != nil {
			t.Fatalf("runSearchCLI: %v", err)
		}
	})
	if !strings.Contains(out, "no results") {
		t.Errorf("empty result hint missing: %q", out)
	}
}
