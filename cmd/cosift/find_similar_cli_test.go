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

func newFindSimilarStub(t *testing.T, resp server.FindSimilarResponse) (*httptest.Server, func() *url.URL) {
	t.Helper()
	var captured *url.URL
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/find_similar" {
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

func captureFindSimilarStdout(t *testing.T, f func()) string {
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

func TestRunFindSimilarCLIJSONPassthrough(t *testing.T) {
	published := time.Date(2026, 5, 22, 0, 0, 0, 0, time.UTC)
	stub := server.FindSimilarResponse{
		URL: "https://x/seed",
		K:   3,
		Hits: []server.SearchHit{
			{URL: "https://x/a", Title: "A", Score: 0.91, Source: "dense", Domain: "x", PublishedAt: &published},
			{URL: "https://x/b", Title: "B", Score: 0.82, Source: "dense"},
		},
		Took: "12ms",
	}
	srv, _ := newFindSimilarStub(t, stub)
	cfg := config.Default()
	out := captureFindSimilarStdout(t, func() {
		if err := runFindSimilarCLI(context.Background(), cfg, "https://x/seed",
			[]string{"-server", srv.URL, "-json"}); err != nil {
			t.Fatalf("runFindSimilarCLI: %v", err)
		}
	})
	var decoded server.FindSimilarResponse
	if err := json.Unmarshal([]byte(out), &decoded); err != nil {
		t.Fatalf("decode output: %v\nout=%q", err, out)
	}
	if decoded.URL != stub.URL || len(decoded.Hits) != 2 {
		t.Errorf("roundtrip mismatch: %+v", decoded)
	}
}

func TestRunFindSimilarCLIHumanOutput(t *testing.T) {
	stub := server.FindSimilarResponse{
		URL: "https://x/seed",
		K:   1,
		Hits: []server.SearchHit{
			{URL: "https://x/a", Title: "Article A", Score: 0.91, Source: "dense", Domain: "x"},
		},
	}
	srv, _ := newFindSimilarStub(t, stub)
	cfg := config.Default()
	out := captureFindSimilarStdout(t, func() {
		if err := runFindSimilarCLI(context.Background(), cfg, "https://x/seed",
			[]string{"-server", srv.URL}); err != nil {
			t.Fatalf("runFindSimilarCLI: %v", err)
		}
	})
	for _, want := range []string{"Similar to: https://x/seed", "https://x/a", "Article A", "[x]"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in output: %q", want, out)
		}
	}
	if strings.HasPrefix(strings.TrimSpace(out), "{") {
		t.Errorf("human output looked like JSON: %q", out)
	}
}

func TestRunFindSimilarCLIFlagsToQueryParams(t *testing.T) {
	srv, captured := newFindSimilarStub(t, server.FindSimilarResponse{URL: "https://x/seed", K: 5})
	cfg := config.Default()
	_ = captureFindSimilarStdout(t, func() {
		if err := runFindSimilarCLI(context.Background(), cfg, "https://x/seed",
			[]string{"-server", srv.URL, "-k", "5", "-json"}); err != nil {
			t.Fatalf("runFindSimilarCLI: %v", err)
		}
	})
	got := captured()
	if got == nil {
		t.Fatal("handler never recorded a request")
	}
	if got.Query().Get("url") != "https://x/seed" {
		t.Errorf("url not forwarded: %q", got.Query().Get("url"))
	}
	if got.Query().Get("k") != "5" {
		t.Errorf("k not forwarded: %q", got.Query().Get("k"))
	}
}

func TestRunFindSimilarCLIServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not indexed", http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)
	cfg := config.Default()
	err := runFindSimilarCLI(context.Background(), cfg, "https://x/missing", []string{"-server", srv.URL})
	if err == nil {
		t.Fatal("expected error from 404 response")
	}
	if !strings.Contains(err.Error(), "404") {
		t.Errorf("error should surface status: %v", err)
	}
}

func TestRunFindSimilarCLIKValidation(t *testing.T) {
	cfg := config.Default()
	if err := runFindSimilarCLI(context.Background(), cfg, "https://x/seed", []string{"-k", "0"}); err == nil {
		t.Errorf("k=0 should error")
	}
	if err := runFindSimilarCLI(context.Background(), cfg, "https://x/seed", []string{"-k", "1000"}); err == nil {
		t.Errorf("k=1000 should error")
	}
}

func TestRunFindSimilarCLIMarkdownFormat(t *testing.T) {
	pub := time.Date(2026, 5, 22, 0, 0, 0, 0, time.UTC)
	stub := server.FindSimilarResponse{
		URL: "https://x/seed",
		K:   2,
		Hits: []server.SearchHit{
			{URL: "https://x/a", Title: "Neighbor A", Score: 0.91, Source: "dense", Domain: "x", PublishedAt: &pub},
			{URL: "https://y/b", Title: "Neighbor B", Score: 0.82, Source: "dense"},
		},
	}
	srv, _ := newFindSimilarStub(t, stub)
	cfg := config.Default()
	for _, alias := range []string{"md", "markdown"} {
		t.Run(alias, func(t *testing.T) {
			out := captureFindSimilarStdout(t, func() {
				if err := runFindSimilarCLI(context.Background(), cfg, "https://x/seed",
					[]string{"-server", srv.URL, "-format", alias}); err != nil {
					t.Fatalf("runFindSimilarCLI: %v", err)
				}
			})
			if !strings.HasPrefix(out, "# Similar to: https://x/seed") {
				t.Errorf("markdown should start with `# Similar to:`: %q", out)
			}
			for _, want := range []string{
				"## 1. [Neighbor A](https://x/a)",
				"_Score: 0.910 · x · 2026-05-22_",
				"## 2. [Neighbor B](https://y/b)",
				"_Score: 0.820_",
			} {
				if !strings.Contains(out, want) {
					t.Errorf("missing %q in output: %q", want, out)
				}
			}
		})
	}
}

func TestRunFindSimilarCLIFormatValidation(t *testing.T) {
	cfg := config.Default()
	if err := runFindSimilarCLI(context.Background(), cfg, "https://x/seed", []string{"-format", "yaml"}); err == nil {
		t.Errorf("invalid format value should error")
	}
}

func TestRunFindSimilarCLIEmptyHits(t *testing.T) {
	srv, _ := newFindSimilarStub(t, server.FindSimilarResponse{URL: "https://x/seed", K: 10, Hits: nil})
	cfg := config.Default()
	out := captureFindSimilarStdout(t, func() {
		if err := runFindSimilarCLI(context.Background(), cfg, "https://x/seed",
			[]string{"-server", srv.URL}); err != nil {
			t.Fatalf("runFindSimilarCLI: %v", err)
		}
	})
	if !strings.Contains(out, "no similar documents") {
		t.Errorf("empty-result hint missing: %q", out)
	}
}
