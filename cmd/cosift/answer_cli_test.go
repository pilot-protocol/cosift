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

func newAnswerStub(t *testing.T, resp server.AnswerResponse) (*httptest.Server, func() *url.URL) {
	t.Helper()
	var captured *url.URL
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/answer" {
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

func captureAnswerStdout(t *testing.T, f func()) string {
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

func TestRunAnswerCLIJSONPassthrough(t *testing.T) {
	pub := time.Date(2026, 5, 20, 0, 0, 0, 0, time.UTC)
	stub := server.AnswerResponse{
		Query:  "what is bm25",
		Answer: "BM25 is a probabilistic ranking function [1].",
		Sources: []server.AnswerSource{
			{ID: 1, URL: "https://x/bm25", Title: "BM25", Domain: "x", PublishedAt: &pub},
		},
		Took: "1.4s",
	}
	srv, _ := newAnswerStub(t, stub)
	cfg := config.Default()
	out := captureAnswerStdout(t, func() {
		if err := runAnswerCLI(context.Background(), cfg, "what is bm25",
			[]string{"-server", srv.URL, "-json"}); err != nil {
			t.Fatalf("runAnswerCLI: %v", err)
		}
	})
	var decoded server.AnswerResponse
	if err := json.Unmarshal([]byte(out), &decoded); err != nil {
		t.Fatalf("decode output: %v\nout=%q", err, out)
	}
	if decoded.Answer != stub.Answer || len(decoded.Sources) != 1 {
		t.Errorf("roundtrip mismatch: %+v", decoded)
	}
}

func TestRunAnswerCLIHumanOutput(t *testing.T) {
	stub := server.AnswerResponse{
		Query:  "what is bm25",
		Answer: "BM25 is a probabilistic ranking function [1].",
		Sources: []server.AnswerSource{
			{ID: 1, URL: "https://x/bm25", Title: "BM25 paper", Domain: "x"},
		},
	}
	srv, _ := newAnswerStub(t, stub)
	cfg := config.Default()
	out := captureAnswerStdout(t, func() {
		if err := runAnswerCLI(context.Background(), cfg, "what is bm25",
			[]string{"-server", srv.URL}); err != nil {
			t.Fatalf("runAnswerCLI: %v", err)
		}
	})
	for _, want := range []string{"Q: what is bm25", "BM25 is a probabilistic", "Sources:", "https://x/bm25", "BM25 paper", "[x]"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in output: %q", want, out)
		}
	}
	if strings.HasPrefix(strings.TrimSpace(out), "{") {
		t.Errorf("human output looked like JSON: %q", out)
	}
}

func TestRunAnswerCLIFlagsToQueryParams(t *testing.T) {
	srv, captured := newAnswerStub(t, server.AnswerResponse{Query: "x", Answer: "y"})
	cfg := config.Default()
	_ = captureAnswerStdout(t, func() {
		if err := runAnswerCLI(context.Background(), cfg, "what is bm25",
			[]string{"-server", srv.URL, "-k", "7", "-expand"}); err != nil {
			t.Fatalf("runAnswerCLI: %v", err)
		}
	})
	got := captured()
	if got == nil {
		t.Fatal("handler never recorded a request")
	}
	q := got.Query()
	if q.Get("q") != "what is bm25" {
		t.Errorf("q not forwarded: %q", q.Get("q"))
	}
	if q.Get("k") != "7" {
		t.Errorf("k not forwarded: %q", q.Get("k"))
	}
	if q.Get("expand") != "true" {
		t.Errorf("expand not forwarded: %q", q.Get("expand"))
	}
}

func TestRunAnswerCLIKValidation(t *testing.T) {
	cfg := config.Default()
	// /answer's server-side cap is 20 (vs /search's 100) — CLI should match.
	if err := runAnswerCLI(context.Background(), cfg, "q", []string{"-k", "0"}); err == nil {
		t.Errorf("k=0 should error")
	}
	if err := runAnswerCLI(context.Background(), cfg, "q", []string{"-k", "21"}); err == nil {
		t.Errorf("k=21 (>20) should error")
	}
}

func TestRunAnswerCLINoSources(t *testing.T) {
	stub := server.AnswerResponse{Query: "q", Answer: "no coverage in sources", Sources: nil}
	srv, _ := newAnswerStub(t, stub)
	cfg := config.Default()
	out := captureAnswerStdout(t, func() {
		if err := runAnswerCLI(context.Background(), cfg, "q",
			[]string{"-server", srv.URL}); err != nil {
			t.Fatalf("runAnswerCLI: %v", err)
		}
	})
	if !strings.Contains(out, "no coverage in sources") {
		t.Errorf("missing answer: %q", out)
	}
	if strings.Contains(out, "Sources:") {
		t.Errorf("Sources: header should be suppressed when empty: %q", out)
	}
}

func TestRunAnswerCLIMarkdownFormat(t *testing.T) {
	pub := time.Date(2026, 5, 22, 0, 0, 0, 0, time.UTC)
	stub := server.AnswerResponse{
		Query:  "what is bm25",
		Answer: "BM25 is a probabilistic ranking function [1].",
		Sources: []server.AnswerSource{
			{ID: 1, URL: "https://x/bm25", Title: "BM25", Domain: "x", PublishedAt: &pub},
		},
	}
	srv, _ := newAnswerStub(t, stub)
	cfg := config.Default()
	for _, alias := range []string{"md", "markdown"} {
		t.Run(alias, func(t *testing.T) {
			out := captureAnswerStdout(t, func() {
				if err := runAnswerCLI(context.Background(), cfg, "what is bm25",
					[]string{"-server", srv.URL, "-format", alias}); err != nil {
					t.Fatalf("runAnswerCLI: %v", err)
				}
			})
			if !strings.HasPrefix(out, "# what is bm25") {
				t.Errorf("markdown should start with `# <query>`: %q", out)
			}
			// /answer has no strategy line — the blockquote shouldn't appear.
			if strings.Contains(out, "> Strategy:") {
				t.Errorf("/answer markdown should NOT include Strategy: %q", out)
			}
			if !strings.Contains(out, "## Sources") {
				t.Errorf("missing Sources header: %q", out)
			}
			if !strings.Contains(out, "1. [BM25](https://x/bm25)") {
				t.Errorf("missing source link: %q", out)
			}
			if !strings.Contains(out, "x, 2026-05-22") {
				t.Errorf("missing source trailing metadata: %q", out)
			}
		})
	}
}

func TestRunAnswerCLIFormatValidation(t *testing.T) {
	cfg := config.Default()
	if err := runAnswerCLI(context.Background(), cfg, "q", []string{"-format", "yaml"}); err == nil {
		t.Errorf("invalid format value should error")
	}
}

func TestRunAnswerCLIServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "rate limited", http.StatusTooManyRequests)
	}))
	t.Cleanup(srv.Close)
	cfg := config.Default()
	err := runAnswerCLI(context.Background(), cfg, "q", []string{"-server", srv.URL})
	if err == nil {
		t.Fatal("expected error from 429 response")
	}
	if !strings.Contains(err.Error(), "429") {
		t.Errorf("error should surface status: %v", err)
	}
}

func TestRunAnswerCLIExpandOmittedWhenFalse(t *testing.T) {
	// Default expand=false → no `expand` query param. Lets server defaults apply.
	srv, captured := newAnswerStub(t, server.AnswerResponse{Query: "q", Answer: "y"})
	cfg := config.Default()
	_ = captureAnswerStdout(t, func() {
		if err := runAnswerCLI(context.Background(), cfg, "q",
			[]string{"-server", srv.URL}); err != nil {
			t.Fatalf("runAnswerCLI: %v", err)
		}
	})
	got := captured()
	if got == nil {
		t.Fatal("handler never recorded a request")
	}
	if _, present := got.Query()["expand"]; present {
		t.Errorf("expand should be absent without -expand flag; got %q", got.RawQuery)
	}
}
