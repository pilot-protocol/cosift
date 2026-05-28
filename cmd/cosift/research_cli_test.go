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

// newResearchStub returns an httptest server replying with `resp` on /research.
// Returns an accessor for the captured request URL so flag-mapping tests can
// assert on the wire shape, mirroring's newSearchStubWithCapture.
func newResearchStub(t *testing.T, resp server.ResearchResponse) (*httptest.Server, func() *url.URL) {
	t.Helper()
	var captured *url.URL
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/research" {
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

func captureResearchStdout(t *testing.T, f func()) string {
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

func TestRunResearchCLIJSONPassthrough(t *testing.T) {
	published := time.Date(2026, 5, 22, 0, 0, 0, 0, time.UTC)
	stub := server.ResearchResponse{
		Query:    "what is bm25",
		Strategy: "planner",
		Plan:     []string{"bm25 formula", "bm25 vs tf-idf"},
		Answer:   "BM25 is a ranking function [1] that extends TF-IDF [2].",
		Sources: []server.AnswerSource{
			{ID: 1, URL: "https://x/bm25", Title: "BM25", Domain: "x", PublishedAt: &published},
			{ID: 2, URL: "https://x/tfidf", Title: "TF-IDF", Domain: "x"},
		},
		Took: "1.2s",
	}
	srv, _ := newResearchStub(t, stub)
	cfg := config.Default()
	out := captureResearchStdout(t, func() {
		if err := runResearchCLI(context.Background(), cfg, "what is bm25",
			[]string{"-server", srv.URL, "-json"}); err != nil {
			t.Fatalf("runResearchCLI: %v", err)
		}
	})
	var decoded server.ResearchResponse
	if err := json.Unmarshal([]byte(out), &decoded); err != nil {
		t.Fatalf("decode output: %v\nout=%q", err, out)
	}
	if decoded.Answer != stub.Answer {
		t.Errorf("answer mismatch: %q", decoded.Answer)
	}
	if len(decoded.Sources) != 2 || decoded.Sources[0].ID != 1 {
		t.Errorf("sources roundtrip lost shape: %+v", decoded.Sources)
	}
	if len(decoded.Plan) != 2 {
		t.Errorf("plan roundtrip lost: %+v", decoded.Plan)
	}
}

func TestRunResearchCLIHumanOutput(t *testing.T) {
	stub := server.ResearchResponse{
		Query:    "what is bm25",
		Strategy: "paraphrase",
		Plan:     []string{"bm25 explained", "ranking functions"},
		Answer:   "BM25 is a probabilistic ranking function [1].",
		Sources: []server.AnswerSource{
			{ID: 1, URL: "https://x/bm25", Title: "BM25 paper", Domain: "x"},
		},
	}
	srv, _ := newResearchStub(t, stub)
	cfg := config.Default()
	out := captureResearchStdout(t, func() {
		if err := runResearchCLI(context.Background(), cfg, "what is bm25",
			[]string{"-server", srv.URL}); err != nil {
			t.Fatalf("runResearchCLI: %v", err)
		}
	})
	// Not JSON.
	trimmed := strings.TrimSpace(out)
	if strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "[") {
		t.Errorf("human output started with JSON delimiter: %q", out)
	}
	// Required sections present.
	for _, want := range []string{"what is bm25", "paraphrase", "BM25 is a probabilistic", "Sources:", "https://x/bm25", "BM25 paper"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in human output: %q", want, out)
		}
	}
	// Plan rendered in the strategy line.
	if !strings.Contains(out, "bm25 explained") {
		t.Errorf("plan not rendered: %q", out)
	}
}

func TestRunResearchCLIStrategyFlag(t *testing.T) {
	srv, captured := newResearchStub(t, server.ResearchResponse{Query: "x", Answer: "y"})
	cfg := config.Default()
	_ = captureResearchStdout(t, func() {
		if err := runResearchCLI(context.Background(), cfg, "q",
			[]string{"-server", srv.URL, "-strategy", "paraphrase"}); err != nil {
			t.Fatalf("runResearchCLI: %v", err)
		}
	})
	got := captured()
	if got == nil {
		t.Fatal("handler never recorded a request")
	}
	if got.Query().Get("strategy") != "paraphrase" {
		t.Errorf("strategy not forwarded: %q", got.Query().Get("strategy"))
	}
	if got.Query().Get("q") != "q" {
		t.Errorf("query not forwarded: %q", got.Query().Get("q"))
	}
}

func TestRunResearchCLIStrategyOmittedWhenEmpty(t *testing.T) {
	// No -strategy → no `strategy` query param. Server gets to apply its default.
	srv, captured := newResearchStub(t, server.ResearchResponse{Query: "x", Answer: "y"})
	cfg := config.Default()
	_ = captureResearchStdout(t, func() {
		if err := runResearchCLI(context.Background(), cfg, "q",
			[]string{"-server", srv.URL}); err != nil {
			t.Fatalf("runResearchCLI: %v", err)
		}
	})
	got := captured()
	if got == nil {
		t.Fatal("handler never recorded a request")
	}
	if _, ok := got.Query()["strategy"]; ok {
		t.Errorf("strategy should be absent when flag empty; got=%q", got.RawQuery)
	}
}

func TestRunResearchCLIServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "rate limited", http.StatusTooManyRequests)
	}))
	t.Cleanup(srv.Close)
	cfg := config.Default()
	err := runResearchCLI(context.Background(), cfg, "x", []string{"-server", srv.URL})
	if err == nil {
		t.Fatal("expected error from 429 response")
	}
	if !strings.Contains(err.Error(), "429") {
		t.Errorf("error should surface status: %v", err)
	}
}

func TestRunResearchCLIMarkdownFormat(t *testing.T) {
	pub := time.Date(2026, 5, 22, 0, 0, 0, 0, time.UTC)
	stub := server.ResearchResponse{
		Query:    "what is bm25",
		Strategy: "planner",
		Plan:     []string{"bm25 formula", "bm25 vs tf-idf"},
		Answer:   "BM25 is a ranking function [1] extending TF-IDF [2].",
		Sources: []server.AnswerSource{
			{ID: 1, URL: "https://x/bm25", Title: "BM25", Domain: "x", PublishedAt: &pub},
			{ID: 2, URL: "https://x/tfidf", Title: "TF-IDF"},
		},
	}
	srv, _ := newResearchStub(t, stub)
	cfg := config.Default()
	for _, alias := range []string{"md", "markdown"} {
		t.Run(alias, func(t *testing.T) {
			out := captureResearchStdout(t, func() {
				if err := runResearchCLI(context.Background(), cfg, "what is bm25",
					[]string{"-server", srv.URL, "-format", alias}); err != nil {
					t.Fatalf("runResearchCLI: %v", err)
				}
			})
			// Heading line: `# what is bm25`
			if !strings.HasPrefix(out, "# what is bm25") {
				t.Errorf("markdown should start with `# <query>`: %q", out)
			}
			// Strategy blockquote with `Strategy:` and the plan.
			if !strings.Contains(out, "> Strategy: `planner`") {
				t.Errorf("missing strategy blockquote: %q", out)
			}
			if !strings.Contains(out, "bm25 formula | bm25 vs tf-idf") {
				t.Errorf("missing plan: %q", out)
			}
			// Sources header (level-2) and link syntax.
			if !strings.Contains(out, "## Sources") {
				t.Errorf("missing Sources header: %q", out)
			}
			if !strings.Contains(out, "1. [BM25](https://x/bm25)") {
				t.Errorf("missing source 1 link: %q", out)
			}
			if !strings.Contains(out, "2. [TF-IDF](https://x/tfidf)") {
				t.Errorf("missing source 2 link: %q", out)
			}
			// Trailing metadata (domain + date) for source 1; undated source 2 has only domain... actually source 2 has no domain either, so it should have NO em-dash trailer.
			if !strings.Contains(out, "x, 2026-05-22") {
				t.Errorf("missing source 1 trailing metadata: %q", out)
			}
		})
	}
}

func TestRunResearchCLIFormatValidation(t *testing.T) {
	cfg := config.Default()
	if err := runResearchCLI(context.Background(), cfg, "q", []string{"-format", "yaml"}); err == nil {
		t.Errorf("invalid format value should error")
	}
}

func TestRunResearchCLINoSources(t *testing.T) {
	// Answer can render without a Sources section.
	stub := server.ResearchResponse{Query: "q", Answer: "no source coverage", Sources: nil}
	srv, _ := newResearchStub(t, stub)
	cfg := config.Default()
	out := captureResearchStdout(t, func() {
		if err := runResearchCLI(context.Background(), cfg, "q",
			[]string{"-server", srv.URL}); err != nil {
			t.Fatalf("runResearchCLI: %v", err)
		}
	})
	if !strings.Contains(out, "no source coverage") {
		t.Errorf("answer missing: %q", out)
	}
	if strings.Contains(out, "Sources:") {
		t.Errorf("Sources: header should be suppressed when empty: %q", out)
	}
}
