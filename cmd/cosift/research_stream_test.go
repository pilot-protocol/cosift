package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/pilot-protocol/cosift/internal/config"
)

// newResearchSSEStub returns an httptest server that emits the supplied SSE
// events on /research?stream=true. Each event in `events` is a (name, data)
// pair; data is encoded verbatim into the stream (caller passes JSON strings).
func newResearchSSEStub(t *testing.T, events [][2]string) (*httptest.Server, func() *url.URL) {
	t.Helper()
	var captured *url.URL
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/research" {
			http.NotFound(w, r)
			return
		}
		u := *r.URL
		captured = &u
		// SSE handshake.
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.WriteHeader(http.StatusOK)
		f, ok := w.(http.Flusher)
		if !ok {
			t.Fatal("httptest.ResponseWriter not a flusher")
		}
		for _, ev := range events {
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ev[0], ev[1])
			f.Flush()
		}
	}))
	t.Cleanup(srv.Close)
	return srv, func() *url.URL { return captured }
}

func captureStreamStdout(t *testing.T, f func()) string {
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

func TestRunResearchCLIStreamHappyPath(t *testing.T) {
	events := [][2]string{
		{"plan", `{"strategy":"planner","variants":["q1","q2"]}`},
		{"retrieved", `{"variant":"q1","urls":["https://x/a","https://x/b"]}`},
		{"retrieved", `{"variant":"q2","urls":["https://x/c"]}`},
		{"synthesizing", `{"sources":3}`},
		{"answer_chunk", `{"text":"BM25 is "}`},
		{"answer_chunk", `{"text":"a ranking function [1]."}`},
		{"done", `{"query":"what is bm25","strategy":"planner","plan":["q1","q2"],"answer":"BM25 is a ranking function [1].","sources":[{"id":1,"url":"https://x/a","title":"BM25"}],"took":"500ms"}`},
	}
	srv, captured := newResearchSSEStub(t, events)
	cfg := config.Default()
	out := captureStreamStdout(t, func() {
		if err := runResearchCLI(context.Background(), cfg, "what is bm25",
			[]string{"-server", srv.URL, "-stream"}); err != nil {
			t.Fatalf("runResearchCLI stream: %v", err)
		}
	})
	got := captured()
	if got == nil {
		t.Fatal("stub never recorded request")
	}
	if got.Query().Get("stream") != "true" {
		t.Errorf("-stream flag should set stream=true: %q", got.RawQuery)
	}
	// Plan event renders strategy line.
	if !strings.Contains(out, "Strategy: planner") || !strings.Contains(out, "q1 | q2") {
		t.Errorf("plan event not rendered: %q", out)
	}
	// Retrieved events render per-variant progress.
	if !strings.Contains(out, `[retrieved 2 url(s) for "q1"]`) {
		t.Errorf("retrieved event not rendered: %q", out)
	}
	// Synthesizing event.
	if !strings.Contains(out, "[synthesizing answer over 3 source(s)]") {
		t.Errorf("synthesizing event not rendered: %q", out)
	}
	// Answer chunks concatenated (no newline between them).
	if !strings.Contains(out, "BM25 is a ranking function [1].") {
		t.Errorf("answer chunks not concatenated: %q", out)
	}
	// Sources section from done event.
	if !strings.Contains(out, "Sources:") || !strings.Contains(out, "https://x/a") {
		t.Errorf("sources from done event not rendered: %q", out)
	}
}

func TestRunResearchCLIStreamErrorEvent(t *testing.T) {
	events := [][2]string{
		{"plan", `{"strategy":"planner","variants":["q"]}`},
		{"error", `{"detail":"plan failed: timeout"}`},
	}
	srv, _ := newResearchSSEStub(t, events)
	cfg := config.Default()
	err := runResearchCLI(context.Background(), cfg, "q",
		[]string{"-server", srv.URL, "-stream"})
	if err == nil {
		t.Fatal("error event should surface as CLI error")
	}
	if !strings.Contains(err.Error(), "plan failed: timeout") {
		t.Errorf("error detail should be in message: %v", err)
	}
}

func TestRunResearchCLIStreamMarkdownFormat(t *testing.T) {
	events := [][2]string{
		{"plan", `{"strategy":"paraphrase","variants":["v1","v2"]}`},
		{"answer_chunk", `{"text":"Answer text."}`},
		{"done", `{"query":"q","strategy":"paraphrase","plan":["v1","v2"],"answer":"Answer text.","sources":[{"id":1,"url":"https://x/a","title":"A","domain":"x"}],"took":"100ms"}`},
	}
	srv, _ := newResearchSSEStub(t, events)
	cfg := config.Default()
	out := captureStreamStdout(t, func() {
		if err := runResearchCLI(context.Background(), cfg, "q",
			[]string{"-server", srv.URL, "-stream", "-format", "md"}); err != nil {
			t.Fatalf("runResearchCLI stream md: %v", err)
		}
	})
	// Markdown strategy blockquote.
	if !strings.Contains(out, "> Strategy: `paraphrase`") {
		t.Errorf("markdown plan rendering missing: %q", out)
	}
	// Markdown sources header.
	if !strings.Contains(out, "## Sources") {
		t.Errorf("markdown sources header missing: %q", out)
	}
	// Source link syntax with trailing domain.
	if !strings.Contains(out, "1. [A](https://x/a) — x") {
		t.Errorf("markdown source link missing: %q", out)
	}
}

func TestRunResearchCLIStreamJSONMutuallyExclusive(t *testing.T) {
	cfg := config.Default()
	err := runResearchCLI(context.Background(), cfg, "q",
		[]string{"-stream", "-json"})
	if err == nil {
		t.Fatal("-stream + -json should error")
	}
	if !strings.Contains(err.Error(), "mutually exclusive") {
		t.Errorf("error should explain the conflict: %v", err)
	}
}

func TestRunResearchCLIStreamNon200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "rate limited", http.StatusTooManyRequests)
	}))
	t.Cleanup(srv.Close)
	cfg := config.Default()
	err := runResearchCLI(context.Background(), cfg, "q",
		[]string{"-server", srv.URL, "-stream"})
	if err == nil {
		t.Fatal("expected error from 429 in stream mode")
	}
	if !strings.Contains(err.Error(), "429") {
		t.Errorf("status should be in error: %v", err)
	}
}

func TestRunResearchCLIStreamWithoutDoneEventSucceeds(t *testing.T) {
	// Server closes stream without a `done` event — CLI should not error,
	// just print a stderr note. Verifies the "stream ended" stderr path.
	events := [][2]string{
		{"plan", `{"strategy":"planner","variants":[]}`},
		{"answer_chunk", `{"text":"partial"}`},
	}
	srv, _ := newResearchSSEStub(t, events)
	cfg := config.Default()
	if err := runResearchCLI(context.Background(), cfg, "q",
		[]string{"-server", srv.URL, "-stream"}); err != nil {
		t.Errorf("stream without `done` shouldn't error: %v", err)
	}
}
