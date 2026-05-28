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

func newAnswerSSEStub(t *testing.T, events [][2]string) (*httptest.Server, func() *url.URL) {
	t.Helper()
	var captured *url.URL
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/answer" {
			http.NotFound(w, r)
			return
		}
		u := *r.URL
		captured = &u
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

func captureAnswerStreamStdout(t *testing.T, f func()) string {
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

func TestRunAnswerCLIStreamHappyPath(t *testing.T) {
	events := [][2]string{
		{"retrieved", `{"urls":["https://x/a","https://x/b"]}`},
		{"synthesizing", `{"sources":2}`},
		{"answer_chunk", `{"text":"Widgets "}`},
		{"answer_chunk", `{"text":"are useful [1]."}`},
		{"done", `{"query":"widgets","answer":"Widgets are useful [1].","sources":[{"id":1,"url":"https://x/a","title":"Widgets"}],"took":"180ms"}`},
	}
	srv, captured := newAnswerSSEStub(t, events)
	cfg := config.Default()
	out := captureAnswerStreamStdout(t, func() {
		if err := runAnswerCLI(context.Background(), cfg, "widgets",
			[]string{"-server", srv.URL, "-stream"}); err != nil {
			t.Fatalf("answer stream: %v", err)
		}
	})
	got := captured()
	if got == nil {
		t.Fatal("stub never recorded request")
	}
	if got.Query().Get("stream") != "true" {
		t.Errorf("-stream should set ?stream=true; got %q", got.RawQuery)
	}
	// /answer retrieved event renders as `[retrieved N source(s)]` (different
	// from /research's per-variant rendering).
	if !strings.Contains(out, "[retrieved 2 source(s)]") {
		t.Errorf("retrieved event not rendered: %q", out)
	}
	if !strings.Contains(out, "[synthesizing answer over 2 source(s)]") {
		t.Errorf("synthesizing event not rendered: %q", out)
	}
	// Answer chunks concatenated, no newlines between.
	if !strings.Contains(out, "Widgets are useful [1].") {
		t.Errorf("answer chunks not concatenated: %q", out)
	}
	// Sources section from done event.
	if !strings.Contains(out, "Sources:") || !strings.Contains(out, "https://x/a") {
		t.Errorf("sources from done event not rendered: %q", out)
	}
}

func TestRunAnswerCLIStreamErrorEvent(t *testing.T) {
	events := [][2]string{
		{"retrieved", `{"urls":[]}`},
		{"error", `{"detail":"no sources matched the query"}`},
	}
	srv, _ := newAnswerSSEStub(t, events)
	cfg := config.Default()
	err := runAnswerCLI(context.Background(), cfg, "q",
		[]string{"-server", srv.URL, "-stream"})
	if err == nil {
		t.Fatal("error event should surface as CLI error")
	}
	if !strings.Contains(err.Error(), "no sources matched") {
		t.Errorf("error detail should be in message: %v", err)
	}
}

func TestRunAnswerCLIStreamMarkdownFormat(t *testing.T) {
	events := [][2]string{
		{"retrieved", `{"urls":["https://x/a"]}`},
		{"answer_chunk", `{"text":"Answer text."}`},
		{"done", `{"query":"q","answer":"Answer text.","sources":[{"id":1,"url":"https://x/a","title":"A","domain":"x"}],"took":"100ms"}`},
	}
	srv, _ := newAnswerSSEStub(t, events)
	cfg := config.Default()
	out := captureAnswerStreamStdout(t, func() {
		if err := runAnswerCLI(context.Background(), cfg, "q",
			[]string{"-server", srv.URL, "-stream", "-format", "md"}); err != nil {
			t.Fatalf("answer stream md: %v", err)
		}
	})
	if !strings.Contains(out, "## Sources") {
		t.Errorf("markdown sources header missing: %q", out)
	}
	if !strings.Contains(out, "1. [A](https://x/a) — x") {
		t.Errorf("markdown source link missing: %q", out)
	}
}

func TestRunAnswerCLIStreamJSONMutuallyExclusive(t *testing.T) {
	cfg := config.Default()
	err := runAnswerCLI(context.Background(), cfg, "q", []string{"-stream", "-json"})
	if err == nil {
		t.Fatal("-stream + -json should error")
	}
	if !strings.Contains(err.Error(), "mutually exclusive") {
		t.Errorf("error should explain the conflict: %v", err)
	}
}

func TestRunAnswerCLIStreamNon200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "rate limited", http.StatusTooManyRequests)
	}))
	t.Cleanup(srv.Close)
	cfg := config.Default()
	err := runAnswerCLI(context.Background(), cfg, "q",
		[]string{"-server", srv.URL, "-stream"})
	if err == nil {
		t.Fatal("expected error from 429 in stream mode")
	}
	if !strings.Contains(err.Error(), "429") {
		t.Errorf("status should be in error: %v", err)
	}
}

func TestRunAnswerCLIStreamWithoutDoneEventSucceeds(t *testing.T) {
	events := [][2]string{
		{"retrieved", `{"urls":["https://x/a"]}`},
		{"answer_chunk", `{"text":"partial"}`},
	}
	srv, _ := newAnswerSSEStub(t, events)
	cfg := config.Default()
	if err := runAnswerCLI(context.Background(), cfg, "q",
		[]string{"-server", srv.URL, "-stream"}); err != nil {
		t.Errorf("stream without `done` shouldn't error: %v", err)
	}
}

// TestForEachSSEEvent is a focused unit test on the extracted scanner.
// Locks in framing semantics for both research and answer consumers.
func TestForEachSSEEvent(t *testing.T) {
	wire := "event: plan\ndata: {\"strategy\":\"planner\"}\n\n" +
		"event: retrieved\ndata: {\"urls\":[\"a\"]}\n\n" +
		"event: done\ndata: {\"ok\":true}\n\n"
	var got []string
	err := forEachSSEEvent(strings.NewReader(wire), func(event, data string) error {
		got = append(got, event+"="+data)
		return nil
	})
	if err != nil {
		t.Fatalf("forEachSSEEvent: %v", err)
	}
	wants := []string{
		`plan={"strategy":"planner"}`,
		`retrieved={"urls":["a"]}`,
		`done={"ok":true}`,
	}
	if len(got) != len(wants) {
		t.Fatalf("want %d events, got %d: %+v", len(wants), len(got), got)
	}
	for i, w := range wants {
		if got[i] != w {
			t.Errorf("event %d: got %q, want %q", i, got[i], w)
		}
	}
}

func TestForEachSSEEventTerminalDone(t *testing.T) {
	wire := "event: a\ndata: 1\n\n" +
		"event: done\ndata: 2\n\n" +
		"event: ignored\ndata: 3\n\n"
	var got []string
	err := forEachSSEEvent(strings.NewReader(wire), func(event, data string) error {
		got = append(got, event)
		if event == "done" {
			return errSSEDone
		}
		return nil
	})
	if err != nil {
		t.Fatalf("forEachSSEEvent: %v", err)
	}
	// errSSEDone returned from handler → scanner returns nil; "ignored"
	// event after `done` is NOT dispatched.
	if len(got) != 2 || got[0] != "a" || got[1] != "done" {
		t.Errorf("expected [a done], got %+v", got)
	}
}

func TestForEachSSEEventMultilineData(t *testing.T) {
	// SSE spec: multi-line data fields concatenate with \n.
	wire := "event: x\ndata: line1\ndata: line2\n\n"
	var capturedData string
	err := forEachSSEEvent(strings.NewReader(wire), func(event, data string) error {
		capturedData = data
		return nil
	})
	if err != nil {
		t.Fatalf("forEachSSEEvent: %v", err)
	}
	if capturedData != "line1\nline2" {
		t.Errorf("multi-line data should be joined with \\n; got %q", capturedData)
	}
}
