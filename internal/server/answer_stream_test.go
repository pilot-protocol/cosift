package server

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/index"
	"github.com/pilot-protocol/cosift/internal/store"
)

// seedAnswerStreamStore puts two docs that match the query "widgets" so
// /answer always has sources to synthesize from.
func seedAnswerStreamStore(t *testing.T) *store.Store {
	t.Helper()
	// OpenMemory.
	s, err := store.OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	idx := index.NewBM25(s)
	docs := []struct{ url, title, text string }{
		{"https://x/widgets-intro", "Widgets Intro", "Widgets are useful for various tasks."},
		{"https://x/widgets-deep", "Widgets Deep Dive", "Advanced widgets enable complex workflows."},
	}
	for _, d := range docs {
		id, _ := s.UpsertDocument(context.Background(), &store.Document{
			URL: d.url, Title: d.title, Text: d.text, Source: "test", FetchedAt: time.Now(),
		})
		_ = idx.IndexDocument(context.Background(), id, d.title, d.text)
	}
	return s
}

func TestAnswerStreamingBasic(t *testing.T) {
	s := seedAnswerStreamStore(t)
	// scriptedChat returns one reply for /answer (no plan step like /research).
	chat := &scriptedChat{replies: []string{"Widgets are useful [1]."}}
	srv := New(s).WithChat(chat)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	resp, err := http.Get(httpSrv.URL + "/answer?q=widgets&stream=true")
	if err != nil {
		t.Fatalf("answer stream: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("status: got %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/event-stream") {
		t.Errorf("content-type: got %q want SSE", ct)
	}

	body, _ := io.ReadAll(resp.Body)
	out := string(body)

	// /answer events: no "plan" step (unlike /research).
	for _, ev := range []string{"event: retrieved", "event: synthesizing", "event: done"} {
		if !strings.Contains(out, ev) {
			t.Errorf("missing %q in stream:\n%s", ev, out)
		}
	}
	// /answer should NOT emit a plan event — that's research-specific.
	if strings.Contains(out, "event: plan") {
		t.Errorf("/answer stream should NOT emit `plan` event:\n%s", out)
	}
	// retrieved must come before done.
	if strings.Index(out, "event: retrieved") > strings.Index(out, "event: done") {
		t.Errorf("event order wrong: retrieved should precede done")
	}
	// done payload should carry full AnswerResponse JSON.
	if !strings.Contains(out, `"answer":`) || !strings.Contains(out, `"calibrated":false`) {
		t.Errorf("done payload missing AnswerResponse fields:\n%s", out)
	}
	// retrieved event should contain at least one URL.
	if !strings.Contains(out, "https://x/widgets") {
		t.Errorf("retrieved event missing URLs:\n%s", out)
	}
}

func TestAnswerStreamingEmitsChunks(t *testing.T) {
	s := seedAnswerStreamStore(t)
	chat := &scriptedChat{
		replies:     []string{"Widgets are useful for various tasks [1]."},
		streamSynth: true,
	}
	srv := New(s).WithChat(chat)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	resp, err := http.Get(httpSrv.URL + "/answer?q=widgets&stream=true")
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	out := string(body)

	// scriptedChat with streamSynth=true emits one chunk per word.
	// "Widgets are useful for various tasks [1]." → 7 words → 7 chunks.
	chunks := strings.Count(out, "event: answer_chunk")
	if chunks < 5 {
		t.Errorf("expected >=5 answer_chunk events, got %d. stream:\n%s", chunks, out)
	}
	if !strings.Contains(out, "event: done") {
		t.Errorf("missing done event")
	}
}

func TestAnswerStreamingAcceptHeaderTriggers(t *testing.T) {
	// SSE should also be triggered by `Accept: text/event-stream`, not just `?stream=true`.
	s := seedAnswerStreamStore(t)
	chat := &scriptedChat{replies: []string{"Widgets are useful [1]."}}
	srv := New(s).WithChat(chat)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	req, _ := http.NewRequest(http.MethodGet, httpSrv.URL+"/answer?q=widgets", nil)
	req.Header.Set("Accept", "text/event-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/event-stream") {
		t.Errorf("Accept header should trigger SSE; got content-type %q", ct)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "event: done") {
		t.Errorf("expected done event:\n%s", body)
	}
}

func TestAnswerStreamingNoSourcesError(t *testing.T) {
	// Empty store → no retrieval results → "no sources matched" error event.
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	chat := &scriptedChat{replies: []string{"never called"}}
	srv := New(s).WithChat(chat)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	resp, err := http.Get(httpSrv.URL + "/answer?q=nothing&stream=true")
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	out := string(body)

	if !strings.Contains(out, "event: error") {
		t.Errorf("expected error event when no sources match:\n%s", out)
	}
	if !strings.Contains(out, "no sources matched") {
		t.Errorf("error detail should mention no-sources:\n%s", out)
	}
	if strings.Contains(out, "event: done") {
		t.Errorf("error path should NOT also emit done:\n%s", out)
	}
}

func TestAnswerStreamingNonStreamPathStillWorks(t *testing.T) {
	// Regression check: /answer without ?stream=true / Accept SSE returns the
	// JSON response, byte-for-byte compatible with existing clients.
	s := seedAnswerStreamStore(t)
	chat := &scriptedChat{replies: []string{"Widgets are useful [1]."}}
	srv := New(s).WithChat(chat)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	resp, err := http.Get(httpSrv.URL + "/answer?q=widgets")
	if err != nil {
		t.Fatalf("answer non-stream: %v", err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Errorf("non-stream should return JSON; got %q", ct)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), `"answer"`) {
		t.Errorf("expected non-stream JSON response with answer field:\n%s", body)
	}
	// MUST NOT contain SSE event framing.
	if strings.Contains(string(body), "event:") {
		t.Errorf("non-stream path leaked SSE framing:\n%s", body)
	}
}
