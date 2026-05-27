package main

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/embed"
	"github.com/pilot-protocol/cosift/internal/index"
	"github.com/pilot-protocol/cosift/internal/server"
	"github.com/pilot-protocol/cosift/internal/store"
)

// e2eScriptedChat returns scripted replies for sequential Chat/ChatStream calls.
// Mirrors the package-private scriptedChat in internal/server's http_test.go;
// we can't import that, so this is a self-contained copy for the cmd/cosift
// E2E suite. Iter 119.
type e2eScriptedChat struct {
	replies     []string
	calls       int
	streamSynth bool
}

var (
	_ embed.ChatClient          = (*e2eScriptedChat)(nil)
	_ embed.StreamingChatClient = (*e2eScriptedChat)(nil)
)

func (s *e2eScriptedChat) Model() string { return "scripted-e2e" }

func (s *e2eScriptedChat) Chat(_ context.Context, _ []embed.ChatMsg) (string, error) {
	if s.calls >= len(s.replies) {
		return "", nil
	}
	out := s.replies[s.calls]
	s.calls++
	return out, nil
}

func (s *e2eScriptedChat) ChatStream(_ context.Context, _ []embed.ChatMsg, onChunk func(string)) (string, error) {
	if s.calls >= len(s.replies) {
		return "", nil
	}
	out := s.replies[s.calls]
	s.calls++
	if s.streamSynth && onChunk != nil {
		for _, w := range strings.Fields(out) {
			onChunk(w + " ")
		}
	}
	return out, nil
}

// bootE2EServer seeds a store with the given docs, builds a real server.Server
// with the given chat client, and returns a live httptest.Server URL.
// Caller passes -server <url> to the CLI consumer under test.
func bootE2EServer(t *testing.T, docs []store.Document, chat embed.ChatClient) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "data")
	s, err := store.Open(dir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	idx := index.NewBM25(s)
	for i := range docs {
		id, err := s.UpsertDocument(context.Background(), &docs[i])
		if err != nil {
			t.Fatalf("upsert doc %d: %v", i, err)
		}
		if err := idx.IndexDocument(context.Background(), id, docs[i].Title, docs[i].Text); err != nil {
			t.Fatalf("index doc %d: %v", i, err)
		}
	}

	srv := server.New(s)
	if chat != nil {
		srv = srv.WithChat(chat)
	}
	httpSrv := httptest.NewServer(srv.Handler())
	t.Cleanup(httpSrv.Close)
	return httpSrv.URL
}

// TestE2EResearchStreaming is the iter-119 cross-package end-to-end check.
// Real server emits events; real CLI parses them. Catches drift the iter-98
// hand-crafted-events test can't see.
func TestE2EResearchStreaming(t *testing.T) {
	docs := []store.Document{
		{URL: "https://x/go", Title: "Go concurrency", Text: "Go has goroutines for concurrent programming.",
			Source: "test", FetchedAt: time.Now()},
		{URL: "https://x/rust", Title: "Rust ownership", Text: "Rust enforces memory safety via lifetimes.",
			Source: "test", FetchedAt: time.Now()},
	}
	chat := &e2eScriptedChat{
		replies: []string{
			// Plan reply (must be JSON array per researchPlanPrompt):
			`["concurrent programming", "memory safety"]`,
			// Synth reply:
			"Go uses goroutines [1]. Rust uses lifetimes [2].",
		},
	}
	serverURL := bootE2EServer(t, docs, chat)

	cfg := config.Default()
	out := captureStreamStdout(t, func() {
		if err := runResearchCLI(context.Background(), cfg, "compare go and rust",
			[]string{"-server", serverURL, "-stream"}); err != nil {
			t.Fatalf("runResearchCLI: %v", err)
		}
	})

	// The wire format must match between server emission and client parsing.
	// Each assertion below would fail if either side drifted unilaterally:
	//   - server emits `event: plan` → client renders "Strategy: planner"
	//   - server emits `event: retrieved` → client renders "[retrieved N url(s) for ...]"
	//   - server emits `event: synthesizing` → client renders "[synthesizing answer over N source(s)]"
	//   - server emits `event: done` with full ResearchResponse → client renders "Sources:"
	//
	// Synth text (the "answer" string) is verified separately by
	// TestE2EResearchStreamingTokenChunks — non-streaming chat backends don't
	// emit answer_chunk events, so checking for synth text here would conflate
	// the wire-format check with the chunking behavior.
	for _, want := range []string{
		"Strategy: planner",
		"[retrieved ",  // any number of retrieved events from variant fan-out
		"[synthesizing answer over",
		"Sources:",
		"https://x/go",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in E2E output:\n%s", want, out)
		}
	}
}

func TestE2EResearchStreamingTokenChunks(t *testing.T) {
	// Same fixture but with streamSynth=true → server emits answer_chunk
	// events as tokens, client must concatenate them into the answer line.
	docs := []store.Document{
		{URL: "https://x/widgets", Title: "Widgets", Text: "Widgets enable workflows.",
			Source: "test", FetchedAt: time.Now()},
	}
	chat := &e2eScriptedChat{
		replies: []string{
			`["widgets"]`,
			"Widgets enable workflows [1].",
		},
		streamSynth: true,
	}
	serverURL := bootE2EServer(t, docs, chat)

	cfg := config.Default()
	out := captureStreamStdout(t, func() {
		if err := runResearchCLI(context.Background(), cfg, "widgets",
			[]string{"-server", serverURL, "-stream"}); err != nil {
			t.Fatalf("runResearchCLI: %v", err)
		}
	})
	// Chunks arrive as separate events; client must concatenate to produce the answer.
	if !strings.Contains(out, "Widgets enable workflows [1].") {
		t.Errorf("chunked answer not reassembled in client output:\n%s", out)
	}
	if !strings.Contains(out, "Sources:") {
		t.Errorf("Sources section missing:\n%s", out)
	}
}

// e2eStubEmbedder satisfies embed.Embedder for the iter-120 /admin/reembed
// E2E test. Returns fixed 4-dim vectors. Equivalent to the package-private
// reembedStubEmbedder in internal/server/admin_reembed_test.go (cmd/cosift
// can't import _test.go files from other packages).
type e2eStubEmbedder struct{ model string }

func (e *e2eStubEmbedder) Model() string { return e.model }
func (e *e2eStubEmbedder) Dim() int      { return 4 }
func (e *e2eStubEmbedder) Embed(_ context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i := range texts {
		out[i] = []float32{1, 0, 0, 0}
	}
	return out, nil
}

// bootE2EServerWithEmbedder wires store + server + admin token + embedder in
// one call. Used by /admin/reembed E2E test (iter 120) where the embedder is
// the central piece, not the chat client.
func bootE2EServerWithEmbedder(t *testing.T, docs []store.Document, emb embed.Embedder, adminToken string) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "data")
	s, err := store.Open(dir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	for i := range docs {
		if _, err := s.UpsertDocument(context.Background(), &docs[i]); err != nil {
			t.Fatalf("upsert doc %d: %v", i, err)
		}
	}

	srv := server.New(s).WithAdminToken(adminToken).WithVector(nil, emb)
	httpSrv := httptest.NewServer(srv.Handler())
	t.Cleanup(httpSrv.Close)
	return httpSrv.URL
}

func TestE2EAdminReembedStreaming(t *testing.T) {
	// Third E2E streaming test (iter 119 added research + answer). Closes the
	// lock-in coverage for the third SSE endpoint — drift between
	// handleAdminReembed's event emission (iter 112) and consumeReembedSSE's
	// parsing (iter 113) would slip past the existing tests but fail here.
	docs := []store.Document{
		{URL: "https://x/doc1", Title: "Doc 1", Text: strings.Repeat("content ", 30),
			Source: "test", FetchedAt: time.Now()},
		{URL: "https://x/doc2", Title: "Doc 2", Text: strings.Repeat("content ", 30),
			Source: "test", FetchedAt: time.Now()},
		{URL: "https://x/doc3", Title: "Doc 3", Text: strings.Repeat("content ", 30),
			Source: "test", FetchedAt: time.Now()},
	}
	emb := &e2eStubEmbedder{model: "stub-target"}
	serverURL := bootE2EServerWithEmbedder(t, docs, emb, "secret")

	cfg := config.Default()
	out := captureStreamStdout(t, func() {
		if err := runAdmin(context.Background(), cfg, "reembed",
			[]string{"-server", serverURL, "-token", "secret", "-y"}); err != nil {
			t.Fatalf("admin reembed E2E: %v", err)
		}
	})

	// Wire-format roundtrip assertions:
	//   - server emits `event: started` → client renders "[reembed started: N docs, target=X]"
	//   - server emits `event: progress` (≥0 times; depends on per-iteration timing)
	//   - server emits `event: done` with full done payload → client renders "Done: N docs ..."
	for _, want := range []string{
		"[reembed started: 3 docs, target=stub-target]",
		"Done: 3 docs reembedded",
		"passages written",
		"took ",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in admin reembed E2E output:\n%s", want, out)
		}
	}
	// Should NOT contain SSE framing leaked through (raw `event:` text).
	if strings.Contains(out, "event: started") {
		t.Errorf("raw SSE framing leaked into rendered output:\n%s", out)
	}
}

func TestE2EAdminReembedWithSinceStreaming(t *testing.T) {
	// Iter-116/117 added since-filter. E2E verifies the filter propagates
	// from CLI flag → request body → server filter → started total_docs.
	pub2025 := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	pub2024 := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	docs := []store.Document{
		{URL: "https://x/old", Title: "Old", Text: strings.Repeat("c ", 30),
			Source: "test", FetchedAt: time.Now(), PublishedAt: pub2024},
		{URL: "https://x/new", Title: "New", Text: strings.Repeat("c ", 30),
			Source: "test", FetchedAt: time.Now(), PublishedAt: pub2025},
	}
	emb := &e2eStubEmbedder{model: "stub-target"}
	serverURL := bootE2EServerWithEmbedder(t, docs, emb, "k")

	cfg := config.Default()
	out := captureStreamStdout(t, func() {
		if err := runAdmin(context.Background(), cfg, "reembed",
			[]string{"-server", serverURL, "-token", "k", "-y", "-since", "2025-01-01"}); err != nil {
			t.Fatalf("admin reembed E2E -since: %v", err)
		}
	})

	// since=2025-01-01 → old (2024-06) dropped, only new (2025-06) reembedded.
	// started event's total_docs reflects the post-filter count (iter-116).
	if !strings.Contains(out, "[reembed started: 1 docs, target=stub-target]") {
		t.Errorf("since filter not applied; expected 1 doc, got:\n%s", out)
	}
	if !strings.Contains(out, "Done: 1 docs reembedded") {
		t.Errorf("done event reports wrong doc count:\n%s", out)
	}
}

func TestE2EAnswerStreaming(t *testing.T) {
	// /answer has fewer event types than /research (no plan event).
	// Drift on `/answer` specifically would slip past iter-98 tests but be caught here.
	docs := []store.Document{
		{URL: "https://x/raft", Title: "Raft Consensus", Text: "Raft is a consensus algorithm.",
			Source: "test", FetchedAt: time.Now()},
	}
	chat := &e2eScriptedChat{
		replies:     []string{"Raft is a consensus protocol [1]."},
		streamSynth: true, // verify the chunked synth path end-to-end on /answer too
	}
	serverURL := bootE2EServer(t, docs, chat)

	cfg := config.Default()
	out := captureStreamStdout(t, func() {
		if err := runAnswerCLI(context.Background(), cfg, "what is raft",
			[]string{"-server", serverURL, "-stream"}); err != nil {
			t.Fatalf("runAnswerCLI: %v", err)
		}
	})

	// /answer emits `retrieved` (one event, not per-variant), `synthesizing`,
	// `answer_chunk` (per-word with streamSynth=true), `done`. The CLI's
	// consumeAnswerSSE must parse all of them.
	for _, want := range []string{
		"[retrieved",
		"[synthesizing answer over",
		"Raft is a consensus protocol [1].",
		"Sources:",
		"https://x/raft",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in answer E2E output:\n%s", want, out)
		}
	}
	// /answer should NOT emit a `plan` event (research-specific).
	if strings.Contains(out, "Strategy: ") {
		t.Errorf("answer E2E should NOT include Strategy line (research-specific):\n%s", out)
	}
}
