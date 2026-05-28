package embed

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestOpenAIChatHappyPath(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		var req chatReq
		_ = json.Unmarshal(b, &req)
		if req.Model != "test-model" {
			t.Errorf("model: got %q", req.Model)
		}
		if len(req.Messages) != 2 {
			t.Errorf("messages: got %d want 2", len(req.Messages))
		}
		resp := chatResp{}
		resp.Choices = append(resp.Choices, struct {
			Message ChatMsg `json:"message"`
		}{Message: ChatMsg{Role: "assistant", Content: "hello there"}})
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	c := NewOpenAIChat("dummy", srv.URL, "test-model")
	out, err := c.Chat(context.Background(), []ChatMsg{
		{Role: "system", Content: "be brief"},
		{Role: "user", Content: "hi"},
	})
	if err != nil {
		t.Fatalf("chat: %v", err)
	}
	if out != "hello there" {
		t.Errorf("content: got %q", out)
	}
}

func TestOpenAIChatStream(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify the client requested streaming.
		b, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(b), `"stream":true`) {
			t.Errorf("expected stream=true in request: %s", b)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)

		// 4 deltas: "Hello", " ", "world", "!"
		for _, d := range []string{"Hello", " ", "world", "!"} {
			_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"" + d + "\"}}]}\n\n"))
			flusher.Flush()
		}
		// First chunk in real OpenAI is role-only — verify we skip it.
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{}}]}\n\n"))
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
		flusher.Flush()
	}))
	defer srv.Close()

	c := NewOpenAIChat("dummy", srv.URL, "test-model")
	var chunks []string
	full, err := c.ChatStream(context.Background(),
		[]ChatMsg{{Role: "user", Content: "hi"}},
		func(s string) { chunks = append(chunks, s) },
	)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	if full != "Hello world!" {
		t.Errorf("full content: got %q want %q", full, "Hello world!")
	}
	want := []string{"Hello", " ", "world", "!"}
	if len(chunks) != len(want) {
		t.Fatalf("chunk count: got %d want %d (%+v)", len(chunks), len(want), chunks)
	}
	for i := range want {
		if chunks[i] != want[i] {
			t.Errorf("chunk %d: got %q want %q", i, chunks[i], want[i])
		}
	}
}

func TestOpenAIChatStreamSkipsBadChunks(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"good\"}}]}\n\n"))
		_, _ = w.Write([]byte("data: this is not json\n\n"))
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\" still works\"}}]}\n\n"))
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
		flusher.Flush()
	}))
	defer srv.Close()

	c := NewOpenAIChat("k", srv.URL, "m")
	full, err := c.ChatStream(context.Background(), []ChatMsg{{Role: "user", Content: "x"}}, nil)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	if full != "good still works" {
		t.Errorf("garbage chunk should not break the stream; got %q", full)
	}
}

// Compile-time check that OpenAIChatClient satisfies StreamingChatClient.
var _ StreamingChatClient = (*OpenAIChatClient)(nil)

func TestOpenAIChatErrorPropagates(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(429)
		_, _ = w.Write([]byte(`{"error":{"message":"rate limited","type":"rate_limit"}}`))
	}))
	defer srv.Close()

	c := NewOpenAIChat("k", srv.URL, "m")
	_, err := c.Chat(context.Background(), []ChatMsg{{Role: "user", Content: "x"}})
	if err == nil || !strings.Contains(err.Error(), "429") {
		t.Errorf("expected 429 error, got %v", err)
	}
}

// TestStripThinkingBlocks — R1-distill emits <think>...</think>
// before the actual response. Must be stripped so the planner JSON parses
// and /answer responses stay clean.
func TestStripThinkingBlocks(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"no thinking here", "no thinking here"},
		{"<think>reasoning</think>actual answer", "actual answer"},
		{"<think>line1\nline2\nline3</think>\n\nFinal: yes", "Final: yes"},
		{"<think>a</think>before<think>b</think>after", "beforeafter"},
		{"  <think>think</think>  trimmed  ", "trimmed"},
		// Qwen3.5
		// + max_tokens cap can cut a model mid-reasoning, leaving a hanging
		// <think>...; previously we kept the text. The new policy strips the
		// orphan so the visible reply isn't polluted with truncated reasoning.
		{"<think>unclosed but never closed", ""},
		{"answer first<think>then trailing think cut off", "answer first"},
		{"", ""},
	}
	for _, c := range cases {
		got := stripThinkingBlocks(c.in)
		if got != c.want {
			t.Errorf("stripThinkingBlocks(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}
