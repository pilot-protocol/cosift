package embed

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// ChatMsg is one message in a chat completion request.
type ChatMsg struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// ChatClient is the contract every chat-completion provider must satisfy.
type ChatClient interface {
	Chat(ctx context.Context, msgs []ChatMsg) (string, error)
	Model() string
}

// StreamingChatClient is an optional capability. Implementations that support
// server-side streaming (OpenAI, Anthropic, vLLM with stream=true) implement
// this in addition to ChatClient. Callers should type-assert to use it and
// fall back to Chat() when the assertion fails — keeps the surface optional.
type StreamingChatClient interface {
	ChatClient
	// ChatStream calls onChunk for each incoming text delta and returns the full
	// assembled content at the end. Empty deltas (e.g., role-only first chunks)
	// are filtered before onChunk fires.
	ChatStream(ctx context.Context, msgs []ChatMsg, onChunk func(string)) (string, error)
}

// OpenAIChatClient hits /v1/chat/completions. URL is configurable so this same
// client works against OpenAI, Together, Azure, llama.cpp, vLLM — any provider
// speaking the OpenAI completions wire format.
type OpenAIChatClient struct {
	APIKey string
	URL    string // default https://api.openai.com/v1/chat/completions
	model  string
	http   *http.Client
}

// NewOpenAIChat builds a client. URL empty → public OpenAI.
//
// URL is forgiving — accepts either the full endpoint
// ("/v1/chat/completions") or the base ("/v1") and appends
// "/chat/completions" when missing. Mirrors on the embed client.
func NewOpenAIChat(apiKey, url, model string) *OpenAIChatClient {
	if url == "" {
		url = "https://api.openai.com/v1/chat/completions"
	} else if !strings.HasSuffix(strings.TrimRight(url, "/"), "/chat/completions") {
		url = strings.TrimRight(url, "/") + "/chat/completions"
	}
	return &OpenAIChatClient{
		APIKey: apiKey,
		URL:    url,
		model:  model,
		http: &http.Client{
			Timeout: 90 * time.Second,
			Transport: &http.Transport{
				ForceAttemptHTTP2:     true,
				IdleConnTimeout:       90 * time.Second,
				ResponseHeaderTimeout: 60 * time.Second,
			},
		},
	}
}

func (c *OpenAIChatClient) Model() string { return c.model }

type chatReq struct {
	Model              string                 `json:"model"`
	Messages           []ChatMsg              `json:"messages"`
	Temperature        float64                `json:"temperature"`
	MaxTokens          int                    `json:"max_tokens,omitempty"`
	ChatTemplateKwargs map[string]interface{} `json:"chat_template_kwargs,omitempty"`
}

// chatMaxTokens caps generation at a sane bound so a thinking-loop or
// runaway response can't burn the entire 32k context window. Default
// 2048 (enough for 1500-word /answer responses); operators can raise
// via COSIFT_CHAT_MAX_TOKENS for /research-style long outputs.
func chatMaxTokens() int {
	if v := os.Getenv("COSIFT_CHAT_MAX_TOKENS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 2048
}

// chatTemplateKwargs is the per-call extra args dict vLLM forwards into the
// model's Jinja chat template. Today the only consumer is qwen3/qwen3.5
// `enable_thinking` toggle — emitting "Thinking Process:" prose mid-answer
// is a quality + latency regression for /answer. Disable by default; iter
// 477e. Operators wanting to inspect reasoning can flip COSIFT_THINKING=1.
func chatTemplateKwargs() map[string]interface{} {
	if os.Getenv("COSIFT_THINKING") == "1" {
		return nil
	}
	return map[string]interface{}{"enable_thinking": false}
}

type chatResp struct {
	Choices []struct {
		Message ChatMsg `json:"message"`
	} `json:"choices"`
	Error *struct {
		Message string `json:"message"`
		Type    string `json:"type"`
	} `json:"error,omitempty"`
}

// stripThinkingBlocks removes <think>...</think> spans that R1-style reasoning
// models emit before their final answer. cosift's planner parses chat output
// as JSON and the /answer flow surfaces it verbatim; leaving thinking in would
// either break the planner or leak internal reasoning into responses. Greedy,
// dotall, non-nested (R1 doesn't nest).
//
// Operators that WANT to see the reasoning can grep ~/pebble-serve.log — the
// raw chat call isn't logged today but the streaming SSE path emits chunks
// before strip.
var thinkBlockRE = regexp.MustCompile(`(?s)<think>.*?</think>\s*`)

// thinkProsePrefixRE matches qwen3-style thinking-mode prose that some
// models emit even when <think> tags are disabled — typically starts with
// "Thinking Process:" or "Let me think" and runs until a clear answer
// marker ("Final Answer:", "## Answer", or just numbered "5.  **Final"...).
// Anchored to start so we don't strip mid-answer mentions.
var thinkProsePrefixRE = regexp.MustCompile(`(?si)^\s*(?:thinking process|reasoning|let me think|let's think)[\s\S]*?(?:final answer:?\s*|##\s*answer\s*|\*\*final answer\*\*:?\s*)`)

// orphanThinkOpenRE strips a trailing/orphaned `<think>` with no closing tag.
// Happens when the model gets cut off by max_tokens mid-think. We keep
// content before any <think> and discard everything from <think> onward
// as a last-resort scrub.
var orphanThinkOpenRE = regexp.MustCompile(`(?s)<think>.*$`)

func stripThinkingBlocks(s string) string {
	s = thinkBlockRE.ReplaceAllString(s, "")
	s = orphanThinkOpenRE.ReplaceAllString(s, "")
	s = thinkProsePrefixRE.ReplaceAllString(s, "")
	return strings.TrimSpace(s)
}

// Chat sends messages and returns the assistant content. Temperature pinned to
// 0 so /answer is reproducible — caller wraps if they want creative output.
func (c *OpenAIChatClient) Chat(ctx context.Context, msgs []ChatMsg) (string, error) {
	if len(msgs) == 0 {
		return "", errors.New("chat: no messages")
	}
	body, err := json.Marshal(chatReq{Model: c.model, Messages: msgs, Temperature: 0, MaxTokens: chatMaxTokens(), ChatTemplateKwargs: chatTemplateKwargs()})
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.URL, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	if c.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.APIKey)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	respBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 10<<20))
	if resp.StatusCode >= 400 {
		return "", fmt.Errorf("chat http %d: %s", resp.StatusCode, string(respBytes))
	}
	var parsed chatResp
	if err := json.Unmarshal(respBytes, &parsed); err != nil {
		return "", fmt.Errorf("decode: %w", err)
	}
	if parsed.Error != nil {
		return "", fmt.Errorf("api: %s (%s)", parsed.Error.Message, parsed.Error.Type)
	}
	if len(parsed.Choices) == 0 {
		return "", errors.New("chat: no choices in response")
	}
	// R1-distill compatibility — strip <think> blocks.
	return stripThinkingBlocks(parsed.Choices[0].Message.Content), nil
}

// chatStreamReq mirrors chatReq + the stream flag. Kept separate so the
// non-stream Chat() path doesn't bear an unused field at marshal time.
type chatStreamReq struct {
	Model              string                 `json:"model"`
	Messages           []ChatMsg              `json:"messages"`
	Temperature        float64                `json:"temperature"`
	Stream             bool                   `json:"stream"`
	MaxTokens          int                    `json:"max_tokens,omitempty"`
	ChatTemplateKwargs map[string]interface{} `json:"chat_template_kwargs,omitempty"`
}

// ChatStream sends the request with stream=true and parses the SSE response
// chunk-by-chunk. Each delta fires onChunk; the full concatenated content is
// returned at the end. OpenAI, vLLM, llama.cpp, Together, Azure, and most
// other OpenAI-compatible providers speak the same SSE shape:
//
//	data: {"choices":[{"delta":{"content":"hello"}}]}
//	data: {"choices":[{"delta":{"content":" world"}}]}
//	data: [DONE]
func (c *OpenAIChatClient) ChatStream(ctx context.Context, msgs []ChatMsg, onChunk func(string)) (string, error) {
	if len(msgs) == 0 {
		return "", errors.New("chat: no messages")
	}
	body, err := json.Marshal(chatStreamReq{Model: c.model, Messages: msgs, Temperature: 0, Stream: true, MaxTokens: chatMaxTokens(), ChatTemplateKwargs: chatTemplateKwargs()})
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.URL, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	if c.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.APIKey)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")

	resp, err := c.http.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 10<<20))
		return "", fmt.Errorf("chat http %d: %s", resp.StatusCode, string(b))
	}

	type streamChoice struct {
		Delta struct {
			Content string `json:"content"`
		} `json:"delta"`
	}
	type streamChunk struct {
		Choices []streamChoice `json:"choices"`
	}

	var full strings.Builder
	scanner := bufio.NewScanner(resp.Body)
	// Long SSE chunks can exceed the default 64KB scan buffer.
	scanner.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		payload := strings.TrimSpace(line[len("data:"):])
		if payload == "" || payload == "[DONE]" {
			continue
		}
		var ck streamChunk
		if err := json.Unmarshal([]byte(payload), &ck); err != nil {
			continue // skip unparseable chunks rather than failing the whole stream
		}
		for _, ch := range ck.Choices {
			if ch.Delta.Content == "" {
				continue
			}
			full.WriteString(ch.Delta.Content)
			if onChunk != nil {
				onChunk(ch.Delta.Content)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return stripThinkingBlocks(full.String()), err
	}
	// SSE chunks still
	// stream raw so callers wanting to surface the reasoning ('live thinking
	// indicator') can; only the final return value is sanitized.
	return stripThinkingBlocks(full.String()), nil
}
