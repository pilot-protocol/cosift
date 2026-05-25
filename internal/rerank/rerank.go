// Package rerank turns a candidate set from first-stage retrieval into a
// reordered list using a stronger relevance signal.
//
// Two implementations are reasonable in 2026:
//   - Specialized cross-encoder rerankers (Cohere Rerank, Voyage rerank-2.5,
//     bge-reranker, mxbai-rerank). Cheap ($2/1k queries) and fast. Requires
//     a dedicated endpoint + API key.
//   - LLM-as-reranker (RankGPT-style listwise prompting). Expensive
//     ($10-30/1k) but uses the chat client we already have.
//
// We ship the LLM variant first because it reuses the existing OpenAI key.
// The Reranker interface is intentionally narrow so a specialized backend
// drops in without touching callers.
package rerank

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/calinteodor/cosift/internal/embed"
)

// Candidate is one item to score: an ID the caller will recognize, plus the
// text the reranker should read.
type Candidate struct {
	ID   string
	Text string
}

// Reranker reorders candidates by relevance to the query.
// The returned slice is the same Candidate IDs in a new order — the reranker
// may also drop candidates it considers irrelevant.
type Reranker interface {
	Rerank(ctx context.Context, query string, candidates []Candidate) ([]string, error)
	Name() string
}

// --- LLM listwise reranker ---

const llmRerankSystem = `You are a relevance ranker. You receive a query and a numbered list of passages.
Output ONLY a JSON array containing EVERY passage number, ordered from most relevant to least relevant.
You must include ALL passage numbers exactly once — do not drop any, even if you think a passage is irrelevant. The downstream system handles cutoffs.
Example output for 4 passages: [3, 0, 2, 1]`

// LLMReranker implements Reranker via a chat client.
//
// Per-passage character cap keeps the context budget bounded. Default 800
// chars — long enough for the model to judge relevance on short web passages,
// short enough that 50 candidates fit comfortably in a 16k-token window.
type LLMReranker struct {
	chat        embed.ChatClient
	perDocChars int
}

// NewLLMReranker builds the LLM variant.
func NewLLMReranker(chat embed.ChatClient) *LLMReranker {
	return &LLMReranker{chat: chat, perDocChars: 800}
}

// WithPerDocChars overrides the truncation per passage.
func (r *LLMReranker) WithPerDocChars(n int) *LLMReranker {
	if n > 0 {
		r.perDocChars = n
	}
	return r
}

func (r *LLMReranker) Name() string { return "llm:" + r.chat.Model() }

// Rerank sends the listwise prompt and parses the returned indices.
// On any parsing failure, the original order is returned unchanged — we
// don't want a flaky reranker to make retrieval worse than no rerank.
func (r *LLMReranker) Rerank(ctx context.Context, query string, candidates []Candidate) ([]string, error) {
	if len(candidates) <= 1 {
		out := make([]string, len(candidates))
		for i, c := range candidates {
			out[i] = c.ID
		}
		return out, nil
	}

	var sb strings.Builder
	for i, c := range candidates {
		text := c.Text
		if len(text) > r.perDocChars {
			text = text[:r.perDocChars] + "…"
		}
		fmt.Fprintf(&sb, "[%d] %s\n\n", i, text)
	}
	user := fmt.Sprintf("Query: %s\n\nPassages:\n%s\nOutput the ranked passage numbers as a JSON array.", query, sb.String())

	resp, err := r.chat.Chat(ctx, []embed.ChatMsg{
		{Role: "system", Content: llmRerankSystem},
		{Role: "user", Content: user},
	})
	if err != nil {
		return passthrough(candidates), nil // resilient: caller still gets a result
	}
	order, ok := parseIndices(resp, len(candidates))
	if !ok {
		return passthrough(candidates), nil
	}
	out := make([]string, 0, len(order))
	seen := make(map[int]bool, len(order))
	for _, idx := range order {
		if idx < 0 || idx >= len(candidates) || seen[idx] {
			continue
		}
		seen[idx] = true
		out = append(out, candidates[idx].ID)
	}
	return out, nil
}

func passthrough(cs []Candidate) []string {
	out := make([]string, len(cs))
	for i, c := range cs {
		out[i] = c.ID
	}
	return out
}

// parseIndices extracts a JSON int array from the LLM's response. Strips
// common code-fence wrappers ("```json ... ```") and falls back to scanning
// for the first '[' .. ']' pair. Returns false if no valid array can be
// recovered — callers should pass through the original order in that case.
func parseIndices(raw string, n int) ([]int, bool) {
	raw = strings.TrimSpace(raw)
	for _, fence := range []string{"```json", "```"} {
		raw = strings.TrimPrefix(raw, fence)
		raw = strings.TrimSuffix(raw, "```")
	}
	raw = strings.TrimSpace(raw)
	start := strings.Index(raw, "[")
	end := strings.LastIndex(raw, "]")
	if start < 0 || end <= start {
		return nil, false
	}
	var arr []int
	if err := json.Unmarshal([]byte(raw[start:end+1]), &arr); err != nil {
		return nil, false
	}
	// Defensive: clamp to valid indices.
	out := make([]int, 0, len(arr))
	for _, v := range arr {
		if v >= 0 && v < n {
			out = append(out, v)
		}
	}
	if len(out) == 0 {
		return nil, false
	}
	return out, true
}