package server

import (
	"context"
	"strings"
	"sync"

	"github.com/calinteodor/cosift/internal/embed"
	"github.com/calinteodor/cosift/internal/store"
)

// HyDE (Hypothetical Document Embeddings) — Gao et al. 2022. The intuition:
// search queries are typically short ("goroutines"), but the documents that
// actually answer them are long-form prose ("Goroutines are lightweight
// threads managed by the Go runtime…"). The hypothetical-answer embedding
// is closer in vector space to answer-shaped docs than the bare query's is.
//
// Cost: one chat-completion call per cache MISS (~$0.0001 at gpt-4o-mini,
// ~500ms latency). Same 2-level cache as the iter-89 paraphraser: L1
// in-memory map → L2 SQLite-persisted (query_hyde table). Cold processes
// (refresh sidecar, second Cloud Run instance, post-restart server) skip
// the LLM call on popular queries via the L2 hit.
//
// Iter 161 shipped the algorithm; iter 162 adds the cache.

const hydeSystemPrompt = `Write a brief, factual passage (2-4 sentences) that would directly answer the user's question. Output ONLY the passage — no preamble, no commentary, no apology if you're uncertain. If the question is ambiguous, pick the most plausible interpretation and answer that. The passage doesn't need to be true; it needs to be the SHAPE of what a relevant document would say. Embedding this passage and searching by its vector will find documents that look like real answers, even if the user's original query was just a few keywords.`

// hydePassager owns the chat client + 2-level cache.
type hydePassager struct {
	chat    embed.ChatClient
	store   *store.Store // optional L2; nil = L1-only
	metrics *Metrics     // optional cache observability
	mu      sync.Mutex
	cache   map[string]string // L1 key: model + "\x00" + query
}

func newHydePassager(chat embed.ChatClient, s *store.Store, m *Metrics) *hydePassager {
	return &hydePassager{chat: chat, store: s, metrics: m, cache: make(map[string]string)}
}

// Passage returns the hypothetical-answer text for the query, threading the
// L1 → L2 → LLM lookup ladder. Best-effort: any failure (nil chat, LLM error,
// empty response) falls back to the original query so search still runs.
func (p *hydePassager) Passage(ctx context.Context, q string) string {
	if p == nil || p.chat == nil || strings.TrimSpace(q) == "" {
		return q
	}
	key := p.chat.Model() + "\x00" + q

	// L1: in-memory.
	p.mu.Lock()
	if cached, ok := p.cache[key]; ok {
		p.mu.Unlock()
		if p.metrics != nil {
			p.metrics.RecordHyDEL1Hit()
		}
		return cached
	}
	p.mu.Unlock()

	// L2: SQLite.
	if p.store != nil {
		if cached, err := p.store.GetHyDE(ctx, p.chat.Model(), q); err == nil && cached != "" {
			p.mu.Lock()
			p.cache[key] = cached
			p.mu.Unlock()
			if p.metrics != nil {
				p.metrics.RecordHyDEL2Hit()
			}
			return cached
		}
	}

	// L3: LLM.
	if p.metrics != nil {
		p.metrics.RecordHyDEMiss()
	}
	resp, err := p.chat.Chat(ctx, []embed.ChatMsg{
		{Role: "system", Content: hydeSystemPrompt},
		{Role: "user", Content: q},
	})
	if err != nil {
		return q
	}
	passage := strings.TrimSpace(resp)
	if passage == "" {
		return q
	}

	// Save both levels.
	p.mu.Lock()
	p.cache[key] = passage
	p.mu.Unlock()
	if p.store != nil {
		_ = p.store.SaveHyDE(ctx, p.chat.Model(), q, passage) // best-effort
	}
	return passage
}

// hydeQueryKey is the context-value key for the iter-161 HyDE-generated
// passage. Threaded via context so runDense can consult it without a
// signature change, same pattern as iter-143 hybridDenseWeightKey and
// iter-158 mmrParamsKey.
type hydeQueryKey struct{}

// hydeEnabledKey signals "do HyDE on every retrieval against the chat
// client" — used by /research (iter 165) where each sub-query / paraphrase
// needs its OWN hypothetical passage. iter-161's hydeQueryKey carries one
// pre-computed passage; iter-165's enabledKey carries "compute per-variant
// inside the loop." Cache (iter 162) ensures repeated variants stay cheap.
type hydeEnabledKey struct{}
