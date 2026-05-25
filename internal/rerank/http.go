package rerank

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// HTTPReranker speaks the Cohere /v1/rerank shape — the format Cohere Rerank,
// Voyage, Jina, and text-embeddings-inference all converge on.
//
// Request:
//
//	{"query": "...", "documents": ["...", "..."], "model": "name", "top_n": N}
//
// Response:
//
//	{"results": [{"index": 2, "relevance_score": 0.95}, ...]}
//
// Per-query cost: ~$2/1k at Cohere, free self-hosted. Roughly 7× cheaper than
// the LLM listwise reranker and faster (single HTTP round-trip, no JSON parsing
// the LLM has to do).
//
// Drops into anywhere a Reranker is expected — same interface as LLMReranker.
type HTTPReranker struct {
	URL     string
	APIKey  string // optional; passed as Bearer when set
	ModelID string // optional; some endpoints want it, some don't
	HTTP    *http.Client
}

// NewHTTPReranker constructs an HTTP reranker. For Cohere pass APIKey;
// for self-hosted bge-reranker via text-embeddings-inference leave it empty.
func NewHTTPReranker(url, apiKey, model string) *HTTPReranker {
	return &HTTPReranker{
		URL:     url,
		APIKey:  apiKey,
		ModelID: model,
		HTTP: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				ForceAttemptHTTP2:     true,
				IdleConnTimeout:       90 * time.Second,
				ResponseHeaderTimeout: 15 * time.Second,
			},
		},
	}
}

func (r *HTTPReranker) Name() string {
	if r.ModelID != "" {
		return "http:" + r.ModelID
	}
	return "http:rerank"
}

type httpRerankReq struct {
	Query     string   `json:"query"`
	Documents []string `json:"documents"`
	Model     string   `json:"model,omitempty"`
	TopN      int      `json:"top_n,omitempty"`
}

type httpRerankResp struct {
	Results []struct {
		Index          int     `json:"index"`
		RelevanceScore float64 `json:"relevance_score"`
	} `json:"results"`
	// Some implementations return scores in a different field name. Keep both.
	Scores []float64 `json:"scores,omitempty"`
	Error  *struct {
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

// Rerank sends the candidates and returns IDs in the server's chosen order.
// Falls back to passthrough on any error — same resilience guarantee as the
// LLM reranker. Caller-side cutoffs are unaffected; we return EVERY candidate
// the server scored, in its preferred order.
func (r *HTTPReranker) Rerank(ctx context.Context, query string, candidates []Candidate) ([]string, error) {
	if len(candidates) <= 1 {
		return passthrough(candidates), nil
	}
	docs := make([]string, len(candidates))
	for i, c := range candidates {
		docs[i] = c.Text
	}
	body, err := json.Marshal(httpRerankReq{
		Query: query, Documents: docs, Model: r.ModelID, TopN: len(candidates),
	})
	if err != nil {
		return passthrough(candidates), nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.URL, bytes.NewReader(body))
	if err != nil {
		return passthrough(candidates), nil
	}
	req.Header.Set("Content-Type", "application/json")
	if r.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+r.APIKey)
	}
	resp, err := r.HTTP.Do(req)
	if err != nil {
		return passthrough(candidates), nil
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return passthrough(candidates), nil
	}
	respBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	var parsed httpRerankResp
	if err := json.Unmarshal(respBytes, &parsed); err != nil {
		return passthrough(candidates), nil
	}
	if parsed.Error != nil {
		return passthrough(candidates), fmt.Errorf("http rerank: %s", parsed.Error.Message)
	}
	// Cohere/Voyage/Jina shape: results[].index ordered by descending score.
	if len(parsed.Results) > 0 {
		out := make([]string, 0, len(parsed.Results))
		seen := make(map[int]bool, len(parsed.Results))
		for _, r := range parsed.Results {
			if r.Index < 0 || r.Index >= len(candidates) || seen[r.Index] {
				continue
			}
			seen[r.Index] = true
			out = append(out, candidates[r.Index].ID)
		}
		return out, nil
	}
	// Fallback shape: scores[] aligned with input documents, no reorder.
	if len(parsed.Scores) == len(candidates) {
		type pair struct {
			i int
			s float64
		}
		ps := make([]pair, len(candidates))
		for i, s := range parsed.Scores {
			ps[i] = pair{i, s}
		}
		// Stable sort by score descending.
		for i := 1; i < len(ps); i++ {
			for j := i; j > 0 && ps[j].s > ps[j-1].s; j-- {
				ps[j], ps[j-1] = ps[j-1], ps[j]
			}
		}
		out := make([]string, len(ps))
		for i, p := range ps {
			out[i] = candidates[p.i].ID
		}
		return out, nil
	}
	return passthrough(candidates), nil
}
