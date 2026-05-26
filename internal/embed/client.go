// Package embed is the embedding-service client.
//
// The Embedder interface is intentionally tiny: one method (Embed), one config
// (Model, Dim). The OpenAI client speaks the OpenAI /v1/embeddings shape, which
// is also what llama.cpp's HTTP server, vLLM, text-embeddings-inference, and
// most self-hosted alternatives expose. So one client covers both hosted and
// self-hosted.
//
// CachedEmbedder wraps any Embedder with a sha256-keyed disk cache so eval re-runs
// don't repay the API cost. Cache key includes the model name → swapping models
// invalidates correctly without manual eviction.
//
// Zero new deps: stdlib net/http, encoding/json, crypto/sha256.
package embed

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Embedder produces dense vectors for an ordered batch of texts.
type Embedder interface {
	Embed(ctx context.Context, texts []string) ([][]float32, error)
	Model() string
	Dim() int
}

// OpenAIClient calls the OpenAI-compatible /v1/embeddings endpoint.
// URL is configurable so the same client works for OpenAI, Azure, Together,
// llama.cpp, vLLM, text-embeddings-inference, etc.
type OpenAIClient struct {
	APIKey string
	URL    string // default https://api.openai.com/v1/embeddings
	model  string
	dim    int
	http   *http.Client
}

// NewOpenAIClient constructs a client. Model and dim are required; URL defaults
// to the public OpenAI endpoint when empty.
//
// Iter 392: URL is forgiving — accepts either the full endpoint
// ("/v1/embeddings") or the base ("/v1") and appends "/embeddings" when
// missing. Matches what every other OpenAI-compat client expects, so
// operators pointing at Ollama / vLLM / TEI with `http://host:port/v1` no
// longer get 404s.
func NewOpenAIClient(apiKey, url, model string, dim int) *OpenAIClient {
	if url == "" {
		url = "https://api.openai.com/v1/embeddings"
	} else if !strings.HasSuffix(strings.TrimRight(url, "/"), "/embeddings") {
		url = strings.TrimRight(url, "/") + "/embeddings"
	}
	return &OpenAIClient{
		APIKey: apiKey,
		URL:    url,
		model:  model,
		dim:    dim,
		http: &http.Client{
			Timeout: 60 * time.Second,
			Transport: &http.Transport{
				// Go defaults for MaxConnsPerHost (effectively unlimited
				// via 2 idle by default) — iter 443's bump caused queueing
				// at the embedder under heavy crawler load.
				ForceAttemptHTTP2:     true,
				IdleConnTimeout:       90 * time.Second,
				ResponseHeaderTimeout: 45 * time.Second,
			},
		},
	}
}

func (c *OpenAIClient) Model() string { return c.model }
func (c *OpenAIClient) Dim() int      { return c.dim }

type openAIReq struct {
	Model string   `json:"model"`
	Input []string `json:"input"`
}

type openAIResp struct {
	Data []struct {
		Embedding []float32 `json:"embedding"`
		Index     int       `json:"index"`
	} `json:"data"`
	Error *struct {
		Message string `json:"message"`
		Type    string `json:"type"`
	} `json:"error,omitempty"`
}

// Embed returns one vector per input, in the same order. Caller-supplied batch
// size; we don't auto-split. (Practical limit is the provider's per-request token cap.)
func (c *OpenAIClient) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	if len(texts) == 0 {
		return nil, nil
	}
	body, err := json.Marshal(openAIReq{Model: c.model, Input: texts})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.URL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	// Iter 392: only send Authorization when the caller provided a key.
	// Empty-Bearer headers confuse some local servers (e.g. older vLLM).
	if c.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.APIKey)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 50<<20))
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("embed http %d: %s", resp.StatusCode, string(respBytes))
	}
	var parsed openAIResp
	if err := json.Unmarshal(respBytes, &parsed); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	if parsed.Error != nil {
		return nil, fmt.Errorf("api error: %s (%s)", parsed.Error.Message, parsed.Error.Type)
	}
	if len(parsed.Data) != len(texts) {
		return nil, fmt.Errorf("response count mismatch: got %d want %d", len(parsed.Data), len(texts))
	}
	out := make([][]float32, len(texts))
	for _, d := range parsed.Data {
		if d.Index < 0 || d.Index >= len(out) {
			return nil, fmt.Errorf("bad index in response: %d", d.Index)
		}
		out[d.Index] = d.Embedding
	}
	return out, nil
}

// CachedEmbedder wraps any Embedder with a content-addressable disk cache.
type CachedEmbedder struct {
	inner Embedder
	dir   string
}

// NewCachedEmbedder wraps inner. The cache directory is created if missing.
// Pass an empty dir to disable persistence (handy for tests).
func NewCachedEmbedder(inner Embedder, dir string) *CachedEmbedder {
	if dir != "" {
		_ = os.MkdirAll(dir, 0o755)
	}
	return &CachedEmbedder{inner: inner, dir: dir}
}

func (c *CachedEmbedder) Model() string { return c.inner.Model() }
func (c *CachedEmbedder) Dim() int      { return c.inner.Dim() }

func (c *CachedEmbedder) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	miss := make([]int, 0, len(texts))
	missTexts := make([]string, 0, len(texts))

	for i, t := range texts {
		v, ok := c.readCache(t)
		if ok {
			out[i] = v
			continue
		}
		miss = append(miss, i)
		missTexts = append(missTexts, t)
	}
	if len(missTexts) > 0 {
		fresh, err := c.inner.Embed(ctx, missTexts)
		if err != nil {
			return nil, err
		}
		for j, t := range missTexts {
			out[miss[j]] = fresh[j]
			c.writeCache(t, fresh[j])
		}
	}
	return out, nil
}

func (c *CachedEmbedder) cachePath(text string) string {
	if c.dir == "" {
		return ""
	}
	h := sha256Sum(c.inner.Model() + "\x00" + text)
	return filepath.Join(c.dir, hex.EncodeToString(h[:])+".vec")
}

func (c *CachedEmbedder) readCache(text string) ([]float32, bool) {
	p := c.cachePath(text)
	if p == "" {
		return nil, false
	}
	b, err := os.ReadFile(p)
	if err != nil || len(b)%4 != 0 {
		return nil, false
	}
	v := make([]float32, len(b)/4)
	for i := range v {
		v[i] = math.Float32frombits(binary.LittleEndian.Uint32(b[i*4:]))
	}
	return v, true
}

func (c *CachedEmbedder) writeCache(text string, v []float32) {
	p := c.cachePath(text)
	if p == "" {
		return
	}
	b := make([]byte, len(v)*4)
	for i, f := range v {
		binary.LittleEndian.PutUint32(b[i*4:], math.Float32bits(f))
	}
	// Best-effort write — cache misses are recoverable, IO failures shouldn't block eval.
	_ = os.WriteFile(p, b, 0o644)
}

// ErrNoKey is returned by helpers when the API key is missing.
var ErrNoKey = errors.New("OPENAI_API_KEY not set")

// import-friendly sha256 wrapper — keeps the package boundary tight.
func sha256Sum(s string) [32]byte {
	return sha256SumImpl(s)
}
