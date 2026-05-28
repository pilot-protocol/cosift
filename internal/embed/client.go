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
	"sync/atomic"
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
// URL is forgiving — accepts either the full endpoint
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
				// via 2 idle by default) —'s bump caused queueing
				// at the embedder under heavy crawler load.
				ForceAttemptHTTP2:     true,
				IdleConnTimeout:       90 * time.Second,
				ResponseHeaderTimeout: 45 * time.Second,
			},
		},
	}
}

// Model returns the embedding model name (e.g. "text-embedding-3-small").
func (c *OpenAIClient) Model() string { return c.model }

// Dim returns the embedding vector dimension.
func (c *OpenAIClient) Dim() int { return c.dim }

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
	// only send Authorization when the caller provided a key.
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

// BatchingEmbedder coalesces concurrent Embed calls from multiple
// goroutines into single, larger HTTP requests to the inner backend.
// Workers submit their texts and block on a result channel; a single
// drainer goroutine packs texts into a batch until it hits maxBatch
// items OR maxWait has elapsed, then issues one inner.Embed call and
// distributes results back to each waiter.
//
// Why: ollama / vLLM serve N small (5-10 text) requests less efficiently
// than 1 large (50-100 text) request — the per-request HTTP/JSON
// overhead and the per-batch GPU launch cost amortize across more
// inputs. saw 994 docs/min with 16 in-flight per-doc embed
// calls; batching to 64-128 texts per call should give 2-3× more
// embeddings/sec on the same GPU.
//
// Caveat: per-Embed latency rises by up to maxWait (typically 10-30 ms).
// For the crawler this is invisible. For interactive /search embeds it
// hurts — wrap the crawler embedder only, not the search one.
type BatchingEmbedder struct {
	inner    Embedder
	maxBatch int
	maxWait  time.Duration
	queue    chan batchJob
	stop     chan struct{}
}

type batchJob struct {
	ctx   context.Context
	texts []string
	out   chan batchResult
}

type batchResult struct {
	vecs [][]float32
	err  error
}

// NewBatchingEmbedder wraps inner with a coalescing batcher. maxBatch
// = upper bound on number of texts per inner.Embed call (typical 64-
// 128). maxWait = how long the drainer waits to fill a batch before
// flushing what it has (typical 5-30 ms). Spawns one background
// goroutine that lives for the lifetime of the embedder.
func NewBatchingEmbedder(inner Embedder, maxBatch int, maxWait time.Duration) *BatchingEmbedder {
	if maxBatch <= 0 {
		maxBatch = 64
	}
	if maxWait <= 0 {
		maxWait = 20 * time.Millisecond
	}
	b := &BatchingEmbedder{
		inner:    inner,
		maxBatch: maxBatch,
		maxWait:  maxWait,
		queue:    make(chan batchJob, 1024),
		stop:     make(chan struct{}),
	}
	go b.drain()
	return b
}

// Model returns the underlying embedder's model name.
func (b *BatchingEmbedder) Model() string { return b.inner.Model() }

// Dim returns the underlying embedder's vector dimension.
func (b *BatchingEmbedder) Dim() int { return b.inner.Dim() }

// Embed coalesces concurrent calls into batches of up to maxBatch texts
// (or maxWait elapsed) before issuing one inner.Embed call. Returns
// vectors in the same order as texts.
func (b *BatchingEmbedder) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	if len(texts) == 0 {
		return nil, nil
	}
	out := make(chan batchResult, 1)
	select {
	case b.queue <- batchJob{ctx: ctx, texts: texts, out: out}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case r := <-out:
		return r.vecs, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (b *BatchingEmbedder) drain() {
	for {
		// Block for the first job — no timer running until something arrives.
		var first batchJob
		select {
		case first = <-b.queue:
		case <-b.stop:
			return
		}
		jobs := []batchJob{first}
		texts := make([]string, 0, b.maxBatch)
		texts = append(texts, first.texts...)
		// Now coalesce more jobs until we hit maxBatch or maxWait expires.
		timer := time.NewTimer(b.maxWait)
	gather:
		for len(texts) < b.maxBatch {
			select {
			case j := <-b.queue:
				// Skip the timer reset — once we start a batch we want to
				// fire on the original deadline so latency stays bounded.
				// We let the batch overshoot maxBatch rather than push the
				// job back onto the queue: re-queueing under contention
				// would deadlock waiters.
				jobs = append(jobs, j)
				texts = append(texts, j.texts...)
			case <-timer.C:
				break gather
			case <-b.stop:
				timer.Stop()
				return
			}
		}
		timer.Stop()
		// Single inner.Embed call for the coalesced batch.
		vecs, err := b.inner.Embed(first.ctx, texts)
		// Distribute back to each waiter.
		off := 0
		for _, j := range jobs {
			n := len(j.texts)
			if err != nil {
				j.out <- batchResult{err: err}
			} else {
				j.out <- batchResult{vecs: vecs[off : off+n]}
			}
			off += n
		}
	}
}

// Close stops the drainer goroutine. Pending jobs in the queue will be
// dropped — caller should drain or cancel outstanding contexts first.
func (b *BatchingEmbedder) Close() { close(b.stop) }

// RoundRobinEmbedder fans Embed calls across N inner Embedders. Use this
// when a single embedder backend can't saturate the available GPU — e.g.
// multiple ollama instances each holding their own copy of nomic-embed-
// text, or multiple vLLM workers. Per-call round-robin via an atomic
// counter; concurrent calls hit different backends without coordination.
type RoundRobinEmbedder struct {
	inners []Embedder
	next   atomic.Uint64
}

// NewRoundRobinEmbedder takes one or more inner embedders and returns
// a single Embedder that distributes calls across them. Returns inners[0]
// unwrapped when len(inners) == 1; nil when empty.
func NewRoundRobinEmbedder(inners []Embedder) Embedder {
	switch len(inners) {
	case 0:
		return nil
	case 1:
		return inners[0]
	}
	return &RoundRobinEmbedder{inners: inners}
}

// Model returns the model name of inners[0]; all backends must agree on
// model + dim for round-robin to be meaningful.
func (r *RoundRobinEmbedder) Model() string { return r.inners[0].Model() }

// Dim returns the vector dimension of inners[0].
func (r *RoundRobinEmbedder) Dim() int { return r.inners[0].Dim() }

// Embed rotates the next inner embedder atomically and delegates to it.
func (r *RoundRobinEmbedder) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	idx := int(r.next.Add(1)-1) % len(r.inners)
	return r.inners[idx].Embed(ctx, texts)
}

// ThrottledEmbedder wraps an Embedder behind a semaphore that caps the
// number of concurrent in-flight calls. The crawler ingest path runs
// hot — without a cap, 32+ workers can each issue an embed call at the
// same time, queueing N requests at the backend (ollama / vLLM). User
// /search?retriever=dense embeds then wait behind that backlog. Capping
// the crawler side keeps backend queue depth bounded so interactive
// queries find free capacity.
type ThrottledEmbedder struct {
	inner Embedder
	sem   chan struct{}
}

// NewThrottledEmbedder wraps inner with a semaphore of size maxInFlight.
// Embeds block on Acquire when maxInFlight are already running. Pass ≤ 0
// to disable the cap (returns inner unwrapped).
func NewThrottledEmbedder(inner Embedder, maxInFlight int) Embedder {
	if maxInFlight <= 0 {
		return inner
	}
	return &ThrottledEmbedder{inner: inner, sem: make(chan struct{}, maxInFlight)}
}

// Model returns the underlying embedder's model name.
func (t *ThrottledEmbedder) Model() string { return t.inner.Model() }

// Dim returns the underlying embedder's vector dimension.
func (t *ThrottledEmbedder) Dim() int { return t.inner.Dim() }

// Embed blocks on the semaphore (or ctx cancel) before delegating; bounds
// concurrent in-flight inner.Embed calls to maxInFlight.
func (t *ThrottledEmbedder) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	select {
	case t.sem <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { <-t.sem }()
	return t.inner.Embed(ctx, texts)
}

// CachedEmbedder wraps any Embedder with a content-addressable disk cache.
type CachedEmbedder struct {
	inner Embedder
	dir   string
	// atomic hit/miss counters for /stats visibility.
	hits   atomic.Uint64
	misses atomic.Uint64
}

// Hits returns the cumulative number of texts served from the cache.
func (c *CachedEmbedder) Hits() uint64 { return c.hits.Load() }

// Misses returns the cumulative number of texts that fell through to inner.
func (c *CachedEmbedder) Misses() uint64 { return c.misses.Load() }

// NewCachedEmbedder wraps inner. The cache directory is created if missing.
// Pass an empty dir to disable persistence (handy for tests).
func NewCachedEmbedder(inner Embedder, dir string) *CachedEmbedder {
	if dir != "" {
		_ = os.MkdirAll(dir, 0o755)
	}
	return &CachedEmbedder{inner: inner, dir: dir}
}

// Model returns the underlying embedder's model name.
func (c *CachedEmbedder) Model() string { return c.inner.Model() }

// Dim returns the underlying embedder's vector dimension.
func (c *CachedEmbedder) Dim() int { return c.inner.Dim() }

// Embed serves cached vectors from disk where available and routes the rest
// to inner. Misses are written to the cache on success.
func (c *CachedEmbedder) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	miss := make([]int, 0, len(texts))
	missTexts := make([]string, 0, len(texts))

	for i, t := range texts {
		v, ok := c.readCache(t)
		if ok {
			out[i] = v
			c.hits.Add(1)
			continue
		}
		miss = append(miss, i)
		missTexts = append(missTexts, t)
		c.misses.Add(1)
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
