package embed

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeEmbedder is a deterministic in-memory Embedder for testing wrappers.
type fakeEmbedder struct {
	model   string
	dim     int
	calls   atomic.Int64
	totalIn atomic.Int64 // total input texts seen across calls
	err     error
	delay   time.Duration
	maxConc int32 // tracks max concurrency
	curConc int32 // current in-flight calls
	concMu  sync.Mutex
}

func (f *fakeEmbedder) Model() string { return f.model }
func (f *fakeEmbedder) Dim() int      { return f.dim }

func (f *fakeEmbedder) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	cur := atomic.AddInt32(&f.curConc, 1)
	defer atomic.AddInt32(&f.curConc, -1)
	f.concMu.Lock()
	if cur > f.maxConc {
		f.maxConc = cur
	}
	f.concMu.Unlock()

	f.calls.Add(1)
	f.totalIn.Add(int64(len(texts)))
	if f.delay > 0 {
		select {
		case <-time.After(f.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if f.err != nil {
		return nil, f.err
	}
	out := make([][]float32, len(texts))
	for i, t := range texts {
		// Deterministic per-text vector: first byte of text as float, rest zero.
		v := make([]float32, f.dim)
		if len(t) > 0 {
			v[0] = float32(t[0])
		}
		out[i] = v
	}
	return out, nil
}

// --- OpenAIClient pure-method tests ---

func TestOpenAIClientModelAndDim(t *testing.T) {
	c := NewOpenAIClient("k", "http://x", "test-model", 768)
	if c.Model() != "test-model" {
		t.Errorf("Model: %q", c.Model())
	}
	if c.Dim() != 768 {
		t.Errorf("Dim: %d", c.Dim())
	}
}

func TestNewOpenAIChatURLNormalization(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"", "https://api.openai.com/v1/chat/completions"},
		{"https://x/v1", "https://x/v1/chat/completions"},
		{"https://x/v1/", "https://x/v1/chat/completions"},
		{"https://x/v1/chat/completions", "https://x/v1/chat/completions"},
	}
	for _, c := range cases {
		got := NewOpenAIChat("k", c.in, "model-id")
		if got.URL != c.want {
			t.Errorf("NewOpenAIChat(%q).URL: got %q want %q", c.in, got.URL, c.want)
		}
		if got.Model() != "model-id" {
			t.Errorf("Model: %q", got.Model())
		}
	}
}

// --- RoundRobinEmbedder ---

func TestNewRoundRobinEmbedderEmpty(t *testing.T) {
	if got := NewRoundRobinEmbedder(nil); got != nil {
		t.Errorf("empty: got %v want nil", got)
	}
	if got := NewRoundRobinEmbedder([]Embedder{}); got != nil {
		t.Errorf("empty slice: got %v want nil", got)
	}
}

func TestNewRoundRobinEmbedderSingle(t *testing.T) {
	inner := &fakeEmbedder{model: "x", dim: 4}
	got := NewRoundRobinEmbedder([]Embedder{inner})
	// Single element returns the inner unwrapped.
	if _, isRR := got.(*RoundRobinEmbedder); isRR {
		t.Errorf("single-element should NOT wrap as RoundRobinEmbedder")
	}
	if got.Model() != "x" {
		t.Errorf("Model: %q", got.Model())
	}
}

func TestRoundRobinEmbedderDistributes(t *testing.T) {
	a := &fakeEmbedder{model: "m", dim: 4}
	b := &fakeEmbedder{model: "m", dim: 4}
	c := &fakeEmbedder{model: "m", dim: 4}

	rr := NewRoundRobinEmbedder([]Embedder{a, b, c})
	if rr.Model() != "m" || rr.Dim() != 4 {
		t.Errorf("Model/Dim from inners[0]: %q/%d", rr.Model(), rr.Dim())
	}

	ctx := context.Background()
	for i := 0; i < 6; i++ {
		if _, err := rr.Embed(ctx, []string{"t"}); err != nil {
			t.Fatalf("Embed: %v", err)
		}
	}
	// 6 calls across 3 backends → each gets exactly 2.
	if a.calls.Load() != 2 || b.calls.Load() != 2 || c.calls.Load() != 2 {
		t.Errorf("distribution: a=%d b=%d c=%d", a.calls.Load(), b.calls.Load(), c.calls.Load())
	}
}

// --- ThrottledEmbedder ---

func TestNewThrottledEmbedderMaxZero(t *testing.T) {
	inner := &fakeEmbedder{model: "x", dim: 4}
	got := NewThrottledEmbedder(inner, 0)
	if _, ok := got.(*ThrottledEmbedder); ok {
		t.Errorf("max<=0 should return inner unwrapped")
	}
	got = NewThrottledEmbedder(inner, -5)
	if _, ok := got.(*ThrottledEmbedder); ok {
		t.Errorf("max<0 should return inner unwrapped")
	}
}

func TestThrottledEmbedderModelDim(t *testing.T) {
	inner := &fakeEmbedder{model: "x", dim: 4}
	w := NewThrottledEmbedder(inner, 2)
	if w.Model() != "x" || w.Dim() != 4 {
		t.Errorf("delegation broken: %q/%d", w.Model(), w.Dim())
	}
}

func TestThrottledEmbedderCapsConcurrency(t *testing.T) {
	inner := &fakeEmbedder{model: "x", dim: 4, delay: 30 * time.Millisecond}
	w := NewThrottledEmbedder(inner, 2)
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = w.Embed(ctx, []string{"hi"})
		}()
	}
	wg.Wait()
	inner.concMu.Lock()
	got := inner.maxConc
	inner.concMu.Unlock()
	if got > 2 {
		t.Errorf("concurrency cap broken: maxConc=%d want <=2", got)
	}
	if inner.calls.Load() != 8 {
		t.Errorf("all calls should complete: got %d want 8", inner.calls.Load())
	}
}

func TestThrottledEmbedderCancelDuringQueue(t *testing.T) {
	// Saturate with 1 in-flight, then attempt a 2nd that will hang on the sem.
	inner := &fakeEmbedder{model: "x", dim: 4, delay: 100 * time.Millisecond}
	w := NewThrottledEmbedder(inner, 1)
	ctx := context.Background()
	go func() {
		_, _ = w.Embed(ctx, []string{"first"})
	}()
	// Wait a beat so the goroutine actually grabs the slot.
	time.Sleep(10 * time.Millisecond)

	ctx2, cancel := context.WithCancel(context.Background())
	cancel() // immediately cancel
	_, err := w.Embed(ctx2, []string{"second"})
	if err == nil {
		t.Errorf("expected ctx.Err from canceled context")
	}
}

// --- BatchingEmbedder ---

func TestNewBatchingEmbedderDefaults(t *testing.T) {
	inner := &fakeEmbedder{model: "x", dim: 4}
	b := NewBatchingEmbedder(inner, 0, 0)
	defer b.Close()
	if b.maxBatch != 64 {
		t.Errorf("maxBatch default: got %d", b.maxBatch)
	}
	if b.maxWait <= 0 {
		t.Errorf("maxWait default: got %v", b.maxWait)
	}
	if b.Model() != "x" || b.Dim() != 4 {
		t.Errorf("delegation: %q/%d", b.Model(), b.Dim())
	}
}

func TestBatchingEmbedderSingleCall(t *testing.T) {
	inner := &fakeEmbedder{model: "x", dim: 4}
	b := NewBatchingEmbedder(inner, 32, 20*time.Millisecond)
	defer b.Close()

	vecs, err := b.Embed(context.Background(), []string{"hi", "bye"})
	if err != nil {
		t.Fatalf("Embed: %v", err)
	}
	if len(vecs) != 2 {
		t.Errorf("vecs len: got %d want 2", len(vecs))
	}
	if vecs[0][0] != float32('h') || vecs[1][0] != float32('b') {
		t.Errorf("wrong content order: %v %v", vecs[0][0], vecs[1][0])
	}
}

func TestBatchingEmbedderEmpty(t *testing.T) {
	inner := &fakeEmbedder{model: "x", dim: 4}
	b := NewBatchingEmbedder(inner, 32, 5*time.Millisecond)
	defer b.Close()
	vecs, err := b.Embed(context.Background(), nil)
	if err != nil {
		t.Errorf("empty: %v", err)
	}
	if vecs != nil {
		t.Errorf("nil input should return nil vecs, got %v", vecs)
	}
}

func TestBatchingEmbedderCoalesces(t *testing.T) {
	inner := &fakeEmbedder{model: "x", dim: 4}
	b := NewBatchingEmbedder(inner, 32, 50*time.Millisecond)
	defer b.Close()

	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			_, _ = b.Embed(ctx, []string{string(rune('a' + i))})
		}()
	}
	wg.Wait()
	// All 5 callers issued 1 text each. Coalesced into 1-2 batches.
	calls := inner.calls.Load()
	if calls > 3 {
		t.Errorf("expected coalesced (<=3 calls), got %d", calls)
	}
	if inner.totalIn.Load() != 5 {
		t.Errorf("total texts: got %d want 5", inner.totalIn.Load())
	}
}

func TestBatchingEmbedderInnerError(t *testing.T) {
	inner := &fakeEmbedder{model: "x", dim: 4, err: errors.New("boom")}
	b := NewBatchingEmbedder(inner, 32, 5*time.Millisecond)
	defer b.Close()
	_, err := b.Embed(context.Background(), []string{"x"})
	if err == nil {
		t.Errorf("expected inner error to propagate")
	}
}

// --- CachedEmbedder ---

func TestCachedEmbedderModelDim(t *testing.T) {
	inner := &fakeEmbedder{model: "m", dim: 8}
	c := NewCachedEmbedder(inner, "")
	if c.Model() != "m" || c.Dim() != 8 {
		t.Errorf("delegation: %q/%d", c.Model(), c.Dim())
	}
}

func TestCachedEmbedderHitsMisses(t *testing.T) {
	inner := &fakeEmbedder{model: "m", dim: 4}
	c := NewCachedEmbedder(inner, t.TempDir())

	// Initial counters.
	if c.Hits() != 0 || c.Misses() != 0 {
		t.Errorf("initial counters non-zero: hits=%d misses=%d", c.Hits(), c.Misses())
	}

	ctx := context.Background()
	// First call: 2 misses.
	if _, err := c.Embed(ctx, []string{"alpha", "beta"}); err != nil {
		t.Fatalf("Embed: %v", err)
	}
	if c.Misses() != 2 {
		t.Errorf("after first: misses=%d want 2", c.Misses())
	}

	// Second call with same texts: 2 hits.
	if _, err := c.Embed(ctx, []string{"alpha", "beta"}); err != nil {
		t.Fatalf("Embed: %v", err)
	}
	if c.Hits() != 2 {
		t.Errorf("after second: hits=%d want 2", c.Hits())
	}

	// Mixed: 1 hit, 1 miss.
	if _, err := c.Embed(ctx, []string{"alpha", "gamma"}); err != nil {
		t.Fatalf("Embed: %v", err)
	}
	if c.Hits() != 3 || c.Misses() != 3 {
		t.Errorf("after mixed: hits=%d misses=%d", c.Hits(), c.Misses())
	}
}

func TestCachedEmbedderNoDir(t *testing.T) {
	// Empty dir disables persistence; calls still work.
	inner := &fakeEmbedder{model: "m", dim: 4}
	c := NewCachedEmbedder(inner, "")
	if _, err := c.Embed(context.Background(), []string{"x"}); err != nil {
		t.Errorf("empty dir Embed: %v", err)
	}
}
