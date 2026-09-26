package index

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"
)

func trySearchGraph(t *testing.T) *HNSW {
	t.Helper()
	h := NewHNSW(4)
	for i := range 20 {
		h.Add(fmt.Sprintf("https://x.example/%d", i), "t", []float32{float32(i), 1, float32(i % 3), 0.5})
	}
	return h
}

func TestTrySearchMatchesSearch(t *testing.T) {
	h := trySearchGraph(t)
	q := []float32{3, 1, 0, 0.5}
	got, ok := h.TrySearch(context.Background(), q, 5, time.Second)
	if !ok || !reflect.DeepEqual(got, h.Search(context.Background(), q, 5)) {
		t.Fatalf("TrySearch %v %v", got, ok)
	}
	if got, ok := h.TrySearch(context.Background(), []float32{1}, 5, time.Second); !ok || got != nil {
		t.Fatalf("wrong dimension: %v %v", got, ok)
	}
	h.mu.RLock()
	_, ok = h.TrySearch(context.Background(), q, 5, 50*time.Millisecond)
	h.mu.RUnlock()
	if !ok {
		t.Fatal("a concurrent reader blocked TrySearch")
	}
}

func TestTrySearchGivesUpOnWriter(t *testing.T) {
	h := trySearchGraph(t)
	q := []float32{3, 1, 0, 0.5}
	h.mu.Lock()
	start := time.Now()
	_, ok := h.TrySearch(context.Background(), q, 5, 50*time.Millisecond)
	d := time.Since(start)
	if ok || d < 50*time.Millisecond || d > time.Second {
		h.mu.Unlock()
		t.Fatalf("ok=%v after %s", ok, d)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, ok := h.TrySearch(ctx, q, 5, time.Minute); ok {
		h.mu.Unlock()
		t.Fatal("cancelled context searched")
	}
	var wg sync.WaitGroup
	results := make(chan bool, 2*maxProbeWaiters)
	for range 2 * maxProbeWaiters {
		wg.Go(func() {
			_, ok := h.TrySearch(context.Background(), q, 5, 300*time.Millisecond)
			results <- ok
		})
	}
	wg.Wait()
	close(results)
	for ok := range results {
		if ok {
			t.Fatal("searched under the write lock")
		}
	}
	h.mu.Unlock()
	deadline := time.Now().Add(5 * time.Second)
	for h.probeWaiters.Load() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%d probes still queued after the writer left", h.probeWaiters.Load())
		}
		time.Sleep(time.Millisecond)
	}
	if _, ok := h.TrySearch(context.Background(), q, 5, time.Second); !ok {
		t.Fatal("no search after the writer left")
	}
}

func TestTrySearchBoundsQueuedProbes(t *testing.T) {
	h := trySearchGraph(t)
	q := []float32{3, 1, 0, 0.5}
	h.mu.Lock()
	defer h.mu.Unlock()
	var wg sync.WaitGroup
	for range maxProbeWaiters {
		wg.Go(func() { h.TrySearch(context.Background(), q, 5, 20*time.Millisecond) })
	}
	wg.Wait()
	if n := h.probeWaiters.Load(); n != maxProbeWaiters {
		t.Fatalf("%d queued probes", n)
	}
	start := time.Now()
	if _, ok := h.TrySearch(context.Background(), q, 5, time.Minute); ok || time.Since(start) > 100*time.Millisecond {
		t.Fatalf("probe over the bound waited %s", time.Since(start))
	}
}
