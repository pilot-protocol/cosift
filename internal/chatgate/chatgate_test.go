package chatgate

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestGateAcquireRelease(t *testing.T) {
	g := New(2, 1)
	r1, err := g.Acquire(context.Background(), KindRerank)
	if err != nil {
		t.Fatalf("acquire1: %v", err)
	}
	r2, err := g.Acquire(context.Background(), KindRerank)
	if err != nil {
		t.Fatalf("acquire2: %v", err)
	}
	if g.Stats().InFlight != 2 {
		t.Errorf("in-flight: got %d want 2", g.Stats().InFlight)
	}
	r1()
	r2()
	if g.Stats().InFlight != 0 {
		t.Errorf("post-release in-flight: got %d want 0", g.Stats().InFlight)
	}
}

func TestGateBlocksWhenFull(t *testing.T) {
	g := New(1, 1)
	r1, _ := g.Acquire(context.Background(), KindAnswer)
	defer r1()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := g.Acquire(ctx, KindAnswer)
	if err == nil {
		t.Fatalf("expected context deadline error when full")
	}
	if g.Stats().Rejected != 1 {
		t.Errorf("rejected: got %d want 1", g.Stats().Rejected)
	}
}

func TestGateNilIsPassthrough(t *testing.T) {
	var g *Gate
	rel, err := g.Acquire(context.Background(), KindRerank)
	if err != nil {
		t.Fatalf("nil gate: %v", err)
	}
	rel()
	if g.Stats() != (Stats{}) {
		t.Errorf("nil gate stats not zero")
	}
}

func TestGateConcurrency(t *testing.T) {
	g := New(4, 2)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rel, err := g.Acquire(context.Background(), KindRerank)
			if err != nil {
				return
			}
			time.Sleep(5 * time.Millisecond)
			rel()
		}()
	}
	wg.Wait()
	st := g.Stats()
	if st.InFlight != 0 {
		t.Errorf("in-flight leak: got %d want 0", st.InFlight)
	}
	if st.Acquired != 50 {
		t.Errorf("acquired: got %d want 50", st.Acquired)
	}
}
