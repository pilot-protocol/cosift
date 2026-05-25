package crawler

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestHostGateSerializesSameHost(t *testing.T) {
	g := newHostGate(50*time.Millisecond, nil)
	ctx := context.Background()

	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = g.Wait(ctx, "example.com")
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	// 3 calls × 50ms gap = at least 100ms (slot 0 immediate, slot 1 at +50ms, slot 2 at +100ms).
	if elapsed < 90*time.Millisecond {
		t.Errorf("expected at least 90ms for 3 serialized calls, got %v", elapsed)
	}
	if elapsed > 250*time.Millisecond {
		t.Errorf("unexpectedly slow: %v", elapsed)
	}
}

func TestHostGateDifferentHostsConcurrent(t *testing.T) {
	g := newHostGate(100*time.Millisecond, nil)
	ctx := context.Background()

	start := time.Now()
	var wg sync.WaitGroup
	for _, h := range []string{"a.com", "b.com", "c.com"} {
		h := h
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = g.Wait(ctx, h)
		}()
	}
	wg.Wait()

	// Three different hosts, all should fire ~simultaneously (first slot is "now").
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Errorf("different hosts should not serialize: took %v", elapsed)
	}
}

func TestHostGateRespectsContext(t *testing.T) {
	g := newHostGate(10*time.Second, nil)
	// Burn the first slot.
	_ = g.Wait(context.Background(), "slow.com")

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := g.Wait(ctx, "slow.com")
	if err == nil {
		t.Errorf("expected ctx error, got nil")
	}
}

func TestHostGateOverrideAppliesPerHost(t *testing.T) {
	// Iter 128: override map should set a different delay for the named host
	// while leaving others on the default.
	overrides := map[string]time.Duration{
		"slow.example.com": 200 * time.Millisecond,
		"fast.example.com": 5 * time.Millisecond,
	}
	g := newHostGate(50*time.Millisecond, overrides)

	if d := g.delayFor("slow.example.com"); d != 200*time.Millisecond {
		t.Errorf("slow.example.com: want 200ms override, got %v", d)
	}
	if d := g.delayFor("fast.example.com"); d != 5*time.Millisecond {
		t.Errorf("fast.example.com: want 5ms override, got %v", d)
	}
	if d := g.delayFor("other.example.com"); d != 50*time.Millisecond {
		t.Errorf("other.example.com (no override): want default 50ms, got %v", d)
	}
}

func TestHostGateOverrideSerializesAtOverrideRate(t *testing.T) {
	// Verify the override is actually used during Wait (not just delayFor).
	// Override fast.com at 20ms; 3 sequential Waits should take ~40ms total.
	overrides := map[string]time.Duration{"fast.com": 20 * time.Millisecond}
	g := newHostGate(500*time.Millisecond, overrides) // default deliberately slow

	ctx := context.Background()
	start := time.Now()
	for i := 0; i < 3; i++ {
		_ = g.Wait(ctx, "fast.com")
	}
	elapsed := time.Since(start)

	// 3 waits at 20ms each = ~40ms total. Default 500ms would take ~1s.
	if elapsed > 200*time.Millisecond {
		t.Errorf("override not applied — took %v (would be ~40ms at override, ~1s at default)", elapsed)
	}
	if elapsed < 30*time.Millisecond {
		t.Errorf("override applied too fast — took %v, expected ~40ms", elapsed)
	}
}

func TestHostGateNilOverridesIsSafe(t *testing.T) {
	// Constructing with nil overrides should work exactly like the iter-1 single-arg
	// constructor — every host uses the default. This is the path the iter-128
	// crawler instantiation takes when cfg.PerHostOverrides is empty.
	g := newHostGate(10*time.Millisecond, nil)
	if d := g.delayFor("anywhere.com"); d != 10*time.Millisecond {
		t.Errorf("nil overrides: want default 10ms, got %v", d)
	}
}
