package crawler

import (
	"context"
	"sync"
	"time"
)

// hostGate enforces per-host minimum delay between fetches.
//
// Implementation: a map of (host → earliest-allowed-fetch-time). When a worker
// calls Wait, it reads the host's next slot, reserves the slot AFTER that for
// itself (mutating the map), and sleeps until the reserved time arrives.
//
// This is correct under contention because the slot reservation happens under
// the mutex BEFORE the sleep — two workers asking for the same host get
// serialized into back-to-back slots.
//
// Across hosts there's no serialization. N workers can hammer N different hosts
// in parallel.
//
// Why not x/time/rate: we'd need one rate.Limiter per host with manual cleanup.
// 40 LOC of our own code with the same correctness and a simpler model — no dep.
//
// `overrides` map lets operators set per-host delays distinct from
// the default. Strict exact host match (including port if non-default).
type hostGate struct {
	mu        sync.Mutex
	next      map[string]time.Time
	delay     time.Duration
	overrides map[string]time.Duration
}

func newHostGate(delay time.Duration, overrides map[string]time.Duration) *hostGate {
	return &hostGate{
		next:      make(map[string]time.Time),
		delay:     delay,
		overrides: overrides,
	}
}

// delayFor returns the effective delay for a host: override if present,
// default otherwise. Safe to call without holding g.mu — overrides is set
// at construction and not mutated.
func (g *hostGate) delayFor(host string) time.Duration {
	if d, ok := g.overrides[host]; ok {
		return d
	}
	return g.delay
}

// Wait blocks until this worker's reserved slot for the host arrives.
// Returns ctx.Err() if ctx fires first.
func (g *hostGate) Wait(ctx context.Context, host string) error {
	d := g.delayFor(host)
	g.mu.Lock()
	now := time.Now()
	slot, ok := g.next[host]
	if !ok || !now.Before(slot) {
		slot = now
	}
	// Reserve our slot (the current `slot`) and bump the host's next slot
	// by the host-specific delay.
	g.next[host] = slot.Add(d)
	g.mu.Unlock()

	wait := time.Until(slot)
	if wait <= 0 {
		return nil
	}
	t := time.NewTimer(wait)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
