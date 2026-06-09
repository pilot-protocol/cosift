package sla

import (
	"sync"
	"time"
)

// ringBuffer is a fixed-size FIFO of Samples used to bound memory in
// the SLA tracker. Overwriting the oldest slot is intentional: the
// evaluator filters by time-window anyway, so old slots not in window
// are ignored even when they're still in the buffer. Capacity sized
// for ~2 minutes at 8 RPS with one sample per request.
type ringBuffer struct {
	mu  sync.Mutex
	buf []Sample
	pos int
	n   int
}

func newRingBuffer(capacity int) *ringBuffer {
	if capacity <= 0 {
		capacity = 1024
	}
	return &ringBuffer{buf: make([]Sample, capacity)}
}

func (r *ringBuffer) add(s Sample) {
	r.mu.Lock()
	r.buf[r.pos] = s
	r.pos = (r.pos + 1) % len(r.buf)
	if r.n < len(r.buf) {
		r.n++
	}
	r.mu.Unlock()
}

func (r *ringBuffer) snapshot(notBefore time.Time) []Sample {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Sample, 0, r.n)
	for i := 0; i < r.n; i++ {
		s := r.buf[i]
		if s.At.Before(notBefore) {
			continue
		}
		out = append(out, s)
	}
	return out
}
