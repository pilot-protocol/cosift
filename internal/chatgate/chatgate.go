// Package chatgate bounds concurrent LLM (chat-completions) calls so a
// burst of /answer + /rerank traffic can't flood the vLLM backend, pin
// hundreds of goroutines in http.Client.Do, or starve the box of memory.
//
// Each Kind has its own semaphore: rerank is called per-query (usually
// in a tight loop fanout for /research) so its cap is higher; answer is
// the final generation step and burns the largest KV slice.
// Calls block on Acquire until a slot opens or the caller's context
// fires. The wait time is recorded so /metrics surfaces "how long was
// the LLM gate the bottleneck this minute."
package chatgate

import (
	"context"
	"sync/atomic"
	"time"
)

// Kind selects which semaphore to draw from.
type Kind int

const (
	// KindRerank is the per-passage scoring call. Short prompts, small
	// outputs. Cap is the dominant one because /research fans out.
	KindRerank Kind = iota
	// KindAnswer is the /answer synthesis call. Long prompts (retrieved
	// passages) + long outputs. Cap is smaller because each call holds
	// a vLLM sequence slot for seconds.
	KindAnswer
	// KindAux covers query expansion paths (HyDE, paraphrase, planner).
	// Uses the answer pool because they share the long-prompt cost class.
	KindAux
)

// Gate is the bounded-concurrency arbiter for chat calls.
type Gate struct {
	rerank chan struct{}
	answer chan struct{}

	// Counter telemetry: callers exposed via /metrics so operators can
	// see whether the gate is doing work (acquired) or shedding load
	// (rejected) and how much wait time it adds.
	acquired       atomic.Int64
	rejected       atomic.Int64
	waitNanosTotal atomic.Int64
	inFlight       atomic.Int64
}

// New builds a Gate with the given pool caps. Pass 0 to disable a
// channel; nil-Gate.Acquire is a no-op so callers can flip the gate
// off entirely via NewDisabled or by passing nil.
func New(rerankCap, answerCap int) *Gate {
	if rerankCap <= 0 {
		rerankCap = 8
	}
	if answerCap <= 0 {
		answerCap = 4
	}
	return &Gate{
		rerank: make(chan struct{}, rerankCap),
		answer: make(chan struct{}, answerCap),
	}
}

// Acquire blocks until a slot is available or ctx fires. Returns a
// release func that must be called when the LLM call returns. nil
// receiver = unbounded passthrough (testing / disabled).
func (g *Gate) Acquire(ctx context.Context, kind Kind) (func(), error) {
	if g == nil {
		return func() {}, nil
	}
	ch := g.answer
	if kind == KindRerank {
		ch = g.rerank
	}
	start := time.Now()
	select {
	case ch <- struct{}{}:
		g.waitNanosTotal.Add(int64(time.Since(start)))
		g.acquired.Add(1)
		g.inFlight.Add(1)
		return func() {
			<-ch
			g.inFlight.Add(-1)
		}, nil
	case <-ctx.Done():
		g.rejected.Add(1)
		return nil, ctx.Err()
	}
}

// Stats is a snapshot of the gate counters. Read-mostly so /metrics
// and /stats can publish without contending with the hot path.
type Stats struct {
	Acquired       int64 `json:"acquired"`
	Rejected       int64 `json:"rejected"`
	InFlight       int64 `json:"in_flight"`
	RerankCap      int   `json:"rerank_cap"`
	AnswerCap      int   `json:"answer_cap"`
	WaitNanosTotal int64 `json:"wait_nanos_total"`
}

// Stats returns a snapshot. Nil receiver returns the zero value.
func (g *Gate) Stats() Stats {
	if g == nil {
		return Stats{}
	}
	return Stats{
		Acquired:       g.acquired.Load(),
		Rejected:       g.rejected.Load(),
		InFlight:       g.inFlight.Load(),
		RerankCap:      cap(g.rerank),
		AnswerCap:      cap(g.answer),
		WaitNanosTotal: g.waitNanosTotal.Load(),
	}
}
