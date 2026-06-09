package chatgate

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pilot-protocol/cosift/internal/embed"
)

// ErrCircuitOpen is returned when the circuit breaker is OPEN and callers
// must not attempt the LLM. Callers should fall back to a degraded path
// (rerank → passthrough, /answer → "service degraded" response).
var ErrCircuitOpen = errors.New("chatgate: circuit open, LLM degraded")

// Options configure a SafeChat wrapper.
type Options struct {
	// Gate is the bounded-concurrency semaphore. nil = unbounded.
	Gate *Gate
	// Kind picks which gate pool the calls draw from.
	Kind Kind
	// StageDeadline caps the parent ctx for this stage. 0 = inherit
	// parent only. Typical: rerank 5s, answer 30s, aux 10s.
	StageDeadline time.Duration
	// MaxRetries is the max retry attempts on transient errors (net errors,
	// 502/503). 0 disables retry. Default 1.
	MaxRetries int
	// RetryBackoff is the initial wait between attempts; doubles per
	// retry. Default 200ms.
	RetryBackoff time.Duration
	// TripThreshold is the consecutive-failure count that opens the
	// circuit. 0 disables. Default 8.
	TripThreshold int
	// CircuitCooldown is how long the circuit stays OPEN before HALF-OPEN
	// allows a probe. Default 30s.
	CircuitCooldown time.Duration
}

// SafeChat wraps an embed.ChatClient with:
//   - bounded concurrency via Gate
//   - per-stage deadline
//   - retry on transient errors (network, 502, 503)
//   - circuit breaker that opens after N consecutive failures
//
// nil receiver is forbidden — callers should construct with NewSafeChat.
type SafeChat struct {
	inner embed.ChatClient
	opts  Options

	// Circuit-breaker state. Single mutex covers all state — contended
	// only on transitions, not on the hot path (atomic checks suffice
	// to short-circuit when closed).
	mu        sync.Mutex
	state     circuitState
	openUntil time.Time

	consecutiveFails atomic.Int64
	totalCalls       atomic.Int64
	totalFails       atomic.Int64
	retried          atomic.Int64
	tripped          atomic.Int64
}

type circuitState int

const (
	circuitClosed circuitState = iota
	circuitOpen
	circuitHalfOpen
)

// NewSafeChat wraps inner with the given options.
func NewSafeChat(inner embed.ChatClient, opts Options) *SafeChat {
	if opts.MaxRetries == 0 {
		opts.MaxRetries = 1
	}
	if opts.RetryBackoff == 0 {
		opts.RetryBackoff = 200 * time.Millisecond
	}
	if opts.TripThreshold == 0 {
		opts.TripThreshold = 8
	}
	if opts.CircuitCooldown == 0 {
		opts.CircuitCooldown = 30 * time.Second
	}
	return &SafeChat{inner: inner, opts: opts}
}

// Model passes through.
func (s *SafeChat) Model() string { return s.inner.Model() }

// Chat acquires a gate slot, applies the stage deadline, retries on
// transient errors, and respects the circuit breaker.
func (s *SafeChat) Chat(ctx context.Context, msgs []embed.ChatMsg) (string, error) {
	if err := s.canProceed(); err != nil {
		return "", err
	}
	release, err := s.opts.Gate.Acquire(ctx, s.opts.Kind)
	if err != nil {
		return "", err
	}
	defer release()
	return s.callWithRetry(ctx, msgs)
}

// ChatStream forwards to inner.ChatStream if available; otherwise calls
// Chat and emits the whole result as one chunk. Same gate/deadline/retry.
func (s *SafeChat) ChatStream(ctx context.Context, msgs []embed.ChatMsg, onChunk func(string)) (string, error) {
	if err := s.canProceed(); err != nil {
		return "", err
	}
	release, err := s.opts.Gate.Acquire(ctx, s.opts.Kind)
	if err != nil {
		return "", err
	}
	defer release()

	streamer, ok := s.inner.(embed.StreamingChatClient)
	if !ok {
		out, err := s.callWithRetry(ctx, msgs)
		if err == nil && onChunk != nil && out != "" {
			onChunk(out)
		}
		return out, err
	}
	callCtx, cancel := s.withStageDeadline(ctx)
	defer cancel()
	s.totalCalls.Add(1)
	out, err := streamer.ChatStream(callCtx, msgs, onChunk)
	s.observe(err)
	return out, err
}

func (s *SafeChat) callWithRetry(ctx context.Context, msgs []embed.ChatMsg) (string, error) {
	backoff := s.opts.RetryBackoff
	var lastErr error
	for attempt := 0; attempt <= s.opts.MaxRetries; attempt++ {
		callCtx, cancel := s.withStageDeadline(ctx)
		s.totalCalls.Add(1)
		out, err := s.inner.Chat(callCtx, msgs)
		cancel()
		if err == nil {
			s.observe(nil)
			return out, nil
		}
		lastErr = err
		// Don't retry if the context is done — caller's budget is spent.
		if ctx.Err() != nil {
			s.observe(err)
			return "", err
		}
		if !isTransient(err) || attempt == s.opts.MaxRetries {
			s.observe(err)
			return "", err
		}
		s.retried.Add(1)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			s.observe(err)
			return "", ctx.Err()
		}
		backoff *= 2
	}
	s.observe(lastErr)
	return "", lastErr
}

func (s *SafeChat) withStageDeadline(parent context.Context) (context.Context, context.CancelFunc) {
	if s.opts.StageDeadline <= 0 {
		return context.WithCancel(parent)
	}
	return context.WithTimeout(parent, s.opts.StageDeadline)
}

// canProceed checks the circuit. Closed → OK. Open + not cooled down →
// reject. Open + cooled down → flip to HalfOpen and let the call probe.
func (s *SafeChat) canProceed() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch s.state {
	case circuitClosed, circuitHalfOpen:
		return nil
	case circuitOpen:
		if time.Now().After(s.openUntil) {
			s.state = circuitHalfOpen
			return nil
		}
		return ErrCircuitOpen
	}
	return nil
}

// observe updates breaker state from a call outcome. Success in
// HalfOpen → Closed. Consecutive failures above threshold → Open.
func (s *SafeChat) observe(err error) {
	if err == nil {
		s.consecutiveFails.Store(0)
		s.mu.Lock()
		if s.state == circuitHalfOpen {
			s.state = circuitClosed
		}
		s.mu.Unlock()
		return
	}
	s.totalFails.Add(1)
	n := s.consecutiveFails.Add(1)
	if s.opts.TripThreshold <= 0 {
		return
	}
	if n >= int64(s.opts.TripThreshold) {
		s.mu.Lock()
		if s.state != circuitOpen {
			s.state = circuitOpen
			s.openUntil = time.Now().Add(s.opts.CircuitCooldown)
			s.tripped.Add(1)
		}
		s.mu.Unlock()
	}
}

// CircuitState exposes the current breaker state for /stats.
type CircuitState struct {
	State            string `json:"state"`
	OpenUntil        string `json:"open_until,omitempty"`
	TotalCalls       int64  `json:"total_calls"`
	TotalFails       int64  `json:"total_fails"`
	ConsecutiveFails int64  `json:"consecutive_fails"`
	Retried          int64  `json:"retried"`
	Tripped          int64  `json:"tripped"`
}

func (s *SafeChat) CircuitState() CircuitState {
	s.mu.Lock()
	state := s.state
	openUntil := s.openUntil
	s.mu.Unlock()
	out := CircuitState{
		TotalCalls:       s.totalCalls.Load(),
		TotalFails:       s.totalFails.Load(),
		ConsecutiveFails: s.consecutiveFails.Load(),
		Retried:          s.retried.Load(),
		Tripped:          s.tripped.Load(),
	}
	switch state {
	case circuitClosed:
		out.State = "closed"
	case circuitOpen:
		out.State = "open"
		out.OpenUntil = openUntil.Format(time.RFC3339)
	case circuitHalfOpen:
		out.State = "half-open"
	}
	return out
}

// isTransient decides whether an LLM error is worth retrying. Network
// errors and 5xx (especially 502/503 = upstream restart) are. 4xx
// (bad request, auth) and the local context errors are not.
func isTransient(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return false
	}
	var nerr net.Error
	if errors.As(err, &nerr) {
		return true
	}
	msg := err.Error()
	for _, s := range transientHTTPMarkers {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}

// transientHTTPMarkers are substrings the embed.OpenAIChat client formats
// into the error message — we don't get a structured response code back,
// so we look for the http NNN marker the formatter emits.
var transientHTTPMarkers = []string{
	"http 502",
	"http 503",
	"http 504",
	"http 429",
}

// String renders the wrapper for log lines.
func (s *SafeChat) String() string {
	st := s.CircuitState()
	return fmt.Sprintf("safechat[model=%s state=%s calls=%d fails=%d]", s.Model(), st.State, st.TotalCalls, st.TotalFails)
}
