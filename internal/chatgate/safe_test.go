package chatgate

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/embed"
)

type fakeChat struct {
	calls atomic.Int64
	err   atomic.Pointer[error]
	out   string
	model string
}

func (f *fakeChat) Model() string { return f.model }
func (f *fakeChat) Chat(ctx context.Context, _ []embed.ChatMsg) (string, error) {
	f.calls.Add(1)
	if err := f.err.Load(); err != nil && *err != nil {
		return "", *err
	}
	if ctx.Err() != nil {
		return "", ctx.Err()
	}
	return f.out, nil
}

func TestSafeChatHappyPath(t *testing.T) {
	fc := &fakeChat{out: "hi", model: "stub"}
	sc := NewSafeChat(fc, Options{Gate: New(2, 2), Kind: KindRerank, StageDeadline: time.Second})
	out, err := sc.Chat(context.Background(), nil)
	if err != nil {
		t.Fatalf("chat: %v", err)
	}
	if out != "hi" {
		t.Errorf("out: got %q", out)
	}
	if sc.CircuitState().State != "closed" {
		t.Errorf("circuit should stay closed")
	}
}

func TestSafeChatRetriesOn503(t *testing.T) {
	fc := &fakeChat{out: "ok", model: "stub"}
	transient := errors.New("chat http 503: try again")
	fc.err.Store(&transient)

	sc := NewSafeChat(fc, Options{
		Gate:          New(2, 2),
		Kind:          KindAnswer,
		StageDeadline: time.Second,
		MaxRetries:    2,
		RetryBackoff:  10 * time.Millisecond,
	})

	// Let the third call succeed.
	go func() {
		time.Sleep(15 * time.Millisecond)
		fc.err.Store(nil)
	}()

	out, err := sc.Chat(context.Background(), nil)
	if err != nil {
		t.Fatalf("chat: %v", err)
	}
	if out != "ok" {
		t.Errorf("out: got %q", out)
	}
	if fc.calls.Load() < 2 {
		t.Errorf("expected retry; calls=%d", fc.calls.Load())
	}
}

func TestSafeChatDoesNotRetryOn400(t *testing.T) {
	fc := &fakeChat{model: "stub"}
	hard := errors.New("chat http 400: bad request")
	fc.err.Store(&hard)
	sc := NewSafeChat(fc, Options{
		Gate:          New(1, 1),
		Kind:          KindAnswer,
		StageDeadline: time.Second,
		MaxRetries:    3,
		RetryBackoff:  5 * time.Millisecond,
	})
	_, err := sc.Chat(context.Background(), nil)
	if err == nil {
		t.Fatalf("expected error")
	}
	if fc.calls.Load() != 1 {
		t.Errorf("expected 1 call; got %d", fc.calls.Load())
	}
}

func TestSafeChatCircuitOpensAfterThreshold(t *testing.T) {
	fc := &fakeChat{model: "stub"}
	transient := errors.New("chat http 503: down")
	fc.err.Store(&transient)
	sc := NewSafeChat(fc, Options{
		Gate:            New(1, 1),
		Kind:            KindAnswer,
		StageDeadline:   100 * time.Millisecond,
		MaxRetries:      0,
		TripThreshold:   3,
		CircuitCooldown: 100 * time.Millisecond,
	})
	for i := 0; i < 3; i++ {
		_, _ = sc.Chat(context.Background(), nil)
	}
	if state := sc.CircuitState().State; state != "open" {
		t.Fatalf("circuit: got %q want open", state)
	}
	// Open → reject immediately, no inner call.
	startCalls := fc.calls.Load()
	_, err := sc.Chat(context.Background(), nil)
	if !errors.Is(err, ErrCircuitOpen) {
		t.Errorf("expected ErrCircuitOpen, got %v", err)
	}
	if fc.calls.Load() != startCalls {
		t.Errorf("circuit-open call hit inner: before=%d after=%d", startCalls, fc.calls.Load())
	}
}

func TestSafeChatCircuitClosesAfterProbe(t *testing.T) {
	fc := &fakeChat{out: "ok", model: "stub"}
	transient := errors.New("chat http 503: down")
	fc.err.Store(&transient)
	sc := NewSafeChat(fc, Options{
		Gate:            New(1, 1),
		Kind:            KindAnswer,
		StageDeadline:   100 * time.Millisecond,
		MaxRetries:      0,
		TripThreshold:   2,
		CircuitCooldown: 30 * time.Millisecond,
	})
	for i := 0; i < 2; i++ {
		_, _ = sc.Chat(context.Background(), nil)
	}
	if sc.CircuitState().State != "open" {
		t.Fatalf("circuit should be open")
	}
	time.Sleep(40 * time.Millisecond)
	// Half-open probe — fix the upstream and let one through.
	fc.err.Store(nil)
	out, err := sc.Chat(context.Background(), nil)
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if out != "ok" {
		t.Errorf("probe out: got %q", out)
	}
	if state := sc.CircuitState().State; state != "closed" {
		t.Errorf("post-probe circuit: got %q want closed", state)
	}
}
