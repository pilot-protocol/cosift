package server

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/pilot-protocol/cosift/internal/embed"
	"github.com/pilot-protocol/cosift/internal/store"
)

// Iter 165 — polymorphic chat stub that switches reply on the system prompt
// purpose. Lets us exercise /research?hyde=true end-to-end without three
// separate fake chat clients.
type polyChat struct {
	planReply   string
	hydePrefix  string
	synthReply  string
	calls       int
	hydeCalls   int
	hydeQueries []string
	mu          sync.Mutex
}

func (p *polyChat) Model() string { return "poly-chat" }
func (p *polyChat) Chat(_ context.Context, msgs []embed.ChatMsg) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	var system, user string
	for _, m := range msgs {
		switch m.Role {
		case "system":
			system = m.Content
		case "user":
			user = m.Content
		}
	}
	switch {
	case strings.Contains(system, "Decompose"):
		return p.planReply, nil
	case strings.Contains(system, "Write a brief, factual passage"):
		p.hydeCalls++
		p.hydeQueries = append(p.hydeQueries, user)
		return p.hydePrefix + user, nil
	default:
		return p.synthReply, nil
	}
}

// Iter 162 — hydePassager tests.

// fakeHyDEChat is a minimal embed.ChatClient that returns whatever reply is
// configured. calls counts invocations so we can assert cache behaviour.
type fakeHyDEChat struct {
	reply    string
	fakeErr  error
	calls    int
	lastUser string
}

func (f *fakeHyDEChat) Model() string { return "fake-chat" }
func (f *fakeHyDEChat) Chat(_ context.Context, msgs []embed.ChatMsg) (string, error) {
	f.calls++
	for _, m := range msgs {
		if m.Role == "user" {
			f.lastUser = m.Content
		}
	}
	if f.fakeErr != nil {
		return "", f.fakeErr
	}
	return f.reply, nil
}

func TestHydePassagerHappy(t *testing.T) {
	chat := &fakeHyDEChat{reply: "Goroutines are lightweight threads managed by the Go runtime."}
	p := newHydePassager(chat, nil, nil) // L1-only
	got := p.Passage(context.Background(), "goroutines")
	if got != chat.reply {
		t.Errorf("expected hypothetical passage, got %q", got)
	}
	if chat.lastUser != "goroutines" {
		t.Errorf("user prompt should be the raw query, got %q", chat.lastUser)
	}
}

func TestHydePassagerL1HitSkipsLLM(t *testing.T) {
	chat := &fakeHyDEChat{reply: "passage A"}
	p := newHydePassager(chat, nil, nil)

	_ = p.Passage(context.Background(), "q1")
	_ = p.Passage(context.Background(), "q1")
	_ = p.Passage(context.Background(), "q1")
	if chat.calls != 1 {
		t.Errorf("L1 cache: expected 1 LLM call total, got %d", chat.calls)
	}
}

func TestHydePassagerL2HitSurvivesL1Eviction(t *testing.T) {
	// Two passager instances share an L2 store. The second instance has
	// never seen the query — its L1 is empty — but L2 should serve.
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	chat1 := &fakeHyDEChat{reply: "passage B"}
	p1 := newHydePassager(chat1, s, nil)
	got1 := p1.Passage(context.Background(), "q1")
	if got1 != "passage B" {
		t.Fatalf("first passager: got %q", got1)
	}
	if chat1.calls != 1 {
		t.Errorf("first passager should have called LLM once, got %d", chat1.calls)
	}

	// Second passager — same chat MODEL identifier, same store, cold L1.
	chat2 := &fakeHyDEChat{reply: "DIFFERENT"} // would-be reply on cache miss
	p2 := newHydePassager(chat2, s, nil)
	got2 := p2.Passage(context.Background(), "q1")
	if got2 != "passage B" {
		t.Errorf("L2 hit should return original cached passage, got %q (chat2 was never asked)", got2)
	}
	if chat2.calls != 0 {
		t.Errorf("L2 hit must NOT invoke chat2, got %d calls", chat2.calls)
	}
}

func TestHydePassagerLLMErrorFallback(t *testing.T) {
	chat := &fakeHyDEChat{fakeErr: errors.New("upstream 503")}
	p := newHydePassager(chat, nil, nil)
	got := p.Passage(context.Background(), "goroutines")
	if got != "goroutines" {
		t.Errorf("LLM error fallback: got %q want original query", got)
	}
}

func TestHydePassagerEmptyResponseFallback(t *testing.T) {
	chat := &fakeHyDEChat{reply: "   \n  "}
	p := newHydePassager(chat, nil, nil)
	got := p.Passage(context.Background(), "goroutines")
	if got != "goroutines" {
		t.Errorf("empty response fallback: got %q want original query", got)
	}
}

func TestHydePassagerNilSafe(t *testing.T) {
	// Nil chat, nil receiver — must not panic.
	var p *hydePassager
	if got := p.Passage(context.Background(), "q"); got != "q" {
		t.Errorf("nil receiver: got %q want q", got)
	}
	p2 := newHydePassager(nil, nil, nil)
	if got := p2.Passage(context.Background(), "q"); got != "q" {
		t.Errorf("nil chat: got %q want q", got)
	}
}

func TestHydePassagerMetricsRecorded(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })
	m := NewMetrics()
	chat := &fakeHyDEChat{reply: "passage X"}
	p := newHydePassager(chat, s, m)

	// First call: miss (L1 + L2 empty → LLM).
	_ = p.Passage(context.Background(), "q1")
	// Second call: L1 hit.
	_ = p.Passage(context.Background(), "q1")

	// New passager with fresh L1 → L2 hit.
	chat2 := &fakeHyDEChat{reply: "would not be called"}
	p2 := newHydePassager(chat2, s, m)
	_ = p2.Passage(context.Background(), "q1")

	// 1 miss, 1 L1 hit, 1 L2 hit.
	checkCounter := func(name string, want int64) {
		t.Helper()
		var got int64
		switch name {
		case "miss":
			got = m.hydeMisses
		case "l1":
			got = m.hydeHitsL1
		case "l2":
			got = m.hydeHitsL2
		}
		if got != want {
			t.Errorf("counter %q: got %d want %d", name, got, want)
		}
	}
	checkCounter("miss", 1)
	checkCounter("l1", 1)
	checkCounter("l2", 1)
}
