package rerank

import (
	"context"
	"testing"

	"github.com/pilot-protocol/cosift/internal/embed"
)

type scriptedChat struct {
	reply string
	calls int
}

func (s *scriptedChat) Model() string { return "scripted" }
func (s *scriptedChat) Chat(_ context.Context, _ []embed.ChatMsg) (string, error) {
	s.calls++
	return s.reply, nil
}

func TestLLMRerankerHappyPath(t *testing.T) {
	chat := &scriptedChat{reply: "[2, 0, 1]"}
	r := NewLLMReranker(chat)

	cands := []Candidate{
		{ID: "A", Text: "alpha"},
		{ID: "B", Text: "beta"},
		{ID: "C", Text: "gamma"},
	}
	got, err := r.Rerank(context.Background(), "test query", cands)
	if err != nil {
		t.Fatalf("rerank: %v", err)
	}
	want := []string{"C", "A", "B"}
	if len(got) != 3 {
		t.Fatalf("len: got %d want 3", len(got))
	}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("[%d]: got %q want %q (full: %+v)", i, got[i], w, got)
		}
	}
}

func TestLLMRerankerCodeFencedJSON(t *testing.T) {
	chat := &scriptedChat{reply: "```json\n[1, 0]\n```"}
	r := NewLLMReranker(chat)
	cands := []Candidate{{ID: "A"}, {ID: "B"}}
	got, _ := r.Rerank(context.Background(), "q", cands)
	if len(got) != 2 || got[0] != "B" || got[1] != "A" {
		t.Errorf("code-fenced JSON: got %+v", got)
	}
}

func TestLLMRerankerInvalidPassthrough(t *testing.T) {
	chat := &scriptedChat{reply: "i can't rank these sorry"}
	r := NewLLMReranker(chat)
	cands := []Candidate{{ID: "A"}, {ID: "B"}, {ID: "C"}}
	got, err := r.Rerank(context.Background(), "q", cands)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	// Passthrough order: original.
	if len(got) != 3 || got[0] != "A" || got[1] != "B" || got[2] != "C" {
		t.Errorf("invalid JSON should pass through original order, got %+v", got)
	}
}

func TestLLMRerankerDropsIrrelevant(t *testing.T) {
	// The LLM can drop irrelevant passages from its output. We should respect that.
	chat := &scriptedChat{reply: "[0, 2]"} // drops index 1
	r := NewLLMReranker(chat)
	cands := []Candidate{{ID: "A"}, {ID: "B"}, {ID: "C"}}
	got, _ := r.Rerank(context.Background(), "q", cands)
	if len(got) != 2 || got[0] != "A" || got[1] != "C" {
		t.Errorf("expected reranker to honor dropped passages, got %+v", got)
	}
}

func TestLLMRerankerSinglePassthrough(t *testing.T) {
	// 0 or 1 candidates: no LLM call needed.
	chat := &scriptedChat{reply: "should not be called"}
	r := NewLLMReranker(chat)

	got, _ := r.Rerank(context.Background(), "q", []Candidate{{ID: "A"}})
	if len(got) != 1 || got[0] != "A" {
		t.Errorf("single-cand: %+v", got)
	}
	if chat.calls != 0 {
		t.Errorf("expected zero LLM calls for single candidate, got %d", chat.calls)
	}
}

func TestLLMRerankerClampsIndices(t *testing.T) {
	chat := &scriptedChat{reply: "[5, 99, 1, -1, 0]"} // 5 and 99 out of bounds; -1 too
	r := NewLLMReranker(chat)
	cands := []Candidate{{ID: "A"}, {ID: "B"}}
	got, _ := r.Rerank(context.Background(), "q", cands)
	// Should keep only valid indices, in order.
	if len(got) != 2 || got[0] != "B" || got[1] != "A" {
		t.Errorf("clamped indices: %+v", got)
	}
}
