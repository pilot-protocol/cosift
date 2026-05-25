package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/calinteodor/cosift/internal/embed"
)

// stubJudgeChat returns a scripted reply. Lets us test judgeAnswer's parsing
// without burning real LLM calls.
type stubJudgeChat struct {
	reply string
	err   error
	model string
}

func (s *stubJudgeChat) Model() string { return s.model }
func (s *stubJudgeChat) Chat(_ context.Context, _ []embed.ChatMsg) (string, error) {
	if s.err != nil {
		return "", s.err
	}
	return s.reply, nil
}

func sources(urls ...string) []struct {
	ID    int    `json:"id"`
	URL   string `json:"url"`
	Title string `json:"title"`
} {
	out := make([]struct {
		ID    int    `json:"id"`
		URL   string `json:"url"`
		Title string `json:"title"`
	}, len(urls))
	for i, u := range urls {
		out[i].ID = i + 1
		out[i].URL = u
		out[i].Title = u
	}
	return out
}

func TestCitedSourceIDsBasic(t *testing.T) {
	tests := []struct {
		name   string
		answer string
		want   map[int]bool
	}{
		{"single cite", "Go has goroutines [1].", map[int]bool{1: true}},
		{"multiple cites", "Foo [1]. Bar [2]. Baz [3].", map[int]bool{1: true, 2: true, 3: true}},
		{"compound cite", "Both X and Y [1,2] apply here.", map[int]bool{1: true, 2: true}},
		{"compound with space", "Both [1, 3, 5] apply.", map[int]bool{1: true, 3: true, 5: true}},
		{"no cites", "There are no citations in this text.", map[int]bool{}},
		{"only brackets, no digits", "See [section a].", map[int]bool{}},
		{"repeated cite", "First [1]. Second [1]. Third [2,1].", map[int]bool{1: true, 2: true}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := citedSourceIDs(tc.answer)
			if len(got) != len(tc.want) {
				t.Errorf("%q: got %v want %v", tc.answer, got, tc.want)
			}
			for k := range tc.want {
				if !got[k] {
					t.Errorf("%q: missing cite %d (got %v)", tc.answer, k, got)
				}
			}
		})
	}
}

func TestJudgeAnswerHappyPath(t *testing.T) {
	stub := &stubJudgeChat{reply: `{"coverage": 5, "grounding": 4, "comment": "good"}`, model: "gpt-stub"}
	sc, err := judgeAnswer(context.Background(), stub, "q", "answer", sources("https://x/a"))
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if sc.Coverage != 5 || sc.Grounding != 4 || sc.Comment != "good" {
		t.Errorf("parsed: %+v", sc)
	}
}

// LLMs sometimes wrap JSON in code fences. judgeAnswer must strip them.
func TestJudgeAnswerFencedJSON(t *testing.T) {
	stub := &stubJudgeChat{reply: "```json\n{\"coverage\": 3, \"grounding\": 2, \"comment\": \"ok\"}\n```", model: "gpt-stub"}
	sc, err := judgeAnswer(context.Background(), stub, "q", "answer", sources("https://x/a"))
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if sc.Coverage != 3 || sc.Grounding != 2 {
		t.Errorf("parsed: %+v", sc)
	}
}

// Prose before/after the JSON should also be tolerated. The judge prompt asks
// for ONLY JSON, but models occasionally add a preamble.
func TestJudgeAnswerEmbeddedJSON(t *testing.T) {
	stub := &stubJudgeChat{reply: "Here is the score: {\"coverage\": 4, \"grounding\": 5, \"comment\": \"nice\"}. Done.", model: "gpt-stub"}
	sc, err := judgeAnswer(context.Background(), stub, "q", "answer", sources("https://x/a"))
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if sc.Coverage != 4 || sc.Grounding != 5 {
		t.Errorf("parsed: %+v", sc)
	}
}

// Out-of-range scores must reject — judges occasionally output 0 or 6.
func TestJudgeAnswerRejectsOutOfRange(t *testing.T) {
	stub := &stubJudgeChat{reply: `{"coverage": 0, "grounding": 7, "comment": "bad"}`, model: "gpt-stub"}
	_, err := judgeAnswer(context.Background(), stub, "q", "answer", sources("https://x/a"))
	if err == nil {
		t.Errorf("expected error for out-of-range scores")
	}
	if !strings.Contains(err.Error(), "out of 1-5") {
		t.Errorf("error should mention the range constraint, got: %v", err)
	}
}

// Malformed JSON (no closing brace etc) must surface a parse error, not panic.
func TestJudgeAnswerMalformed(t *testing.T) {
	stub := &stubJudgeChat{reply: "not json at all", model: "gpt-stub"}
	_, err := judgeAnswer(context.Background(), stub, "q", "answer", sources("https://x/a"))
	if err == nil {
		t.Errorf("expected error on non-JSON reply")
	}
}

// Chat call failure must propagate.
func TestJudgeAnswerChatError(t *testing.T) {
	stub := &stubJudgeChat{err: errors.New("upstream 429"), model: "gpt-stub"}
	_, err := judgeAnswer(context.Background(), stub, "q", "answer", sources("https://x/a"))
	if err == nil || !strings.Contains(err.Error(), "judge call") {
		t.Errorf("expected wrapped chat error, got: %v", err)
	}
}
