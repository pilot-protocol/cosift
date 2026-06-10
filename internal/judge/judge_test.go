package judge

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/pilot-protocol/cosift/internal/embed"
)

type stubChat struct {
	model string
	resp  string
	err   error
	calls atomic.Int64
}

func (s *stubChat) Model() string { return s.model }
func (s *stubChat) Chat(_ context.Context, _ []embed.ChatMsg) (string, error) {
	s.calls.Add(1)
	return s.resp, s.err
}

func TestJudgeHappyPath(t *testing.T) {
	resp := `{"id":0,"score":0.9}
{"id":1,"score":0.0}
{"id":2,"score":0.7}`
	chat := &stubChat{resp: resp}
	cands := []Candidate{
		{ID: "a", Excerpt: "raft consensus algorithm"},
		{ID: "b", Excerpt: "login required"},
		{ID: "c", Excerpt: "distributed log"},
	}
	verdicts := Judge(context.Background(), chat, "what is raft?", cands, Options{})
	if len(verdicts) != 3 {
		t.Fatalf("got %d verdicts, want 3", len(verdicts))
	}
	if !verdicts[0].Keep || verdicts[1].Keep || !verdicts[2].Keep {
		t.Errorf("keep flags: %+v", verdicts)
	}
	if chat.calls.Load() != 1 {
		t.Errorf("calls: got %d, want 1", chat.calls.Load())
	}
}

func TestJudgeFallsBackOnError(t *testing.T) {
	chat := &stubChat{err: errors.New("timeout")}
	cands := []Candidate{{ID: "a", Excerpt: "..."}, {ID: "b", Excerpt: "..."}}
	verdicts := Judge(context.Background(), chat, "anything", cands, Options{})
	for i, v := range verdicts {
		if !v.Keep || v.Score != 1.0 {
			t.Errorf("verdict[%d] = %+v; should fall back to keep+1.0 on LLM error", i, v)
		}
	}
}

func TestJudgeFallsBackOnGarbageResponse(t *testing.T) {
	chat := &stubChat{resp: "I cannot judge these, sorry!"}
	cands := []Candidate{{ID: "a", Excerpt: "..."}, {ID: "b", Excerpt: "..."}, {ID: "c", Excerpt: "..."}}
	verdicts := Judge(context.Background(), chat, "?", cands, Options{})
	for i, v := range verdicts {
		if !v.Keep {
			t.Errorf("verdict[%d] dropped on unparseable response; should fall back to keep", i)
		}
	}
}

func TestJudgeStripsJSONFence(t *testing.T) {
	resp := "```json\n{\"id\":0,\"score\":0.9}\n{\"id\":1,\"score\":0.1}\n```"
	chat := &stubChat{resp: resp}
	cands := []Candidate{{ID: "a", Excerpt: "..."}, {ID: "b", Excerpt: "..."}}
	v := Judge(context.Background(), chat, "q", cands, Options{})
	if !v[0].Keep || v[1].Keep {
		t.Errorf("verdicts: %+v", v)
	}
}

func TestJudgeMinScoreOverride(t *testing.T) {
	resp := `{"id":0,"score":0.8}
{"id":1,"score":0.4}
{"id":2,"score":0.6}`
	chat := &stubChat{resp: resp}
	cands := []Candidate{{ID: "a", Excerpt: "x"}, {ID: "b", Excerpt: "y"}, {ID: "c", Excerpt: "z"}}
	// MinScore 0.7 drops the 0.6 candidate.
	v := Judge(context.Background(), chat, "q", cands, Options{MinScore: 0.7})
	if !v[0].Keep || v[1].Keep || v[2].Keep {
		t.Errorf("MinScore=0.7 verdicts: %+v", v)
	}
}

func TestJudgeNilChat(t *testing.T) {
	cands := []Candidate{{ID: "a", Excerpt: "x"}}
	v := Judge(context.Background(), nil, "q", cands, Options{})
	if len(v) != 1 || !v[0].Keep {
		t.Errorf("nil chat should pass through: %+v", v)
	}
}
