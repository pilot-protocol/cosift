// Package judge filters retrieved candidates by LLM-rated relevance to
// the user's query, dropping garbage before the synthesis step.
//
// The reranker re-orders candidates by relevance but doesn't gate them
// — even the lowest-ranked candidate enters the synth context. That
// produces "based on the sources, here is some unrelated trivia"
// answers when retrieval pulls in marginal content (Wikipedia
// diff/admin pages, dead .edu PDFs, JS-only landing pages with stub
// HTML). The judge gates with a hard yes/no decision so synthesis
// only sees genuinely useful sources.
//
// Design points:
//
//   - One Chat call (not one per candidate). The judge prompt asks
//     the LLM to score each candidate in a single pass — token-
//     efficient and avoids the N×latency multiplier.
//   - Resilient: on any LLM failure (timeout, parse error,
//     unparseable output) the function returns "all pass," matching
//     today's behavior. The judge is a quality-improver, not a
//     correctness gate.
//   - Stateless. Caller composes with retrieval / rerank / synth.
package judge

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/pilot-protocol/cosift/internal/embed"
)

// Candidate is the minimal shape the judge needs. Excerpt is what the
// LLM sees when scoring; ID is round-tripped back to the caller.
type Candidate struct {
	ID      string
	Excerpt string
}

// Verdict is the per-candidate output.
type Verdict struct {
	ID    string  `json:"id"`
	Score float64 `json:"score"` // 0 = irrelevant, 1 = highly relevant
	Keep  bool    `json:"keep"`  // score >= MinScore
}

// Options configure the judge.
type Options struct {
	// MinScore is the threshold below which a candidate is dropped.
	// Default 0.5.
	MinScore float64
	// PerCandidateChars caps each excerpt in the prompt — keeps the
	// total prompt token count bounded regardless of how long the
	// individual source pages are. Default 600.
	PerCandidateChars int
	// SystemPrompt override. Default is judgeSystem.
	SystemPrompt string
}

// Judge calls the LLM once with all candidates and returns one Verdict
// per input candidate in input order. On any LLM error or parse failure
// the function falls back to keeping every candidate — judge is a
// quality-improver, never a correctness blocker.
func Judge(ctx context.Context, chat embed.ChatClient, query string, cands []Candidate, opts Options) []Verdict {
	if opts.MinScore == 0 {
		opts.MinScore = 0.5
	}
	if opts.PerCandidateChars == 0 {
		opts.PerCandidateChars = 600
	}
	if opts.SystemPrompt == "" {
		opts.SystemPrompt = judgeSystem
	}
	out := make([]Verdict, len(cands))
	for i, c := range cands {
		out[i] = Verdict{ID: c.ID, Score: 1.0, Keep: true}
	}
	if chat == nil || len(cands) == 0 || strings.TrimSpace(query) == "" {
		return out
	}

	var sb strings.Builder
	for i, c := range cands {
		text := c.Excerpt
		if len(text) > opts.PerCandidateChars {
			text = text[:opts.PerCandidateChars] + "…"
		}
		fmt.Fprintf(&sb, "[%d] %s\n\n", i, text)
	}
	user := fmt.Sprintf("Query: %s\n\nCandidates:\n%s\nFor each candidate, output a JSON object on its own line: {\"id\": <number>, \"score\": <float 0.0-1.0>}. score=0 means completely irrelevant; score=1 means directly answers the query. Output only the JSON lines, nothing else.", query, sb.String())

	resp, err := chat.Chat(ctx, []embed.ChatMsg{
		{Role: "system", Content: opts.SystemPrompt},
		{Role: "user", Content: user},
	})
	if err != nil {
		return out
	}
	scores, ok := parseJudgeOutput(resp, len(cands))
	if !ok {
		return out
	}
	for i := range out {
		if i < len(scores) {
			out[i].Score = scores[i]
			out[i].Keep = scores[i] >= opts.MinScore
		}
	}
	return out
}

// Filter keeps only Candidates whose Verdict.Keep is true, preserving
// input order. Convenience for callers that don't care about scores.
func Filter(cands []Candidate, verdicts []Verdict) []Candidate {
	if len(cands) != len(verdicts) {
		return cands
	}
	out := make([]Candidate, 0, len(cands))
	for i, c := range cands {
		if verdicts[i].Keep {
			out = append(out, c)
		}
	}
	return out
}

const judgeSystem = `You are a relevance judge. For each candidate passage, decide how directly it answers the user's query.
- Score 1.0: directly answers the query, contains the specific facts asked for.
- Score 0.7: substantially on-topic, partial answer, useful context.
- Score 0.4: mentions the topic but lacks the specific information asked.
- Score 0.0: irrelevant, wiki history/diff page, login form, image-only, navigation stub, off-topic.

Be strict. Wikipedia edit/diff/admin pages, login flows, image directories, and similar non-content pages score 0. Real article content scores at least 0.4. Direct answers score 0.8+.

Output one JSON line per candidate. Nothing else.`

// parseJudgeOutput extracts {id, score} JSON objects from the LLM
// response. The LLM occasionally adds prose wrappers ("Here are the
// scores:") or fences output in ```json blocks; we strip those before
// parsing. Returns (scores in input order, true) on success or (nil,
// false) if fewer than half the expected lines parsed.
func parseJudgeOutput(resp string, n int) ([]float64, bool) {
	resp = strings.TrimSpace(resp)
	resp = jsonFenceRE.ReplaceAllString(resp, "$1")

	scores := make([]float64, n)
	for i := range scores {
		scores[i] = 1.0
	}
	parsed := 0
	for _, line := range strings.Split(resp, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || !strings.HasPrefix(line, "{") {
			continue
		}
		var rec struct {
			ID    int     `json:"id"`
			Score float64 `json:"score"`
		}
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			continue
		}
		if rec.ID < 0 || rec.ID >= n {
			continue
		}
		if rec.Score < 0 {
			rec.Score = 0
		}
		if rec.Score > 1 {
			rec.Score = 1
		}
		scores[rec.ID] = rec.Score
		parsed++
	}
	if parsed < n/2 {
		return nil, false
	}
	return scores, true
}

var jsonFenceRE = regexp.MustCompile("(?s)```(?:json)?\\s*(.*?)\\s*```")
