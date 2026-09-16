package main

import (
	"strings"

	"github.com/pilot-protocol/cosift/internal/promptsafe"
)

// The [N] numbering inside the fenced block is what cmd_eval.go's citationRE
// and the CLI renderers parse — fence around it, never reformat it.
func sourcesUserMsg(env promptsafe.Envelope, questionLabel, q, promptSources string) string {
	return questionLabel + ": " + q + "\n\nSources:\n\n" +
		env.Wrap(promptsafe.LabelSources, promptSources) +
		"\n" + questionLabel + ": " + q
}

func findSynthUserMsg(env promptsafe.Envelope, q, candidates string) string {
	return "Request: " + q + "\n\nCandidate resources:\n" +
		env.Wrap(promptsafe.LabelCandidates, candidates) +
		"\nRequest: " + q
}

func planUserMsg(env promptsafe.Envelope, q string, sites []siteScope, titles []string) (msg string, fenced bool) {
	if len(sites) == 0 {
		return q, false
	}
	hints := make([]string, len(sites))
	for i, ss := range sites {
		hints[i] = ss.host
		if ss.path != "" {
			hints[i] += ss.path
		}
	}
	msg = q + "\n\nSite filter: " + strings.Join(hints, ", ") + ". Generate sub-queries using specific terminology likely found on this site."
	if len(titles) == 0 {
		return msg, false
	}
	return msg + "\n\nExample pages on this site:\n" + env.Wrap(promptsafe.LabelSiteTitles, strings.Join(titles, ", ")), true
}
