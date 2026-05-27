package main

import "testing"

// TestAnswerLooksLikeNoInfo locks down the matcher used by /answer to decide
// whether to emit a suggest_escalation hint. The 60-Q eval harness uses the
// same phrase list; if these drift apart, eval results stop predicting how
// often the chat UI surfaces the "Try research mode" button. Iter 489.
func TestAnswerLooksLikeNoInfo(t *testing.T) {
	cases := []struct {
		in   string
		want bool
		name string
	}{
		// Positive cases — the canonical bail phrases.
		{"The provided sources do not contain information about X.", true, "do not contain"},
		{"the sources do not provide details on Y.", true, "do not provide"},
		{"The sources don't have specifics on this topic.", true, "sources don"},
		{"The sources do not include the formula for BM25.", true, "do not include"},
		{"the provided texts do not cover this question.", true, "do not cover"},
		{"the sources do not mention any details about Z.", true, "do not mention"},
		{"There is no information regarding the inventor.", true, "no information"},
		{"There is no mention of the parameter in the sources.", true, "no mention"},

		// Negative cases — real answers that mention sources without bailing.
		{"BM25 is a ranking function used by search engines [1]. The formula uses TF and IDF [2].", false, "real answer"},
		{"", false, "empty"},
		{"yes", false, "tiny answer"},

		// Cap: long answers with a single disclaimer line are NOT bails.
		// The 800-char threshold prevents this from misfiring on substantive
		// answers that happen to disclaim one missing detail.
		{longAnswerWithDisclaimer(), false, "long answer with disclaimer"},
	}

	for _, c := range cases {
		got := answerLooksLikeNoInfo(c.in)
		if got != c.want {
			preview := c.in
			if len(preview) > 80 {
				preview = preview[:80] + "..."
			}
			t.Errorf("%s: answerLooksLikeNoInfo(%q) = %v, want %v", c.name, preview, got, c.want)
		}
	}
}

func longAnswerWithDisclaimer() string {
	// Construct a >800-char answer that includes one "do not contain"
	// phrase but is otherwise substantive. Should NOT trigger escalation.
	body := "BM25 is a probabilistic ranking function widely used in information retrieval systems. " +
		"It is based on the assumption that documents containing query terms are more relevant to the user. " +
		"The formula incorporates term frequency, inverse document frequency, and document length normalization [1]. " +
		"Specifically, the BM25 score for a document D given a query Q is computed by summing over each query term [2]. " +
		"Each term's contribution depends on its frequency in the document scaled by a saturation parameter k1, " +
		"and adjusted by a length normalization parameter b that compares the document's length to the average [3]. " +
		"The sources do not contain the exact k1 and b default values, but typical implementations use k1=1.2 and b=0.75 [4]. " +
		"BM25 remains the gold standard for lexical retrieval and is implemented in Lucene, Elasticsearch, and OpenSearch."
	if len(body) <= 800 {
		// Pad to ensure we exceed the threshold.
		body += " " + body
	}
	return body
}

