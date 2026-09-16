package community

import "strings"

// ObviousQualityProblem is a conservative prefilter, not a measure of writing
// style or language. Borderline pages still require the model's explicit allow.
// It never treats AI authorship, code, medical terms or non-English text as junk.
func ObviousQualityProblem(doc ModerationDocument) (status, reason string) {
	title := strings.Trim(strings.ToLower(strings.Join(strings.Fields(doc.Title), " ")), " .!…")
	text := strings.TrimSpace(doc.Text)
	// Exact short-page titles avoid rejecting articles discussing these messages.
	if len([]rune(text)) < 2000 {
		switch title {
		case "just a moment", "access denied", "attention required! | cloudflare", "checking your browser", "verify you are human", "page not found", "404 not found", "sign in", "log in", "login":
			return "unverified", "This appears to be an error, login, or bot-check page rather than readable content."
		case "domain for sale", "this domain is for sale", "buy this domain", "website coming soon", "under construction":
			return "rejected", "Parked domains and placeholder pages are not accepted."
		}
	}
	// Large amounts of repeated eight-word spans identify keyword stuffing and
	// copied filler. Short pages and ordinary quotations cannot meet this bound.
	words := strings.Fields(strings.ToLower(text))
	if len(words) >= 160 {
		spans := map[string]int{}
		duplicates := 0
		for i := 0; i+8 <= len(words); i++ {
			key := strings.Join(words[i:i+8], " ")
			spans[key]++
			if spans[key] > 3 {
				duplicates++
			}
		}
		if duplicates*100 >= (len(words)-7)*80 {
			return "rejected", "Excessive repetition or keyword stuffing is not accepted."
		}
	}
	return "", ""
}
