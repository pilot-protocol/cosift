package main

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Feedback — the usefulness signal. A caller (human UI or, in the Pilot app, the
// calling agent) rates an answer by the query_id we returned via the
// X-Cosift-Query-Id header. Ratings are appended to a JSONL log and joined to
// the query log by qid to form a labeled dataset: (query, latency, empty?, rating).
//
// rating: -1 (penalize / unhelpful), +1 (reward / helpful). 0 accepted as neutral.
// Feedback is weak, abusable signal (the endpoint is public) — treat it as
// aggregated evidence, never absolute truth. caller is logged for dedup/abuse.

type feedbackRec struct {
	TS     string `json:"ts"`
	Qid    string `json:"qid"`
	Rating int    `json:"rating"`
	Reason string `json:"reason,omitempty"`
	Caller string `json:"caller,omitempty"`
}

// feedbackLogPath resolves the feedback log location: explicit env, else beside
// the query log (so one COSIFT_QUERY_LOG setting enables both), else disabled.
func feedbackLogPath() string {
	if p := os.Getenv("COSIFT_FEEDBACK_LOG"); p != "" {
		return p
	}
	if q := os.Getenv("COSIFT_QUERY_LOG"); q != "" {
		return filepath.Join(filepath.Dir(q), "feedback-log.jsonl")
	}
	return ""
}

func (s *pebbleHTTP) writeFeedback(rec feedbackRec) {
	if s.fbFile == nil {
		return
	}
	b, err := json.Marshal(rec)
	if err != nil {
		return
	}
	s.fbMu.Lock()
	defer s.fbMu.Unlock()
	_, _ = s.fbFile.Write(append(b, '\n'))
}

// handleFeedback records a rating. Public (no admin token) like the query
// endpoints — agents call it after using an answer. Body:
// {"query_id":"...","rating":1,"reason":"..."}.
func (s *pebbleHTTP) handleFeedback(w http.ResponseWriter, r *http.Request) {
	if s.fbFile == nil {
		writeProblem(w, http.StatusNotImplemented, "feedback disabled (COSIFT_QUERY_LOG/COSIFT_FEEDBACK_LOG unset)")
		return
	}
	// Per-client rate limit (feedback is public/unauthed and abusable). Keyed on
	// the real client via XFF, not the Caddy peer.
	clientIP := r.Header.Get("X-Forwarded-For")
	if i := strings.IndexByte(clientIP, ','); i >= 0 {
		clientIP = clientIP[:i]
	}
	clientIP = strings.TrimSpace(clientIP)
	if clientIP == "" {
		clientIP = stripPort(r.RemoteAddr)
	}
	if s.fbRL != nil && !s.fbRL.allow(clientIP) {
		w.Header().Set("Retry-After", "60")
		writeProblem(w, http.StatusTooManyRequests, "feedback rate limit exceeded")
		return
	}
	var req struct {
		QueryID string `json:"query_id"`
		Rating  int    `json:"rating"`
		Reason  string `json:"reason"`
	}
	body, _ := io.ReadAll(io.LimitReader(r.Body, 16<<10))
	if err := json.Unmarshal(body, &req); err != nil || req.QueryID == "" {
		writeProblem(w, http.StatusBadRequest, `expected {"query_id":"...","rating":-1|0|1,"reason":"..."}`)
		return
	}
	if req.Rating < -1 || req.Rating > 1 {
		writeProblem(w, http.StatusBadRequest, "rating must be -1, 0, or 1")
		return
	}
	if len(req.Reason) > 1000 {
		req.Reason = req.Reason[:1000]
	}
	s.writeFeedback(feedbackRec{
		TS:     time.Now().UTC().Format(time.RFC3339),
		Qid:    req.QueryID,
		Rating: req.Rating,
		Reason: req.Reason,
		Caller: clientIP,
	})
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "query_id": req.QueryID, "rating": req.Rating})
}

// handleFeedbackList returns recent feedback (?n=N) or, with ?summary=1, an
// aggregate that's immediately consumable: per-endpoint and overall
// reward/penalty tallies joined to the query log by qid. This is the "make it
// useful" surface — the labeled signal without an external pipeline.
func (s *pebbleHTTP) handleFeedbackList(w http.ResponseWriter, r *http.Request) {
	if !peerTokenOK(r, s.cluster.PeerAuthToken) {
		writeProblem(w, http.StatusUnauthorized, "missing or invalid admin token")
		return
	}
	fp := feedbackLogPath()
	if fp == "" {
		writeProblem(w, http.StatusNotImplemented, "feedback disabled")
		return
	}
	if r.URL.Query().Get("summary") == "1" {
		writeJSON(w, http.StatusOK, s.feedbackSummary(fp))
		return
	}
	n := 100
	if v := r.URL.Query().Get("n"); v != "" {
		if k, err := strconv.Atoi(v); err == nil && k >= 1 && k <= 5000 {
			n = k
		}
	}
	lines := tailLines(fp, n)
	recs := make([]json.RawMessage, 0, len(lines))
	for _, ln := range lines {
		recs = append(recs, json.RawMessage(ln))
	}
	writeJSON(w, http.StatusOK, map[string]any{"count": len(recs), "entries": recs})
}

// feedbackSummary joins feedback ⋈ query-log on qid and tallies the signal:
// total +/- by endpoint, plus the worst-rated recent queries (the penalize
// targets). This is the consumable form of the reward/penalty signal.
func (s *pebbleHTTP) feedbackSummary(fbPath string) map[string]any {
	// qid -> query text (from the query log, recent window).
	qtext := map[string]string{}
	qep := map[string]string{}
	for _, ln := range tailLines(os.Getenv("COSIFT_QUERY_LOG"), 20000) {
		var qr queryLogRec
		if json.Unmarshal([]byte(ln), &qr) == nil && qr.Qid != "" {
			qtext[qr.Qid] = qr.Q
			qep[qr.Qid] = qr.Ep
		}
	}
	type tally struct{ Up, Down int }
	byEp := map[string]*tally{}
	byQuery := map[string]*tally{}
	var up, down int
	for _, ln := range tailLines(fbPath, 50000) {
		var fr feedbackRec
		if json.Unmarshal([]byte(ln), &fr) != nil {
			continue
		}
		ep := qep[fr.Qid]
		if ep == "" {
			ep = "(unknown)"
		}
		if byEp[ep] == nil {
			byEp[ep] = &tally{}
		}
		q := qtext[fr.Qid]
		if q != "" {
			if byQuery[q] == nil {
				byQuery[q] = &tally{}
			}
		}
		switch {
		case fr.Rating > 0:
			up++
			byEp[ep].Up++
			if q != "" {
				byQuery[q].Up++
			}
		case fr.Rating < 0:
			down++
			byEp[ep].Down++
			if q != "" {
				byQuery[q].Down++
			}
		}
	}
	// Worst-rated queries = penalize targets (net negative).
	type qScore struct {
		Query string `json:"query"`
		Up    int    `json:"up"`
		Down  int    `json:"down"`
		Net   int    `json:"net"`
	}
	var worst []qScore
	for q, t := range byQuery {
		net := t.Up - t.Down
		if net < 0 {
			worst = append(worst, qScore{Query: q, Up: t.Up, Down: t.Down, Net: net})
		}
	}
	return map[string]any{
		"total_up":     up,
		"total_down":   down,
		"by_endpoint":  byEp,
		"penalize_top": worst, // queries with net-negative feedback
	}
}
