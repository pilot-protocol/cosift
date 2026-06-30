package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// Query logging — the observability substrate cosift lacked. Until now nothing
// recorded what users ask, what returns empty, or how slow queries are, so
// "is it useful?" was unanswerable except by eyeballing. qlog appends one JSON
// line per query-endpoint request; /admin/query-log tails it.
//
// Privacy: this records query text for a public search service. Keep retention
// conservative (rotate/truncate the file); don't hoard raw queries indefinitely.

type queryLogRec struct {
	Qid    string `json:"qid"` // stable id; returned to caller via X-Cosift-Query-Id so feedback can reference it
	TS     string `json:"ts"`
	Ep     string `json:"ep"`               // path, e.g. /search
	Q      string `json:"q"`                // the q= param (empty for POST-body queries)
	Status int    `json:"status"`           // HTTP status
	MS     int64  `json:"ms"`               // server-side latency
	Bytes  int64  `json:"bytes"`            // response body bytes (empty-result proxy)
	Caller string `json:"caller,omitempty"` // X-Forwarded-For or RemoteAddr — separates self/test from organic
}

// newQueryID returns a short random hex id for correlating a query with later
// feedback. crypto/rand so concurrent requests don't collide.
func newQueryID() string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		// Fallback: time-based (collision-tolerant; feedback is weak signal).
		return "q" + strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return hex.EncodeToString(b[:])
}

// qlog wraps a query handler to append a structured log line after it runs.
// No-op when COSIFT_QUERY_LOG is unset (s.qlogFile == nil).
func (s *pebbleHTTP) qlog(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.qlogFile == nil {
			h(w, r)
			return
		}
		start := time.Now()
		qid := newQueryID()
		sw := &statusCapturingWriter{ResponseWriter: w, status: 200}
		// Set the id header BEFORE the handler writes — the adapter surfaces it
		// to the caller so feedback can reference this exact answer.
		sw.Header().Set("X-Cosift-Query-Id", qid)
		h(sw, r)
		caller := r.Header.Get("X-Forwarded-For")
		if caller == "" {
			caller = r.RemoteAddr
		}
		s.writeQueryLog(queryLogRec{
			Qid:    qid,
			TS:     time.Now().UTC().Format(time.RFC3339),
			Ep:     r.URL.Path,
			Q:      r.URL.Query().Get("q"),
			Status: sw.status,
			MS:     time.Since(start).Milliseconds(),
			Bytes:  sw.bytes,
			Caller: caller,
		})
	}
}

func (s *pebbleHTTP) writeQueryLog(rec queryLogRec) {
	b, err := json.Marshal(rec)
	if err != nil {
		return
	}
	s.qlogMu.Lock()
	defer s.qlogMu.Unlock()
	_, _ = s.qlogFile.Write(append(b, '\n'))
}

// handleQueryLog tails the query log. ?n=N (default 100, max 5000) returns the
// last N JSON lines as a JSON array; ?raw=1 returns them as raw JSONL.
func (s *pebbleHTTP) handleQueryLog(w http.ResponseWriter, r *http.Request) {
	if want := s.cluster.PeerAuthToken; want != "" {
		got := r.Header.Get("Authorization")
		if got != "Bearer "+want {
			writeProblem(w, http.StatusUnauthorized, "missing or invalid admin token")
			return
		}
	}
	path := os.Getenv("COSIFT_QUERY_LOG")
	if path == "" {
		writeProblem(w, http.StatusNotImplemented, "query log disabled (COSIFT_QUERY_LOG unset)")
		return
	}
	n := 100
	if v := r.URL.Query().Get("n"); v != "" {
		if k, err := strconv.Atoi(v); err == nil && k > 0 && k <= 5000 {
			n = k
		}
	}
	lines := tailLines(path, n)
	if r.URL.Query().Get("raw") == "1" {
		w.Header().Set("Content-Type", "application/x-ndjson")
		for _, ln := range lines {
			_, _ = w.Write(append([]byte(ln), '\n'))
		}
		return
	}
	recs := make([]json.RawMessage, 0, len(lines))
	for _, ln := range lines {
		recs = append(recs, json.RawMessage(ln))
	}
	writeJSON(w, http.StatusOK, map[string]any{"count": len(recs), "entries": recs})
}

// tailLines returns the last n newline-delimited lines of a file. Reads at most
// the trailing window so a large log doesn't blow memory.
func tailLines(path string, n int) []string {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil
	}
	const window = 2 << 20 // 2 MB tail window
	size := st.Size()
	start := int64(0)
	if size > window {
		start = size - window
	}
	buf := make([]byte, size-start)
	if _, err := f.ReadAt(buf, start); err != nil && err.Error() != "EOF" {
		return nil
	}
	parts := strings.Split(string(buf), "\n")
	// If we started mid-file, the first element is a partial line — drop it.
	if start > 0 && len(parts) > 0 {
		parts = parts[1:]
	}
	// Drop empty trailing element (file ends in newline) + any blanks.
	lines := parts[:0]
	for _, p := range parts {
		if p != "" {
			lines = append(lines, p)
		}
	}
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return lines
}
