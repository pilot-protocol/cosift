package community

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
)

func exhaust(t *testing.T, s *Server, rounds int) {
	t.Helper()
	for i := 0; i < rounds; i++ {
		if err := s.dispatch(context.Background()); err != nil {
			t.Fatal(err)
		}
		if _, err := s.db.Exec(`UPDATE submissions SET next_attempt=0`); err != nil {
			t.Fatal(err)
		}
	}
}

func TestDeliveryFailuresStopAtTheAttemptCap(t *testing.T) {
	captureLog(t)
	var moderated, delivered atomic.Int32
	s := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/community/moderate":
			moderated.Add(1)
			io.WriteString(w, `{"decision":"allow","category":"safe"}`)
		case "/v1/community/enqueue":
			delivered.Add(1)
			w.WriteHeader(502)
		}
	}))
	cookie := account(t, s, "member@example.com")
	expect(t, request(t, s, "POST", "/api/submissions", map[string]any{"urls": []string{"https://example.com/guide"}}, cookie), 202)
	exhaust(t, s, 40)
	got := listSubmissions(t, s, cookie)
	if len(got) != 1 || got[0].Status != "unverified" || got[0].Reason == "" {
		t.Fatalf("a submission that never delivers must end unverified: %+v", got)
	}
	if delivered.Load() > 20 || moderated.Load() > 20 {
		t.Fatalf("retries are unbounded: %d moderations, %d deliveries in 40 rounds", moderated.Load(), delivered.Load())
	}
}

func TestModerationFailuresStopAtTheAttemptCap(t *testing.T) {
	captureLog(t)
	var moderated atomic.Int32
	s := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		moderated.Add(1)
		w.WriteHeader(500)
	}))
	cookie := account(t, s, "member@example.com")
	expect(t, request(t, s, "POST", "/api/submissions", map[string]any{"urls": []string{"https://example.com/guide"}}, cookie), 202)
	exhaust(t, s, 40)
	got := listSubmissions(t, s, cookie)
	if got[0].Status != "unverified" || moderated.Load() > 20 {
		t.Fatalf("status %s after %d moderations", got[0].Status, moderated.Load())
	}
}

func TestUnreachablePagesStopAtTheAttemptCap(t *testing.T) {
	captureLog(t)
	s := testServer(t, nil)
	s.pageClient = &http.Client{Transport: pageTransport(func(r *http.Request) (*http.Response, error) {
		return nil, io.ErrUnexpectedEOF
	})}
	cookie := account(t, s, "member@example.com")
	expect(t, request(t, s, "POST", "/api/submissions", map[string]any{"urls": []string{"https://example.com/guide"}}, cookie), 202)
	exhaust(t, s, 40)
	var status string
	var attempts int
	s.db.QueryRow(`SELECT status,attempts FROM submissions`).Scan(&status, &attempts)
	if status != "unverified" || attempts > 20 {
		t.Fatalf("status %s after %d attempts", status, attempts)
	}
}

func TestExactAttemptCapAllowsOneLastSuccess(t *testing.T) {
	captureLog(t)
	var delivered atomic.Int32
	s := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/community/moderate":
			io.WriteString(w, `{"decision":"allow","category":"safe"}`)
		case "/v1/community/enqueue":
			if delivered.Add(1) < maxSubmissionAttempts {
				w.WriteHeader(502)
				return
			}
			io.WriteString(w, `{"indexed":true,"novel":false,"content_hash":"`+strings.Repeat("a", 64)+`"}`)
		}
	}))
	cookie := account(t, s, "member@example.com")
	expect(t, request(t, s, "POST", "/api/submissions", map[string]any{"urls": []string{"https://example.com/guide"}}, cookie), 202)
	exhaust(t, s, maxSubmissionAttempts)
	if got := listSubmissions(t, s, cookie); got[0].Status != "indexed" {
		t.Fatalf("the last permitted attempt succeeded but the row is %s", got[0].Status)
	}
}

func TestAttemptCapIsExact(t *testing.T) {
	captureLog(t)
	var delivered atomic.Int32
	s := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/community/moderate":
			io.WriteString(w, `{"decision":"allow","category":"safe"}`)
		case "/v1/community/enqueue":
			delivered.Add(1)
			w.WriteHeader(502)
		}
	}))
	cookie := account(t, s, "member@example.com")
	expect(t, request(t, s, "POST", "/api/submissions", map[string]any{"urls": []string{"https://example.com/guide"}}, cookie), 202)
	exhaust(t, s, maxSubmissionAttempts+5)
	if delivered.Load() != maxSubmissionAttempts {
		t.Fatalf("%d deliveries, want exactly %d", delivered.Load(), maxSubmissionAttempts)
	}
}
