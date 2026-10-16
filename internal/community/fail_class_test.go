package community

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/pilot-protocol/cosift/internal/crawler"
)

type timeoutError struct{}

func (timeoutError) Error() string   { return "i/o timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

// pageAnswering serves robots.txt freely and answers every page with one status, header set and body.
func pageAnswering(s *Server, status int, header http.Header, body string, err error) {
	s.pageClient = &http.Client{Transport: pageTransport(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/robots.txt" {
			return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/plain"}}, Body: io.NopCloser(strings.NewReader("User-agent: *\nAllow: /\n")), Request: r}, nil
		}
		if err != nil {
			return nil, err
		}
		h := http.Header{"Content-Type": []string{"text/html"}}
		for k, v := range header {
			h[k] = v
		}
		return &http.Response{StatusCode: status, Header: h, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
	})}
	s.moderationRobots = crawler.NewRobots(s.pageClient, "Cosift-Community/1.0")
}

const challengePageHTML = `<html><head><title>Just a moment...</title></head><body>Checking your browser before accessing the site. This process is automatic and your browser will redirect shortly. Please enable cookies.</body></html>`

func TestAnArticleWhoseTitleStartsLikeABotCheckIsNotBlocked(t *testing.T) {
	for _, title := range []string{"Security checklist for small teams", "Access denied: a history of locks", "Just a moment in time", "Attention required for safe lifting"} {
		if challengeTitle(title) {
			t.Errorf("%q was taken for a bot check", title)
		}
	}
	for _, title := range []string{"Just a moment...", "Attention Required! | Cloudflare", "  Access denied ", "Are you a robot?", "Verify you are human", "Security Check", "Checking your browser…"} {
		if !challengeTitle(title) {
			t.Errorf("%q was not taken for a bot check", title)
		}
	}
}

func TestPrevalidateClassesOnlyBlockingFailuresAsFetch(t *testing.T) {
	article := `<html><title>Technical documentation</title><body><article>This educational page explains programming tools, reliable systems, and defensive software engineering. It contains useful public documentation for developers.</article></body></html>`
	for _, tc := range []struct {
		name   string
		status int
		header http.Header
		body   string
		err    error
		want   string
	}{
		{"forbidden", 403, nil, "denied", nil, failFetch},
		{"too many requests", 429, nil, "slow down", nil, failFetch},
		{"a timeout", 0, nil, "", timeoutError{}, failFetch},
		{"a bot check on a 503", 503, http.Header{"Cf-Mitigated": []string{"challenge"}}, challengePageHTML, nil, failFetch},
		{"a challenge page served as 200", 200, nil, challengePageHTML, nil, failFetch},
		{"not found", 404, nil, "gone", nil, ""},
		{"gone", 410, nil, "gone", nil, ""},
		{"unauthorised", 401, nil, "login", nil, ""},
		{"a server error", 500, nil, "oops", nil, ""},
		{"a plain 503", 503, nil, "busy", nil, ""},
		{"a dns failure", 0, nil, "", errors.New("no such host"), ""},
		{"a readable page", 200, nil, article, nil, ""},
	} {
		s := testServer(t, nil)
		pageAnswering(s, tc.status, tc.header, tc.body, tc.err)
		_, _, _, class := s.prevalidate(context.Background(), job{url: "https://example.com/page"})
		if class != tc.want {
			t.Errorf("%s: class %q, want %q", tc.name, class, tc.want)
		}
	}
}

func TestDispatchRecordsAndClearsTheFailClass(t *testing.T) {
	s := testServer(t, recordingEngine(t, new([]engineCall), new(sync.Mutex)))
	cookie := account(t, s, "member@example.com")
	pageAnswering(s, 403, nil, "denied", nil)
	expect(t, request(t, s, "POST", "/api/submissions", map[string]any{"urls": []string{"https://example.com/guide"}}, cookie), 202)
	if err := s.dispatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	class := func() string {
		var c string
		s.db.QueryRow(`SELECT fail_class FROM submissions`).Scan(&c)
		return c
	}
	if class() != failFetch {
		t.Fatalf("after a 403: %q", class())
	}
	pageAnswering(s, 404, nil, "gone", nil)
	s.db.Exec(`UPDATE submissions SET status='pending',next_attempt=0`)
	s.dispatch(context.Background())
	if class() != "" {
		t.Fatalf("after a 404: %q", class())
	}
	pageAnswering(s, 429, nil, "slow", nil)
	s.db.Exec(`UPDATE submissions SET status='pending',next_attempt=0`)
	s.dispatch(context.Background())
	if class() != failFetch {
		t.Fatalf("after a 429: %q", class())
	}
	setTestPage(s, `<html><title>Technical documentation</title><body><article>This educational page explains programming tools, reliable systems, and defensive software engineering. It contains useful public documentation for developers.</article></body></html>`)
	s.db.Exec(`UPDATE submissions SET status='pending',next_attempt=0`)
	s.dispatch(context.Background())
	var status string
	s.db.QueryRow(`SELECT status FROM submissions`).Scan(&status)
	if class() != "" || status == "pending" {
		t.Fatalf("after delivery: class %q status %q", class(), status)
	}
}
