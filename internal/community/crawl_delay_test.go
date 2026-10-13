package community

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/crawler"
)

func delayedSite(t *testing.T, s *Server, crawlDelay string) {
	t.Helper()
	page := `<html><title>Technical documentation</title><body><article>This educational page explains programming tools, reliable systems, and defensive software engineering. It contains useful public documentation for developers.</article></body></html>`
	s.pageClient = &http.Client{Transport: pageTransport(func(r *http.Request) (*http.Response, error) {
		body, kind := page, "text/html"
		if r.URL.Path == "/robots.txt" {
			body, kind = "User-agent: *\nAllow: /\nCrawl-delay: "+crawlDelay+"\n", "text/plain"
		}
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{kind}}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
	})}
	s.moderationRobots = crawler.NewRobots(s.pageClient, "Cosift-Community/1.0")
}

func twoPages(t *testing.T, s *Server) *http.Cookie {
	t.Helper()
	cookie := account(t, s, "member@example.com")
	expect(t, request(t, s, "POST", "/api/submissions", map[string]any{"urls": []string{"https://example.com/one", "https://example.com/two"}}, cookie), 202)
	return cookie
}

func TestCrawlDelayRequeuesInsteadOfSleepingInTheLoop(t *testing.T) {
	b := &approvalBackend{}
	s := testServer(t, b.handler(t))
	delayedSite(t, s, "5")
	cookie := twoPages(t, s)
	start := time.Now()
	if err := s.dispatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("dispatch slept %s inside the loop", took)
	}
	states := map[string]int{}
	for _, v := range listSubmissions(t, s, cookie) {
		states[v.Status]++
	}
	if states["indexed"] != 1 || states["pending"] != 1 {
		t.Fatalf("want one page checked and one waiting, got %v", states)
	}
	var attempts int
	var next int64
	s.db.QueryRow(`SELECT attempts,next_attempt FROM submissions WHERE status='pending'`).Scan(&attempts, &next)
	if attempts != 0 || next < time.Now().Unix()+3 || next > time.Now().Unix()+6 {
		t.Fatalf("the waiting page should be requeued for about 5s without using an attempt: attempts=%d, next in %ds", attempts, next-time.Now().Unix())
	}
	if b.moderations.Load() != 1 {
		t.Fatalf("%d moderations before the delay passed, want 1", b.moderations.Load())
	}
}

func TestCrawlDelayedPageProceedsOnceTheGapPasses(t *testing.T) {
	b := &approvalBackend{}
	s := testServer(t, b.handler(t))
	delayedSite(t, s, "1")
	cookie := twoPages(t, s)
	if err := s.dispatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1100 * time.Millisecond)
	s.db.Exec(`UPDATE submissions SET next_attempt=0`)
	if err := s.dispatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, v := range listSubmissions(t, s, cookie) {
		if v.Status != "indexed" {
			t.Fatalf("%s is %s after the delay", v.URL, v.Status)
		}
	}
	if b.deliveries.Load() != 2 {
		t.Fatalf("%d deliveries, want 2", b.deliveries.Load())
	}
}

func TestExcessiveCrawlDelayIsUnverified(t *testing.T) {
	b := &approvalBackend{}
	s := testServer(t, b.handler(t))
	delayedSite(t, s, "30")
	cookie := account(t, s, "member@example.com")
	expect(t, request(t, s, "POST", "/api/submissions", map[string]any{"urls": []string{"https://example.com/one"}}, cookie), 202)
	if err := s.dispatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := listSubmissions(t, s, cookie); got[0].Status != "unverified" || !strings.Contains(got[0].Reason, "crawl delay") {
		t.Fatalf("%+v", got)
	}
}
