package community

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// outageBackend allows every page and indexes every delivery after the first failDeliveries ones fail.
type outageBackend struct{ failDeliveries atomic.Int32 }

func (b *outageBackend) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/admin/community-moderate":
			io.WriteString(w, `{"decision":"allow","category":"safe"}`)
		case "/admin/community-enqueue":
			if b.failDeliveries.Add(-1) >= 0 {
				w.WriteHeader(502)
				return
			}
			io.WriteString(w, `{"indexed":true,"novel":false,"content_hash":"`+strings.Repeat("a", 64)+`"}`)
		}
	})
}

func refusedBackend(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	return "http://" + addr
}

func attemptsAndStatus(t *testing.T, s *Server) (status string, attempts int) {
	t.Helper()
	if err := s.db.QueryRow(`SELECT status,attempts FROM submissions`).Scan(&status, &attempts); err != nil {
		t.Fatal(err)
	}
	return
}

func TestModerationOutageDoesNotUseUpAttempts(t *testing.T) {
	captureLog(t)
	b := &outageBackend{}
	s := testServer(t, b.handler())
	live := s.cfg.Backend
	cookie := account(t, s, "member@example.com")
	expect(t, request(t, s, "POST", "/api/submissions", map[string]any{"urls": []string{"https://example.com/guide"}}, cookie), 202)
	s.cfg.Backend = refusedBackend(t)
	exhaust(t, s, 3*maxSubmissionAttempts)
	if status, attempts := attemptsAndStatus(t, s); status != "pending" || attempts != 0 {
		t.Fatalf("an unreachable backend left the row %s after %d attempts", status, attempts)
	}
	s.cfg.Backend = live
	exhaust(t, s, 1)
	if got := listSubmissions(t, s, cookie); got[0].Status != "indexed" {
		t.Fatalf("the submission did not recover with the backend: %s", got[0].Status)
	}
}

func TestDeliveryOutageDoesNotUseUpAttempts(t *testing.T) {
	captureLog(t)
	b := &outageBackend{}
	b.failDeliveries.Store(1)
	s := testServer(t, b.handler())
	live := s.cfg.Backend
	cookie := account(t, s, "member@example.com")
	expect(t, request(t, s, "POST", "/api/submissions", map[string]any{"urls": []string{"https://example.com/guide"}}, cookie), 202)
	exhaust(t, s, 1)
	s.cfg.Backend = refusedBackend(t)
	exhaust(t, s, 3*maxSubmissionAttempts)
	if status, attempts := attemptsAndStatus(t, s); status != "pending" || attempts != 1 {
		t.Fatalf("an unreachable backend left the row %s after %d attempts, want pending after the one real failure", status, attempts)
	}
	s.cfg.Backend = live
	exhaust(t, s, 1)
	if got := listSubmissions(t, s, cookie); got[0].Status != "indexed" {
		t.Fatalf("the submission did not recover with the backend: %s", got[0].Status)
	}
}

func TestBackendOutageSpacesTheNextProbe(t *testing.T) {
	captureLog(t)
	s := testServer(t, nil)
	cookie := account(t, s, "member@example.com")
	expect(t, request(t, s, "POST", "/api/submissions", map[string]any{"urls": []string{"https://example.com/guide"}}, cookie), 202)
	s.cfg.Backend = refusedBackend(t)
	if err := s.dispatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	var next int64
	s.db.QueryRow(`SELECT next_attempt FROM submissions`).Scan(&next)
	if want := time.Now().Add(backendDownRetry - 10*time.Second).Unix(); next < want {
		t.Fatalf("the next probe is %ds away, want about %s", next-time.Now().Unix(), backendDownRetry)
	}
}

func TestDroppedConnectionsStillUseUpAttempts(t *testing.T) {
	captureLog(t)
	s := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if conn, _, err := w.(http.Hijacker).Hijack(); err == nil {
			conn.Close()
		}
	}))
	cookie := account(t, s, "member@example.com")
	expect(t, request(t, s, "POST", "/api/submissions", map[string]any{"urls": []string{"https://example.com/guide"}}, cookie), 202)
	exhaust(t, s, 2*maxSubmissionAttempts)
	if status, _ := attemptsAndStatus(t, s); status != "unverified" {
		t.Fatalf("a backend that accepts and drops connections ended %s", status)
	}
}

func countPageFetches(s *Server) *atomic.Int32 {
	var n atomic.Int32
	base := s.pageClient.Transport
	s.pageClient.Transport = pageTransport(func(r *http.Request) (*http.Response, error) {
		n.Add(1)
		return base.RoundTrip(r)
	})
	return &n
}

func TestBackendOutageCostsNoPageFetchesAfterTheFirstRound(t *testing.T) {
	captureLog(t)
	b := &outageBackend{}
	s := testServer(t, b.handler())
	live := s.cfg.Backend
	cookie := account(t, s, "member@example.com")
	urls := []string{"https://example.com/one", "https://example.com/two", "https://example.org/three"}
	expect(t, request(t, s, "POST", "/api/submissions", map[string]any{"urls": urls}, cookie), 202)
	fetches := countPageFetches(s)
	s.cfg.Backend = refusedBackend(t)
	exhaust(t, s, 1)
	onset := fetches.Load()
	if onset == 0 || onset > 2 {
		t.Fatalf("the round that found the outage made %d fetches, want one submission's page and robots file", onset)
	}
	exhaust(t, s, 11)
	if n := fetches.Load(); n != onset {
		t.Fatalf("the outage cost %d more third-party fetches over 11 rounds", n-onset)
	}
	s.cfg.Backend = live
	exhaust(t, s, 1)
	for _, got := range listSubmissions(t, s, cookie) {
		if got.Status != "indexed" {
			t.Fatalf("%s did not recover with the backend: %s", got.URL, got.Status)
		}
	}
}

func TestBackendIsProbedOnceAfterAnOutageAndNeverWhileHealthy(t *testing.T) {
	captureLog(t)
	var probes atomic.Int32
	b := &outageBackend{}
	inner := b.handler()
	s := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			probes.Add(1)
			return
		}
		inner.ServeHTTP(w, r)
	}))
	live := s.cfg.Backend
	cookie := account(t, s, "member@example.com")
	expect(t, request(t, s, "POST", "/api/submissions", map[string]any{"urls": []string{"https://example.com/zero"}}, cookie), 202)
	exhaust(t, s, 1)
	if probes.Load() != 0 {
		t.Fatalf("a healthy round made %d backend checks", probes.Load())
	}
	urls := []string{"https://example.com/one", "https://example.com/two", "https://example.org/three"}
	expect(t, request(t, s, "POST", "/api/submissions", map[string]any{"urls": urls}, cookie), 202)
	s.cfg.Backend = refusedBackend(t)
	exhaust(t, s, 3)
	s.cfg.Backend = live
	exhaust(t, s, 1)
	if probes.Load() != 1 {
		t.Fatalf("the round after the outage made %d backend checks for %d submissions, want 1", probes.Load(), len(urls))
	}
	for _, got := range listSubmissions(t, s, cookie) {
		if got.Status != "indexed" {
			t.Fatalf("%s: %s", got.URL, got.Status)
		}
	}
	expect(t, request(t, s, "POST", "/api/submissions", map[string]any{"urls": []string{"https://example.com/four"}}, cookie), 202)
	exhaust(t, s, 1)
	if probes.Load() != 1 {
		t.Fatalf("a recovered backend was checked again: %d checks", probes.Load())
	}
}

func TestResettingBackendAfterAnOutageStillUsesUpAttempts(t *testing.T) {
	captureLog(t)
	s := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if conn, _, err := w.(http.Hijacker).Hijack(); err == nil {
			conn.Close()
		}
	}))
	live := s.cfg.Backend
	cookie := account(t, s, "member@example.com")
	expect(t, request(t, s, "POST", "/api/submissions", map[string]any{"urls": []string{"https://example.com/guide"}}, cookie), 202)
	s.cfg.Backend = refusedBackend(t)
	exhaust(t, s, 2)
	s.cfg.Backend = live
	exhaust(t, s, 2*maxSubmissionAttempts)
	if status, _ := attemptsAndStatus(t, s); status != "unverified" {
		t.Fatalf("a backend that resets connections after an outage ended %s", status)
	}
}
