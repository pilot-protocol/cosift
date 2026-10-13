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
