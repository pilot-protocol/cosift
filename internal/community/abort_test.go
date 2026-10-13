package community

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func abortedRetrieval(t *testing.T, s *Server, path string, cookie *http.Cookie, started <-chan struct{}) int {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := httptest.NewRequest("GET", path, nil).WithContext(ctx)
	r.Header.Set("X-Cosift-Client", "community")
	if cookie != nil {
		r.AddCookie(cookie)
	}
	w := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { defer close(done); s.ServeHTTP(w, r) }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("the backend was never called")
	}
	cancel()
	<-done
	return w.Code
}

func spent(t *testing.T, s *Server) (charged, caps, guests int) {
	t.Helper()
	s.db.QueryRow(`SELECT COALESCE(-sum(delta),0) FROM credit_ledger WHERE reason='extra_request'`).Scan(&charged)
	s.db.QueryRow(`SELECT COALESCE(sum(count),0) FROM retrieval_usage`).Scan(&caps)
	s.db.QueryRow(`SELECT count(*) FROM guest_usage`).Scan(&guests)
	return
}

func TestCallerAbortKeepsTheMemberCharge(t *testing.T) {
	started := make(chan struct{}, 8)
	s := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started <- struct{}{}
		<-r.Context().Done()
	}))
	cookie := account(t, s, "griefer@example.com")
	for i := 0; i < 3; i++ {
		abortedRetrieval(t, s, "/api/research?q=expensive", cookie, started)
	}
	if charged, caps, _ := spent(t, s); charged != 9 || caps != 3 {
		t.Fatalf("aborted research runs: charged %d credits and %d cap slots, want 9 and 3", charged, caps)
	}
	expect(t, request(t, s, "GET", "/api/research?q=again", nil, cookie), 429)
}

func TestCallerAbortKeepsTheGuestCooldown(t *testing.T) {
	started := make(chan struct{}, 8)
	s := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started <- struct{}{}
		<-r.Context().Done()
	}))
	abortedRetrieval(t, s, "/api/research?q=expensive", nil, started)
	if _, caps, guests := spent(t, s); guests != 1 || caps != 1 {
		t.Fatalf("aborted guest run left %d cooldowns and %d cap slots, want 1 and 1", guests, caps)
	}
	expect(t, request(t, s, "GET", "/api/research?q=again", nil, nil), 429)
}

func TestBackendFailuresStillRefundEveryReservation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		path    string
		handler http.HandlerFunc
		client  time.Duration
		status  int
	}{
		{"server error", "/api/search?q=x", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(500) }, 0, 502},
		{"bad gateway", "/api/answer?q=x", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(502) }, 0, 502},
		{"unavailable", "/api/research?q=x", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }, 0, 502},
		{"not implemented", "/api/answer?q=x", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(501) }, 0, 503},
		{"invalid body", "/api/search?q=x", func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, `{"hits":`) }, 0, 502},
		{"truncated body", "/api/search?q=x", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Length", "100")
			io.WriteString(w, `{"hits":`)
		}, 0, 502},
		{"backend timeout", "/api/search?q=x", func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-r.Context().Done():
			case <-time.After(3 * time.Second):
			}
		}, 50 * time.Millisecond, 502},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := testServer(t, tc.handler)
			if tc.client > 0 {
				s.client.Timeout = tc.client
			}
			cookie := account(t, s, "member@example.com")
			expect(t, request(t, s, "GET", tc.path, nil, cookie), tc.status)
			expect(t, request(t, s, "GET", tc.path, nil, nil), tc.status)
			if charged, caps, guests := spent(t, s); charged != 0 || caps != 0 || guests != 0 {
				t.Fatalf("backend failure kept charge=%d cap=%d guest=%d", charged, caps, guests)
			}
		})
	}
}

type bodyReadSignal struct {
	inner   http.RoundTripper
	started chan<- struct{}
}

type signalOnRead struct {
	io.ReadCloser
	started chan<- struct{}
	once    sync.Once
}

func (b *signalOnRead) Read(p []byte) (int, error) {
	b.once.Do(func() { b.started <- struct{}{} })
	return b.ReadCloser.Read(p)
}

func (t bodyReadSignal) RoundTrip(r *http.Request) (*http.Response, error) {
	res, err := t.inner.RoundTrip(r)
	if err == nil {
		res.Body = &signalOnRead{ReadCloser: res.Body, started: t.started}
	}
	return res, err
}

func TestCallerAbortDuringTheBodyKeepsTheCharge(t *testing.T) {
	started := make(chan struct{}, 8)
	s := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "1000")
		io.WriteString(w, `{"hits":`)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	s.client.Transport = bodyReadSignal{inner: http.DefaultTransport, started: started}
	cookie := account(t, s, "member@example.com")
	if code := abortedRetrieval(t, s, "/api/research?q=expensive", cookie, started); code != 502 {
		t.Fatalf("status %d", code)
	}
	if charged, caps, _ := spent(t, s); charged != 3 || caps != 1 {
		t.Fatalf("a caller who left while the body was streaming: charged %d credits and %d cap slots, want 3 and 1", charged, caps)
	}
}
