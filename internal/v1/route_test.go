package v1

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
)

func TestRoutePattern(t *testing.T) {
	routes := []Route{
		{Method: http.MethodPost, Path: "/v1/articles/match"},
		{Method: http.MethodGet, Path: "/v1/articles/{id}"},
		{Method: http.MethodPut, Path: "/v1/articles/{id}"},
		{Method: http.MethodDelete, Path: "/v1/articles/{id}"},
		{Method: http.MethodGet, Path: "/v1/articles/by-slug/{slug}"},
		{Method: http.MethodGet, Path: "/v1/articles/stats"},
	}
	mux := http.NewServeMux()
	for _, rt := range routes {
		mux.Handle(rt.Pattern(), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(r.Pattern))
		}))
	}
	requests := []struct{ method, path, want string }{
		{http.MethodPost, "/v1/articles/match", "POST /v1/articles/match"},
		{http.MethodGet, "/v1/articles/01J8ZC2Q7W4X9M3K5N6P8R0T2V", "GET /v1/articles/{id}"},
		{http.MethodPut, "/v1/articles/01J8ZC2Q7W4X9M3K5N6P8R0T2V", "PUT /v1/articles/{id}"},
		{http.MethodDelete, "/v1/articles/01J8ZC2Q7W4X9M3K5N6P8R0T2V", "DELETE /v1/articles/{id}"},
		{http.MethodGet, "/v1/articles/by-slug/rust-async-runtimes", "GET /v1/articles/by-slug/{slug}"},
		{http.MethodGet, "/v1/articles/stats", "GET /v1/articles/stats"},
	}
	for _, q := range requests {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(q.method, q.path, nil))
		if got := rec.Body.String(); got != q.want {
			t.Errorf("%s %s matched %q, want %q", q.method, q.path, got, q.want)
		}
	}
}

func TestReadinessGate(t *testing.T) {
	var rd Readiness
	var calls int
	h := rd.Gate(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusNoContent)
	}))
	serve := func() *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/articles/stats", nil))
		return rec
	}

	if rd.Ready() {
		t.Fatal("zero Readiness is ready")
	}
	rec := serve()
	if rec.Code != http.StatusServiceUnavailable || calls != 0 {
		t.Fatalf("before ready: status %d, handler calls %d", rec.Code, calls)
	}
	if got, want := rec.Body.String(), wireJSON(t, `{"error": "Service Unavailable", "status": 503, "code": "index_unavailable", "detail": "index not ready"}`); got != want {
		t.Errorf("body\n got %s\nwant %s", got, want)
	}
	checkHeaders(t, rec, map[string]string{"Retry-After": "5"})

	rd.SetReady()
	rd.SetReady()
	if !rd.Ready() {
		t.Fatal("not ready after SetReady")
	}
	if rec := serve(); rec.Code != http.StatusNoContent || calls != 1 {
		t.Fatalf("after ready: status %d, handler calls %d", rec.Code, calls)
	}
}

func TestReadinessConcurrent(t *testing.T) {
	var rd Readiness
	var served atomic.Int64
	h := rd.Gate(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		served.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	const workers, iters = 8, 200
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			<-start
			ready := false
			for range iters {
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/articles/stats", nil))
				switch rec.Code {
				case http.StatusNoContent:
					ready = true
				case http.StatusServiceUnavailable:
					if ready {
						t.Error("503 after the gate had opened")
						return
					}
				default:
					t.Errorf("status %d", rec.Code)
					return
				}
			}
		})
	}
	close(start)
	rd.SetReady()
	wg.Wait()

	before := served.Load()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/articles/stats", nil))
	if rec.Code != http.StatusNoContent || served.Load() != before+1 {
		t.Errorf("after SetReady: status %d", rec.Code)
	}
}
