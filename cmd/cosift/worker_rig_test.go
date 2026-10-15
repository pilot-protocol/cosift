package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/crawler"
)

type fakeApp struct {
	mu       sync.Mutex
	claims   []int
	submits  []map[string]any
	onClaim  func(n, max int) (int, http.Header, string)
	onSubmit func(n int, body map[string]any) (int, http.Header, string)
	srv      *httptest.Server
}

func newFakeApp(t *testing.T) *fakeApp {
	t.Helper()
	a := &fakeApp{}
	a.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		a.mu.Lock()
		var status int
		var header http.Header
		var out string
		switch r.URL.Path {
		case "/api/work/claim":
			max := int(body["max"].(float64))
			a.claims = append(a.claims, max)
			status, header, out = a.onClaim(len(a.claims), max)
		case "/api/work/submit":
			a.submits = append(a.submits, body)
			status, header, out = a.onSubmit(len(a.submits), body)
		default:
			status, out = 404, `{"code":"not_found"}`
		}
		a.mu.Unlock()
		for k, v := range header {
			w.Header()[k] = v
		}
		w.WriteHeader(status)
		fmt.Fprint(w, out)
	}))
	t.Cleanup(a.srv.Close)
	return a
}

func (a *fakeApp) claimCalls() []int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]int(nil), a.claims...)
}

func (a *fakeApp) submitted() []map[string]any {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]map[string]any(nil), a.submits...)
}

func leasesJSON(version int, paths ...string) string {
	var parts []string
	for i, p := range paths {
		parts = append(parts, fmt.Sprintf(`{"lease_id":"lease%02d","url":"http://pages.example.com%s","source":"errored","expires_at":"2026-10-20T10:10:00Z","extract_version":%d}`, i, p, version))
	}
	return `{"leases":[` + strings.Join(parts, ",") + `]}`
}

func ok(body string) (int, http.Header, string) { return 200, nil, body }

// queue serves the paths as leases, at most max a claim, then an empty pool.

func queue(version int, paths ...string) func(n, max int) (int, http.Header, string) {
	var mu sync.Mutex
	next := 0
	return func(_, max int) (int, http.Header, string) {
		mu.Lock()
		defer mu.Unlock()
		if next >= len(paths) {
			return ok(emptyPool)
		}
		end := min(next+max, len(paths))
		var parts []string
		for i := next; i < end; i++ {
			parts = append(parts, fmt.Sprintf(`{"lease_id":"lease%02d","url":"http://pages.example.com%s","source":"errored","expires_at":"2026-10-20T10:10:00Z","extract_version":%d}`, i, paths[i], version))
		}
		next = end
		return ok(`{"leases":[` + strings.Join(parts, ",") + `]}`)
	}
}

func once(first, then string) func(n, max int) (int, http.Header, string) {
	return func(n, _ int) (int, http.Header, string) {
		if n == 1 {
			return ok(first)
		}
		return ok(then)
	}
}

const (
	verdictWaiting = `{"verdict":"awaiting_quorum","agreeing_count":0,"indexed":false}`
	emptyPool      = `{"leases":[],"retry_after_s":30}`
)

type sleepLog struct {
	mu sync.Mutex
	d  []time.Duration
}

func (s *sleepLog) sleep(ctx context.Context, d time.Duration) error {
	s.mu.Lock()
	s.d = append(s.d, d)
	s.mu.Unlock()
	return ctx.Err()
}

func (s *sleepLog) slept() []time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]time.Duration(nil), s.d...)
}

type workerRun struct {
	w      *worker
	out    bytes.Buffer
	errOut bytes.Buffer
	sleeps *sleepLog
	origin *httptest.Server
	hits   *headerLog
}

func newWorkerRun(t *testing.T, app *fakeApp, opts workerOptions) *workerRun {
	t.Helper()
	origin, hits := fixtureOrigin(t)
	r := &workerRun{sleeps: &sleepLog{}, origin: origin, hits: hits}
	r.w = newWorker(newWorkAPI(app.srv.URL, workerTestToken), originClient(origin), opts, &r.out, &r.errOut)
	r.w.sleep = r.sleeps.sleep
	r.w.grace = time.Second
	return r
}

func (r *workerRun) run(t *testing.T) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	err := r.w.run(ctx)
	if strings.Contains(r.out.String()+r.errOut.String()+fmt.Sprint(err), "AAAAAAAAAAAAAAAA") {
		t.Fatalf("the token reached the output: %q %q %v", r.out.String(), r.errOut.String(), err)
	}
	return err
}

func (r *workerRun) events(t *testing.T) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(r.out.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("stdout line is not JSON: %q", line)
		}
		out = append(out, m)
	}
	return out
}

func (r *workerRun) eventsNamed(t *testing.T, name string) []map[string]any {
	var out []map[string]any
	for _, e := range r.events(t) {
		if e["event"] == name {
			out = append(out, e)
		}
	}
	return out
}

var ev = crawler.ExtractVersion

func blockingOrigin(t *testing.T) (srv *httptest.Server, gate chan struct{}, release func()) {
	t.Helper()
	gate, open := make(chan struct{}, 16), make(chan struct{})
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gate <- struct{}{}
		<-open
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, "<html><title>T</title><body><article>"+strings.Repeat("A sentence of ordinary words for the page body. ", 6)+"</article></body></html>")
	}))
	t.Cleanup(srv.Close)
	var once sync.Once
	release = func() { once.Do(func() { close(open) }) }
	t.Cleanup(release)
	return srv, gate, release
}
