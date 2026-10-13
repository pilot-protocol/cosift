package community

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/sharedaccount"
)

type fakeWork struct {
	mu       sync.Mutex
	joins    map[string]sharedaccount.JoinState
	joinErr  error
	joinHits map[string]int
	stall    func(op string)
	counters map[string]map[string]int64
	missing  map[string]bool
	addErr   error
	adds     int
	alerts   []sharedaccount.Alert
	alertErr error
	alertTry int
}

func newFakeWork() *fakeWork {
	return &fakeWork{joins: map[string]sharedaccount.JoinState{}, joinHits: map[string]int{}, counters: map[string]map[string]int64{}, missing: map[string]bool{}}
}

func (f *fakeWork) enter(op string) {
	f.mu.Lock()
	stall := f.stall
	f.mu.Unlock()
	if stall != nil {
		stall(op)
	}
}

func (f *fakeWork) JoinState(_ context.Context, uid string) (sharedaccount.JoinState, error) {
	f.enter("JoinState")
	f.mu.Lock()
	defer f.mu.Unlock()
	f.joinHits[uid]++
	if f.joinErr != nil {
		return sharedaccount.JoinState{}, f.joinErr
	}
	return f.joins[uid], nil
}

func (f *fakeWork) AddCounters(_ context.Context, cid string, add map[string]int64) error {
	f.enter("AddCounters")
	f.mu.Lock()
	defer f.mu.Unlock()
	f.adds++
	if f.addErr != nil {
		return f.addErr
	}
	if f.missing[cid] {
		return sharedaccount.ErrNotFound
	}
	if f.counters[cid] == nil {
		f.counters[cid] = map[string]int64{}
	}
	for k, v := range add {
		f.counters[cid][k] += v
	}
	return nil
}

func (f *fakeWork) RaiseAlert(_ context.Context, a sharedaccount.Alert) error {
	f.enter("RaiseAlert")
	f.mu.Lock()
	defer f.mu.Unlock()
	f.alertTry++
	if f.alertErr != nil {
		return f.alertErr
	}
	f.alerts = append(f.alerts, a)
	return nil
}

func (f *fakeWork) counter(cid, name string) int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.counters[cid][name]
}

func (f *fakeWork) hits(uid string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.joinHits[uid]
}

// clock is a test clock the server and its fakes share.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *clock { return &clock{t: time.Date(2026, 10, 14, 9, 30, 0, 0, time.UTC)} }

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

type engineRequest struct {
	method, path, query, auth string
	body                      []byte
}

type fakeEngine struct {
	mu     sync.Mutex
	calls  []engineRequest
	reply  func(r engineRequest) (int, string)
	header map[string]string
}

func (f *fakeEngine) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	call := engineRequest{r.Method, r.URL.Path, r.URL.RawQuery, r.Header.Get("Authorization"), body}
	f.mu.Lock()
	f.calls = append(f.calls, call)
	reply, header := f.reply, f.header
	f.mu.Unlock()
	status, out := http.StatusOK, `{}`
	if reply != nil {
		status, out = reply(call)
	}
	for k, v := range header {
		w.Header().Set(k, v)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	io.WriteString(w, out)
}

func (f *fakeEngine) on(path string) []engineRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []engineRequest
	for _, c := range f.calls {
		if c.path == path {
			out = append(out, c)
		}
	}
	return out
}

func (f *fakeEngine) answer(reply func(r engineRequest) (int, string)) {
	f.mu.Lock()
	f.reply = reply
	f.mu.Unlock()
}

func fixed(status int, body string) func(engineRequest) (int, string) {
	return func(engineRequest) (int, string) { return status, body }
}

const testClientAddr = "203.0.113.9:4000"

type workRig struct {
	s      *Server
	f      *fakeWork
	clk    *clock
	engine *fakeEngine
	userID string
	uid    string
}

func newWorkRig(t *testing.T) *workRig {
	t.Helper()
	engine := &fakeEngine{}
	s := testServer(t, engine)
	f, clk := newFakeWork(), newClock()
	s.cfg.Shared, s.cfg.Work, s.cfg.Environment = &fakeShared{}, true, "prod"
	if err := s.initWork(); err != nil {
		t.Fatal(err)
	}
	s.work.source, s.now = f, clk.now
	r := &workRig{s: s, f: f, clk: clk, engine: engine, uid: "0123456789abcdef"}
	w := bearerRequest(s, "/api/me", sharedTestToken)
	if w.Code != 200 {
		t.Fatalf("setup: %d %s", w.Code, w.Body)
	}
	var u User
	json.Unmarshal(w.Body.Bytes(), &u)
	r.userID = u.ID
	f.joins[r.uid] = sharedaccount.JoinState{CID: testCID, TermsVersion: termsVersion}
	return r
}

func (r *workRig) post(path, token string, body any, mutate func(*http.Request)) *httptest.ResponseRecorder {
	raw, ok := body.([]byte)
	if !ok {
		raw, _ = json.Marshal(body)
	}
	req := httptest.NewRequest("POST", path, bytes.NewReader(raw))
	req.RemoteAddr = testClientAddr
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Cosift-Client", "community")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if mutate != nil {
		mutate(req)
	}
	w := httptest.NewRecorder()
	r.s.ServeHTTP(w, req)
	return w
}

func (r *workRig) claim(body any) *httptest.ResponseRecorder {
	return r.post("/api/work/claim", sharedTestToken, body, nil)
}

func (r *workRig) rows(query string, args ...any) int {
	var n int
	r.s.db.QueryRow(query, args...).Scan(&n)
	return n
}
