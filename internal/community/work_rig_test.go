package community

import (
	"context"
	"sync"
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
