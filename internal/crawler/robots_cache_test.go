package crawler

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type robotsRT func(*http.Request) (*http.Response, error)

func (f robotsRT) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func robotsClient(body string, hits *atomic.Int32) *http.Client {
	return &http.Client{Transport: robotsRT(func(r *http.Request) (*http.Response, error) {
		hits.Add(1)
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
	})}
}

func robotsAllowed(t *testing.T, r *Robots, host string) {
	t.Helper()
	if _, _, err := r.Allowed(context.Background(), "https://"+host+"/page"); err != nil {
		t.Fatal(err)
	}
}

func TestRobotsCacheEvictsLeastRecentlyUsedByEntries(t *testing.T) {
	var hits atomic.Int32
	r := NewRobotsWithLimits(robotsClient("User-agent: *\nDisallow: /private\n", &hits), "ua", 3, 1<<30)
	for _, h := range []string{"a.example", "b.example", "c.example"} {
		robotsAllowed(t, r, h)
	}
	robotsAllowed(t, r, "a.example") // a is now the most recently used
	robotsAllowed(t, r, "d.example") // evicts b
	if got := len(r.cache); got != 3 {
		t.Fatalf("entries = %d, want 3", got)
	}
	before := hits.Load()
	robotsAllowed(t, r, "a.example")
	robotsAllowed(t, r, "c.example")
	robotsAllowed(t, r, "d.example")
	if hits.Load() != before {
		t.Fatal("live entries were refetched")
	}
	robotsAllowed(t, r, "b.example")
	if hits.Load() != before+1 {
		t.Fatal("evicted host was served from the cache")
	}
}

func TestRobotsCacheBoundsRetainedBytes(t *testing.T) {
	var hits atomic.Int32
	body := "User-agent: *\n" + strings.Repeat("Disallow: /some/long/path/prefix/for/size\n", 250)
	r := NewRobotsWithLimits(robotsClient(body, &hits), "ua", 1000, 25_000)
	for i := 0; i < 20; i++ {
		robotsAllowed(t, r, fmt.Sprintf("h%d.example", i))
	}
	if got := len(r.cache); got == 0 || got >= 20 {
		t.Fatalf("entries = %d: not bounded by size", got)
	}
	robotsAllowed(t, r, "h19.example")
	if hits.Load() != 20 {
		t.Fatalf("most recent host was evicted: %d requests", hits.Load())
	}
}

func TestRobotsCacheLiveEntriesKeepTTLSemantics(t *testing.T) {
	var hits atomic.Int32
	r := NewRobotsWithLimits(robotsClient("User-agent: *\nDisallow: /private\n", &hits), "ua", 10, 1<<30)
	robotsAllowed(t, r, "a.example")
	robotsAllowed(t, r, "a.example")
	if hits.Load() != 1 {
		t.Fatalf("live entry refetched: %d requests", hits.Load())
	}
	r.ttl = time.Nanosecond
	time.Sleep(time.Millisecond)
	robotsAllowed(t, r, "a.example")
	if hits.Load() != 2 || len(r.cache) != 1 {
		t.Fatalf("expired entry not refetched in place: requests=%d entries=%d", hits.Load(), len(r.cache))
	}
	ok, _, _ := r.Allowed(context.Background(), "https://a.example/private/x")
	if ok {
		t.Fatal("cached rules were not applied")
	}
}

func TestRobotsCacheDoesNotRetainFetchedBody(t *testing.T) {
	var hits atomic.Int32
	big := "User-agent: *\nDisallow: /private\n# " + strings.Repeat("x", 499_000) + "\n"
	r := NewRobotsWithLimits(robotsClient(big, &hits), "ua", 10_000, 1<<30)
	heap := func() uint64 {
		var m runtime.MemStats
		runtime.GC()
		runtime.GC()
		runtime.ReadMemStats(&m)
		return m.HeapAlloc
	}
	before := heap()
	for i := 0; i < 100; i++ {
		robotsAllowed(t, r, fmt.Sprintf("h%d.attacker.example", i))
	}
	after := heap()
	runtime.KeepAlive(r)
	if after > before && after-before > 8<<20 {
		t.Fatalf("100 cached hosts retain %d MB of heap", (after-before)>>20)
	}
}
