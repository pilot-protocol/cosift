package crawler

import (
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
)

func TestRobotsCacheByteAccountingMatchesEntries(t *testing.T) {
	var hits atomic.Int32
	body := "User-agent: *\n" + strings.Repeat("Disallow: /some/long/path/prefix/for/size\n", 250)
	r := NewRobotsWithLimits(robotsClient(body, &hits), "ua", 1000, 25_000)
	for i := 0; i < 20; i++ {
		robotsAllowed(t, r, fmt.Sprintf("h%d.example", i))
	}
	var sum int64
	for el := r.lru.Front(); el != nil; el = el.Next() {
		sum += el.Value.(*robotsEntry).size
	}
	if r.bytes > 25_000 || sum != r.bytes || r.lru.Len() != len(r.cache) {
		t.Fatalf("tracked bytes %d, entries sum %d, list %d, map %d", r.bytes, sum, r.lru.Len(), len(r.cache))
	}
}
