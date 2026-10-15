package community

import (
	"testing"
	"time"
)

func TestLimiterRefusesEverythingAtAZeroLimitAndReportsTheWindow(t *testing.T) {
	l := newLimiter(10)
	now := time.Unix(1000, 0)
	ok, until := l.take("k", 0, time.Minute, now)
	if ok || !until.Equal(now.Add(time.Minute)) {
		t.Fatalf("take = %v until %v", ok, until)
	}
	if len(l.keys) != 0 {
		t.Fatal("a refused zero-limit call was remembered")
	}
}

func TestLimiterStartsAFreshWindowForAnExpiredKeyBehindALongerOne(t *testing.T) {
	l := newLimiter(10)
	now := time.Unix(1000, 0)
	l.allow("long", 1, time.Hour, now)
	l.allow("short", 1, time.Second, now)
	later := now.Add(5 * time.Second)
	if !l.allow("short", 1, time.Second, later) {
		t.Fatal("an expired key stayed blocked behind a longer window")
	}
	if l.allow("short", 1, time.Second, later) {
		t.Fatal("the fresh window allowed a second hit at limit 1")
	}
	if l.allow("long", 1, time.Hour, later) {
		t.Fatal("the unexpired key was forgotten")
	}
}
