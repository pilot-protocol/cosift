package answercache

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCacheGetSet(t *testing.T) {
	c := New(time.Second, 16)
	if _, ok := c.Get("k"); ok {
		t.Errorf("empty cache returned ok")
	}
	c.Set("k", []byte("v"))
	v, ok := c.Get("k")
	if !ok || string(v) != "v" {
		t.Errorf("get after set: ok=%v v=%q", ok, v)
	}
}

func TestCacheExpiry(t *testing.T) {
	c := New(20*time.Millisecond, 16)
	c.Set("k", []byte("v"))
	time.Sleep(40 * time.Millisecond)
	if _, ok := c.Get("k"); ok {
		t.Errorf("expired entry still cached")
	}
}

func TestCacheCapEviction(t *testing.T) {
	c := New(time.Second, 4)
	for i := 0; i < 10; i++ {
		c.Set(string(rune('a'+i)), []byte("v"))
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.store) > 4 {
		t.Errorf("store grew past cap: %d > 4", len(c.store))
	}
}

func TestSingleflight(t *testing.T) {
	c := New(time.Second, 16)
	var calls atomic.Int64
	fn := func() ([]byte, error) {
		calls.Add(1)
		time.Sleep(30 * time.Millisecond)
		return []byte("ans"), nil
	}

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err, _ := c.Do("q", fn)
			if err != nil {
				t.Errorf("Do: %v", err)
			}
			if string(v) != "ans" {
				t.Errorf("value: got %q", v)
			}
		}()
	}
	wg.Wait()
	if got := calls.Load(); got != 1 {
		t.Errorf("singleflight failed; fn called %d times, want 1", got)
	}
	if c.Stats().Shared < 1 {
		t.Errorf("expected shared > 0")
	}
}

func TestSingleflightDoesNotCacheError(t *testing.T) {
	c := New(time.Second, 16)
	boom := errors.New("boom")
	_, err, _ := c.Do("q", func() ([]byte, error) { return nil, boom })
	if !errors.Is(err, boom) {
		t.Fatalf("err: %v", err)
	}
	if _, ok := c.Get("q"); ok {
		t.Errorf("error result was cached")
	}
}

func TestCacheNilSafe(t *testing.T) {
	var c *Cache
	if _, ok := c.Get("x"); ok {
		t.Errorf("nil get returned ok")
	}
	c.Set("x", []byte("v")) // must not panic
	st := c.Stats()
	if st != (Stats{}) {
		t.Errorf("nil stats: %+v", st)
	}
}
