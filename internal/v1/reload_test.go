package v1

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var errInvalid = errors.New("invalid")

type fakeFile struct {
	next, active string
	calls        int
}

func (f *fakeFile) reload() error {
	f.calls++
	if strings.HasPrefix(f.next, "bad") {
		return errInvalid
	}
	f.active = f.next
	return nil
}

func TestReloadersIndependentFiles(t *testing.T) {
	cases := []struct {
		name             string
		auth, articles   string
		wantAuth, wantAr string
		wantErrs         []string
	}{
		{"both valid", "auth-2", "articles-2", "auth-2", "articles-2", nil},
		{"first invalid", "bad", "articles-2", "auth-1", "articles-2", []string{"service-auth.json: invalid"}},
		{"second invalid", "auth-2", "bad", "auth-2", "articles-1", []string{"articles.json: invalid"}},
		{"both invalid", "bad", "bad", "auth-1", "articles-1", []string{"service-auth.json: invalid", "articles.json: invalid"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			auth := &fakeFile{active: "auth-1", next: c.auth}
			articles := &fakeFile{active: "articles-1", next: c.articles}
			var rs Reloaders
			rs.Add("service-auth.json", auth.reload)
			rs.Add("articles.json", articles.reload)

			err := rs.Reload()
			if auth.calls != 1 || articles.calls != 1 {
				t.Errorf("calls = %d, %d; want 1, 1", auth.calls, articles.calls)
			}
			if auth.active != c.wantAuth || articles.active != c.wantAr {
				t.Errorf("active = %q, %q; want %q, %q", auth.active, articles.active, c.wantAuth, c.wantAr)
			}
			if len(c.wantErrs) == 0 {
				if err != nil {
					t.Errorf("err = %v", err)
				}
				return
			}
			if !errors.Is(err, errInvalid) {
				t.Errorf("err %v does not wrap the reloader's error", err)
			}
			var got []string
			if j, ok := err.(interface{ Unwrap() []error }); ok {
				for _, e := range j.Unwrap() {
					got = append(got, e.Error())
				}
			}
			if strings.Join(got, "|") != strings.Join(c.wantErrs, "|") {
				t.Errorf("errors = %q, want %q", got, c.wantErrs)
			}
		})
	}
}

func TestReloadersPanic(t *testing.T) {
	after := &fakeFile{active: "old", next: "new"}
	var rs Reloaders
	rs.Add("broken.json", func() error { panic("index out of range") })
	rs.Add("articles.json", after.reload)
	err := rs.Reload()
	if err == nil || err.Error() != "broken.json: panic: index out of range" {
		t.Errorf("err = %v", err)
	}
	if after.active != "new" {
		t.Error("a panicking reloader stopped the next one")
	}
}

func TestReloadersEmpty(t *testing.T) {
	var rs Reloaders
	if err := rs.Reload(); err != nil {
		t.Errorf("err = %v", err)
	}
}

func TestReloadersConcurrent(t *testing.T) {
	var rs Reloaders
	var inFlight, overlaps, runs atomic.Int64
	rs.Add("slow.json", func() error {
		if inFlight.Add(1) > 1 {
			overlaps.Add(1)
		}
		time.Sleep(time.Millisecond)
		inFlight.Add(-1)
		runs.Add(1)
		return nil
	})
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 5 {
				if err := rs.Reload(); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Go(func() {
		for range 20 {
			rs.Add("noop.json", func() error { return nil })
		}
	})
	wg.Wait()
	if overlaps.Load() != 0 {
		t.Errorf("%d reloads ran concurrently", overlaps.Load())
	}
	if runs.Load() != 40 {
		t.Errorf("slow.json ran %d times, want 40", runs.Load())
	}
}
