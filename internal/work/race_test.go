package work

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
)

func TestConcurrentQuorumCompletionsIndexOnce(t *testing.T) {
	a, b, _ := fixture(t, "rotated-ad-and-timestamp")
	for round := 0; round < 25; round++ {
		h := newHarness(t)
		const urls = 6
		var targets []string
		for i := 0; i < urls; i++ {
			u := fmt.Sprintf("https://h%d.example.org/p", i)
			targets = append(targets, u)
			h.add(ClassErrored, u)
		}
		type job struct {
			req SubmitRequest
		}
		var jobs []job
		for _, u := range targets {
			for _, text := range []string{a, b, a} {
				w, n := h.nextPair()
				jobs = append(jobs, job{request(h.grant(u, w, n), w, n, u, text)})
			}
		}
		var wg, sweepers sync.WaitGroup
		start := make(chan struct{})
		stop := make(chan struct{})
		for i := 0; i < 3; i++ {
			sweepers.Add(1)
			go func() {
				defer sweepers.Done()
				<-start
				for {
					select {
					case <-stop:
						return
					default:
						h.s.Sweep(context.Background())
					}
				}
			}()
		}
		for _, j := range jobs {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				if _, err := h.s.Submit(j.req); err != nil {
					t.Error(err)
				}
			}()
		}
		close(start)
		wg.Wait()
		close(stop)
		sweepers.Wait()
		h.sweep()
		h.f.mu.Lock()
		seen := map[string]int{}
		for _, d := range h.f.indexed {
			seen[d.LeaseURL]++
		}
		h.f.mu.Unlock()
		for _, u := range targets {
			if seen[u] != 1 {
				t.Fatalf("round %d: %s indexed %d times", round, u, seen[u])
			}
			if rec := h.record(u); rec.State != StateDone {
				t.Fatalf("round %d: %s %s", round, u, rec.State)
			}
		}
		if h.textCount() != 0 {
			t.Fatal("texts left behind")
		}
		for _, ev := range h.events("verified") {
			if len(ev.Workers) < 1 {
				t.Fatal("empty event")
			}
		}
	}
}

func TestConcurrentClaimsNeverOverLeaseOneURL(t *testing.T) {
	h := newHarness(t)
	h.push(pageURL)
	var wg sync.WaitGroup
	var mu sync.Mutex
	granted := 0
	for i := 1; i <= 40; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := h.s.Claim(hexID(i), hexID(1000+i), 1)
			if err != nil {
				t.Error(err)
			}
			mu.Lock()
			granted += len(res.Leases)
			mu.Unlock()
		}()
	}
	wg.Wait()
	if granted != h.cfg.DistinctLeasesPerURL {
		t.Fatalf("%d leases on one URL, want %d", granted, h.cfg.DistinctLeasesPerURL)
	}
}

func FuzzSubmitFields(f *testing.F) {
	good := strings.Repeat("Tidal turbines turn slowly on the seabed. ", 4)
	f.Add(good, "A page", "en", "https://example.org/blocked-page", 200, 1)
	f.Add("short", "", "xx-yy", "ftp://x", 404, 0)
	f.Add("\xff\xfe\x00 bad bytes", strings.Repeat("t", 2000), "en_US", "https://example.org/\x00", 99, 1)
	f.Fuzz(func(t *testing.T, text, title, lang, finalURL string, status, version int) {
		h := newHarness(t)
		h.push(pageURL)
		w, n := h.nextPair()
		l, ok := h.claimFor(w, n)
		if !ok {
			t.Fatal("no lease")
		}
		req := request(l, w, n, finalURL, text)
		req.Title, req.Lang, req.HTTPStatus, req.ExtractVersion = &title, &lang, &status, &version
		res, err := h.s.Submit(req)
		if err != nil {
			e, ok := err.(*Error)
			if !ok || e.Status < 400 || e.Status > 599 || e.Code == "" {
				t.Fatalf("unexpected error %v", err)
			}
			return
		}
		if res.Verdict == "" || res.Agreeing == nil {
			t.Fatalf("empty answer %+v", res)
		}
	})
}
