package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pilot-protocol/cosift/internal/crawler"
)

func TestWorkerClaimsFetchesAndSubmitsAnAttestation(t *testing.T) {
	app := newFakeApp(t)
	app.onClaim = queue(ev, "/plain.html", "/german.html")
	app.onSubmit = func(n int, _ map[string]any) (int, http.Header, string) {
		return ok(`{"verdict":"verified","agreeing_count":2,"indexed":true,"credits_earned":2}`)
	}
	r := newWorkerRun(t, app, workerOptions{MaxPages: 2, Concurrency: 4})
	if err := r.run(t); err != nil {
		t.Fatal(err)
	}
	subs := app.submitted()
	if len(subs) != 2 {
		t.Fatalf("%d submissions", len(subs))
	}
	for _, s := range subs {
		for _, name := range []string{"lease_id", "final_url", "http_status", "title", "text", "lang", "content_hash", "simhash", "extract_version"} {
			if _, has := s[name]; !has {
				t.Errorf("submission lacks %s", name)
			}
		}
		for _, name := range []string{"worker", "net", "artifact"} {
			if _, has := s[name]; has {
				t.Errorf("submission carries %s", name)
			}
		}
		text := s["text"].(string)
		sum, _ := crawler.Simhash(text)
		if s["http_status"] != float64(200) || s["content_hash"] != crawler.ContentHash(text) || s["simhash"] != crawler.SimhashHex(sum) || s["extract_version"] != float64(ev) {
			t.Errorf("inconsistent submission: %v", s)
		}
	}
	sub := r.eventsNamed(t, "submitted")
	if len(sub) != 2 || sub[0]["verdict"] != "verified" || sub[0]["agreeing_count"] != float64(2) || sub[0]["indexed"] != true || sub[0]["credits_earned"] != float64(2) {
		t.Errorf("progress lines: %v", sub)
	}
	if c := r.eventsNamed(t, "claimed"); len(c) != 1 || c[0]["count"] != float64(2) {
		t.Errorf("claimed lines: %v", c)
	}
	last := r.events(t)[len(r.events(t))-1]
	if last["event"] != "summary" || last["claimed"] != float64(2) || last["submitted"] != float64(2) || last["stop_reason"] != "max_pages" {
		t.Errorf("summary: %v", last)
	}
	if v := last["verdicts"].(map[string]any); v["verified"] != float64(2) {
		t.Errorf("verdicts: %v", v)
	}
}

func TestWorkerReportsAFailureForNon200AndForNothingElse(t *testing.T) {
	app := newFakeApp(t)
	app.onClaim = queue(ev, "/blocked", "/missing", "/partial", "/image.png", "/empty.html", "/to-private")
	app.onSubmit = func(int, map[string]any) (int, http.Header, string) {
		return ok(`{"verdict":"released","agreeing_count":0,"indexed":false}`)
	}
	r := newWorkerRun(t, app, workerOptions{MaxPages: 6, Concurrency: 6})
	if err := r.run(t); err != nil {
		t.Fatal(err)
	}
	got := map[string]float64{}
	for _, s := range app.submitted() {
		if len(s) != 2 || s["lease_id"] == nil || s["http_status"] == nil {
			t.Errorf("a failure report carries only lease_id and http_status: %v", s)
		}
		got[s["lease_id"].(string)] = s["http_status"].(float64)
	}
	want := map[string]float64{"lease00": 403, "lease01": 404, "lease02": 206}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("reports %v, want %v", got, want)
	}
	sum := r.w.summary()
	if sum.Reported != 3 || sum.Skipped != 3 || sum.Submitted != 0 {
		t.Errorf("summary %+v", sum)
	}
}

func TestWorkerSkipsLeasesItCannotSubmitWithoutFetchingForeignTargets(t *testing.T) {
	app := newFakeApp(t)
	app.onClaim = once(`{"leases":[{"lease_id":"l0","url":"http://127.0.0.1/secret","extract_version":`+fmt.Sprint(ev)+`},{"lease_id":"l1","url":"http://intranet.internal/x","extract_version":`+fmt.Sprint(ev)+`},{"lease_id":"l2","url":"http://pages.example.com/tiny","extract_version":`+fmt.Sprint(ev)+`}]}`, emptyPool)
	app.onSubmit = func(int, map[string]any) (int, http.Header, string) { return ok(verdictWaiting) }
	r := newWorkerRun(t, app, workerOptions{MaxPages: 3, Concurrency: 3})
	short := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r.hits.add(req)
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, "<html><title>x</title><body>too short</body></html>")
	}))
	defer short.Close()
	r.w.fetch = originClient(short)
	if err := r.run(t); err != nil {
		t.Fatal(err)
	}
	if len(r.hits.seen) != 1 || len(app.submitted()) != 0 || r.w.summary().Skipped != 3 {
		t.Fatalf("%d fetches, %d submissions, %+v", len(r.hits.seen), len(app.submitted()), r.w.summary())
	}
}
