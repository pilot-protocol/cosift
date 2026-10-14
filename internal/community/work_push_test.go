package community

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

func (r *workRig) submission(user, url, status, class string, at time.Time) {
	r.s.db.Exec(`INSERT INTO users(id,email,name,salt,password_hash,created_at) VALUES(?,?,?,?,?,0) ON CONFLICT DO NOTHING`, user, user+"@example.com", user, "s", "")
	if _, err := r.s.db.Exec(`INSERT INTO submissions(id,user_id,url,status,fail_class,created_at) VALUES(?,?,?,?,?,?)`, fmt.Sprintf("%s-%x", user, sha256.Sum256([]byte(url)))[:40], user, url, status, class, at.Unix()); err != nil {
		panic(err)
	}
}

func (r *workRig) pushedURLs() [][]string {
	var out [][]string
	for _, c := range r.engine.on("/v1/work/push") {
		var in struct{ URLs []string }
		json.Unmarshal(c.body, &in)
		out = append(out, in.URLs)
	}
	return out
}

func urlSha(u string) string { h := sha256.Sum256([]byte(u)); return hex.EncodeToString(h[:]) }

func TestPushSendsOnlyBlockingFailuresOnceADay(t *testing.T) {
	r := newWorkRig(t)
	ctx := context.Background()
	r.engine.answer(fixed(200, `{"accepted":3,"duplicates":0,"rejected":[]}`))
	now := r.clk.now()
	r.submission("alice", "https://example.org/blocked-403", "unverified", failFetch, now)
	r.submission("bob", "https://example.org/blocked-403", "pending", failFetch, now)
	r.submission("alice", "https://example.org/rate-limited", "pending", failFetch, now.Add(time.Second))
	r.submission("alice", "https://example.org/missing", "unverified", "", now)
	r.submission("alice", "https://example.org/indexed", "indexed", failFetch, now)
	r.submission("alice", "https://example.org/rejected", "rejected", failFetch, now)
	if err := r.s.pushBlocked(ctx); err != nil {
		t.Fatal(err)
	}
	pushed := r.pushedURLs()
	if len(pushed) != 1 || strings.Join(pushed[0], " ") != "https://example.org/blocked-403 https://example.org/rate-limited" {
		t.Fatalf("pushed %v", pushed)
	}
	if c := r.engine.on("/v1/work/push")[0]; c.method != "POST" || c.auth != "Bearer "+testBrokerKey {
		t.Fatalf("%+v", c)
	}
	r.s.pushBlocked(ctx)
	if len(r.pushedURLs()) != 1 {
		t.Fatal("a URL was pushed twice within a day")
	}
	r.clk.advance(23*time.Hour + 59*time.Minute)
	r.s.pushBlocked(ctx)
	if len(r.pushedURLs()) != 1 {
		t.Fatal("pushed again before 24 hours")
	}
	r.clk.advance(2 * time.Minute)
	r.s.pushBlocked(ctx)
	if len(r.pushedURLs()) != 2 || len(r.pushedURLs()[1]) != 2 {
		t.Fatalf("not pushed again after 24 hours: %v", r.pushedURLs())
	}
}

func TestPushSendsAtMostOneHundredAndTheRestFollow(t *testing.T) {
	r := newWorkRig(t)
	ctx := context.Background()
	r.engine.answer(fixed(200, `{"accepted":100,"duplicates":0,"rejected":[]}`))
	for i := 0; i < 130; i++ {
		r.submission("alice", fmt.Sprintf("https://example.org/p/%03d", i), "unverified", failFetch, r.clk.now().Add(time.Duration(i)*time.Second))
	}
	r.s.pushBlocked(ctx)
	r.s.pushBlocked(ctx)
	r.s.pushBlocked(ctx)
	pushed := r.pushedURLs()
	if len(pushed) != 2 || len(pushed[0]) != 100 || len(pushed[1]) != 30 {
		t.Fatalf("%d calls: %v", len(pushed), func() []int {
			var n []int
			for _, p := range pushed {
				n = append(n, len(p))
			}
			return n
		}())
	}
	seen := map[string]bool{}
	for _, p := range pushed {
		for _, u := range p {
			if seen[u] {
				t.Fatalf("%s sent twice", u)
			}
			seen[u] = true
		}
	}
}

func TestPushRemembersWhatWasSentButNotWhatThePoolRefusedAsFull(t *testing.T) {
	r := newWorkRig(t)
	ctx := context.Background()
	r.engine.answer(fixed(200, `{"accepted":1,"duplicates":0,"rejected":[{"index":0,"rule":"pool_full"},{"index":1,"rule":"policy"}]}`))
	now := r.clk.now()
	r.submission("alice", "https://example.org/a", "unverified", failFetch, now)
	r.submission("alice", "https://example.org/b", "unverified", failFetch, now.Add(time.Second))
	r.submission("alice", "https://example.org/c", "unverified", failFetch, now.Add(2*time.Second))
	r.s.pushBlocked(ctx)
	r.engine.answer(fixed(200, `{"accepted":1,"duplicates":0,"rejected":[]}`))
	r.s.pushBlocked(ctx)
	pushed := r.pushedURLs()
	if len(pushed) != 2 || strings.Join(pushed[1], " ") != "https://example.org/a" {
		t.Fatalf("second push %v: want only the pool_full URL again", pushed)
	}
	if r.rows(`SELECT count(*) FROM work_pushed WHERE url_sha=?`, urlSha("https://example.org/b")) != 1 {
		t.Fatal("a policy refusal was not remembered")
	}
}

func TestPushWritesNothingWhenTheEngineFails(t *testing.T) {
	r := newWorkRig(t)
	ctx := context.Background()
	r.submission("alice", "https://example.org/a", "unverified", failFetch, r.clk.now())
	for _, status := range []int{422, 429, 503, 500, 401} {
		r.engine.answer(fixed(status, `{"code":"x"}`))
		logs := captureLog(t)
		if err := r.s.pushBlocked(ctx); err == nil {
			t.Errorf("status %d: no error", status)
		}
		if r.rows(`SELECT count(*) FROM work_pushed`) != 0 {
			t.Errorf("status %d: the push was remembered", status)
		}
		if strings.Contains(logs.String(), testBrokerKey) || strings.Contains(logs.String(), "example.org") {
			t.Errorf("status %d: logged a key or a URL: %s", status, logs)
		}
	}
	r.engine.answer(fixed(200, `{"accepted":1,"duplicates":0,"rejected":[]}`))
	if err := r.s.pushBlocked(ctx); err != nil || r.rows(`SELECT count(*) FROM work_pushed`) != 1 {
		t.Fatalf("recovery: %v", err)
	}
}

func TestPushCallsNothingWithNothingToSendAndForgetsOldMemory(t *testing.T) {
	r := newWorkRig(t)
	ctx := context.Background()
	r.s.db.Exec(`INSERT INTO work_pushed(url_sha,pushed_at) VALUES('old',?),('fresh',?)`, r.clk.now().Add(-48*time.Hour).Unix(), r.clk.now().Unix())
	if err := r.s.pushBlocked(ctx); err != nil || len(r.engine.calls) != 0 {
		t.Fatalf("%v, %d calls", err, len(r.engine.calls))
	}
	if r.rows(`SELECT count(*) FROM work_pushed WHERE url_sha='old'`) != 0 || r.rows(`SELECT count(*) FROM work_pushed WHERE url_sha='fresh'`) != 1 {
		t.Fatal("the push memory was not trimmed to a day")
	}
}
