package community

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestEngineWithoutAKeySendsNothingAndCountsAsReachable(t *testing.T) {
	c, err := newEngineClient("http://127.0.0.1:1", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.post(context.Background(), enqueueRoute, []byte(`{}`), time.Second); !errors.Is(err, errNoBrokerKey) {
		t.Fatalf("post without a key: %v", err)
	}
	if !c.reachable(context.Background()) {
		t.Fatal("a missing key was reported as an unreachable engine")
	}
}

func TestHostGapsForgetHostsOlderThanTheLongestCrawlDelay(t *testing.T) {
	var g hostGaps
	now := time.Unix(1000, 0)
	if wait := g.reserve("old.example", time.Second, now); wait != 0 {
		t.Fatalf("first check waits %v", wait)
	}
	if wait := g.reserve("new.example", time.Second, now.Add(maxCrawlDelay)); wait != 0 {
		t.Fatalf("second host waits %v", wait)
	}
	if _, kept := g.last["old.example"]; kept {
		t.Fatal("a host older than the longest crawl delay was kept")
	}
	if wait := g.reserve("new.example", 5*time.Second, now.Add(maxCrawlDelay+time.Second)); wait != 4*time.Second {
		t.Fatalf("a recent host must still be spaced: waits %v", wait)
	}
}

func TestRecordApprovalIgnoresAnonymousChecksAndLogsAFailedWrite(t *testing.T) {
	s := testServer(t, nil)
	logs := captureLog(t)
	ctx := context.Background()
	s.recordApproval(ctx, "", "hash")
	var n int
	s.db.QueryRow(`SELECT count(*) FROM submission_approvals`).Scan(&n)
	if n != 0 || logs.String() != "" {
		t.Fatalf("an approval without a submission was attempted: %d %q", n, logs.String())
	}
	if _, err := s.db.Exec(`INSERT INTO submissions(id,url,created_at) VALUES('sub-1','https://example.com/a',0)`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`CREATE TRIGGER refuse_approval BEFORE INSERT ON submission_approvals BEGIN SELECT RAISE(ABORT, 'store offline'); END`); err != nil {
		t.Fatal(err)
	}
	s.recordApproval(ctx, "sub-1", "hash")
	if !strings.Contains(logs.String(), "could not record a content approval") || s.approvalRecorded(ctx, "sub-1", "hash") {
		t.Fatalf("log %q", logs.String())
	}
}
