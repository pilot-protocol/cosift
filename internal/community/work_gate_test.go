package community

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/sharedaccount"
)

const testCID = "0f1e2d3c4b5a6978"

func gateRig(t *testing.T) (*Server, *fakeWork, *clock) {
	t.Helper()
	s := workServer(t)
	f, c := newFakeWork(), newClock()
	s.work.source, s.now = f, c.now
	return s, f, c
}

func linkUser(t *testing.T, s *Server, userID, uid string) {
	t.Helper()
	s.db.Exec(`INSERT INTO users(id,email,name,salt,password_hash,created_at) VALUES(?,?,?,?,?,0) ON CONFLICT DO NOTHING`, userID, userID+"@example.com", userID, "s", "")
	if _, err := s.db.Exec(`INSERT INTO shared_identities(uid,user_id) VALUES(?,?)`, uid, userID); err != nil {
		t.Fatal(err)
	}
}

func TestJoinGateRefusesWhoHasNotJoinedIsBannedOrOnOldTerms(t *testing.T) {
	s, f, _ := gateRig(t)
	for i, tc := range []struct {
		state  sharedaccount.JoinState
		err    error
		status int
		code   string
	}{
		{sharedaccount.JoinState{}, nil, 403, "not_joined"},
		{sharedaccount.JoinState{CID: testCID, TermsVersion: termsVersion, Banned: true}, nil, 403, "banned"},
		{sharedaccount.JoinState{CID: testCID, TermsVersion: termsVersion - 1}, nil, 403, "terms_outdated"},
		{sharedaccount.JoinState{CID: testCID, TermsVersion: 0}, nil, 403, "terms_outdated"},
		{sharedaccount.JoinState{CID: testCID, TermsVersion: termsVersion + 1}, nil, 0, ""},
		{sharedaccount.JoinState{CID: testCID, TermsVersion: termsVersion}, nil, 0, ""},
		{sharedaccount.JoinState{}, sharedaccount.ErrUnavailable, 503, "contributors_unavailable"},
	} {
		user, uid := fmt.Sprintf("user-%d", i), fmt.Sprintf("a00000000000000%d", i)
		linkUser(t, s, user, uid)
		f.joins[uid], f.joinErr = tc.state, tc.err
		member, refusal := s.joinGate(context.Background(), user)
		switch {
		case tc.code == "" && (refusal != nil || member.cid != testCID):
			t.Errorf("case %d: %+v %+v, want the member", i, member, refusal)
		case tc.code != "" && (refusal == nil || refusal.status != tc.status || refusal.code != tc.code):
			t.Errorf("case %d: %+v, want %d %s", i, refusal, tc.status, tc.code)
		}
	}
}

func TestJoinGateCachesAnAnswerForSixtySecondsButNeverAnError(t *testing.T) {
	s, f, clk := gateRig(t)
	linkUser(t, s, "alice", "a000000000000001")
	f.joins["a000000000000001"] = sharedaccount.JoinState{CID: testCID, TermsVersion: termsVersion}
	ask := func() *workRefusal { _, r := s.joinGate(context.Background(), "alice"); return r }
	if ask() != nil || ask() != nil || f.hits("a000000000000001") != 1 {
		t.Fatalf("%d reads for two calls", f.hits("a000000000000001"))
	}
	f.joins["a000000000000001"] = sharedaccount.JoinState{CID: testCID, TermsVersion: termsVersion, Banned: true}
	clk.advance(59 * time.Second)
	if ask() != nil {
		t.Fatal("a ban showed before the cache expired")
	}
	clk.advance(2 * time.Second)
	if r := ask(); r == nil || r.code != "banned" || f.hits("a000000000000001") != 2 {
		t.Fatalf("after a minute: %+v, %d reads", r, f.hits("a000000000000001"))
	}
	f.joinErr = sharedaccount.ErrUnavailable
	clk.advance(2 * time.Minute)
	if r := ask(); r == nil || r.code != "contributors_unavailable" {
		t.Fatalf("%+v", r)
	}
	f.joinErr = nil
	f.joins["a000000000000001"] = sharedaccount.JoinState{CID: testCID, TermsVersion: termsVersion}
	if ask() != nil {
		t.Fatal("an error was cached")
	}
}

func TestJoinCacheHoldsTenThousandEntriesAndEvictsTheOldest(t *testing.T) {
	c := newJoinCache(10000)
	now := newClock().now()
	for i := 0; i < 10050; i++ {
		c.put(fmt.Sprintf("uid-%d", i), sharedaccount.JoinState{CID: testCID}, now.Add(time.Duration(i)*time.Millisecond))
	}
	if n := c.len(); n != 10000 {
		t.Fatalf("%d entries", n)
	}
	if _, ok := c.get("uid-0", now.Add(time.Minute-time.Second)); ok {
		t.Fatal("the oldest entry survived")
	}
	if _, ok := c.get("uid-10049", now.Add(time.Second)); !ok {
		t.Fatal("the newest entry is gone")
	}
}

func TestJoinGateRefusesAnAccountWithNoSharedIdentityAsNotJoined(t *testing.T) {
	s, _, _ := gateRig(t)
	if _, r := s.joinGate(context.Background(), "nobody"); r == nil || r.code != "not_joined" {
		t.Fatalf("%+v", r)
	}
}

func TestJoinGateGivesTheFirestoreReadTenSeconds(t *testing.T) {
	s, f, _ := gateRig(t)
	linkUser(t, s, "alice", "a000000000000001")
	var deadline time.Time
	var has bool
	s.work.source = deadlineProbe{f, func(d time.Time, ok bool) { deadline, has = d, ok }}
	f.joins["a000000000000001"] = sharedaccount.JoinState{CID: testCID, TermsVersion: termsVersion}
	start := time.Now()
	if _, r := s.joinGate(context.Background(), "alice"); r != nil {
		t.Fatal(r)
	}
	if !has || deadline.Sub(start) > 11*time.Second || deadline.Sub(start) < 9*time.Second {
		t.Fatalf("deadline %v (set %v)", deadline.Sub(start), has)
	}
}

type deadlineProbe struct {
	*fakeWork
	seen func(time.Time, bool)
}

func (d deadlineProbe) JoinState(ctx context.Context, uid string) (sharedaccount.JoinState, error) {
	d.seen(ctx.Deadline())
	return d.fakeWork.JoinState(ctx, uid)
}
