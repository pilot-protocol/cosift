package community

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/sharedaccount"
)

type awardRec struct {
	a      sharedaccount.Award
	result *sharedaccount.AwardResult
}

type fakeAwards struct {
	mu           sync.Mutex
	awards       map[string]*awardRec
	contributors map[string]sharedaccount.Contributor
	accounts     map[string]sharedaccount.Identity
	accountErr   map[string]error
	contribErr   error
	pendingErr   error
	markErr      error
	stall        func(op string)
	calls        map[string]int
}

func newFakeAwards() *fakeAwards {
	return &fakeAwards{awards: map[string]*awardRec{}, contributors: map[string]sharedaccount.Contributor{}, accounts: map[string]sharedaccount.Identity{}, accountErr: map[string]error{}, calls: map[string]int{}}
}

func (f *fakeAwards) enter(op string) {
	f.mu.Lock()
	f.calls[op]++
	stall := f.stall
	f.mu.Unlock()
	if stall != nil {
		stall(op)
	}
}

func (f *fakeAwards) PendingAwards(_ context.Context, after sharedaccount.AwardPosition, limit int) ([]sharedaccount.Award, error) {
	f.enter("PendingAwards")
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.pendingErr != nil {
		return nil, f.pendingErr
	}
	var out []sharedaccount.Award
	for _, r := range f.awards {
		a := r.a
		if r.result != nil || a.Status != "pending" {
			continue
		}
		if !after.CreatedAt.IsZero() && (a.CreatedAt.Before(after.CreatedAt) || a.CreatedAt.Equal(after.CreatedAt) && a.ID <= after.ID) {
			continue
		}
		out = append(out, a)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.Before(out[j].CreatedAt)
		}
		return out[i].ID < out[j].ID
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (f *fakeAwards) MarkApplied(_ context.Context, id string, r sharedaccount.AwardResult) error {
	f.enter("MarkApplied")
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.markErr != nil {
		return f.markErr
	}
	rec := f.awards[id]
	if rec == nil {
		return sharedaccount.ErrNotFound
	}
	if rec.result != nil {
		return sharedaccount.ErrNotPending
	}
	rec.result = &r
	return nil
}

func (f *fakeAwards) Contributor(_ context.Context, cid string) (sharedaccount.Contributor, error) {
	f.enter("Contributor")
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.contribErr != nil {
		return sharedaccount.Contributor{}, f.contribErr
	}
	c, ok := f.contributors[cid]
	if !ok {
		return sharedaccount.Contributor{}, sharedaccount.ErrNotFound
	}
	return c, nil
}

func (f *fakeAwards) Account(_ context.Context, uid string) (sharedaccount.Identity, error) {
	f.enter("Account")
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.accountErr[uid]; err != nil {
		return sharedaccount.Identity{}, err
	}
	id, ok := f.accounts[uid]
	if !ok {
		return sharedaccount.Identity{}, sharedaccount.ErrUnauthorized
	}
	return id, nil
}

func (f *fakeAwards) result(id string) *sharedaccount.AwardResult {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.awards[id].result
}

type fakeArticles struct {
	mu    sync.Mutex
	rec   map[string]ArticleRecord
	err   map[string]error
	calls int
	stall func()
}

func (f *fakeArticles) ArticleByID(_ context.Context, id string) (ArticleRecord, error) {
	f.mu.Lock()
	f.calls++
	stall := f.stall
	rec, ok := f.rec[id]
	err := f.err[id]
	f.mu.Unlock()
	if stall != nil {
		stall()
	}
	if err != nil {
		return ArticleRecord{}, err
	}
	if !ok {
		return ArticleRecord{}, ErrArticleGone
	}
	return rec, nil
}

type awardRig struct {
	logs *bytes.Buffer
	s    *Server
	src  *fakeAwards
	art  *fakeArticles
	now  time.Time
	n    int
}

func newAwardRig(t *testing.T) *awardRig {
	t.Helper()
	r := &awardRig{logs: captureLog(t), s: testServer(t, nil), src: newFakeAwards(), art: &fakeArticles{rec: map[string]ArticleRecord{}, err: map[string]error{}}, now: time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)}
	r.restart()
	return r
}

// restart replaces the poller, as a process restart would: the cursor and every back-off are gone.
func (r *awardRig) restart() {
	r.s.awards = newAwardPoller(r.src, r.art)
	r.s.awards.now = func() time.Time { return r.now }
}

func cidN(n int) string   { return fmt.Sprintf("%016x", 0x0f1e2d3c4b5a0000+n) }
func uidN(n int) string   { return fmt.Sprintf("%016x", uint64(0xa1b2c3d4e5f60000)+uint64(n)) }
func ulidN(n int) string  { return fmt.Sprintf("01J9A3Q7W4X9M3K5N6P8R%05d", n) }
func emailN(n int) string { return fmt.Sprintf("member%d@example.com", n) }

// person registers contributor n and a verified account for it.
func (r *awardRig) person(n int) {
	r.src.mu.Lock()
	defer r.src.mu.Unlock()
	r.src.contributors[cidN(n)] = sharedaccount.Contributor{UID: uidN(n)}
	r.src.accounts[uidN(n)] = sharedaccount.Identity{UID: uidN(n), Email: emailN(n)}
}

func (r *awardRig) add(a sharedaccount.Award) sharedaccount.Award {
	a.Valid, a.Status = true, "pending"
	return r.addRaw(a)
}

func (r *awardRig) addRaw(a sharedaccount.Award) sharedaccount.Award {
	if a.CreatedAt.IsZero() {
		r.n++
		a.CreatedAt = r.now.Add(-time.Hour + time.Duration(r.n)*time.Millisecond)
	}
	r.src.mu.Lock()
	r.src.awards[a.ID] = &awardRec{a: a}
	r.src.mu.Unlock()
	return a
}

// articleAward is a valid art: award for person n, with a matching engine record.
func (r *awardRig) articleAward(n, contribution, amount int) sharedaccount.Award {
	r.person(n)
	art := ulidN(900 + n)
	r.art.mu.Lock()
	r.art.rec[art] = ArticleRecord{Origin: "community", Contributors: []string{cidN(n)}, Version: 1}
	r.art.mu.Unlock()
	return r.add(sharedaccount.Award{ID: "art:" + ulidN(contribution), UID: uidN(n), CID: cidN(n), Amount: int64(amount), Reason: "article",
		Ref: sharedaccount.AwardRef{ContributionID: ulidN(contribution), ArticleID: art, Version: 1}})
}

func (r *awardRig) clawback(n, seq, amount int) sharedaccount.Award {
	r.person(n)
	req := ulidN(800+n) + "_" + ulidN(seq)
	return r.add(sharedaccount.Award{ID: "claw:" + req + ":" + cidN(n), UID: uidN(n), CID: cidN(n), Amount: int64(-amount), Reason: "clawback",
		Ref: sharedaccount.AwardRef{ArticleID: ulidN(800 + n), RequestID: req}})
}

func (r *awardRig) royalty(n, amount int, day string) sharedaccount.Award {
	r.person(n)
	art := ulidN(700 + n)
	return r.add(sharedaccount.Award{ID: "roy:" + day + ":" + art + ":" + cidN(n), UID: uidN(n), CID: cidN(n), Amount: int64(amount), Reason: "royalty",
		Ref: sharedaccount.AwardRef{ArticleID: art, Day: day}})
}

func (r *awardRig) tick(t *testing.T) {
	t.Helper()
	if err := r.s.pollAwards(context.Background()); err != nil {
		t.Fatalf("poll: %v", err)
	}
}

func (r *awardRig) ledger(t *testing.T, awardID string) (delta int, found bool) {
	t.Helper()
	err := r.s.db.QueryRow(`SELECT delta FROM credit_ledger WHERE id=?`, "award:"+awardID).Scan(&delta)
	return delta, err == nil
}

func (r *awardRig) userID(t *testing.T, n int) string {
	t.Helper()
	var id string
	if err := r.s.db.QueryRow(`SELECT user_id FROM shared_identities WHERE uid=?`, uidN(n)).Scan(&id); err != nil {
		t.Fatalf("person %d is not linked: %v", n, err)
	}
	return id
}

func (r *awardRig) balance(t *testing.T, userID string) int {
	t.Helper()
	var b int
	r.s.db.QueryRow(`SELECT COALESCE(sum(delta),0) FROM credit_ledger WHERE user_id=?`, userID).Scan(&b)
	return b
}

func (r *awardRig) ledgerRows(t *testing.T) int {
	t.Helper()
	var n int
	r.s.db.QueryRow(`SELECT count(*) FROM credit_ledger WHERE id LIKE 'award:%'`).Scan(&n)
	return n
}
