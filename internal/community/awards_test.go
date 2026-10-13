package community

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/sharedaccount"
)

func TestAwardIsAppliedOnceAndMarked(t *testing.T) {
	r := newAwardRig(t)
	a := r.articleAward(1, 1, 1000)
	r.tick(t)
	if delta, found := r.ledger(t, a.ID); !found || delta != 1000 {
		t.Fatalf("ledger %d %v", delta, found)
	}
	var reason string
	r.s.db.QueryRow(`SELECT reason FROM credit_ledger WHERE id=?`, "award:"+a.ID).Scan(&reason)
	res := r.src.result(a.ID)
	if reason != "contribution_award" || res == nil || res.Status != "applied" || res.AppliedDelta != 1000 || res.LedgerID != "award:"+a.ID {
		t.Fatalf("reason %q result %+v", reason, res)
	}
	if r.balance(t, r.userID(t, 1)) != 1000 {
		t.Fatal("the user was not credited")
	}
	before := r.src.calls["Contributor"]
	r.tick(t)
	if r.ledgerRows(t) != 1 || r.src.calls["Contributor"] != before {
		t.Fatal("a marked award was touched again")
	}
}

func TestEveryReasonLandsInItsLedgerReason(t *testing.T) {
	r := newAwardRig(t)
	art := r.articleAward(1, 1, 1000)
	rev := r.articleAward(2, 2, 500)
	rev.ID, rev.Reason = "rev:"+ulidN(2), "revision"
	r.src.awards[art.ID].a.CreatedAt = r.now.Add(-time.Hour)
	delete(r.src.awards, "art:"+ulidN(2))
	r.addRaw(rev)
	roy := r.royalty(3, 7, "2026-10-19")
	claw := r.clawback(1, 1, 300)
	r.tick(t)
	want := map[string]string{art.ID: "contribution_award", rev.ID: "contribution_award", roy.ID: "royalty", claw.ID: "clawback"}
	for id, reason := range want {
		var got string
		if err := r.s.db.QueryRow(`SELECT reason FROM credit_ledger WHERE id=?`, "award:"+id).Scan(&got); err != nil || got != reason {
			t.Errorf("%s: %q %v", id, got, err)
		}
	}
}

func TestAwardIDFormsAndLedgerIDsFollowTheSharedVectors(t *testing.T) {
	raw, err := os.ReadFile("testdata/award-vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var vectors struct {
		AwardIDs []struct {
			Reason  string
			Parts   map[string]string
			AwardID string `json:"award_id"`
			Ledger  string `json:"ledger_id"`
		} `json:"award_ids"`
	}
	if err := json.Unmarshal(raw, &vectors); err != nil {
		t.Fatal(err)
	}
	if len(vectors.AwardIDs) != 4 {
		t.Fatalf("%d vectors", len(vectors.AwardIDs))
	}
	for _, v := range vectors.AwardIDs {
		r := newAwardRig(t)
		r.person(1)
		a := sharedaccount.Award{ID: v.AwardID, UID: uidN(1), CID: cidN(1), Reason: v.Reason, Amount: 10, Valid: true, Status: "pending"}
		switch v.Reason {
		case "article", "revision":
			a.Ref = sharedaccount.AwardRef{ContributionID: v.Parts["contribution_id"], ArticleID: ulidN(5), Version: 1}
			r.art.rec[ulidN(5)] = ArticleRecord{Origin: "community", Contributors: []string{cidN(1)}, Version: 1}
		case "royalty":
			a.CID = v.Parts["cid"]
			a.Ref = sharedaccount.AwardRef{ArticleID: v.Parts["article_id"], Day: v.Parts["day"]}
		case "clawback":
			a.CID, a.Amount = v.Parts["cid"], -10
			a.Ref = sharedaccount.AwardRef{ArticleID: ulidN(5), RequestID: v.Parts["request_id"]}
		}
		r.src.contributors[a.CID] = sharedaccount.Contributor{UID: uidN(1)}
		r.addRaw(a)
		if rule := validateAward(a); rule != "" {
			t.Fatalf("%s: rejected for %s", v.AwardID, rule)
		}
		r.tick(t)
		if _, found := r.ledger(t, v.AwardID); !found || "award:"+v.AwardID != v.Ledger {
			t.Errorf("%s: ledger id %q found=%v", v.AwardID, v.Ledger, found)
		}
	}
}

func TestClawbackClampFollowsTheSharedVectors(t *testing.T) {
	raw, _ := os.ReadFile("testdata/award-vectors.json")
	var vectors struct {
		Clamp []struct {
			Balance      int
			AbsAmount    int `json:"abs_amount"`
			AppliedDelta int `json:"applied_delta"`
		} `json:"ledger_clamp"`
	}
	if err := json.Unmarshal(raw, &vectors); err != nil || len(vectors.Clamp) < 6 {
		t.Fatalf("%v %d", err, len(vectors.Clamp))
	}
	for i, v := range vectors.Clamp {
		r := newAwardRig(t)
		r.person(1)
		user, err := r.s.awardUser(context.Background(), sharedaccount.Identity{UID: uidN(1), Email: emailN(1)})
		if err != nil {
			t.Fatal(err)
		}
		r.s.db.Exec(`INSERT INTO credit_ledger(id,user_id,delta,reason,created_at) VALUES('seed',?,?,'test',0)`, user, v.Balance)
		a := r.clawback(1, i+1, v.AbsAmount)
		r.tick(t)
		delta, found := r.ledger(t, a.ID)
		res := r.src.result(a.ID)
		if !found || delta != v.AppliedDelta || res == nil || res.AppliedDelta != int64(v.AppliedDelta) {
			t.Errorf("balance %d claw %d: ledger %d (%v) result %+v, want %d", v.Balance, v.AbsAmount, delta, found, res, v.AppliedDelta)
		}
	}
}
