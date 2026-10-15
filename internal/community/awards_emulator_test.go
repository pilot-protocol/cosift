package community

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/pilot-protocol/cosift/internal/sharedaccount"
)

func TestEmulatorPollerAppliesAwardsEndToEnd(t *testing.T) {
	if os.Getenv("FIRESTORE_EMULATOR_HOST") == "" {
		t.Skip("FIRESTORE_EMULATOR_HOST is not set")
	}
	ctx := context.Background()
	db, err := firestore.NewClientWithDatabase(ctx, "emulator-community", "(default)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, name := range []string{"credit_awards", "contributors", "accounts"} {
		docs, _ := db.Collection(name).Documents(ctx).GetAll()
		for _, d := range docs {
			d.Ref.Delete(ctx)
		}
	}
	engine := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+wikiTestKey {
			w.WriteHeader(401)
			return
		}
		if strings.HasSuffix(r.URL.Path, ulidN(40)) {
			w.WriteHeader(410)
			return
		}
		w.Write([]byte(`{"origin":"community","contributors":["` + cidN(1) + `"],"version":2}`))
	}))
	defer engine.Close()
	client, _ := newEngineClient(engine.URL, wikiTestKey)
	r := newAwardRig(t)
	r.s.awards = newAwardPoller(sharedaccount.NewAwardSource(db), engineArticles{client})

	verified := time.Now().UTC()
	db.Collection("contributors").Doc(cidN(1)).Set(ctx, map[string]any{"uid": uidN(1)})
	db.Collection("accounts").Doc(uidN(1)).Set(ctx, map[string]any{"email": emailN(1), "verified_at": verified})
	seed := func(id, reason string, amount int64, created time.Time, ref map[string]any) {
		_, err := db.Collection("credit_awards").Doc(id).Create(ctx, map[string]any{"schema_version": int64(1), "uid": uidN(1), "cid": cidN(1), "amount": amount, "reason": reason, "ref": ref, "status": "pending", "created_at": created})
		if err != nil {
			t.Fatal(err)
		}
	}
	base := time.Now().UTC().Add(-time.Hour)
	grant := "art:" + ulidN(1)
	gone := "art:" + ulidN(2)
	claw := "claw:" + ulidN(3) + "_" + ulidN(4) + ":" + cidN(1)
	seed(grant, "article", 1000, base, map[string]any{"contribution_id": ulidN(1), "article_id": ulidN(30), "version": int64(1)})
	seed(gone, "article", 1000, base.Add(time.Second), map[string]any{"contribution_id": ulidN(2), "article_id": ulidN(40), "version": int64(1)})
	seed(claw, "clawback", -5000, base.Add(2*time.Second), map[string]any{"article_id": ulidN(3), "request_id": ulidN(3) + "_" + ulidN(4)})
	if err := r.s.pollAwards(ctx); err != nil {
		t.Fatal(err)
	}
	user := r.userID(t, 1)
	if r.balance(t, user) != 0 {
		t.Fatalf("balance %d: the clawback should take back exactly what the award gave", r.balance(t, user))
	}
	want := map[string][]any{grant: {"applied", int64(1000), "award:" + grant}, gone: {"skipped", int64(0), nil}, claw: {"applied", int64(-1000), "award:" + claw}}
	for id, w := range want {
		doc, err := db.Collection("credit_awards").Doc(id).Get(ctx)
		if err != nil {
			t.Fatal(err)
		}
		d := doc.Data()
		if d["status"] != w[0] || d["applied_delta"] != w[1] || d["ledger_id"] != w[2] || d["applied_at"] == nil {
			t.Errorf("%s: %v", id, d)
		}
		if id == gone && d["skip_reason"] != "article_unavailable" {
			t.Errorf("skip reason %v", d["skip_reason"])
		}
	}
	if err := r.s.pollAwards(ctx); err != nil || r.ledgerRows(t) != 2 {
		t.Fatalf("a second poll changed the ledger: %d rows, %v", r.ledgerRows(t), err)
	}
}
