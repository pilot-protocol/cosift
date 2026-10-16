package community

import (
	"context"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/pilot-protocol/cosift/internal/sharedaccount"
)

func TestEmulatorWorkRunsEndToEndAgainstFirestore(t *testing.T) {
	if os.Getenv("FIRESTORE_EMULATOR_HOST") == "" {
		t.Skip("FIRESTORE_EMULATOR_HOST is not set")
	}
	ctx := context.Background()
	db, err := firestore.NewClientWithDatabase(ctx, "emulator-community-work", "(default)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, name := range []string{"accounts", "contributors", "alerts"} {
		docs, _ := db.Collection(name).Documents(ctx).GetAll()
		for _, d := range docs {
			d.Ref.Delete(ctx)
		}
	}
	r, fd, ws := settledRig(t)
	r.s.work.source = sharedaccount.NewWorkSource(db)
	cid := "0f1e2d3c4b5a6978"
	if _, err := db.Collection("accounts").Doc(r.uid).Set(ctx, map[string]any{"email": "shared@example.com", "verified_at": time.Now().UTC(), "contributor_cid": cid}); err != nil {
		t.Fatal(err)
	}
	contributor := db.Collection("contributors").Doc(cid)
	if _, err := contributor.Set(ctx, map[string]any{"uid": r.uid, "handle": "tidal-fan", "terms_version": int64(1), "counters": map[string]any{"published": int64(2)}}); err != nil {
		t.Fatal(err)
	}
	r.engine.answer(func(call engineRequest) (int, string) {
		if call.path == "/v1/work/claim" {
			return 200, claimReply[:len(claimReply)-1] + `,"moderation_ceiling":true}`
		}
		return fd.serve(call)
	})

	if _, err := contributor.Update(ctx, []firestore.Update{{Path: "banned", Value: true}}); err != nil {
		t.Fatal(err)
	}
	if w := r.claim(map[string]any{"max": 2}); w.Code != 403 || errBody(t, w).Code != "banned" {
		t.Fatalf("banned: %d %s", w.Code, w.Body)
	}
	contributor.Update(ctx, []firestore.Update{{Path: "banned", Value: false}})
	r.s.work.joins = newJoinCache(joinCacheKeys)
	if w := r.claim(map[string]any{"max": 2}); w.Code != 200 {
		t.Fatalf("joined: %d %s", w.Code, w.Body)
	}

	fd.add("verified", true, sha(1), r.s.workerID(r.userID), ws.b)
	if err := r.s.pollSettled(ctx); err != nil {
		t.Fatal(err)
	}
	r.s.flushFetchCounters(ctx)
	r.s.drainAlerts(ctx)

	doc, err := contributor.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	data := doc.Data()
	counters, _ := data["counters"].(map[string]any)
	if counters["fetch_claimed"] != int64(2) || counters["fetch_verified"] != int64(1) || counters["fetch_credits"] != int64(2) || counters["published"] != int64(2) || data["handle"] != "tidal-fan" || data["banned"] != false {
		t.Fatalf("contributor %v", data)
	}
	alert, err := db.Collection("alerts").Doc(sharedaccount.AlertDedupKey("work_moderation_ceiling", "2026-10-14")).Get(ctx)
	if err != nil {
		t.Fatalf("alert: %v", err)
	}
	if a := alert.Data(); a["source"] != "community" || a["status"] != "firing" || a["count"] != int64(1) || a["labels"].(map[string]any)["environment"] != "prod" {
		t.Fatalf("alert %v", a)
	}

	contributor.Delete(ctx)
	r.s.bumpFetch(cid, fetchClaimed, 1)
	r.s.flushFetchCounters(ctx)
	if r.s.work.counters.size() != 0 {
		t.Fatal("the counters of a deleted contributor were kept")
	}
}
