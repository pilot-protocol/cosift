package sharedaccount

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"
)

func emulatorClient(t *testing.T) *Client {
	t.Helper()
	if os.Getenv("FIRESTORE_EMULATOR_HOST") == "" {
		t.Skip("FIRESTORE_EMULATOR_HOST is not set")
	}
	ctx := context.Background()
	db, err := firestore.NewClientWithDatabase(ctx, "emulator-sharedaccount", "(default)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	for _, name := range []string{"credit_awards", "contributors", "accounts"} {
		it := db.Collection(name).Documents(ctx)
		for {
			doc, err := it.Next()
			if errors.Is(err, iterator.Done) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			doc.Ref.Delete(ctx)
		}
	}
	return &Client{db: db}
}

func seedAward(t *testing.T, c *Client, id string, created time.Time, extra map[string]any) {
	t.Helper()
	data := awardData()
	data["created_at"] = created
	for k, v := range extra {
		data[k] = v
	}
	if _, err := c.db.Collection("credit_awards").Doc(id).Create(context.Background(), data); err != nil {
		t.Fatal(err)
	}
}

func awardIDs(awards []Award) []string {
	var ids []string
	for _, a := range awards {
		ids = append(ids, a.ID)
	}
	return ids
}

func TestEmulatorPendingAwardsPageInCreatedAtThenIDOrderAfterACursor(t *testing.T) {
	c := emulatorClient(t)
	base := time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC)
	seedAward(t, c, "art:B", base.Add(time.Second), nil)
	seedAward(t, c, "art:A", base.Add(time.Second), nil)
	seedAward(t, c, "art:C", base, nil)
	seedAward(t, c, "art:D", base.Add(2*time.Second), nil)
	seedAward(t, c, "art:E", base.Add(3*time.Second), map[string]any{"status": "applied"})
	ctx := context.Background()
	page, err := c.PendingAwards(ctx, AwardPosition{}, 3)
	if err != nil || fmt.Sprint(awardIDs(page)) != "[art:C art:A art:B]" {
		t.Fatalf("first page %v %v", awardIDs(page), err)
	}
	last := page[len(page)-1]
	page, err = c.PendingAwards(ctx, AwardPosition{CreatedAt: last.CreatedAt, ID: last.ID}, 3)
	if err != nil || fmt.Sprint(awardIDs(page)) != "[art:D]" {
		t.Fatalf("second page %v %v", awardIDs(page), err)
	}
	if !page[0].Valid || page[0].Amount != 1000 || page[0].Ref.Version != 2 {
		t.Fatalf("decoded %+v", page[0])
	}
}

func TestEmulatorMarkAppliedWritesOnceAndRefusesANonPendingAward(t *testing.T) {
	c := emulatorClient(t)
	ctx := context.Background()
	seedAward(t, c, "art:A", time.Now().UTC(), nil)
	if err := c.MarkApplied(ctx, "art:A", AwardResult{Status: "applied", AppliedDelta: 1000, LedgerID: "award:art:A"}); err != nil {
		t.Fatal(err)
	}
	doc, _ := c.db.Collection("credit_awards").Doc("art:A").Get(ctx)
	data := doc.Data()
	if data["status"] != "applied" || data["applied_delta"] != int64(1000) || data["ledger_id"] != "award:art:A" || data["applied_at"] == nil || data["skip_reason"] != nil {
		t.Fatalf("%v", data)
	}
	if err := c.MarkApplied(ctx, "art:A", AwardResult{Status: "applied", AppliedDelta: 5, LedgerID: "x"}); !errors.Is(err, ErrNotPending) {
		t.Fatalf("second mark: %v", err)
	}
	if err := c.MarkApplied(ctx, "art:missing", AwardResult{Status: "applied"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing: %v", err)
	}
	seedAward(t, c, "art:S", time.Now().UTC(), nil)
	if err := c.MarkApplied(ctx, "art:S", AwardResult{Status: "skipped", SkipReason: "unknown_account"}); err != nil {
		t.Fatal(err)
	}
	doc, _ = c.db.Collection("credit_awards").Doc("art:S").Get(ctx)
	if data = doc.Data(); data["status"] != "skipped" || data["skip_reason"] != "unknown_account" || data["applied_delta"] != int64(0) || data["ledger_id"] != nil {
		t.Fatalf("%v", data)
	}
}

func TestEmulatorConcurrentMarksSucceedOnce(t *testing.T) {
	c := emulatorClient(t)
	seedAward(t, c, "art:A", time.Now().UTC(), nil)
	var wg sync.WaitGroup
	results := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- c.MarkApplied(context.Background(), "art:A", AwardResult{Status: "applied", AppliedDelta: 1, LedgerID: "award:art:A"})
		}()
	}
	wg.Wait()
	close(results)
	won := 0
	for err := range results {
		if err == nil {
			won++
		} else if !errors.Is(err, ErrNotPending) {
			t.Errorf("unexpected %v", err)
		}
	}
	if won != 1 {
		t.Fatalf("%d marks succeeded", won)
	}
}

func TestEmulatorContributorAndAccountReads(t *testing.T) {
	c := emulatorClient(t)
	ctx := context.Background()
	db := c.db
	db.Collection("contributors").Doc("0f1e2d3c4b5a6978").Set(ctx, map[string]any{"uid": "a1b2c3d4e5f60718", "banned": true})
	db.Collection("contributors").Doc("1111111111111111").Set(ctx, map[string]any{"uid": "a1b2c3d4e5f60719"})
	db.Collection("contributors").Doc("2222222222222222").Set(ctx, map[string]any{"uid": "a1b2c3d4e5f60720", "banned": "yes"})
	if got, err := c.Contributor(ctx, "0f1e2d3c4b5a6978"); err != nil || got.UID != "a1b2c3d4e5f60718" || !got.Banned {
		t.Fatalf("banned: %+v %v", got, err)
	}
	if got, err := c.Contributor(ctx, "1111111111111111"); err != nil || got.Banned || got.UID != "a1b2c3d4e5f60719" {
		t.Fatalf("plain: %+v %v", got, err)
	}
	if _, err := c.Contributor(ctx, "2222222222222222"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("odd banned: %v", err)
	}
	if _, err := c.Contributor(ctx, "3333333333333333"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing: %v", err)
	}
	verified := time.Now().UTC()
	db.Collection("accounts").Doc("a1b2c3d4e5f60718").Set(ctx, map[string]any{"email": "user@example.com", "verified_at": verified})
	db.Collection("accounts").Doc("a1b2c3d4e5f60719").Set(ctx, map[string]any{"email": "user2@example.com"})
	db.Collection("accounts").Doc("a1b2c3d4e5f60720").Set(ctx, map[string]any{"email": "user3@example.com", "verified_at": verified, "banned": true})
	if got, err := c.Account(ctx, "a1b2c3d4e5f60718"); err != nil || got.Email != "user@example.com" {
		t.Fatalf("account: %+v %v", got, err)
	}
	if _, err := c.Account(ctx, "a1b2c3d4e5f60719"); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("unverified: %v", err)
	}
	if _, err := c.Account(ctx, "a1b2c3d4e5f60720"); !errors.Is(err, ErrBanned) {
		t.Fatalf("banned: %v", err)
	}
	if _, err := c.Account(ctx, "ffffffffffffffff"); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("missing: %v", err)
	}
}
