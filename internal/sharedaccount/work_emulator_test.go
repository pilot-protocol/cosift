package sharedaccount

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestEmulatorJoinStateReadsTheAccountAndItsContributor(t *testing.T) {
	c := emulatorClient(t)
	ctx := context.Background()
	db := c.db
	verified := time.Now().UTC()
	account := func(uid string, extra map[string]any) {
		data := map[string]any{"email": uid + "@example.com", "verified_at": verified}
		for k, v := range extra {
			data[k] = v
		}
		if _, err := db.Collection("accounts").Doc(uid).Set(ctx, data); err != nil {
			t.Fatal(err)
		}
	}
	contributor := func(cid string, data map[string]any) {
		if _, err := db.Collection("contributors").Doc(cid).Set(ctx, data); err != nil {
			t.Fatal(err)
		}
	}
	account("a000000000000001", nil)
	account("a000000000000002", map[string]any{"contributor_cid": "c000000000000002"})
	contributor("c000000000000002", map[string]any{"uid": "a000000000000002", "terms_version": int64(1)})
	account("a000000000000003", map[string]any{"contributor_cid": "c000000000000003"})
	contributor("c000000000000003", map[string]any{"uid": "a000000000000003", "terms_version": int64(2), "banned": true})
	account("a000000000000004", map[string]any{"contributor_cid": "c000000000000004"})
	contributor("c000000000000004", map[string]any{"uid": "a000000000000004"})
	account("a000000000000005", map[string]any{"contributor_cid": "c000000000000005"})
	account("a000000000000006", map[string]any{"contributor_cid": "c000000000000006"})
	contributor("c000000000000006", map[string]any{"uid": "a0000000000000ff", "terms_version": int64(1)})
	account("a000000000000007", map[string]any{"contributor_cid": "nothex"})
	account("a000000000000008", map[string]any{"contributor_cid": "c000000000000008"})
	contributor("c000000000000008", map[string]any{"uid": "a000000000000008", "terms_version": "1"})

	for _, tc := range []struct {
		uid  string
		want JoinState
		err  error
	}{
		{"a000000000000001", JoinState{}, nil},
		{"ffffffffffffffff", JoinState{}, nil},
		{"a000000000000002", JoinState{CID: "c000000000000002", TermsVersion: 1}, nil},
		{"a000000000000003", JoinState{CID: "c000000000000003", TermsVersion: 2, Banned: true}, nil},
		{"a000000000000004", JoinState{CID: "c000000000000004"}, nil},
		{"a000000000000005", JoinState{}, ErrUnavailable},
		{"a000000000000006", JoinState{}, ErrUnavailable},
		{"a000000000000007", JoinState{}, ErrUnavailable},
		{"a000000000000008", JoinState{}, ErrUnavailable},
	} {
		got, err := c.JoinState(ctx, tc.uid)
		if !errors.Is(err, tc.err) || got != tc.want {
			t.Errorf("%s: got %+v %v, want %+v %v", tc.uid, got, err, tc.want, tc.err)
		}
	}
}

func TestEmulatorAddCountersIncrementsOnlyTheFetchFields(t *testing.T) {
	c := emulatorClient(t)
	ctx := context.Background()
	ref := c.db.Collection("contributors").Doc("c000000000000001")
	if _, err := ref.Set(ctx, map[string]any{"uid": "a000000000000001", "handle": "tidal-fan", "counters": map[string]any{"published": int64(3), "fetch_claimed": int64(5)}}); err != nil {
		t.Fatal(err)
	}
	if err := c.AddCounters(ctx, "c000000000000001", map[string]int64{"fetch_claimed": 2, "fetch_credits": 4}); err != nil {
		t.Fatal(err)
	}
	if err := c.AddCounters(ctx, "c000000000000001", map[string]int64{"fetch_claimed": 1}); err != nil {
		t.Fatal(err)
	}
	doc, _ := ref.Get(ctx)
	counters, _ := doc.Data()["counters"].(map[string]any)
	if counters["published"] != int64(3) || counters["fetch_claimed"] != int64(8) || counters["fetch_credits"] != int64(4) || doc.Data()["handle"] != "tidal-fan" {
		t.Fatalf("%v", doc.Data())
	}
	if _, err := c.db.Collection("contributors").Doc("c000000000000002").Set(ctx, map[string]any{"uid": "a000000000000002"}); err != nil {
		t.Fatal(err)
	}
	if err := c.AddCounters(ctx, "c000000000000002", map[string]int64{"fetch_verified": 1}); err != nil {
		t.Fatal(err)
	}
	doc, _ = c.db.Collection("contributors").Doc("c000000000000002").Get(ctx)
	if counters, _ = doc.Data()["counters"].(map[string]any); counters["fetch_verified"] != int64(1) {
		t.Fatalf("no counters map yet: %v", doc.Data())
	}
}

func TestEmulatorAddCountersRefusesAMissingContributorAndAnyOtherField(t *testing.T) {
	c := emulatorClient(t)
	ctx := context.Background()
	if err := c.AddCounters(ctx, "c0000000000000ff", map[string]int64{"fetch_claimed": 1}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing contributor: %v", err)
	}
	if _, err := c.db.Collection("contributors").Doc("c000000000000003").Set(ctx, map[string]any{"uid": "a000000000000003", "banned": false}); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"banned", "published", "fetch_", "fetch_claimed.x", "uid"} {
		if err := c.AddCounters(ctx, "c000000000000003", map[string]int64{field: 1}); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: %v", field, err)
		}
	}
	doc, _ := c.db.Collection("contributors").Doc("c000000000000003").Get(ctx)
	if len(doc.Data()) != 2 {
		t.Fatalf("a refused call wrote: %v", doc.Data())
	}
}

func TestEmulatorConcurrentCounterAddsAllLand(t *testing.T) {
	c := emulatorClient(t)
	ctx := context.Background()
	c.db.Collection("contributors").Doc("c000000000000004").Set(ctx, map[string]any{"uid": "a000000000000004"})
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := c.AddCounters(ctx, "c000000000000004", map[string]int64{"fetch_submitted": 1, "fetch_mismatch": 2}); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	doc, _ := c.db.Collection("contributors").Doc("c000000000000004").Get(ctx)
	counters, _ := doc.Data()["counters"].(map[string]any)
	if counters["fetch_submitted"] != int64(12) || counters["fetch_mismatch"] != int64(24) {
		t.Fatalf("%v", counters)
	}
}
