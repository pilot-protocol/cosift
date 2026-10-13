package sharedaccount

import (
	"context"
	"errors"
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
