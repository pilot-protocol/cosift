package sharedaccount

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func seedContributor(t *testing.T, c *Client, cid string, data map[string]any) {
	t.Helper()
	if _, err := c.db.Collection("contributors").Doc(cid).Set(context.Background(), data); err != nil {
		t.Fatal(err)
	}
}

func TestEmulatorHandlesReadOnlyTheMaskedFields(t *testing.T) {
	c := emulatorClient(t)
	seedContributor(t, c, "0f1e2d3c4b5a6978", map[string]any{"handle": "tidal-fan", "uid": "a1b2c3d4e5f60718", "terms_version": 1, "counters": map[string]any{"published": 3}})
	seedContributor(t, c, "5b8a1c4e9d207f36", map[string]any{"handle": "rusty", "banned": true, "uid": "a1b2c3d4e5f60719"})
	seedContributor(t, c, "1111111111111111", map[string]any{"handle": "odd-ban", "banned": "yes", "uid": "a1b2c3d4e5f60720"})
	seedContributor(t, c, "2222222222222222", map[string]any{"handle": 7, "banned": nil})
	got, err := c.Handles(context.Background(), []string{"0f1e2d3c4b5a6978", "5b8a1c4e9d207f36", "1111111111111111", "2222222222222222", "3333333333333333"})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]ContributorHandle{
		"0f1e2d3c4b5a6978": {Handle: "tidal-fan"},
		"5b8a1c4e9d207f36": {Handle: "rusty", Banned: true},
		"1111111111111111": {Handle: "odd-ban", Banned: true},
		"2222222222222222": {},
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestEmulatorHandlesBatchPastTheInFilterLimit(t *testing.T) {
	c := emulatorClient(t)
	var cids []string
	for i := 0; i < 65; i++ {
		cid := fmt.Sprintf("%016x", i+1)
		cids = append(cids, cid)
		seedContributor(t, c, cid, map[string]any{"handle": fmt.Sprintf("member-%02d", i)})
	}
	got, err := c.Handles(context.Background(), cids)
	if err != nil || len(got) != 65 || got[cids[64]].Handle != "member-64" {
		t.Fatalf("%d handles, err %v", len(got), err)
	}
}

func TestHandlesRefuseAMalformedCIDBeforeAnyRead(t *testing.T) {
	c := &Client{}
	for _, bad := range []string{"", "ABCDEF0123456789", "short", "0f1e2d3c4b5a6978/../x"} {
		if _, err := c.Handles(context.Background(), []string{"0f1e2d3c4b5a6978", bad}); !errors.Is(err, ErrInvalid) {
			t.Fatalf("%q: %v", bad, err)
		}
	}
}

func TestParseHandleRefusesAMemberOutsideTheMask(t *testing.T) {
	if _, ok := parseHandle(map[string]any{"handle": "tidal-fan", "uid": "a1b2c3d4e5f60718"}); ok {
		t.Fatal("a document with an unmasked member was accepted")
	}
	if h, ok := parseHandle(map[string]any{"handle": "tidal-fan", "banned": false}); !ok || h.Handle != "tidal-fan" || h.Banned {
		t.Fatalf("%+v %v", h, ok)
	}
}
