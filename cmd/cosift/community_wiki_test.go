package main

import (
	"context"
	"strings"
	"testing"
)

const wikiTestKey = "csk_0123456789abcdef_ABCDEFGHIJKLMNOPQRSTUVWXYZ234567ABCDEFGHIJKLMNOPQRST"

func TestWikiSwitchIsExactlyOne(t *testing.T) {
	for v, want := range map[string]bool{"1": true, "": false, "0": false, "true": false, "yes": false, " 1": false, "1 ": false, "on": false} {
		t.Setenv("COSIFT_WIKI_PUBLIC", v)
		if wikiConfig().Public != want {
			t.Fatalf("COSIFT_WIKI_PUBLIC=%q: public = %v", v, !want)
		}
	}
}

func TestCommunityRefusesAPublicWikiWithoutKeyOrMailto(t *testing.T) {
	cases := []struct {
		name, key, mailto, gsc, field string
	}{
		{"bad key", "csk_secret-value-that-must-not-leak", "reports@example.org", "", "COSIFT_WIKI_ENGINE_KEY"},
		{"no key", "", "reports@example.org", "", "COSIFT_WIKI_ENGINE_KEY"},
		{"no mailto", wikiTestKey, "", "", "COSIFT_REPORT_MAILTO"},
		{"bad gsc", wikiTestKey, "reports@example.org", "not valid!", "COSIFT_GSC_VERIFICATION"},
	}
	for _, tc := range cases {
		t.Setenv("COSIFT_COMMUNITY_ADMIN_TOKEN", "admin")
		t.Setenv("COSIFT_WIKI_PUBLIC", "1")
		t.Setenv("COSIFT_WIKI_ENGINE_KEY", tc.key)
		t.Setenv("COSIFT_REPORT_MAILTO", tc.mailto)
		t.Setenv("COSIFT_GSC_VERIFICATION", tc.gsc)
		err := runCommunity(context.Background(), []string{"-addr", "127.0.0.1:0", "-data-dir", t.TempDir()})
		if err == nil || !strings.Contains(err.Error(), tc.field) {
			t.Fatalf("%s: got %v, want a refusal naming %s", tc.name, err, tc.field)
		}
		if tc.key != "" && strings.Contains(err.Error(), tc.key) {
			t.Fatalf("%s: the error echoes the key", tc.name)
		}
	}
}

func TestCommunityStartsDarkWithoutWikiConfig(t *testing.T) {
	t.Setenv("COSIFT_COMMUNITY_ADMIN_TOKEN", "admin")
	t.Setenv("COSIFT_WIKI_PUBLIC", "0")
	t.Setenv("COSIFT_WIKI_ENGINE_KEY", "")
	t.Setenv("COSIFT_REPORT_MAILTO", "")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := runCommunity(ctx, []string{"-addr", "127.0.0.1:0", "-data-dir", t.TempDir()}); err != nil {
		t.Fatalf("the dark app refused to start: %v", err)
	}
}
