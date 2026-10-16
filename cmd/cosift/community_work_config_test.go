package main

import (
	"context"
	"strings"
	"testing"
)

func TestCommunityWorkEnv(t *testing.T) {
	for _, tc := range []struct {
		name, work, cap, env string
		wantWork             bool
		wantCap              int
		bad                  string
	}{
		{"unset", "", "", "", false, 0, ""},
		{"off", "0", "", "", false, 0, ""},
		{"on", "1", "", "prod", true, 0, ""},
		{"on with a cap", " 1 ", "7", "staging", true, 7, ""},
		{"cap alone", "", "3", "", false, 3, ""},
		{"a word for on", "on", "", "", false, 0, "COSIFT_COMMUNITY_WORK"},
		{"two", "2", "", "", false, 0, "COSIFT_COMMUNITY_WORK"},
		{"zero cap", "1", "0", "prod", false, 0, "COSIFT_COMMUNITY_FETCH_DAILY_CAP"},
		{"negative cap", "1", "-4", "prod", false, 0, "COSIFT_COMMUNITY_FETCH_DAILY_CAP"},
		{"text cap", "1", "many", "prod", false, 0, "COSIFT_COMMUNITY_FETCH_DAILY_CAP"},
		{"huge cap", "1", "10001", "prod", false, 0, "COSIFT_COMMUNITY_FETCH_DAILY_CAP"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("COSIFT_COMMUNITY_WORK", tc.work)
			t.Setenv("COSIFT_COMMUNITY_FETCH_DAILY_CAP", tc.cap)
			t.Setenv("COSIFT_ENV", tc.env)
			work, cap, env, err := communityWorkFromEnv()
			if tc.bad != "" {
				if err == nil || !strings.Contains(err.Error(), tc.bad) {
					t.Fatalf("got %v, want an error naming %s", err, tc.bad)
				}
				return
			}
			if err != nil || work != tc.wantWork || cap != tc.wantCap || env != tc.env {
				t.Fatalf("got %v %d %q %v, want %v %d %q", work, cap, env, err, tc.wantWork, tc.wantCap, tc.env)
			}
		})
	}
}

func TestCommunityRefusesWorkItCannotRun(t *testing.T) {
	for _, tc := range []struct {
		name, work, cap, key, want string
	}{
		{"an invalid switch", "yes", "", "", "COSIFT_COMMUNITY_WORK"},
		{"an invalid cap", "1", "0", "", "COSIFT_COMMUNITY_FETCH_DAILY_CAP"},
		{"work without shared accounts", "1", "", "csk_0123456789abcdef_" + strings.Repeat("A", 52), "COSIFT_COMMUNITY_WORK requires COSIFT_AUTH_MODE=shared"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("COSIFT_COMMUNITY_WORK", tc.work)
			t.Setenv("COSIFT_COMMUNITY_FETCH_DAILY_CAP", tc.cap)
			t.Setenv("COSIFT_COMMUNITY_BROKER_KEY", tc.key)
			t.Setenv("COSIFT_ENV", "prod")
			t.Setenv("COSIFT_AUTH_MODE", "")
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			err := runCommunity(ctx, []string{"-addr", "127.0.0.1:0", "-data-dir", t.TempDir()})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want %q", err, tc.want)
			}
		})
	}
}
