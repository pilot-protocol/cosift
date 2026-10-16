package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/pilot-protocol/cosift/internal/community"
)

func TestCommunityModerationShareEnv(t *testing.T) {
	for _, tc := range []struct {
		name, value string
		want        int
		bad         bool
	}{
		{"unset", "", 0, false},
		{"blank", "  ", 0, false},
		{"the floor", "1", 1, false},
		{"a value", " 250 ", 250, false},
		{"the ceiling", "10000", 10000, false},
		{"zero", "0", 0, true},
		{"negative", "-4", 0, true},
		{"above the ceiling", "10001", 0, true},
		{"text", "many", 0, true},
		{"a fraction", "2.5", 0, true},
		{"overflow", "99999999999999999999", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("COSIFT_COMMUNITY_MODERATIONS_PER_ACCOUNT", tc.value)
			got, err := communityModerationShareFromEnv()
			if tc.bad {
				if err == nil || !strings.Contains(err.Error(), "COSIFT_COMMUNITY_MODERATIONS_PER_ACCOUNT") {
					t.Fatalf("got %d, %v, want an error naming the variable", got, err)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("got %d, %v, want %d", got, err, tc.want)
			}
		})
	}
}

func TestCommunityRefusesAnInvalidModerationShare(t *testing.T) {
	for _, v := range []string{"0", "10001", "lots"} {
		t.Setenv("COSIFT_COMMUNITY_MODERATIONS_PER_ACCOUNT", v)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := runCommunity(ctx, []string{"-addr", "127.0.0.1:0", "-data-dir", t.TempDir()})
		if err == nil || !strings.Contains(err.Error(), "COSIFT_COMMUNITY_MODERATIONS_PER_ACCOUNT") {
			t.Fatalf("%q: got %v", v, err)
		}
	}
}

func TestCommunityPassesTheModerationShareToTheApp(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  int
	}{{"", 0}, {"20", 20}, {"10000", 10000}} {
		t.Run("value "+tc.value, func(t *testing.T) {
			t.Setenv("COSIFT_COMMUNITY_MODERATIONS_PER_ACCOUNT", tc.value)
			errStop := errors.New("stop before serving")
			var opened community.Config
			previous := openCommunity
			openCommunity = func(cfg community.Config) (*community.Server, error) {
				opened = cfg
				return nil, errStop
			}
			t.Cleanup(func() { openCommunity = previous })
			if err := runCommunity(context.Background(), []string{"-addr", "127.0.0.1:0", "-data-dir", t.TempDir()}); !errors.Is(err, errStop) {
				t.Fatalf("got %v", err)
			}
			if opened.ModerationsPerAccount != tc.want {
				t.Fatalf("the app got a share of %d, want %d", opened.ModerationsPerAccount, tc.want)
			}
		})
	}
}
