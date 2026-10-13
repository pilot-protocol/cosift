package main

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestCommunityRewardEnv(t *testing.T) {
	for _, tc := range []struct {
		name, on, cap string
		wantOn        bool
		wantCap       int
		bad           string
	}{
		{"unset", "", "", false, 0, ""},
		{"off", "off", "", false, 0, ""},
		{"on", "on", "", true, 0, ""},
		{"on with cap", " ON ", "7", true, 7, ""},
		{"cap alone", "", "3", false, 3, ""},
		{"unknown switch", "yes", "", false, 0, "COSIFT_COMMUNITY_CONTRIBUTION_REWARD"},
		{"zero cap", "on", "0", false, 0, "COSIFT_COMMUNITY_CONTRIBUTION_REWARD_DAILY_CAP"},
		{"negative cap", "on", "-4", false, 0, "COSIFT_COMMUNITY_CONTRIBUTION_REWARD_DAILY_CAP"},
		{"text cap", "on", "many", false, 0, "COSIFT_COMMUNITY_CONTRIBUTION_REWARD_DAILY_CAP"},
		{"huge cap", "on", "10001", false, 0, "COSIFT_COMMUNITY_CONTRIBUTION_REWARD_DAILY_CAP"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("COSIFT_COMMUNITY_CONTRIBUTION_REWARD", tc.on)
			t.Setenv("COSIFT_COMMUNITY_CONTRIBUTION_REWARD_DAILY_CAP", tc.cap)
			on, cap, err := communityRewardFromEnv()
			if tc.bad != "" {
				if err == nil || !strings.Contains(err.Error(), tc.bad) {
					t.Fatalf("got %v, want an error naming %s", err, tc.bad)
				}
				return
			}
			if err != nil || on != tc.wantOn || cap != tc.wantCap {
				t.Fatalf("got %v %d %v, want %v %d", on, cap, err, tc.wantOn, tc.wantCap)
			}
		})
	}
}

func TestCommunityRefusesAnInvalidRewardSwitch(t *testing.T) {
	t.Setenv("COSIFT_COMMUNITY_ADMIN_TOKEN", "admin")
	t.Setenv("COSIFT_COMMUNITY_CONTRIBUTION_REWARD", "enabled")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := runCommunity(ctx, []string{"-addr", "127.0.0.1:0", "-data-dir", t.TempDir()})
	if err == nil || !strings.Contains(err.Error(), "COSIFT_COMMUNITY_CONTRIBUTION_REWARD") {
		t.Fatalf("got %v", err)
	}
}

func TestCommunityRewardEnvReachesTheCreditsEndpoint(t *testing.T) {
	for _, tc := range []struct {
		on, cap string
		want    bool
	}{{"", "", false}, {"on", "7", true}} {
		t.Setenv("COSIFT_COMMUNITY_ADMIN_TOKEN", "admin")
		t.Setenv("COSIFT_COMMUNITY_CONTRIBUTION_REWARD", tc.on)
		t.Setenv("COSIFT_COMMUNITY_CONTRIBUTION_REWARD_DAILY_CAP", tc.cap)
		t.Setenv("COSIFT_WIKI_PUBLIC", "0")
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := l.Addr().String()
		l.Close()
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- runCommunity(ctx, []string{"-addr", addr, "-data-dir", t.TempDir()}) }()
		base := "http://" + addr
		for i := 0; ; i++ {
			if res, err := http.Get(base + "/healthz"); err == nil {
				res.Body.Close()
				break
			}
			if i > 100 {
				t.Fatal("the community app did not start")
			}
			time.Sleep(50 * time.Millisecond)
		}
		post, _ := http.NewRequest("POST", base+"/api/register", strings.NewReader(`{"email":"reward@example.com","name":"Reward Person","password":"a-test-password-123"}`))
		post.Header.Set("Content-Type", "application/json")
		post.Header.Set("X-Cosift-Client", "community")
		res, err := http.DefaultClient.Do(post)
		if err != nil || res.StatusCode != 200 {
			t.Fatalf("register: %v %v", err, res)
		}
		res.Body.Close()
		get, _ := http.NewRequest("GET", base+"/api/credits", nil)
		get.Header.Set("X-Cosift-Client", "community")
		for _, c := range res.Cookies() {
			get.AddCookie(c)
		}
		res, err = http.DefaultClient.Do(get)
		if err != nil || res.StatusCode != 200 {
			t.Fatalf("credits: %v %v", err, res)
		}
		var credits map[string]any
		json.NewDecoder(res.Body).Decode(&credits)
		res.Body.Close()
		reward, has := credits["verified_contribution_reward"]
		if has != tc.want || (tc.want && (reward != float64(10) || credits["verified_contribution_daily_cap"] != float64(7))) {
			t.Fatalf("switch %q cap %q: credits = %v", tc.on, tc.cap, credits)
		}
		cancel()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
}
