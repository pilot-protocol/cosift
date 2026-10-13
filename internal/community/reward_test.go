package community

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"testing"
	"time"
)

type receiptFields map[string]any

func rewardServer(t *testing.T, mutate func(*Config), receipt func(url string) receiptFields) *Server {
	t.Helper()
	s := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/community/moderate":
			io.WriteString(w, `{"decision":"allow","category":"safe"}`)
		case "/v1/community/enqueue":
			var in struct {
				URL string `json:"url"`
			}
			json.NewDecoder(r.Body).Decode(&in)
			json.NewEncoder(w).Encode(receipt(in.URL))
		}
	}))
	if mutate != nil {
		mutate(&s.cfg)
	}
	return s
}

func novelReceipt(complete bool) func(string) receiptFields {
	return func(url string) receiptFields {
		sum := sha256.Sum256([]byte(url))
		out := receiptFields{"indexed": true, "novel": true, "content_hash": hex.EncodeToString(sum[:])}
		if complete {
			out["content_index_complete"] = true
		}
		return out
	}
}

func submitAll(t *testing.T, s *Server, email string, urls ...string) string {
	t.Helper()
	cookie := account(t, s, email)
	expect(t, request(t, s, "POST", "/api/submissions", map[string]any{"urls": urls}, cookie), 202)
	if err := s.dispatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	var id string
	s.db.QueryRow(`SELECT id FROM users WHERE email=?`, email).Scan(&id)
	return id
}

func rewards(t *testing.T, s *Server, user string) int {
	t.Helper()
	var n int
	if err := s.db.QueryRow(`SELECT count(*) FROM credit_ledger WHERE user_id=? AND reason='verified_contribution'`, user).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestRewardIsOffByDefault(t *testing.T) {
	s := rewardServer(t, nil, novelReceipt(true))
	id := submitAll(t, s, "farmer@example.com", "https://docs.example.com/guide?ref=1")
	if n := rewards(t, s, id); n != 0 {
		t.Fatalf("minted %d rewards with the switch off", n)
	}
	var status string
	s.db.QueryRow(`SELECT status FROM submissions`).Scan(&status)
	if status != "indexed" {
		t.Fatalf("withholding the reward must not block the submission: %s", status)
	}
}

func TestRewardNeedsACompleteContentIndex(t *testing.T) {
	var logs bytes.Buffer
	log.SetOutput(&logs)
	defer log.SetOutput(io.Discard)
	s := rewardServer(t, func(c *Config) { c.ContributionReward = true }, novelReceipt(false))
	id := submitAll(t, s, "member@example.com", "https://docs.example.com/guide")
	if n := rewards(t, s, id); n != 0 {
		t.Fatalf("minted %d rewards while the engine's content index was incomplete", n)
	}
	if !strings.Contains(logs.String(), "content index") {
		t.Fatalf("the withheld reward was not logged: %q", logs.String())
	}
}

func TestRewardPaysOnceForNewContentWhenEnabled(t *testing.T) {
	s := rewardServer(t, func(c *Config) { c.ContributionReward = true }, novelReceipt(true))
	id := submitAll(t, s, "member@example.com", "https://docs.example.com/guide")
	if n := rewards(t, s, id); n != 1 {
		t.Fatalf("rewards = %d, want 1", n)
	}
	var balance int
	s.db.QueryRow(`SELECT sum(delta) FROM credit_ledger WHERE user_id=?`, id).Scan(&balance)
	if balance != contributionReward {
		t.Fatalf("balance %d, want %d", balance, contributionReward)
	}
}

func TestRewardSkipsNonNovelReceipts(t *testing.T) {
	s := rewardServer(t, func(c *Config) { c.ContributionReward = true }, func(url string) receiptFields {
		r := novelReceipt(true)(url)
		r["novel"] = false
		return r
	})
	id := submitAll(t, s, "member@example.com", "https://docs.example.com/guide")
	if n := rewards(t, s, id); n != 0 {
		t.Fatalf("minted %d rewards for known content", n)
	}
}

func TestRewardDailyCapIsPerAccountAndRolls(t *testing.T) {
	s := rewardServer(t, func(c *Config) { c.ContributionReward, c.ContributionRewardDailyCap = true, 3 }, novelReceipt(true))
	var urls []string
	for i := 0; i < 5; i++ {
		urls = append(urls, fmt.Sprintf("https://docs.example.com/page-%d", i))
	}
	first := submitAll(t, s, "first@example.com", urls...)
	if n := rewards(t, s, first); n != 3 {
		t.Fatalf("first account earned %d rewards, want the cap of 3", n)
	}
	second := submitAll(t, s, "second@example.com", "https://other.example.com/new")
	if n := rewards(t, s, second); n != 1 {
		t.Fatalf("a second account shares the first one's cap: %d", n)
	}
	if _, err := s.db.Exec(`UPDATE credit_ledger SET created_at=created_at-90000 WHERE reason='verified_contribution'`); err != nil {
		t.Fatal(err)
	}
	if err := s.rewardContribution(context.Background(), firstSubmission(t, s, first), strings.Repeat("c", 64), true); err != nil {
		t.Fatal(err)
	}
	if n := rewards(t, s, first); n != 4 {
		t.Fatalf("rewards after the window rolled = %d, want 4", n)
	}
}

func TestRewardDailyCapWindowIsADayAndCountsOnlyRewards(t *testing.T) {
	s := rewardServer(t, func(c *Config) { c.ContributionReward, c.ContributionRewardDailyCap = true, 3 }, novelReceipt(true))
	cookie := account(t, s, "member@example.com")
	var id string
	if err := s.db.QueryRow(`SELECT id FROM users WHERE email='member@example.com'`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	for i, row := range []struct {
		reason string
		delta  int
	}{{"monthly_free", 1000}, {"stripe_purchase", 50000}, {"extra_request", -1}, {"extra_request", -2}} {
		if _, err := s.db.Exec(`INSERT INTO credit_ledger(id,user_id,delta,reason,created_at) VALUES(?,?,?,?,?)`, fmt.Sprint("other-", i), id, row.delta, row.reason, time.Now().Unix()); err != nil {
			t.Fatal(err)
		}
	}
	var urls []string
	for i := 0; i < 5; i++ {
		urls = append(urls, fmt.Sprintf("https://docs.example.com/page-%d", i))
	}
	expect(t, request(t, s, "POST", "/api/submissions", map[string]any{"urls": urls}, cookie), 202)
	if err := s.dispatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := rewards(t, s, id); n != 3 {
		t.Fatalf("rewards = %d, want 3: other recent ledger rows must not count toward the cap", n)
	}
	if _, err := s.db.Exec(`UPDATE credit_ledger SET created_at=created_at-23*3600 WHERE reason='verified_contribution'`); err != nil {
		t.Fatal(err)
	}
	if err := s.rewardContribution(context.Background(), firstSubmission(t, s, id), strings.Repeat("d", 64), true); err != nil {
		t.Fatal(err)
	}
	if n := rewards(t, s, id); n != 3 {
		t.Fatalf("rewards 23 hours old stopped counting toward the daily cap: %d", n)
	}
}

func firstSubmission(t *testing.T, s *Server, user string) string {
	t.Helper()
	var id string
	if err := s.db.QueryRow(`SELECT id FROM submissions WHERE user_id=? ORDER BY created_at,id LIMIT 1`, user).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

func TestCreditsAdvertisesTheRewardOnlyWhileEnabled(t *testing.T) {
	for _, on := range []bool{false, true} {
		s := testServer(t, nil)
		s.cfg.ContributionReward, s.cfg.ContributionRewardDailyCap = on, 7
		cookie := account(t, s, "credits@example.com")
		var got map[string]any
		w := request(t, s, "GET", "/api/credits", nil, cookie)
		expect(t, w, 200)
		json.Unmarshal(w.Body.Bytes(), &got)
		reward, has := got["verified_contribution_reward"]
		cap, hasCap := got["verified_contribution_daily_cap"]
		if on && (!has || reward != float64(contributionReward) || !hasCap || cap != float64(7)) {
			t.Fatalf("enabled: reward=%v cap=%v", reward, cap)
		}
		if !on && (has || hasCap) {
			t.Fatalf("disabled but advertised: %v", got)
		}
	}
}

func TestRewardConfigValidation(t *testing.T) {
	for _, tc := range []struct {
		cap     int
		want    int
		wantErr bool
	}{{0, 10, false}, {1, 1, false}, {10000, 10000, false}, {-1, 0, true}, {10001, 0, true}} {
		c := Config{ContributionRewardDailyCap: tc.cap}
		err := c.defaultLimits()
		if (err != nil) != tc.wantErr || (err == nil && c.ContributionRewardDailyCap != tc.want) {
			t.Errorf("cap %d: err=%v resolved=%d, want %d (error %v)", tc.cap, err, c.ContributionRewardDailyCap, tc.want, tc.wantErr)
		}
	}
}

func TestInsufficientCreditsAdvisesContributingOnlyWhileRewarded(t *testing.T) {
	for _, on := range []bool{false, true} {
		s := testServer(t, nil)
		s.cfg.ContributionReward = on
		_, cookie := fundedCreditAccount(t, s, 0)
		w := request(t, s, "GET", "/api/search?q=x", nil, cookie)
		expect(t, w, 429)
		if got := strings.Contains(w.Body.String(), "contribute"); got != on {
			t.Fatalf("reward enabled=%v but the guidance says: %s", on, w.Body.String())
		}
	}
}
