package community

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"
	"unicode"

	"github.com/pilot-protocol/cosift/internal/sharedaccount"
)

func decodeBody(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("%v: %s", err, body)
	}
	return out
}

func TestContributionsAreLimitedTo30AMinutePerUserInTheRouteStore(t *testing.T) {
	p := joinedProvider()
	s := contribServer(t, p, contribArticles())
	for i := 0; i < 30; i++ {
		if w := getContributions(s); w.Code != 200 {
			t.Fatalf("call %d: %d", i+1, w.Code)
		}
	}
	w := getContributions(s)
	body := decodeBody(t, w.Body.Bytes())
	if w.Code != 429 || body["code"] != "rate_limited" || w.Header().Get("Retry-After") == "" || w.Header().Get("Cache-Control") != "private, no-store" {
		t.Fatalf("31st call: %d %s %v", w.Code, w.Body, w.Header())
	}
	if p.members != 30 {
		t.Fatalf("a refused call reached the store (%d reads)", p.members)
	}
	if w = bearerRequest(s, "/api/contributions", sharedOtherToken); w.Code != 200 {
		t.Fatalf("another user shares the window: %d", w.Code)
	}
	var userID string
	if err := s.db.QueryRow(`SELECT user_id FROM shared_identities WHERE uid=?`, "0123456789abcdef").Scan(&userID); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.routeLimits.keys["contributions:"+userID]; !ok {
		t.Fatal("the window is not the contributions route's own")
	}
	if len(s.userLimits.keys) != 0 || len(s.anonLimits.keys) != 0 || len(s.routeLimits.keys) != 2 {
		t.Fatalf("limiter keys: %d user, %d anon, %d route", len(s.userLimits.keys), len(s.anonLimits.keys), len(s.routeLimits.keys))
	}
}

func TestContributionsCarryNoHintText(t *testing.T) {
	raw, err := os.ReadFile("testdata/contribution-vectors-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var vectors struct {
		Refusals     map[string]struct{ Hint string } `json:"refusals"`
		LostHints    map[string]string                `json:"lost_hints"`
		OutcomeHints map[string]string                `json:"outcome_hints"`
	}
	if err := json.Unmarshal(raw, &vectors); err != nil {
		t.Fatal(err)
	}
	var hints []string
	for _, r := range vectors.Refusals {
		hints = append(hints, r.Hint)
	}
	for _, h := range vectors.LostHints {
		hints = append(hints, h)
	}
	for _, h := range vectors.OutcomeHints {
		hints = append(hints, h)
	}
	if len(hints) < 50 {
		t.Fatalf("%d hints loaded", len(hints))
	}
	p := joinedProvider()
	p.recs = sampleRecords()
	body := getContributions(contribServer(t, p, contribArticles())).Body.String()
	for _, h := range hints {
		if strings.Contains(body, h) {
			t.Fatalf("hint text %q in the answer", h)
		}
	}
	if strings.Contains(body, `"hint`) {
		t.Fatalf("a hint member in\n%s", body)
	}
}

func TestContributionsHandleAndTitleInjectionStaysInert(t *testing.T) {
	raw, err := os.ReadFile("wiki/testdata/injection.json")
	if err != nil {
		t.Fatal(err)
	}
	var corpus []struct{ Name, Text string }
	if err := json.Unmarshal(raw, &corpus); err != nil {
		t.Fatal(err)
	}
	for _, tc := range corpus {
		p := joinedProvider()
		p.member.Handle = tc.Text
		p.recs = []sharedaccount.ContributionRecord{{ID: contribID(1), Kind: "new", Status: "refused", StatusAt: time.Unix(1792000000, 0), Title: tc.Text,
			Refusals: []sharedaccount.RefusalCode{{Code: "promotional", Field: tc.Text}}}}
		w := getContributions(contribServer(t, p, nil))
		out := decodeBody(t, w.Body.Bytes())
		if _, ok := out["handle"]; ok {
			t.Fatalf("%s: a handle outside the grammar was shown: %v", tc.Name, out["handle"])
		}
		item := out["contributions"].([]any)[0].(map[string]any)
		title, _ := item["title"].(string)
		for _, r := range title {
			if unicode.In(r, unicode.Cc, unicode.Cf, unicode.Zl, unicode.Zp) {
				t.Fatalf("%s: control character %U in the title", tc.Name, r)
			}
		}
		if len([]rune(title)) > 200 || strings.ContainsAny(title, "\n\r") {
			t.Fatalf("%s: title %q", tc.Name, title)
		}
		if f, ok := item["refusals"].([]any)[0].(map[string]any)["field"]; ok && !regexp.MustCompile(`^[A-Za-z0-9_.\[\]]{1,80}$`).MatchString(f.(string)) {
			t.Fatalf("%s: refusal field %q", tc.Name, f)
		}
		if w.Header().Get("X-Content-Type-Options") != "nosniff" || w.Header().Get("Content-Type") != "application/json" {
			t.Fatalf("%s: headers %v", tc.Name, w.Header())
		}
	}
}

func TestContributionsKeepAGrammarHandleVerbatim(t *testing.T) {
	for _, handle := range []string{"_a_", "x_y-z", "abc", strings.Repeat("a", 24)} {
		p := joinedProvider()
		p.member.Handle = handle
		if got := decodeBody(t, getContributions(contribServer(t, p, nil)).Body.Bytes())["handle"]; got != handle {
			t.Fatalf("%q came back as %v", handle, got)
		}
	}
	for _, handle := range []string{"ab", strings.Repeat("a", 25), "UPPER", "a b", "a<b", ""} {
		p := joinedProvider()
		p.member.Handle = handle
		if got, ok := decodeBody(t, getContributions(contribServer(t, p, nil)).Body.Bytes())["handle"]; ok {
			t.Fatalf("%q was shown as %v", handle, got)
		}
	}
}

func TestContributionsDropMalformedRecordsAndMembers(t *testing.T) {
	at := time.Unix(1792000000, 0).UTC()
	p := joinedProvider()
	p.recs = []sharedaccount.ContributionRecord{
		{ID: "not-a-ulid", Kind: "new", Status: "queued", StatusAt: at},
		{ID: contribID(1), Kind: "other", Status: "queued", StatusAt: at},
		{ID: contribID(2), Kind: "new", Status: "Queued!", StatusAt: at},
		{ID: contribID(3), Kind: "new", Status: "queued"},
		{ID: contribID(4), Kind: "new", Status: "published", StatusAt: at, Title: "Neg‮\nTitle", Credits: -5, Version: -1, ArticleID: "bad"},
		{ID: contribID(5), Kind: "new", Status: "refused", StatusAt: at, Refusals: []sharedaccount.RefusalCode{{Code: "Bad Code", Field: "x"}, {Code: "ok_code", Field: "a b<"}}},
		{ID: contribID(6), Kind: "new", Status: "lost", StatusAt: at, LostReason: "<b>"},
		{ID: contribID(7), Kind: "new", Status: "needs_sources", StatusAt: at, URLs: []string{"javascript:alert(1)", "https://ok.example/a", "ftp://x/", "http://ok.example/‮b"}},
	}
	out := decodeBody(t, getContributions(contribServer(t, p, nil)).Body.Bytes())
	items := out["contributions"].([]any)
	if len(items) != 4 {
		t.Fatalf("%d items kept: %v", len(items), items)
	}
	pub := items[0].(map[string]any)
	if pub["title"] != "Neg Title" || pub["credits"] != float64(0) || pub["article"] != nil {
		t.Fatalf("published %v", pub)
	}
	if ref := items[1].(map[string]any)["refusals"].([]any); len(ref) != 1 || ref[0].(map[string]any)["code"] != "ok_code" || ref[0].(map[string]any)["field"] != nil {
		t.Fatalf("refusals %v", ref)
	}
	if items[2].(map[string]any)["lost_reason"] != nil {
		t.Fatalf("lost %v", items[2])
	}
	if urls := items[3].(map[string]any)["urls"].([]any); len(urls) != 2 || urls[0] != "https://ok.example/a" || urls[1] != "http://ok.example/b" {
		t.Fatalf("urls %v", urls)
	}
}

func TestContributionsCreditsAreTheStoredAmountNotAFixedOne(t *testing.T) {
	p := joinedProvider()
	for i, credits := range []int64{1500, 0, 7, 2} {
		p.recs = append(p.recs, sharedaccount.ContributionRecord{ID: contribID(i + 1), Kind: "new", Status: "published", StatusAt: time.Unix(1792000000, 0), Credits: credits, ArticleID: articleTidal, Version: 1})
	}
	items := decodeBody(t, getContributions(contribServer(t, p, contribArticles())).Body.Bytes())["contributions"].([]any)
	for i, want := range []float64{1500, 0, 7, 2} {
		if got := items[i].(map[string]any)["credits"]; got != want {
			t.Fatalf("item %d credits %v, want %v", i, got, want)
		}
	}
}

func TestContributionsEarnedRowsAreThisUsersNewestFiftyOfTheEarningReasons(t *testing.T) {
	p := joinedProvider()
	s := contribServer(t, p, nil)
	getContributions(s)
	var rows [][]any
	for i := 0; i < 60; i++ {
		rows = append(rows, []any{"fetch:page" + string(rune('a'+i%26)) + string(rune('a'+i/26)), 2, "fetch_verified", 1792000000 + i})
	}
	rows = append(rows, []any{"award:art:" + contribID(1), 1000, "contribution_award", 1790000000}, []any{"request:x", -1, "extra_request", 1792000100}, []any{"stripe:y", 500, "stripe_purchase", 1792000100})
	seedLedger(t, s, rows)
	other := bearerRequest(s, "/api/contributions", sharedOtherToken)
	if other.Code != 200 {
		t.Fatal(other.Code)
	}
	if _, err := s.db.Exec(`INSERT INTO credit_ledger(id,user_id,delta,reason,created_at) SELECT 'fetch:other',user_id,2,'fetch_verified',1793000000 FROM shared_identities WHERE uid='fedcba9876543210'`); err != nil {
		t.Fatal(err)
	}
	earned := decodeBody(t, getContributions(s).Body.Bytes())["earned"].([]any)
	if len(earned) != 50 {
		t.Fatalf("%d rows", len(earned))
	}
	first, last := earned[0].(map[string]any), earned[49].(map[string]any)
	if first["at"] != time.Unix(1792000059, 0).UTC().Format(time.RFC3339) || last["at"] != time.Unix(1792000010, 0).UTC().Format(time.RFC3339) {
		t.Fatalf("order: %v … %v", first, last)
	}
	for _, e := range earned {
		if e.(map[string]any)["reason"] != "fetch_verified" || e.(map[string]any)["award_id"] != nil {
			t.Fatalf("unexpected row %v", e)
		}
	}
}

func TestContributionsArticleURLsAreResolvedOnceAndOnlyForServedArticles(t *testing.T) {
	arts := &fakeArticles{rec: map[string]ArticleRecord{articleTidal: {Slug: "tidal-power"}, "01J9A3Q8B2C3D4E5F6G7H8J9K2": {Slug: "../etc"}},
		err: map[string]error{"01J9A3Q8B2C3D4E5F6G7H8J9K3": errors.New("engine unreachable")}}
	p := joinedProvider()
	at := time.Unix(1792000000, 0)
	for i, id := range []string{articleTidal, articleGone, "01J9A3Q8B2C3D4E5F6G7H8J9K2", "01J9A3Q8B2C3D4E5F6G7H8J9K3"} {
		p.recs = append(p.recs, sharedaccount.ContributionRecord{ID: contribID(i + 1), Kind: "new", Status: "published", StatusAt: at, ArticleID: id, Version: 1})
	}
	s := contribServer(t, p, arts)
	urlsOf := func() []any {
		var urls []any
		for _, it := range decodeBody(t, getContributions(s).Body.Bytes())["contributions"].([]any) {
			urls = append(urls, it.(map[string]any)["article"].(map[string]any)["url"])
		}
		return urls
	}
	got := urlsOf()
	if got[0] != contribPublicURL+"/wiki/tidal-power" || got[1] != nil || got[2] != nil || got[3] != nil {
		t.Fatalf("urls %v", got)
	}
	first := arts.calls
	urlsOf()
	if arts.calls-first != 1 {
		t.Fatalf("%d engine reads on the second call, want only the transient failure retried", arts.calls-first)
	}
}

func TestContributionsWithoutAnArticleReaderStillAnswer(t *testing.T) {
	p := joinedProvider()
	p.recs = sampleRecords()
	w := getContributions(contribServer(t, p, nil))
	if w.Code != 200 || strings.Contains(w.Body.String(), `"url"`) {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
}

func TestContributionsFetchCountsAreNeverNegativeAndAlwaysSixKeys(t *testing.T) {
	p := joinedProvider()
	p.member.Fetch = map[string]int64{"claimed": -3, "verified": 8, "other": 5}
	fetch := decodeBody(t, getContributions(contribServer(t, p, nil)).Body.Bytes())["fetch"].(map[string]any)
	want := map[string]any{"claimed": 0.0, "submitted": 0.0, "verified": 8.0, "mismatch": 0.0, "rejected": 0.0, "credits": 0.0}
	if len(fetch) != 6 {
		t.Fatalf("%v", fetch)
	}
	for k, v := range want {
		if fetch[k] != v {
			t.Fatalf("%s = %v, want %v in %v", k, fetch[k], v, fetch)
		}
	}
}

func TestContributionsReadTheCallersOwnMember(t *testing.T) {
	p := joinedProvider()
	p.byUID = map[string]sharedaccount.Member{
		"0123456789abcdef": {CID: "aaaaaaaaaaaaaaaa", Handle: "alpha-one", Fetch: map[string]int64{"claimed": 1}},
		"fedcba9876543210": {CID: "bbbbbbbbbbbbbbbb", Handle: "beta-two", Fetch: map[string]int64{"claimed": 2}},
	}
	s := contribServer(t, p, contribArticles())
	first := decodeBody(t, getContributions(s).Body.Bytes())
	second := decodeBody(t, bearerRequest(s, "/api/contributions", sharedOtherToken).Body.Bytes())
	if first["handle"] != "alpha-one" || second["handle"] != "beta-two" {
		t.Fatalf("handles %v and %v", first["handle"], second["handle"])
	}
	if fmt.Sprint(p.uids) != "[0123456789abcdef fedcba9876543210]" || fmt.Sprint(p.cids) != "[aaaaaaaaaaaaaaaa bbbbbbbbbbbbbbbb]" {
		t.Fatalf("members read for %v, contributions for %v", p.uids, p.cids)
	}
}

func TestContributionsGiveCreditsAndArticlesToPublishedAndWonRowsOnly(t *testing.T) {
	p := joinedProvider()
	at := time.Date(2026, 10, 14, 9, 12, 31, 0, time.UTC)
	for i, status := range []string{"published", "won", "taken_down", "reverted"} {
		p.recs = append(p.recs, sharedaccount.ContributionRecord{ID: contribID(i + 1), Kind: "new", Status: status, StatusAt: at, Title: "T", ArticleID: articleTidal, Version: 1, Credits: 500})
	}
	body := decodeBody(t, getContributions(contribServer(t, p, contribArticles())).Body.Bytes())
	items, _ := body["contributions"].([]any)
	if len(items) != 4 {
		t.Fatalf("%d items", len(items))
	}
	for _, raw := range items {
		it, _ := raw.(map[string]any)
		_, hasCredits := it["credits"]
		_, hasArticle := it["article"]
		paid := it["status"] == "published" || it["status"] == "won"
		if hasCredits != paid || hasArticle != paid {
			t.Fatalf("%v: credits %v, article %v", it["status"], hasCredits, hasArticle)
		}
	}
}
