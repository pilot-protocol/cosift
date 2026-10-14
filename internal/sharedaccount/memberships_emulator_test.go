package sharedaccount

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"
)

const memberUID, memberCID = "a1b2c3d4e5f60718", "0f1e2d3c4b5a6978"

func seedAccount(t *testing.T, c *Client, uid string, data map[string]any) {
	t.Helper()
	if _, err := c.db.Collection("accounts").Doc(uid).Set(context.Background(), data); err != nil {
		t.Fatal(err)
	}
}

func seedContribution(t *testing.T, c *Client, id string, data map[string]any) {
	t.Helper()
	if _, err := c.db.Collection("contributions").Doc(id).Set(context.Background(), data); err != nil {
		t.Fatal(err)
	}
}

func TestEmulatorMemberReadsHandleAndFetchCountersOnly(t *testing.T) {
	c := emulatorClient(t)
	seedAccount(t, c, memberUID, map[string]any{"contributor_cid": memberCID, "email": "private@example.com"})
	seedContributor(t, c, memberCID, map[string]any{"handle": "tidal-fan", "uid": memberUID, "terms_version": 1,
		"counters": map[string]any{"submitted": 4, "published": 2, "fetch_claimed": 12, "fetch_submitted": 10, "fetch_verified": 8, "fetch_mismatch": 0, "fetch_rejected": 1, "fetch_credits": 16, "fetch_odd": "x"}})
	m, err := c.Member(context.Background(), memberUID)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]int64{"claimed": 12, "submitted": 10, "verified": 8, "mismatch": 0, "rejected": 1, "credits": 16}
	if m.CID != memberCID || m.Handle != "tidal-fan" || fmt.Sprint(m.Fetch) != fmt.Sprint(want) {
		t.Fatalf("%+v", m)
	}
}

func TestEmulatorMemberIsNotFoundWithoutAJoinedContributor(t *testing.T) {
	c := emulatorClient(t)
	ctx := context.Background()
	seedAccount(t, c, memberUID, map[string]any{"email": "private@example.com"})
	if _, err := c.Member(ctx, memberUID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("no contributor_cid: %v", err)
	}
	if _, err := c.Member(ctx, "ffffffffffffffff"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("no account: %v", err)
	}
	seedAccount(t, c, "1111111111111111", map[string]any{"contributor_cid": "2222222222222222"})
	if _, err := c.Member(ctx, "1111111111111111"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("no contributor document: %v", err)
	}
	seedAccount(t, c, "3333333333333333", map[string]any{"contributor_cid": memberCID})
	seedContributor(t, c, memberCID, map[string]any{"handle": "someone", "uid": memberUID})
	if _, err := c.Member(ctx, "3333333333333333"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("a contributor that belongs to another uid: %v", err)
	}
	seedAccount(t, c, "4444444444444444", map[string]any{"contributor_cid": "NOT-A-CID"})
	if _, err := c.Member(ctx, "4444444444444444"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("malformed cid: %v", err)
	}
	if _, err := c.Member(ctx, "bad"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("malformed uid: %v", err)
	}
}

func TestEmulatorMemberQueriesMaskEveryOtherField(t *testing.T) {
	c := emulatorClient(t)
	seedAccount(t, c, memberUID, map[string]any{"contributor_cid": memberCID, "email": "private@example.com", "tokens": []any{"x"}})
	seedContributor(t, c, memberCID, map[string]any{"handle": "tidal-fan", "uid": memberUID, "terms_version": 1, "banned": true, "ip_hash": "secret",
		"counters": map[string]any{"submitted": 4, "fetch_claimed": 12, "fetch_credits": 16}})
	for name, q := range map[string]firestore.Query{"account": accountQuery(c, memberUID), "contributor": contributorQuery(c, memberCID)} {
		doc, err := q.Limit(1).Documents(context.Background()).Next()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		data := doc.Data()
		for _, banned := range []string{"email", "tokens", "terms_version", "banned", "ip_hash"} {
			if _, ok := data[banned]; ok {
				t.Fatalf("%s: %s was requested", name, banned)
			}
		}
		if counters, _ := data["counters"].(map[string]any); counters["submitted"] != nil {
			t.Fatalf("%s: counters %v", name, counters)
		}
	}
}

func TestEmulatorMemberRefusesAContributorIDOutsideTheGrammar(t *testing.T) {
	c := emulatorClient(t)
	seedAccount(t, c, memberUID, map[string]any{"contributor_cid": "NOT-A-CID"})
	seedContributor(t, c, "NOT-A-CID", map[string]any{"handle": "someone", "uid": memberUID})
	if _, err := c.Member(context.Background(), memberUID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("%v", err)
	}
}

func contributionData(status string, at time.Time, extra map[string]any) map[string]any {
	data := map[string]any{"cid": memberCID, "kind": "new", "status": status, "status_at": at, "created_at": at.Add(-time.Hour),
		"submission":     map[string]any{"title": "Submitted title", "lead": "private lead", "body_md": strings.Repeat("secret ", 50)},
		"judge":          map[string]any{"criteria": map[string]any{"a": true}},
		"decision":       map[string]any{"path": "new"},
		"last_error":     "engine_5xx",
		"lease_owner":    "exec-1",
		"submission_sha": "abc",
	}
	for k, v := range extra {
		data[k] = v
	}
	return data
}

func TestEmulatorContributionsAreNewestFirstWithOneExtraRowForMore(t *testing.T) {
	c := emulatorClient(t)
	base := time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC)
	for i := 0; i < 5; i++ {
		seedContribution(t, c, fmt.Sprintf("01J9A3Q7W4X9M3K5N6P8R0T2V%d", i), contributionData("queued", base.Add(time.Duration(i)*time.Minute), nil))
	}
	other := contributionData("queued", base.Add(time.Hour), nil)
	other["cid"] = "1111111111111111"
	seedContribution(t, c, "01J9A3Q7W4X9M3K5N6P8R0T2V9", other)
	got, err := c.Contributions(context.Background(), memberCID, 3)
	if err != nil || len(got) != 3 || got[0].ID != "01J9A3Q7W4X9M3K5N6P8R0T2V4" || got[2].ID != "01J9A3Q7W4X9M3K5N6P8R0T2V2" {
		t.Fatalf("%v %v", got, err)
	}
	if _, err := c.Contributions(context.Background(), "NOT-A-CID", 3); !errors.Is(err, ErrInvalid) {
		t.Fatalf("malformed cid: %v", err)
	}
}

func TestEmulatorContributionsDecodeOutcomesAndNeverReadTheSubmissionText(t *testing.T) {
	c := emulatorClient(t)
	at := time.Date(2026, 10, 14, 9, 12, 31, 0, time.UTC)
	seedContribution(t, c, "01J9A3Q7W4X9M3K5N6P8R0T2V4", contributionData("published", at, map[string]any{
		"generic_title": "Tidal power",
		"result":        map[string]any{"article_id": "01J9A3Q8B2C3D4E5F6G7H8J9K0", "slug": "tidal-power", "version": 1, "credits": 1000, "cost_usd": 0.2, "awards": []any{"art:01J9A3Q7W4X9M3K5N6P8R0T2V4"}},
	}))
	seedContribution(t, c, "01J9A3Q7W4X9M3K5N6P8R0T2V5", contributionData("refused", at.Add(time.Minute), map[string]any{
		"kind":    "revision",
		"refusal": map[string]any{"codes": []any{map[string]any{"code": "promotional", "field": "body_md"}, map[string]any{"code": "title_invalid", "field": "title", "rule": "x"}}},
	}))
	seedContribution(t, c, "01J9A3Q7W4X9M3K5N6P8R0T2V6", contributionData("lost", at.Add(2*time.Minute), map[string]any{"lost_reason": "not_better"}))
	seedContribution(t, c, "01J9A3Q7W4X9M3K5N6P8R0T2V7", contributionData("needs_sources", at.Add(3*time.Minute), map[string]any{
		"sources_pushed": false,
		"needs_sources":  map[string]any{"urls": []any{"https://docs.example.org/a"}, "first_at": at, "rechecks": 0, "recheck_at": at.Add(6 * time.Hour)},
	}))
	seedContribution(t, c, "01J9A3Q7W4X9M3K5N6P8R0T2V8", contributionData("taken_down", at.Add(4*time.Minute), map[string]any{"result": map[string]any{"article_id": "01J9A3Q8B2C3D4E5F6G7H8J9K0", "version": 1, "credits": 1000}}))
	got, err := c.Contributions(context.Background(), memberCID, 10)
	if err != nil || len(got) != 5 {
		t.Fatalf("%v %v", got, err)
	}
	byID := map[string]ContributionRecord{}
	for _, r := range got {
		byID[r.ID] = r
	}
	pub := byID["01J9A3Q7W4X9M3K5N6P8R0T2V4"]
	if pub.Kind != "new" || pub.Status != "published" || !pub.StatusAt.Equal(at) || pub.Title != "Tidal power" || pub.ArticleID != "01J9A3Q8B2C3D4E5F6G7H8J9K0" || pub.Version != 1 || pub.Credits != 1000 {
		t.Fatalf("published %+v", pub)
	}
	ref := byID["01J9A3Q7W4X9M3K5N6P8R0T2V5"]
	if ref.Title != "Submitted title" || fmt.Sprint(ref.Refusals) != "[{promotional body_md} {title_invalid title}]" {
		t.Fatalf("refused %+v", ref)
	}
	if byID["01J9A3Q7W4X9M3K5N6P8R0T2V6"].LostReason != "not_better" {
		t.Fatalf("lost %+v", byID["01J9A3Q7W4X9M3K5N6P8R0T2V6"])
	}
	ns := byID["01J9A3Q7W4X9M3K5N6P8R0T2V7"]
	if fmt.Sprint(ns.URLs) != "[https://docs.example.org/a]" || !ns.RecheckAt.Equal(at.Add(6*time.Hour)) || ns.SourcesPushed {
		t.Fatalf("needs_sources %+v", ns)
	}
	if byID["01J9A3Q7W4X9M3K5N6P8R0T2V8"].Status != "taken_down" {
		t.Fatalf("taken down %+v", byID["01J9A3Q7W4X9M3K5N6P8R0T2V8"])
	}
}

func TestEmulatorContributionQueryMaskExcludesTheSubmissionTextAndTheJudges(t *testing.T) {
	c := emulatorClient(t)
	at := time.Date(2026, 10, 14, 9, 12, 31, 0, time.UTC)
	seedContribution(t, c, "01J9A3Q7W4X9M3K5N6P8R0T2V4", contributionData("published", at, map[string]any{"generic_title": "Tidal power",
		"result": map[string]any{"article_id": "01J9A3Q8B2C3D4E5F6G7H8J9K0", "slug": "tidal-power", "version": 1, "credits": 1000, "cost_usd": 0.2}}))
	it := contributionQuery(c, memberCID, 5).Documents(context.Background())
	defer it.Stop()
	doc, err := it.Next()
	if err != nil && !errors.Is(err, iterator.Done) {
		t.Fatal(err)
	}
	data := doc.Data()
	for _, banned := range []string{"judge", "decision", "last_error", "lease_owner", "submission_sha", "cid"} {
		if _, ok := data[banned]; ok {
			t.Fatalf("%s was requested", banned)
		}
	}
	if sub, _ := data["submission"].(map[string]any); len(sub) != 1 || sub["title"] != "Submitted title" {
		t.Fatalf("submission %v", data["submission"])
	}
	if res, _ := data["result"].(map[string]any); res["slug"] != nil || res["cost_usd"] != nil {
		t.Fatalf("result %v", data["result"])
	}
}
