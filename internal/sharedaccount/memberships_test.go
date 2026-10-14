package sharedaccount

import (
	"fmt"
	"testing"
	"time"
)

func TestParseContributionDropsAMemberOfTheWrongType(t *testing.T) {
	at := time.Date(2026, 10, 14, 9, 0, 0, 0, time.FixedZone("x", 3600))
	r := parseContribution("01J9A3Q7W4X9M3K5N6P8R0T2V4", map[string]any{
		"kind": 7, "status": "refused", "status_at": at, "generic_title": 3, "submission": map[string]any{"title": "Submitted"},
		"refusal":        map[string]any{"codes": []any{map[string]any{"code": "promotional", "field": "body_md"}, "junk", map[string]any{"field": "x"}, map[string]any{"code": 5}}},
		"needs_sources":  map[string]any{"urls": []any{"https://a.example/", 4, "https://b.example/\nx"}, "recheck_at": "soon"},
		"sources_pushed": "yes", "lost_reason": 1, "result": map[string]any{"article_id": 9, "version": "2", "credits": 2.5},
	})
	if r.Kind != "" || r.Status != "refused" || !r.StatusAt.Equal(at) || r.StatusAt.Location() != time.UTC || r.Title != "Submitted" {
		t.Fatalf("%+v", r)
	}
	if fmt.Sprint(r.Refusals) != "[{promotional body_md}]" || fmt.Sprint(r.URLs) != "[https://a.example/]" || !r.RecheckAt.IsZero() || r.SourcesPushed || r.LostReason != "" || r.ArticleID != "" || r.Version != 0 || r.Credits != 0 {
		t.Fatalf("%+v", r)
	}
}

func TestParseContributionPrefersTheGenericTitleAndSurvivesABlankedDocument(t *testing.T) {
	r := parseContribution("x", map[string]any{"status": "published", "generic_title": "Tidal power", "submission": map[string]any{"title": "Mine"}})
	if r.Title != "Tidal power" {
		t.Fatalf("%q", r.Title)
	}
	if r := parseContribution("x", map[string]any{"status": "taken_down"}); r.Title != "" || r.ArticleID != "" {
		t.Fatalf("%+v", r)
	}
}

func TestParseMemberRefusesAnotherUIDAndReadsFetchCounters(t *testing.T) {
	if _, err := parseMember("0f1e2d3c4b5a6978", "a1b2c3d4e5f60718", map[string]any{"uid": "a1b2c3d4e5f60719", "handle": "x"}); err != ErrNotFound {
		t.Fatalf("%v", err)
	}
	m, err := parseMember("0f1e2d3c4b5a6978", "a1b2c3d4e5f60718", map[string]any{"uid": "a1b2c3d4e5f60718", "handle": "tidal-fan", "counters": map[string]any{"fetch_verified": int64(8), "fetch_credits": 1.5}})
	if err != nil || m.Handle != "tidal-fan" || m.Fetch["verified"] != 8 || m.Fetch["credits"] != 0 || len(m.Fetch) != 6 {
		t.Fatalf("%+v %v", m, err)
	}
}
