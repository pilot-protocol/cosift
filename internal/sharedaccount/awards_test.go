package sharedaccount

import (
	"testing"
	"time"
)

func awardData() map[string]any {
	return map[string]any{
		"schema_version": int64(1), "uid": "a1b2c3d4e5f60718", "cid": "0f1e2d3c4b5a6978", "amount": int64(1000), "reason": "article",
		"ref":    map[string]any{"contribution_id": "01J9A3Q7W4X9M3K5N6P8R0T2V4", "article_id": "01J9A3Q8B2C3D4E5F6G7H8J9K0", "version": int64(2)},
		"status": "pending", "created_at": time.Date(2026, 10, 14, 9, 12, 31, 0, time.UTC),
	}
}

func TestParseAwardReadsTheDocument(t *testing.T) {
	a := ParseAward("art:01J9A3Q7W4X9M3K5N6P8R0T2V4", awardData())
	if !a.Valid || a.Applied || a.UID != "a1b2c3d4e5f60718" || a.CID != "0f1e2d3c4b5a6978" || a.Amount != 1000 || a.Reason != "article" || a.Status != "pending" ||
		a.Ref.ContributionID != "01J9A3Q7W4X9M3K5N6P8R0T2V4" || a.Ref.ArticleID != "01J9A3Q8B2C3D4E5F6G7H8J9K0" || a.Ref.Version != 2 || !a.CreatedAt.Equal(time.Date(2026, 10, 14, 9, 12, 31, 0, time.UTC)) {
		t.Fatalf("%+v", a)
	}
}

func TestParseAwardRefusesWrongTypesAndFlagsAppliedMembers(t *testing.T) {
	for name, mutate := range map[string]func(map[string]any){
		"amount string":  func(d map[string]any) { d["amount"] = "1000" },
		"amount float":   func(d map[string]any) { d["amount"] = 1000.5 },
		"uid number":     func(d map[string]any) { d["uid"] = int64(7) },
		"no created_at":  func(d map[string]any) { delete(d, "created_at") },
		"ref not a map":  func(d map[string]any) { d["ref"] = "x" },
		"version string": func(d map[string]any) { d["ref"].(map[string]any)["version"] = "2" },
		"article number": func(d map[string]any) { d["ref"].(map[string]any)["article_id"] = int64(1) },
	} {
		d := awardData()
		mutate(d)
		if ParseAward("x", d).Valid {
			t.Errorf("%s: accepted", name)
		}
	}
	for _, key := range []string{"applied_at", "applied_delta", "ledger_id"} {
		d := awardData()
		d[key] = int64(1)
		if !ParseAward("x", d).Applied {
			t.Errorf("%s not flagged", key)
		}
	}
}
