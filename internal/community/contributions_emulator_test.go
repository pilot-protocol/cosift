package community

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/pilot-protocol/cosift/internal/sharedaccount"
)

type firestoreContributions struct {
	*fakeShared
	sharedaccount.ContributionSource
}

func TestEmulatorContributionsEndToEnd(t *testing.T) {
	if os.Getenv("FIRESTORE_EMULATOR_HOST") == "" {
		t.Skip("FIRESTORE_EMULATOR_HOST is not set")
	}
	ctx := context.Background()
	db, err := firestore.NewClientWithDatabase(ctx, "emulator-community-contributions", "(default)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, name := range []string{"contributions", "contributors", "accounts"} {
		docs, _ := db.Collection(name).Documents(ctx).GetAll()
		for _, d := range docs {
			d.Ref.Delete(ctx)
		}
	}
	const uid, cid = "0123456789abcdef", "0f1e2d3c4b5a6978"
	set := func(path string, data map[string]any) {
		t.Helper()
		if _, err := db.Doc(path).Set(ctx, data); err != nil {
			t.Fatal(err)
		}
	}
	set("accounts/"+uid, map[string]any{"contributor_cid": cid, "email": "shared@example.com"})
	set("contributors/"+cid, map[string]any{"uid": uid, "handle": "tidal-fan", "terms_version": int64(1), "counters": map[string]any{"fetch_claimed": int64(12), "fetch_verified": int64(8), "published": int64(1)}})
	at := time.Date(2026, 10, 14, 9, 12, 31, 0, time.UTC)
	set("contributions/"+contribID(1), map[string]any{"cid": cid, "kind": "new", "status": "published", "status_at": at, "created_at": at.Add(-time.Hour), "generic_title": "Tidal power",
		"submission": map[string]any{"title": "Mine", "body_md": "PRIVATE-BODY-TEXT"}, "judge": map[string]any{"criteria": map[string]any{"x": true}},
		"result": map[string]any{"article_id": articleTidal, "slug": "tidal-power", "version": int64(1), "credits": int64(1500), "cost_usd": 0.4}})
	set("contributions/"+contribID(2), map[string]any{"cid": cid, "kind": "revision", "status": "refused", "status_at": at.Add(time.Minute), "created_at": at,
		"submission": map[string]any{"title": "Rust async runtimes", "body_md": "PRIVATE-BODY-TEXT"}, "refusal": map[string]any{"codes": []any{map[string]any{"code": "promotional", "field": "body_md"}}}})
	set("contributions/"+contribID(3), map[string]any{"cid": "1111111111111111", "kind": "new", "status": "queued", "status_at": at, "created_at": at.Add(time.Hour), "submission": map[string]any{"title": "Someone else"}})
	src := sharedaccount.NewAwardSource(db).(sharedaccount.ContributionSource)
	s := contribServer(t, &contribProvider{fakeShared: &fakeShared{}}, contribArticles())
	s.cfg.Shared = firestoreContributions{&fakeShared{}, src}
	w := getContributions(s)
	expect(t, w, 200)
	body := w.Body.String()
	if strings.Contains(body, "PRIVATE-BODY-TEXT") || strings.Contains(body, "Someone else") || strings.Contains(body, "email") {
		t.Fatalf("private data in\n%s", body)
	}
	goldenJSON(t, "contributions-emulator.json", w.Body.Bytes())
}
