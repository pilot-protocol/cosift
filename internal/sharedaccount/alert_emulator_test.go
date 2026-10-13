package sharedaccount

import (
	"context"
	"sync"
	"testing"
	"time"
)

func ceilingAlert(subject string, now time.Time) Alert {
	return Alert{Kind: "work_moderation_ceiling", Severity: "warning", Subject: subject, SubjectKind: "day", Title: "Work moderation ceiling reached",
		Detail: "Fetch-work moderation calls are halted for the rest of the UTC day.", Labels: map[string]string{"environment": "prod"}, Now: now}
}

func TestEmulatorRaiseAlertCreatesTheRowThenCountsIt(t *testing.T) {
	c := emulatorClient(t)
	ctx := context.Background()
	first := time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC)
	if err := c.RaiseAlert(ctx, ceilingAlert("2026-10-14", first)); err != nil {
		t.Fatal(err)
	}
	if err := c.RaiseAlert(ctx, ceilingAlert("2026-10-14", first.Add(time.Hour))); err != nil {
		t.Fatal(err)
	}
	doc, err := c.db.Collection("alerts").Doc(AlertDedupKey("work_moderation_ceiling", "2026-10-14")).Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	d := doc.Data()
	labels, _ := d["labels"].(map[string]any)
	want := map[string]any{"schema_version": int64(1), "kind": "work_moderation_ceiling", "severity": "warning", "subject": "2026-10-14", "subject_kind": "day", "source": "community", "status": "firing", "count": int64(2), "reopened_count": int64(0)}
	for k, v := range want {
		if d[k] != v {
			t.Errorf("%s = %v, want %v", k, d[k], v)
		}
	}
	if d["dedup_key"] != doc.Ref.ID || labels["environment"] != "prod" || !d["first_seen_at"].(time.Time).Equal(first) || !d["last_seen_at"].(time.Time).Equal(first.Add(time.Hour)) {
		t.Errorf("%v", d)
	}
	if d["acked_at"] != nil || d["resolved_at"] != nil {
		t.Errorf("producer wrote an operator field: %v", d)
	}
}

func TestEmulatorRaiseAlertReopensAResolvedRowAndLeavesAnAckedOne(t *testing.T) {
	c := emulatorClient(t)
	ctx := context.Background()
	now := time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC)
	for _, subject := range []string{"2026-10-14", "2026-10-15"} {
		if err := c.RaiseAlert(ctx, ceilingAlert(subject, now)); err != nil {
			t.Fatal(err)
		}
	}
	resolved := c.db.Collection("alerts").Doc(AlertDedupKey("work_moderation_ceiling", "2026-10-14"))
	acked := c.db.Collection("alerts").Doc(AlertDedupKey("work_moderation_ceiling", "2026-10-15"))
	if _, err := resolved.Set(ctx, map[string]any{"status": "resolved", "resolved_at": now, "resolved_by": "op", "acked_at": now, "acked_by": "op", "note": "looked"}, firestoreMerge()); err != nil {
		t.Fatal(err)
	}
	if _, err := acked.Set(ctx, map[string]any{"status": "acked", "acked_at": now, "acked_by": "op"}, firestoreMerge()); err != nil {
		t.Fatal(err)
	}
	for _, subject := range []string{"2026-10-14", "2026-10-15"} {
		if err := c.RaiseAlert(ctx, ceilingAlert(subject, now.Add(time.Minute))); err != nil {
			t.Fatal(err)
		}
	}
	doc, _ := resolved.Get(ctx)
	d := doc.Data()
	if d["status"] != "firing" || d["reopened_count"] != int64(1) || d["count"] != int64(2) || d["acked_at"] != nil || d["acked_by"] != nil || d["resolved_at"] != nil || d["resolved_by"] != nil || d["note"] != "looked" {
		t.Errorf("resolved row: %v", d)
	}
	doc, _ = acked.Get(ctx)
	d = doc.Data()
	if d["status"] != "acked" || d["reopened_count"] != int64(0) || d["count"] != int64(2) || d["acked_by"] != "op" {
		t.Errorf("acked row: %v", d)
	}
}

func TestEmulatorRaiseAlertRefusesALeakAndWritesNothing(t *testing.T) {
	c := emulatorClient(t)
	ctx := context.Background()
	a := ceilingAlert("2026-10-14", time.Now())
	a.Labels = map[string]string{"ceiling": "100000"}
	if err := c.RaiseAlert(ctx, a); err == nil {
		t.Fatal("a one-time-code shaped label was written")
	}
	docs, _ := c.db.Collection("alerts").Documents(ctx).GetAll()
	if len(docs) != 0 {
		t.Fatalf("%d documents written", len(docs))
	}
}

func TestEmulatorConcurrentRaisesCountEveryOccurrence(t *testing.T) {
	c := emulatorClient(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := c.RaiseAlert(ctx, ceilingAlert("2026-10-14", time.Now())); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	doc, _ := c.db.Collection("alerts").Doc(AlertDedupKey("work_moderation_ceiling", "2026-10-14")).Get(ctx)
	if doc.Data()["count"] != int64(8) {
		t.Fatalf("count %v", doc.Data()["count"])
	}
}
