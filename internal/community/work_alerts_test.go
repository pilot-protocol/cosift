package community

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/sharedaccount"
)

func alertRig(t *testing.T) (*Server, *fakeWork, *clock) {
	s, f, c := gateRig(t)
	s.cfg.Work, s.cfg.Environment = true, "prod"
	return s, f, c
}

func TestTheFirstCeilingAnswerOfADayRaisesOneAlertWithTheFixedText(t *testing.T) {
	s, f, clk := alertRig(t)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		s.noteCeiling(ceilingWork)
	}
	s.drainAlerts(ctx)
	s.noteCeiling(ceilingWork)
	s.drainAlerts(ctx)
	if len(f.alerts) != 1 {
		t.Fatalf("%d alerts for one day", len(f.alerts))
	}
	a := f.alerts[0]
	if a.Kind != "work_moderation_ceiling" || a.Subject != "2026-10-14" || a.SubjectKind != "day" || a.Severity != "warning" ||
		a.Title != "Work moderation ceiling reached" || a.Detail != "Fetch-work moderation calls are halted for the rest of the UTC day." ||
		len(a.Labels) != 1 || a.Labels["environment"] != "prod" || !a.Now.Equal(clk.now()) {
		t.Fatalf("%+v", a)
	}
	if err := a.Validate(); err != nil {
		t.Fatal(err)
	}
	clk.advance(24 * time.Hour)
	s.noteCeiling(ceilingWork)
	s.drainAlerts(ctx)
	if len(f.alerts) != 2 || f.alerts[1].Subject != "2026-10-15" {
		t.Fatalf("next day: %+v", f.alerts)
	}
}

func TestTheModerateCeilingHasItsOwnSubjectAndText(t *testing.T) {
	s, f, _ := alertRig(t)
	s.cfg.Environment = "staging"
	s.noteCeiling(ceilingWork)
	s.noteCeiling(ceilingModerate)
	s.drainAlerts(context.Background())
	if len(f.alerts) != 2 {
		t.Fatalf("%d alerts", len(f.alerts))
	}
	moderate := f.alerts[1]
	if moderate.Subject != "2026-10-14/moderate" || moderate.Detail != "Community moderation calls are halted for the rest of the UTC day." || moderate.Labels["environment"] != "staging" || moderate.Title != "Work moderation ceiling reached" {
		t.Fatalf("%+v", moderate)
	}
	if sharedaccount.AlertDedupKey(f.alerts[0].Kind, f.alerts[0].Subject) == sharedaccount.AlertDedupKey(moderate.Kind, moderate.Subject) {
		t.Fatal("the two ceilings share a row")
	}
	if err := moderate.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestAFailedAlertWriteIsRetriedEveryMinuteUntilItLands(t *testing.T) {
	s, f, clk := alertRig(t)
	ctx := context.Background()
	f.alertErr = errors.New("firestore down")
	s.noteCeiling(ceilingWork)
	s.drainAlerts(ctx)
	clk.advance(10 * time.Second)
	s.drainAlerts(ctx)
	clk.advance(40 * time.Second)
	s.drainAlerts(ctx)
	if len(f.alerts) != 0 || f.alertTry != 1 {
		t.Fatalf("%d alerts, %d tries within the first minute", len(f.alerts), f.alertTry)
	}
	clk.advance(11 * time.Second)
	s.drainAlerts(ctx)
	if f.alertTry != 2 {
		t.Fatalf("%d tries after a minute", f.alertTry)
	}
	f.alertErr = nil
	s.drainAlerts(ctx)
	clk.advance(time.Minute)
	s.drainAlerts(ctx)
	s.drainAlerts(ctx)
	if len(f.alerts) != 1 {
		t.Fatalf("%d alerts after the recovery", len(f.alerts))
	}
}

func TestShutdownTriesAFailedAlertOnceMoreWhateverTheWait(t *testing.T) {
	s, f, _ := alertRig(t)
	f.alertErr = errors.New("firestore down")
	s.noteCeiling(ceilingWork)
	s.drainAlerts(context.Background())
	f.alertErr = nil
	s.drainAlertsAt(context.Background(), true)
	if len(f.alerts) != 1 {
		t.Fatalf("%d alerts", len(f.alerts))
	}
}

func TestNoAlertIsQueuedWhileWorkIsOff(t *testing.T) {
	s, f, _ := alertRig(t)
	s.cfg.Work = false
	s.noteCeiling(ceilingWork)
	s.noteCeiling(ceilingModerate)
	s.drainAlerts(context.Background())
	if len(f.alerts) != 0 {
		t.Fatalf("%d alerts", len(f.alerts))
	}
}
