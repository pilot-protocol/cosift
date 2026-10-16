package community

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/pilot-protocol/cosift/internal/sharedaccount"
)

const (
	ceilingWork     = ""
	ceilingModerate = "/moderate"
	alertWait       = 10 * time.Second
	alertRetryWait  = time.Minute
)

// alertQueue holds the ceiling alerts of the day until a loop writes them; a subject is queued once per process and day.
type alertQueue struct {
	mu      sync.Mutex
	pending []string
	seen    map[string]bool
	retryAt time.Time
}

// noteCeiling records that an engine answer reported a moderation ceiling; the first one of a UTC day per subject raises the alert.
func (s *Server) noteCeiling(suffix string) {
	if !s.cfg.Work {
		return
	}
	subject := s.now().UTC().Format("2006-01-02") + suffix
	q := &s.work.alerts
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.seen[subject] {
		return
	}
	if q.seen == nil {
		q.seen = map[string]bool{}
	}
	q.seen[subject] = true
	q.pending = append(q.pending, subject)
}

func (s *Server) ceilingAlert(subject string, now time.Time) sharedaccount.Alert {
	detail := "Fetch-work moderation calls are halted for the rest of the UTC day."
	if len(subject) > len("2006-01-02") {
		detail = "Community moderation calls are halted for the rest of the UTC day."
	}
	return sharedaccount.Alert{Kind: "work_moderation_ceiling", Severity: "warning", Subject: subject, SubjectKind: "day", Title: "Work moderation ceiling reached", Detail: detail, Labels: map[string]string{"environment": s.cfg.Environment}, Now: now}
}

// drainAlerts writes the queued alerts; one that fails stays queued and is tried again after alertRetryWait.
func (s *Server) drainAlerts(ctx context.Context) { s.drainAlertsAt(ctx, false) }

func (s *Server) drainAlertsAt(ctx context.Context, force bool) {
	q := &s.work.alerts
	q.mu.Lock()
	if !force && s.now().Before(q.retryAt) {
		q.mu.Unlock()
		return
	}
	batch := q.pending
	q.pending = nil
	q.mu.Unlock()
	var failed []string
	for _, subject := range batch {
		callCtx, cancel := context.WithTimeout(ctx, alertWait)
		err := s.work.source.RaiseAlert(callCtx, s.ceilingAlert(subject, s.now()))
		cancel()
		if err != nil {
			failed = append(failed, subject)
		}
	}
	if len(failed) > 0 {
		log.Printf("community: work_alert_failed alerts=%d", len(failed))
		q.mu.Lock()
		q.pending = append(failed, q.pending...)
		q.retryAt = s.now().Add(alertRetryWait)
		q.mu.Unlock()
	}
}
