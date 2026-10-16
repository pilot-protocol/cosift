package work

import (
	"encoding/json"
	"time"

	"github.com/cockroachdb/pebble"
)

type dayCount struct {
	Day string `json:"day"`
	N   int    `json:"n"`
}

func nextMidnight(now time.Time) time.Time {
	y, m, d := now.UTC().Date()
	return time.Date(y, m, d+1, 0, 0, 0, 0, time.UTC)
}

// today reads the day's moderation count; modMu must be held.
func (s *Service) today(now time.Time) (dayCount, error) {
	var c dayCount
	v, err := s.get(keyModerated)
	if err != nil {
		return c, err
	}
	if v != nil {
		if err := json.Unmarshal(v, &c); err != nil {
			return c, err
		}
	}
	if day := now.UTC().Format(time.DateOnly); c.Day != day {
		c = dayCount{Day: day}
	}
	return c, nil
}

// ceiling reports whether today's moderation allowance is spent, and the time to 00:00 UTC.
func (s *Service) ceiling(now time.Time) (retry time.Duration, reached bool) {
	s.modMu.Lock()
	defer s.modMu.Unlock()
	c, err := s.today(now)
	if err != nil {
		return 0, false
	}
	return nextMidnight(now).Sub(now), c.N >= s.cfg.WorkModerationsPerDay
}

// reserve takes one unit of today's allowance before a model call.
func (s *Service) reserve() error {
	now := s.now()
	s.modMu.Lock()
	defer s.modMu.Unlock()
	c, err := s.today(now)
	if err != nil {
		return err
	}
	if c.N >= s.cfg.WorkModerationsPerDay {
		return &CeilingError{Retry: nextMidnight(now).Sub(now)}
	}
	c.N++
	v, _ := json.Marshal(c)
	return s.db.Set(keyModerated, v, pebble.Sync)
}

// kick wakes the moderation worker.
func (s *Service) kick() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}
