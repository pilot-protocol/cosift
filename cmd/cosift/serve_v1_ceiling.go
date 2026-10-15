package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/pilot-protocol/cosift/internal/config"
)

// communityModerationsKey is the 'W' family's moderation counter for the community route.
var communityModerationsKey = []byte("Wmcommunity_moderations")

type dayCount struct {
	Day string `json:"day"`
	N   int    `json:"n"`
}

// ceilingError reports the daily moderation ceiling; retry is the time to 00:00 UTC.
type ceilingError struct{ retry time.Duration }

func (e *ceilingError) Error() string { return "daily moderation ceiling reached" }

// communityModerationCounts counts the answers of POST /v1/community/moderate.
type communityModerationCounts struct{ ok, ceiling, errs atomic.Int64 }

func (s *pebbleHTTP) communityTime() time.Time {
	if s.communityNow != nil {
		return s.communityNow()
	}
	return time.Now()
}

// reserveCommunityModeration takes one unit of today's allowance before a model call.
func (s *pebbleHTTP) reserveCommunityModeration() error {
	limit := config.MaxCommunityModerationsPerDay
	if s.communityModerationsPerDay != nil {
		limit = *s.communityModerationsPerDay
	}
	if s.store == nil {
		return errors.New("moderation counter unavailable")
	}
	now := s.communityTime().UTC()
	s.communityModerationMu.Lock()
	defer s.communityModerationMu.Unlock()
	db := s.store.DB()
	var c dayCount
	b, closer, err := db.Get(communityModerationsKey)
	switch {
	case err == nil:
		err = json.Unmarshal(b, &c)
		closer.Close()
		if err != nil {
			return err
		}
	case !errors.Is(err, pebble.ErrNotFound):
		return err
	}
	if day := now.Format(time.DateOnly); c.Day != day {
		c = dayCount{Day: day}
	}
	if c.N >= limit {
		y, m, d := now.Date()
		return &ceilingError{retry: time.Date(y, m, d+1, 0, 0, 0, 0, time.UTC).Sub(now)}
	}
	c.N++
	data, _ := json.Marshal(c)
	return db.Set(communityModerationsKey, data, pebble.Sync)
}

func writeCommunityModerationMetrics(w io.Writer, s *pebbleHTTP) {
	fmt.Fprintf(w, "# HELP cosift_community_moderations_total Answers of POST /v1/community/moderate by result.\n")
	fmt.Fprintf(w, "# TYPE cosift_community_moderations_total counter\n")
	fmt.Fprintf(w, "cosift_community_moderations_total{result=\"ok\"} %d\n", s.communityModerations.ok.Load())
	fmt.Fprintf(w, "cosift_community_moderations_total{result=\"ceiling\"} %d\n", s.communityModerations.ceiling.Load())
	fmt.Fprintf(w, "cosift_community_moderations_total{result=\"error\"} %d\n", s.communityModerations.errs.Load())
}
