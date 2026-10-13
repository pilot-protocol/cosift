package config

import "fmt"

// MaxCommunityModerationsPerDay is the ceiling's own ceiling: a typo can tighten it, never loosen it.
const MaxCommunityModerationsPerDay = 1000

// Work holds the community and fetch-work settings.
type Work struct {
	// CommunityModerationsPerDay caps the model calls of POST /v1/community/moderate per UTC day.
	// Unset is MaxCommunityModerationsPerDay and 0 halts the route.
	CommunityModerationsPerDay *int `json:"community_moderations_per_day,omitempty"`
}

// CommunityModerationLimit is the daily ceiling, or an error naming the key.
func (w Work) CommunityModerationLimit() (int, error) {
	if w.CommunityModerationsPerDay == nil {
		return MaxCommunityModerationsPerDay, nil
	}
	n := *w.CommunityModerationsPerDay
	if n < 0 || n > MaxCommunityModerationsPerDay {
		return 0, fmt.Errorf("work.community_moderations_per_day: must be 0-%d", MaxCommunityModerationsPerDay)
	}
	return n, nil
}
