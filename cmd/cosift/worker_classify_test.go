package main

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestBackoffDoublesToACap(t *testing.T) {
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second, 30 * time.Second}
	for i, w := range want {
		if got := backoff(i); got != w {
			t.Errorf("backoff(%d) = %v, want %v", i, got, w)
		}
	}
	if backoff(1000) != 30*time.Second {
		t.Error("the cap does not hold")
	}
}

// blockingOrigin holds every request until release is closed; gate receives one value per request that arrived.

func TestClassifyWorkErrTable(t *testing.T) {
	api := func(status int, code, field, rule string, retry time.Duration) error {
		return fmt.Errorf("wrapped: %w", &workAPIError{Status: status, Code: code, Field: field, Rule: rule, RetryAfter: retry})
	}
	for name, tc := range map[string]struct {
		err    error
		act    action
		reason string
		fail   bool
		wait   time.Duration
	}{
		"transport":          {errors.New("connection refused"), actBackoff, "", false, 0},
		"503":                {api(503, "engine_unavailable", "", "", 0), actBackoff, "", false, 0},
		"502":                {api(502, "", "", "", 0), actBackoff, "", false, 0},
		"rate limited":       {api(429, "rate_limited", "", "", 9*time.Second), actBackoff, "", false, 9 * time.Second},
		"rate limit floor":   {api(429, "rate_limited", "", "", 0), actBackoff, "", false, time.Second},
		"rate limit cap":     {api(429, "rate_limited", "", "", time.Hour), actBackoff, "", false, 2 * time.Minute},
		"daily cap":          {api(429, "daily_cap", "", "", time.Hour), actStop, "daily_cap", false, time.Hour},
		"lease expired":      {api(409, "lease_expired", "", "", 0), actRetryLease, "", false, 0},
		"lease not owner":    {api(409, "lease_not_owner", "", "", 0), actRetryLease, "", false, 0},
		"version":            {api(409, "extract_version_mismatch", "", "", 0), actStop, "upgrade_required", true, 0},
		"hash mismatch":      {api(422, "invalid_field", "content_hash", "mismatch", 0), actStop, "upgrade_required", true, 0},
		"simhash mismatch":   {api(422, "invalid_field", "simhash", "mismatch", 0), actStop, "upgrade_required", true, 0},
		"off host":           {api(422, "invalid_field", "final_url", "off_host", 0), actSkip, "invalid_field", false, 0},
		"mismatch elsewhere": {api(422, "invalid_field", "title", "mismatch", 0), actSkip, "invalid_field", false, 0},
		"not joined":         {api(403, "not_joined", "", "", 0), actStop, "not_joined", true, 0},
		"banned":             {api(403, "banned", "", "", 0), actStop, "banned", true, 0},
		"terms":              {api(403, "terms_outdated", "", "", 0), actStop, "terms_outdated", true, 0},
		"other 403":          {api(403, "", "", "", 0), actStop, "forbidden", true, 0},
		"unauthorized":       {api(401, "bearer_required", "", "", 0), actStop, "unauthorized", true, 0},
		"not found":          {api(404, "not_found", "", "", 0), actStop, "not_available", true, 0},
		"bad body":           {api(400, "invalid_body", "", "", 0), actSkip, "invalid_body", false, 0},
		"too large":          {api(413, "body_too_large", "", "", 0), actSkip, "body_too_large", false, 0},
	} {
		d := classifyWorkErr(tc.err)
		if d.act != tc.act || d.reason != tc.reason || d.fail != tc.fail || d.wait != tc.wait {
			t.Errorf("%s: got %+v", name, d)
		}
	}
}
