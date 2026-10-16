package main

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/pilot-protocol/cosift/internal/crawler"
)

const (
	maxTries      = 5
	backoffBase   = time.Second
	backoffCap    = 30 * time.Second
	retryAfterCap = 2 * time.Minute
	leaseRetries  = 2
)

func backoff(n int) time.Duration {
	d := backoffBase
	for i := 0; i < n && d < backoffCap; i++ {
		d *= 2
	}
	return min(d, backoffCap)
}

type action int

const (
	actBackoff action = iota
	actRetryLease
	actStop
	actSkip
)

// decision is what to do about a refusal: wait and retry, retry a lost lease, stop the run, or skip the lease.
type decision struct {
	act    action
	wait   time.Duration
	reason string
	msg    string
	fail   bool
}

const upgradeAdvice = "upgrade cosift (re-run the installer) and try again"

func upgradeStop(server int) decision {
	msg := fmt.Sprintf("this cosift extracts pages as version %d", crawler.ExtractVersion)
	if server > 0 {
		msg += fmt.Sprintf(" and the server expects version %d", server)
	}
	return decision{act: actStop, reason: "upgrade_required", fail: true, msg: msg + ": " + upgradeAdvice}
}

func classifyWorkErr(err error) decision {
	var e *workAPIError
	if !errors.As(err, &e) {
		return decision{act: actBackoff}
	}
	switch {
	case e.Status == http.StatusUnauthorized:
		return decision{act: actStop, reason: "unauthorized", fail: true, msg: "the server refused the bearer token (HTTP 401); worker mode needs the ck_ token from cosift-install in COSIFT_TOKEN or a saved session, and a cookie-only session is refused"}
	case e.Status == http.StatusForbidden:
		switch e.Code {
		case "not_joined":
			return decision{act: actStop, reason: e.Code, fail: true, msg: "this account has not joined the contributor network; join first, then run the worker again"}
		case "banned":
			return decision{act: actStop, reason: e.Code, fail: true, msg: "this account is suspended from contributing"}
		case "terms_outdated":
			return decision{act: actStop, reason: e.Code, fail: true, msg: "accept the current contributor terms, then run the worker again"}
		}
		return decision{act: actStop, reason: "forbidden", fail: true, msg: "the server refused this account (HTTP 403 " + e.Code + ")"}
	case e.Status == http.StatusNotFound:
		return decision{act: actStop, reason: "not_available", fail: true, msg: "this server does not offer worker mode"}
	case e.Status == http.StatusConflict && (e.Code == "lease_expired" || e.Code == "lease_not_owner"):
		return decision{act: actRetryLease}
	case e.Status == http.StatusConflict && e.Code == "extract_version_mismatch":
		return upgradeStop(e.ExtractVersion)
	case e.Status == http.StatusUnprocessableEntity && e.Rule == "mismatch" && (e.Field == "content_hash" || e.Field == "simhash"):
		return upgradeStop(0)
	case e.Status == http.StatusTooManyRequests && e.Code == "daily_cap":
		return decision{act: actStop, reason: "daily_cap", wait: e.RetryAfter}
	case e.Status == http.StatusTooManyRequests:
		return decision{act: actBackoff, wait: min(max(e.RetryAfter, time.Second), retryAfterCap)}
	case e.Status >= 500:
		return decision{act: actBackoff}
	}
	return decision{act: actSkip, reason: e.Code}
}
