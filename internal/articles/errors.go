package articles

import (
	"net/http"
	"time"

	v1 "github.com/pilot-protocol/cosift/internal/v1"
)

func errInvalidParam(field string) v1.Error {
	return v1.Error{Status: http.StatusBadRequest, Code: "invalid_param", Detail: "invalid query parameter", Field: field}
}

func errInvalidCursor() v1.Error {
	return v1.Error{Status: http.StatusBadRequest, Code: "invalid_cursor", Detail: "invalid cursor"}
}

func errModerationLocked(status string) v1.Error {
	return v1.Error{Status: http.StatusConflict, Code: "moderation_locked", Detail: "record is locked by moderation", CurrentStatus: status}
}

// conflictWith names the conflicting record as the caller's environment allows.
func conflictWith(e v1.Error, p v1.Principal, r *Record) v1.Error {
	if p.SameEnv(r.Prelive) {
		e.ArticleID, e.ClaimedStatus = r.ID, r.Status
	} else {
		e.OtherEnv = true
	}
	return e
}

func errTopicClaimed(p v1.Principal, claimant *Record) v1.Error {
	return conflictWith(v1.Error{Status: http.StatusConflict, Code: "topic_claimed", Detail: "a topic is claimed by another article"}, p, claimant)
}

func errBlocked(p v1.Principal, blocker *Record) v1.Error {
	return conflictWith(v1.Error{Status: http.StatusConflict, Code: "blocked", Detail: "title is blocked by a moderated record"}, p, blocker)
}

func errVersionConflict(current *int) v1.Error {
	return v1.Error{Status: http.StatusConflict, Code: "version_conflict", Detail: "version conflict", CurrentVersion: current}
}

func errExists(status string) v1.Error {
	return v1.Error{Status: http.StatusConflict, Code: "exists", Detail: "record exists", CurrentStatus: status}
}

func errNotPending(status string) v1.Error {
	return v1.Error{Status: http.StatusConflict, Code: "not_pending", Detail: "record is not pending", CurrentStatus: status}
}

func errIllegalTransition(status string) v1.Error {
	return v1.Error{Status: http.StatusConflict, Code: "illegal_transition", Detail: "action not allowed from the current status", CurrentStatus: status}
}

func errStagingWriters(ids []string) v1.Error {
	return v1.Error{Status: http.StatusConflict, Code: "staging_writers_present", Detail: "staging principals still hold write scopes", Principals: ids}
}

func errCountChanged(records int) v1.Error {
	return v1.Error{Status: http.StatusConflict, Code: "count_changed", Detail: "prelive record count changed", Records: &records}
}

func errGone() v1.Error {
	return v1.Error{Status: http.StatusGone, Code: "gone", Detail: "article removed"}
}

func errGolive() v1.Error {
	return v1.Error{Status: http.StatusGone, Code: "golive", Detail: "go-live already applied"}
}

func errEmbedderUnavailable() v1.Error {
	return v1.Error{Status: http.StatusServiceUnavailable, Code: "embedder_unavailable", Detail: "embedder unavailable", RetryAfter: 5 * time.Second}
}

func errRestorePending() v1.Error {
	return v1.Error{Status: http.StatusServiceUnavailable, Code: "restore_pending", Detail: "store predates the recorded go-live", RetryAfter: 60 * time.Second}
}
