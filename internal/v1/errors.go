package v1

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"
)

// Error is a /v1 error body. Optional fields are omitted when zero.
type Error struct {
	Status         int      `json:"status"`
	Code           string   `json:"code"`
	Detail         string   `json:"detail"`
	Field          string   `json:"field,omitempty"`
	Rule           string   `json:"rule,omitempty"`
	CurrentStatus  string   `json:"current_status,omitempty"`
	CurrentVersion *int     `json:"current_version,omitempty"`
	ArticleID      string   `json:"article_id,omitempty"`
	ClaimedStatus  string   `json:"claimed_status,omitempty"`
	OtherEnv       bool     `json:"other_env,omitempty"`
	Principals     []string `json:"principals,omitempty"`
	Records        *int     `json:"records,omitempty"`
	// RetryAfter, when positive, is sent as Retry-After in whole seconds, rounded up.
	RetryAfter time.Duration `json:"-"`
}

type errorBody struct {
	Text string `json:"error"`
	Error
}

// WriteJSON writes v with the headers every /v1 response carries.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		status = http.StatusInternalServerError
		b, _ = json.Marshal(errorBody{http.StatusText(status), InternalError()})
	}
	h := w.Header()
	h.Set("Content-Type", "application/json")
	h.Set("Cache-Control", "no-store")
	h.Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_, _ = w.Write(append(b, '\n'))
}

func WriteError(w http.ResponseWriter, e Error) {
	if e.Status == http.StatusUnauthorized {
		w.Header().Set("WWW-Authenticate", `Bearer realm="cosift-v1"`)
	}
	if e.RetryAfter > 0 {
		secs := e.RetryAfter / time.Second
		if e.RetryAfter%time.Second != 0 {
			secs++
		}
		w.Header().Set("Retry-After", strconv.FormatInt(int64(secs), 10))
	}
	WriteJSON(w, e.Status, errorBody{http.StatusText(e.Status), e})
}

// The errors below have a fixed status, detail and headers on every route.

func Unauthenticated() Error {
	return Error{Status: http.StatusUnauthorized, Code: "unauthenticated", Detail: "missing or invalid credential"}
}

func MissingScope(s Scope) Error {
	return Error{Status: http.StatusForbidden, Code: "missing_scope", Detail: "requires scope " + string(s)}
}

func EnvMismatch() Error {
	return Error{Status: http.StatusForbidden, Code: "env_mismatch", Detail: "record belongs to another environment"}
}

func EnvGolive() Error {
	return Error{Status: http.StatusForbidden, Code: "env_golive", Detail: "staging writes are closed"}
}

func RateLimited(retry time.Duration) Error {
	return Error{Status: http.StatusTooManyRequests, Code: "rate_limited", Detail: "rate limit exceeded", RetryAfter: max(retry, time.Second)}
}

func WriteBudget(retry time.Duration) Error {
	return Error{Status: http.StatusTooManyRequests, Code: "write_budget", Detail: "write budget exceeded", RetryAfter: max(retry, time.Second)}
}

func AuthThrottled(retry time.Duration) Error {
	return Error{Status: http.StatusTooManyRequests, Code: "auth_throttled", Detail: "too many failed attempts", RetryAfter: max(retry, time.Second)}
}

func AuthUnavailable() Error {
	return Error{Status: http.StatusServiceUnavailable, Code: "auth_unavailable", Detail: "authentication temporarily unavailable", RetryAfter: 10 * time.Second}
}

func WritesFrozen() Error {
	return Error{Status: http.StatusServiceUnavailable, Code: "writes_frozen", Detail: "writes are frozen", RetryAfter: 60 * time.Second}
}

func IndexUnavailable() Error {
	return Error{Status: http.StatusServiceUnavailable, Code: "index_unavailable", Detail: "index not ready", RetryAfter: 5 * time.Second}
}

func UnsupportedMediaType() Error {
	return Error{Status: http.StatusUnsupportedMediaType, Code: "unsupported_media_type", Detail: "content type must be application/json"}
}

func BodyTooLarge() Error {
	return Error{Status: http.StatusRequestEntityTooLarge, Code: "body_too_large", Detail: "request body too large"}
}

func UnknownField(name string) Error {
	return Error{Status: http.StatusBadRequest, Code: "unknown_field", Detail: "unknown field", Field: name}
}

func InvalidField(field, rule string) Error {
	return Error{Status: http.StatusUnprocessableEntity, Code: "invalid_field", Detail: "invalid field", Field: field, Rule: rule}
}

func InvalidBody() Error {
	return Error{Status: http.StatusBadRequest, Code: "invalid_body", Detail: "invalid JSON body"}
}

func NotFound() Error {
	return Error{Status: http.StatusNotFound, Code: "not_found", Detail: "not found"}
}

func InternalError() Error {
	return Error{Status: http.StatusInternalServerError, Code: "internal_error", Detail: "internal error"}
}
