package v1

import (
	"bytes"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"
)

var baseHeaders = map[string]string{
	"Content-Type":           "application/json",
	"Cache-Control":          "no-store",
	"X-Content-Type-Options": "nosniff",
}

func wireJSON(t *testing.T, doc string) string {
	t.Helper()
	var b bytes.Buffer
	if err := json.Compact(&b, []byte(doc)); err != nil {
		t.Fatalf("compact %q: %v", doc, err)
	}
	return b.String() + "\n"
}

func checkHeaders(t *testing.T, rec *httptest.ResponseRecorder, extra map[string]string) {
	t.Helper()
	want := map[string]string{}
	for k, v := range baseHeaders {
		want[http.CanonicalHeaderKey(k)] = v
	}
	for k, v := range extra {
		want[http.CanonicalHeaderKey(k)] = v
	}
	got := rec.Header()
	for k, v := range want {
		if vs := got.Values(k); len(vs) != 1 || vs[0] != v {
			t.Errorf("header %s = %q, want [%q]", k, vs, v)
		}
	}
	for k := range got {
		if _, ok := want[k]; !ok {
			t.Errorf("unexpected header %s: %q", k, got.Values(k))
		}
	}
}

func TestWriteError(t *testing.T) {
	n := func(v int) *int { return &v }
	cases := []struct {
		name    string
		err     Error
		doc     string
		headers map[string]string
	}{
		{"unauthenticated", Unauthenticated(),
			`{"error": "Unauthorized", "status": 401, "code": "unauthenticated", "detail": "missing or invalid credential"}`,
			map[string]string{"WWW-Authenticate": `Bearer realm="cosift-v1"`}},
		{"missing_scope", MissingScope(ScopeArticlesWrite),
			`{"error": "Forbidden", "status": 403, "code": "missing_scope", "detail": "requires scope articles:write"}`, nil},
		{"env_mismatch", EnvMismatch(),
			`{"error": "Forbidden", "status": 403, "code": "env_mismatch", "detail": "record belongs to another environment"}`, nil},
		{"gone", Error{Status: 410, Code: "gone", Detail: "article removed"},
			`{"error": "Gone", "status": 410, "code": "gone", "detail": "article removed"}`, nil},

		{"env_golive", EnvGolive(),
			`{"error": "Forbidden", "status": 403, "code": "env_golive", "detail": "staging writes are closed"}`, nil},
		{"rate_limited", RateLimited(1500 * time.Millisecond),
			`{"error": "Too Many Requests", "status": 429, "code": "rate_limited", "detail": "rate limit exceeded"}`,
			map[string]string{"Retry-After": "2"}},
		{"rate_limited without delay", RateLimited(0),
			`{"error": "Too Many Requests", "status": 429, "code": "rate_limited", "detail": "rate limit exceeded"}`,
			map[string]string{"Retry-After": "1"}},
		{"write_budget", WriteBudget(90 * time.Second),
			`{"error": "Too Many Requests", "status": 429, "code": "write_budget", "detail": "write budget exceeded"}`,
			map[string]string{"Retry-After": "90"}},
		{"auth_throttled", AuthThrottled(2*time.Second + time.Nanosecond),
			`{"error": "Too Many Requests", "status": 429, "code": "auth_throttled", "detail": "too many failed attempts"}`,
			map[string]string{"Retry-After": "3"}},
		{"auth_unavailable", AuthUnavailable(),
			`{"error": "Service Unavailable", "status": 503, "code": "auth_unavailable", "detail": "authentication temporarily unavailable"}`,
			map[string]string{"Retry-After": "10"}},
		{"writes_frozen", WritesFrozen(),
			`{"error": "Service Unavailable", "status": 503, "code": "writes_frozen", "detail": "writes are frozen"}`,
			map[string]string{"Retry-After": "60"}},
		{"index_unavailable", IndexUnavailable(),
			`{"error": "Service Unavailable", "status": 503, "code": "index_unavailable", "detail": "index not ready"}`,
			map[string]string{"Retry-After": "5"}},
		{"not_found", NotFound(),
			`{"error": "Not Found", "status": 404, "code": "not_found", "detail": "not found"}`, nil},
		{"internal_error", InternalError(),
			`{"error": "Internal Server Error", "status": 500, "code": "internal_error", "detail": "internal error"}`, nil},

		{"unknown_field", Error{Status: 400, Code: "unknown_field", Detail: "unknown field", Field: "decay"},
			`{"error": "Bad Request", "status": 400, "code": "unknown_field", "detail": "unknown field", "field": "decay"}`, nil},
		{"invalid_field", Error{Status: 422, Code: "invalid_field", Detail: "invalid field", Field: "citations[0].url", Rule: "not_in_corpus"},
			`{"error": "Unprocessable Entity", "status": 422, "code": "invalid_field", "detail": "invalid field", "field": "citations[0].url", "rule": "not_in_corpus"}`, nil},
		{"InvalidField citation url", InvalidField("citations[0].url", "not_in_corpus"),
			`{"error": "Unprocessable Entity", "status": 422, "code": "invalid_field", "detail": "invalid field", "field": "citations[0].url", "rule": "not_in_corpus"}`, nil},
		{"InvalidField title", InvalidField("title", "title_immutable"),
			`{"error": "Unprocessable Entity", "status": 422, "code": "invalid_field", "detail": "invalid field", "field": "title", "rule": "title_immutable"}`, nil},
		{"InvalidField alias", InvalidField("aliases[2]", "first_second_person"),
			`{"error": "Unprocessable Entity", "status": 422, "code": "invalid_field", "detail": "invalid field", "field": "aliases[2]", "rule": "first_second_person"}`, nil},
		{"InvalidField array cap", InvalidField("topic_ids", "too_many"),
			`{"error": "Unprocessable Entity", "status": 422, "code": "invalid_field", "detail": "invalid field", "field": "topic_ids", "rule": "too_many"}`, nil},
		{"version_conflict on a stub", Error{Status: 409, Code: "version_conflict", Detail: "version conflict", CurrentVersion: n(0)},
			`{"error": "Conflict", "status": 409, "code": "version_conflict", "detail": "version conflict", "current_version": 0}`, nil},
		{"version_conflict on an absent record", Error{Status: 409, Code: "version_conflict", Detail: "version conflict"},
			`{"error": "Conflict", "status": 409, "code": "version_conflict", "detail": "version conflict"}`, nil},
		{"moderation_locked", Error{Status: 409, Code: "moderation_locked", Detail: "moderation locked", CurrentStatus: "tombstoned"},
			`{"error": "Conflict", "status": 409, "code": "moderation_locked", "detail": "moderation locked", "current_status": "tombstoned"}`, nil},
		{"topic_claimed", Error{Status: 409, Code: "topic_claimed", Detail: "topic claimed", ArticleID: "01J8ZC2Q7W4X9M3K5N6P8R0T2V", ClaimedStatus: "pending"},
			`{"error": "Conflict", "status": 409, "code": "topic_claimed", "detail": "topic claimed", "article_id": "01J8ZC2Q7W4X9M3K5N6P8R0T2V", "claimed_status": "pending"}`, nil},
		{"blocked in the other environment", Error{Status: 409, Code: "blocked", Detail: "blocked", OtherEnv: true},
			`{"error": "Conflict", "status": 409, "code": "blocked", "detail": "blocked", "other_env": true}`, nil},
		{"staging_writers_present", Error{Status: 409, Code: "staging_writers_present", Detail: "staging writers present", Principals: []string{"synth-staging", "resolver-staging"}},
			`{"error": "Conflict", "status": 409, "code": "staging_writers_present", "detail": "staging writers present", "principals": ["synth-staging", "resolver-staging"]}`, nil},
		{"count_changed to zero", Error{Status: 409, Code: "count_changed", Detail: "count changed", Records: n(0)},
			`{"error": "Conflict", "status": 409, "code": "count_changed", "detail": "count changed", "records": 0}`, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			WriteError(rec, c.err)
			if rec.Code != c.err.Status {
				t.Errorf("status = %d, want %d", rec.Code, c.err.Status)
			}
			if got, want := rec.Body.String(), wireJSON(t, c.doc); got != want {
				t.Errorf("body\n got %s\nwant %s", got, want)
			}
			checkHeaders(t, rec, c.headers)
		})
	}
}

func TestWriteJSON(t *testing.T) {
	rec := httptest.NewRecorder()
	WriteJSON(rec, 200, struct {
		Verdict string   `json:"verdict"`
		Claims  []string `json:"claims"`
		Writes  string   `json:"writes"`
	}{"none", []string{}, "open"})
	if rec.Code != 200 {
		t.Errorf("status = %d", rec.Code)
	}
	if got, want := rec.Body.String(), wireJSON(t, `{"verdict": "none", "claims": [], "writes": "open"}`); got != want {
		t.Errorf("body\n got %s\nwant %s", got, want)
	}
	checkHeaders(t, rec, nil)
}

func TestWriteJSONUnencodable(t *testing.T) {
	rec := httptest.NewRecorder()
	WriteJSON(rec, 200, map[string]float64{"score": math.NaN()})
	if rec.Code != 500 {
		t.Errorf("status = %d, want 500", rec.Code)
	}
	want := wireJSON(t, `{"error": "Internal Server Error", "status": 500, "code": "internal_error", "detail": "internal error"}`)
	if got := rec.Body.String(); got != want {
		t.Errorf("body\n got %s\nwant %s", got, want)
	}
	checkHeaders(t, rec, nil)
}

func TestErrorBodyNeverCarriesRetryAfter(t *testing.T) {
	b, err := json.Marshal(errorBody{"Service Unavailable", WritesFrozen()})
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	if want := []string{"code", "detail", "error", "status"}; !slices.Equal(keys, want) {
		t.Errorf("keys = %v, want %v", keys, want)
	}
}
