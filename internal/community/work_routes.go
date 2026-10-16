package community

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"math"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	claimRoute       = "/v1/work/claim"
	workRouteTimeout = 15 * time.Second
	workReplyBytes   = 256 << 10

	defaultFetchDailyCap = 500
)

var workTokenPattern = regexp.MustCompile(`^[a-z_]{1,32}$`)

var workMessages = map[string]string{
	"lease_expired":            "the lease has expired",
	"lease_not_owner":          "the lease belongs to another worker",
	"extract_version_mismatch": "the extract version is out of date",
	"invalid_field":            "invalid field",
	"invalid_body":             "invalid request body",
	"unknown_field":            "unknown field",
	"body_too_large":           "request body too large",
}

// workRoute serves a work route to a bearer-authenticated account, and answers 404 while work is off.
func (s *Server) workRoute(next userHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.cfg.Work || s.cfg.Shared == nil {
			problemCode(w, http.StatusNotFound, "work is not enabled", "not_found", nil)
			return
		}
		if strings.TrimSpace(r.Header.Get("Authorization")) == "" {
			problemCode(w, http.StatusUnauthorized, "a bearer token is required", "bearer_required", nil)
			return
		}
		s.sharedAuth(w, r, next, false)
	}
}

func (s *Server) fetchCap() int {
	if s.cfg.FetchDailyCap > 0 {
		return s.cfg.FetchDailyCap
	}
	return defaultFetchDailyCap
}

func utcDayStart(t time.Time) time.Time {
	y, m, d := t.UTC().Date()
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

func (s *Server) fetchCreditedToday(ctx context.Context, userID string) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM credit_ledger WHERE user_id=? AND reason='fetch_verified' AND created_at>=?`, userID, utcDayStart(s.now()).Unix()).Scan(&n)
	return n, err
}

type workLease struct {
	LeaseID        string `json:"lease_id"`
	URL            string `json:"url"`
	Source         string `json:"source"`
	ExpiresAt      string `json:"expires_at"`
	ExtractVersion int    `json:"extract_version"`
}

type claimAnswer struct {
	Leases     []workLease `json:"leases"`
	RetryAfter *int        `json:"retry_after_s,omitempty"`
	Ceiling    bool        `json:"moderation_ceiling,omitempty"`
}

func (s *Server) claimWork(w http.ResponseWriter, r *http.Request, u User) {
	if !s.allowRoute(w, "work_claim", u.ID, workClaimPerMinute) {
		return
	}
	var in struct {
		Max *int `json:"max"`
	}
	if decode(r, &in) != nil {
		problemCode(w, http.StatusBadRequest, "invalid request body", "invalid_body", nil)
		return
	}
	max := 1
	if in.Max != nil {
		max = *in.Max
	}
	if max < 1 || max > 32 {
		problemCode(w, http.StatusUnprocessableEntity, "invalid field", "invalid_field", map[string]any{"field": "max", "rule": "range"})
		return
	}
	ctx := r.Context()
	member, refusal := s.joinGate(ctx, u.ID)
	if refusal != nil {
		problemCode(w, refusal.status, refusalMessages[refusal.code], refusal.code, nil)
		return
	}
	if !s.underFetchCap(w, ctx, u.ID) {
		return
	}
	worker, net, ok := s.mintIdentity(w, r, u.ID)
	if !ok || !s.registerWorker(w, ctx, u.ID, worker, member) {
		return
	}
	body, _ := json.Marshal(map[string]any{"worker": worker, "net": net, "max": max})
	res, err := s.engine.post(ctx, claimRoute, body, workRouteTimeout)
	if err != nil {
		log.Printf("community: POST %s failed", claimRoute)
		problemCode(w, http.StatusServiceUnavailable, workUnavailable, "engine_unavailable", nil)
		return
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		s.relayWorkError(w, claimRoute, res)
		return
	}
	var out claimAnswer
	if json.NewDecoder(io.LimitReader(res.Body, workReplyBytes)).Decode(&out) != nil {
		log.Printf("community: POST %s returned an unreadable body", claimRoute)
		problemCode(w, http.StatusServiceUnavailable, workUnavailable, "engine_unavailable", nil)
		return
	}
	if out.Leases == nil {
		out.Leases = []workLease{}
	}
	s.bumpFetch(member.cid, fetchClaimed, int64(len(out.Leases)))
	if out.Ceiling {
		s.noteCeiling(ceilingWork)
	}
	respond(w, http.StatusOK, out)
}

const workUnavailable = "the work service is unavailable"

var refusalMessages = map[string]string{
	"not_joined":               "join the contributor network first",
	"banned":                   "account suspended from contributing",
	"terms_outdated":           "accept the current contributor terms first",
	"contributors_unavailable": "contributor status is unavailable; please retry",
}

// underFetchCap answers 429 daily_cap, with the wait to 00:00 UTC, once the account has been credited its day's pages.
func (s *Server) underFetchCap(w http.ResponseWriter, ctx context.Context, userID string) bool {
	n, err := s.fetchCreditedToday(ctx, userID)
	if err != nil {
		problemCode(w, http.StatusInternalServerError, "work unavailable", "work_unavailable", nil)
		return false
	}
	if n < s.fetchCap() {
		return true
	}
	now := s.now()
	seconds := max(1, int(math.Ceil(utcDayStart(now).Add(24*time.Hour).Sub(now).Seconds())))
	w.Header().Set("Retry-After", strconv.Itoa(seconds))
	problemCode(w, http.StatusTooManyRequests, "daily fetch credit cap reached", "daily_cap", map[string]any{"retry_after_seconds": seconds})
	return false
}

// mintIdentity derives the caller's worker and net ids; it refuses a request whose client address does not parse.
func (s *Server) mintIdentity(w http.ResponseWriter, r *http.Request, userID string) (worker, net string, ok bool) {
	net, valid := s.netID(s.clientIP(r))
	if !valid {
		problemCode(w, http.StatusBadRequest, "the client address could not be read", "invalid_client_address", nil)
		return "", "", false
	}
	return s.workerID(userID), net, true
}

// registerWorker records which account a worker id is, so a settled outcome can be credited.
func (s *Server) registerWorker(w http.ResponseWriter, ctx context.Context, userID, worker string, member workMember) bool {
	if _, err := s.db.ExecContext(ctx, `INSERT INTO work_workers(worker,user_id,cid,first_seen) VALUES(?,?,?,?) ON CONFLICT(worker) DO UPDATE SET cid=excluded.cid`, worker, userID, member.cid, s.now().Unix()); err != nil {
		problemCode(w, http.StatusInternalServerError, "work unavailable", "work_unavailable", nil)
		return false
	}
	return true
}

// relayWorkError passes through the engine's lease and field refusals, and turns anything else into one fixed answer.
func (s *Server) relayWorkError(w http.ResponseWriter, route string, res *http.Response) {
	var e struct {
		Code           string `json:"code"`
		Field          string `json:"field"`
		Rule           string `json:"rule"`
		ExtractVersion *int   `json:"extract_version"`
	}
	data, _ := io.ReadAll(io.LimitReader(res.Body, 4096))
	_ = json.Unmarshal(data, &e)
	switch {
	case res.StatusCode == http.StatusConflict && (e.Code == "lease_expired" || e.Code == "lease_not_owner" || e.Code == "extract_version_mismatch"),
		res.StatusCode == http.StatusUnprocessableEntity && e.Code == "invalid_field":
		extra := map[string]any{}
		if workTokenPattern.MatchString(e.Field) && workTokenPattern.MatchString(e.Rule) {
			extra["field"], extra["rule"] = e.Field, e.Rule
		}
		if e.ExtractVersion != nil && e.Code == "extract_version_mismatch" {
			extra["extract_version"] = *e.ExtractVersion
		}
		problemCode(w, res.StatusCode, workMessages[e.Code], e.Code, extra)
	case res.StatusCode == http.StatusTooManyRequests:
		seconds, _ := strconv.Atoi(res.Header.Get("Retry-After"))
		seconds = min(max(seconds, 1), 3600)
		w.Header().Set("Retry-After", strconv.Itoa(seconds))
		problemCode(w, http.StatusTooManyRequests, "rate limit reached", "rate_limited", map[string]any{"retry_after_seconds": seconds})
	case res.StatusCode == http.StatusServiceUnavailable && e.Code == "work_disabled":
		problemCode(w, http.StatusServiceUnavailable, "work is disabled", "work_disabled", nil)
	default:
		log.Printf("community: POST %s returned HTTP %d", route, res.StatusCode)
		problemCode(w, http.StatusServiceUnavailable, workUnavailable, "engine_unavailable", nil)
	}
}
