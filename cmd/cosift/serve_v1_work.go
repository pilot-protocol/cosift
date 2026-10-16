package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/crawler"
	v1 "github.com/pilot-protocol/cosift/internal/v1"
	"github.com/pilot-protocol/cosift/internal/work"
)

const (
	v1WorkClaimBodyLimit  = 1 << 10
	v1WorkSubmitBodyLimit = 2 << 20
	v1WorkPushBodyLimit   = 64 << 10
	maxSettledLimit       = 200
)

// workHooks is the server's fetch-work state; svc exists even with work switched off, for the operator commands.
type workHooks struct {
	settings config.WorkSettings
	enabled  bool
	frontier bool
	svc      *work.Service
	crawler  atomic.Pointer[crawler.Crawler]
}

// loadWorkConfig reads the work block and flags; a bad value refuses to start the server.
func loadWorkConfig(cfg *config.Config) (*workHooks, error) {
	settings, err := cfg.Work.Settings()
	if err != nil {
		return nil, err
	}
	enabled, err := config.WorkEnabled()
	if err != nil {
		return nil, err
	}
	frontier, err := config.WorkFrontier()
	if err != nil {
		return nil, err
	}
	return &workHooks{settings: settings, enabled: enabled, frontier: frontier}, nil
}

// attach hands the work service the crawler and, when work is on, starts its feeders and sweeper.
func (h *workHooks) attach(ctx context.Context, c *crawler.Crawler, wg *sync.WaitGroup) {
	if h == nil {
		return
	}
	h.crawler.Store(c)
	if h.enabled && h.svc != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h.svc.Run(ctx)
		}()
	}
}

// v1WorkRoutes are the broker's four work calls.
func (s *pebbleHTTP) v1WorkRoutes() []v1.Route {
	broker := []v1.Scope{v1.ScopeWorkBroker}
	return []v1.Route{
		{Method: http.MethodPost, Path: "/v1/work/claim", Scopes: broker, BodyLimit: v1WorkClaimBodyLimit, Ungated: true, Handler: http.HandlerFunc(s.handleWorkClaim)},
		{Method: http.MethodPost, Path: "/v1/work/submit", Scopes: broker, BodyLimit: v1WorkSubmitBodyLimit, Ungated: true, Handler: http.HandlerFunc(s.handleWorkSubmit)},
		{Method: http.MethodPost, Path: "/v1/work/push", Scopes: broker, BodyLimit: v1WorkPushBodyLimit, Ungated: true, Handler: http.HandlerFunc(s.handleWorkPush)},
		{Method: http.MethodGet, Path: "/v1/work/settled", Scopes: broker, Ungated: true, Handler: http.HandlerFunc(s.handleWorkSettled)},
	}
}

// workErrorBody is the /v1 error envelope plus the engine's extraction version on a version mismatch.
type workErrorBody struct {
	Text string `json:"error"`
	v1.Error
	ExtractVersion int `json:"extract_version,omitempty"`
}

var workDetails = map[string]string{
	"invalid_field":            "invalid field",
	"lease_expired":            "lease expired",
	"lease_not_owner":          "lease belongs to another worker",
	"extract_version_mismatch": "extraction version differs",
	"index_unavailable":        "index not ready",
	"invalid_url":              "invalid url",
}

func writeWorkError(w http.ResponseWriter, err error) {
	var we *work.Error
	if !errors.As(err, &we) {
		v1.WriteError(w, v1.InternalError())
		return
	}
	detail := workDetails[we.Code]
	if detail == "" {
		detail = we.Code
	}
	e := v1.Error{Status: we.Status, Code: we.Code, Detail: detail, Field: we.Field, Rule: we.Rule, RetryAfter: we.RetryAfter}
	if e.Status == http.StatusServiceUnavailable && e.RetryAfter == 0 {
		e.RetryAfter = 5 * time.Second
	}
	if e.RetryAfter > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(int((e.RetryAfter+time.Second-1)/time.Second)))
	}
	v1.WriteJSON(w, e.Status, workErrorBody{Text: http.StatusText(e.Status), Error: e, ExtractVersion: we.ExtractVersion})
}

// workService answers the 503 of a switched-off or not yet ready service and returns nil, or the service.
func (s *pebbleHTTP) workService(w http.ResponseWriter) *work.Service {
	switch {
	case s.work == nil || !s.work.enabled:
		v1.WriteError(w, v1Problem(http.StatusServiceUnavailable, "work_disabled", "work is switched off"))
	case s.work.svc == nil || s.work.crawler.Load() == nil || s.store == nil:
		v1.WriteError(w, v1.IndexUnavailable())
	default:
		return s.work.svc
	}
	return nil
}

func (s *pebbleHTTP) handleWorkClaim(w http.ResponseWriter, r *http.Request) {
	svc := s.workService(w)
	if svc == nil {
		return
	}
	var req struct {
		Worker string `json:"worker"`
		Net    string `json:"net"`
		Max    *int   `json:"max"`
	}
	if !v1.Decode(w, r, &req, v1WorkClaimBodyLimit) {
		return
	}
	max := 1
	if req.Max != nil {
		max = *req.Max
	}
	res, err := svc.Claim(req.Worker, req.Net, max)
	if err != nil {
		writeWorkError(w, err)
		return
	}
	type leaseOut struct {
		ID             string    `json:"lease_id"`
		URL            string    `json:"url"`
		Source         string    `json:"source"`
		ExpiresAt      time.Time `json:"expires_at"`
		ExtractVersion int       `json:"extract_version"`
	}
	out := struct {
		Leases            []leaseOut `json:"leases"`
		RetryAfterS       int        `json:"retry_after_s,omitempty"`
		ModerationCeiling bool       `json:"moderation_ceiling,omitempty"`
	}{Leases: make([]leaseOut, 0, len(res.Leases)), RetryAfterS: res.RetryAfterS, ModerationCeiling: res.ModerationCeiling}
	for _, l := range res.Leases {
		out.Leases = append(out.Leases, leaseOut{l.ID, l.URL, string(l.Source), l.ExpiresAt, l.ExtractVersion})
	}
	v1.WriteJSON(w, http.StatusOK, out)
}

// submitBody is the submit request; a pointer member is one the body may leave out.
type submitBody struct {
	LeaseID        string         `json:"lease_id"`
	Worker         string         `json:"worker"`
	Net            string         `json:"net"`
	FinalURL       *string        `json:"final_url"`
	HTTPStatus     *int           `json:"http_status"`
	Title          *string        `json:"title"`
	Text           *string        `json:"text"`
	Lang           *string        `json:"lang"`
	ContentHash    *string        `json:"content_hash"`
	Simhash        *string        `json:"simhash"`
	ExtractVersion *int           `json:"extract_version"`
	Artifact       *work.Artifact `json:"artifact"`
}

func (b submitBody) request() work.SubmitRequest {
	return work.SubmitRequest{LeaseID: b.LeaseID, Worker: b.Worker, Net: b.Net, HTTPStatus: b.HTTPStatus, FinalURL: b.FinalURL, Title: b.Title,
		Text: b.Text, Lang: b.Lang, ContentHash: b.ContentHash, Simhash: b.Simhash, ExtractVersion: b.ExtractVersion, Artifact: b.Artifact}
}

func (s *pebbleHTTP) handleWorkSubmit(w http.ResponseWriter, r *http.Request) {
	svc := s.workService(w)
	if svc == nil {
		return
	}
	var body submitBody
	if !v1.Decode(w, r, &body, v1WorkSubmitBodyLimit) {
		return
	}
	res, err := svc.Submit(body.request())
	if err != nil {
		writeWorkError(w, err)
		return
	}
	v1.WriteJSON(w, http.StatusOK, struct {
		Verdict         string     `json:"verdict"`
		Agreeing        []string   `json:"agreeing"`
		Indexed         bool       `json:"indexed"`
		WindowExpiresAt *time.Time `json:"window_expires_at,omitempty"`
		Reason          string     `json:"reason,omitempty"`
	}{res.Verdict, res.Agreeing, res.Indexed, res.WindowExpiresAt, res.Reason})
}

func (s *pebbleHTTP) handleWorkPush(w http.ResponseWriter, r *http.Request) {
	svc := s.workService(w)
	if svc == nil {
		return
	}
	var req struct {
		URLs *[]string `json:"urls"`
	}
	if !v1.Decode(w, r, &req, v1WorkPushBodyLimit) {
		return
	}
	var urls []string
	if req.URLs != nil {
		urls = *req.URLs
	}
	res, err := svc.Push(r.Context(), urls)
	if err != nil {
		writeWorkError(w, err)
		return
	}
	type rejected struct {
		Index int    `json:"index"`
		Rule  string `json:"rule"`
	}
	out := struct {
		Accepted   int        `json:"accepted"`
		Duplicates int        `json:"duplicates"`
		Rejected   []rejected `json:"rejected"`
	}{res.Accepted, res.Duplicates, make([]rejected, 0, len(res.Rejected))}
	for _, rj := range res.Rejected {
		out.Rejected = append(out.Rejected, rejected{rj.Index, rj.Rule})
	}
	v1.WriteJSON(w, http.StatusOK, out)
}

func (s *pebbleHTTP) handleWorkSettled(w http.ResponseWriter, r *http.Request) {
	svc := s.workService(w)
	if svc == nil {
		return
	}
	after, limit := uint64(0), 100
	q := r.URL.Query()
	if v := q.Get("after"); v != "" {
		n, err := strconv.ParseUint(v, 10, 63)
		if err != nil {
			v1.WriteError(w, v1.InvalidField("after", "range"))
			return
		}
		after = n
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > maxSettledLimit {
			v1.WriteError(w, v1.InvalidField("limit", "range"))
			return
		}
		limit = n
	}
	res, err := svc.Settled(after, limit)
	if err != nil {
		writeWorkError(w, err)
		return
	}
	v1.WriteJSON(w, http.StatusOK, struct {
		Events            []work.Event `json:"events"`
		Next              uint64       `json:"next"`
		LastSeq           uint64       `json:"last_seq"`
		ModerationCeiling bool         `json:"moderation_ceiling,omitempty"`
	}{res.Events, res.Next, res.LastSeq, res.ModerationCeiling})
}

// addWorkStats puts the work block in the /stats body when work is on.
func (s *pebbleHTTP) addWorkStats(out map[string]any) {
	if s.work != nil && s.work.enabled && s.work.svc != nil {
		out["work"] = s.work.svc.Stats()
	}
}

func (s *pebbleHTTP) writeWorkMetrics(w io.Writer) {
	if s.work != nil && s.work.enabled && s.work.svc != nil {
		s.work.svc.WriteMetrics(w)
	}
}
