package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pilot-protocol/cosift/internal/community"
	"github.com/pilot-protocol/cosift/internal/crawler"
	v1 "github.com/pilot-protocol/cosift/internal/v1"
)

const (
	v1EnqueueBodyLimit  = 1 << 20
	v1ModerateBodyLimit = 100 << 10
	// maxReceiptFinalURL keeps the receipt under the app's 4096-byte read cap even if JSON escaping expands every byte sixfold.
	maxReceiptFinalURL = 512
)

// communityCaps are the per-principal calls a minute each community route accepts.
type communityCaps struct{ enqueue, moderate int }

const communityCapMax = 30

// communityCapsFromEnv reads COSIFT_V1_ENQUEUE_PER_MIN and COSIFT_V1_MODERATE_PER_MIN; a value outside 1-30 is an error.
func communityCapsFromEnv() (communityCaps, error) {
	c := communityCaps{enqueue: communityCapMax, moderate: communityCapMax}
	for _, f := range []struct {
		name string
		dst  *int
	}{{"COSIFT_V1_ENQUEUE_PER_MIN", &c.enqueue}, {"COSIFT_V1_MODERATE_PER_MIN", &c.moderate}} {
		v := os.Getenv(f.name)
		if v == "" {
			continue
		}
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > communityCapMax {
			return c, fmt.Errorf("%s: must be an integer from 1 to %d", f.name, communityCapMax)
		}
		*f.dst = n
	}
	return c, nil
}

// v1CommunityRoutes are the community app's engine calls: intake, and the work broker's.
func (s *pebbleHTTP) v1CommunityRoutes() []v1.Route {
	intake := []v1.Scope{v1.ScopeCommunityIntake}
	caps := s.communityCaps
	if caps == (communityCaps{}) {
		caps = communityCaps{communityCapMax, communityCapMax}
	}
	return append([]v1.Route{
		{Method: http.MethodPost, Path: "/v1/community/enqueue", Scopes: intake, PerMinute: caps.enqueue, BodyLimit: v1EnqueueBodyLimit, Ungated: true, Handler: http.HandlerFunc(s.handleCommunityEnqueue)},
		{Method: http.MethodPost, Path: "/v1/community/moderate", Scopes: intake, PerMinute: caps.moderate, BodyLimit: v1ModerateBodyLimit, Ungated: true, Handler: http.HandlerFunc(s.handleCommunityModerate)},
	}, s.v1WorkRoutes()...)
}

func v1Problem(status int, code, detail string) v1.Error {
	return v1.Error{Status: status, Code: code, Detail: detail}
}

// handleCommunityEnqueue indexes a page the app already moderated; the body is decoded leniently on purpose,
// because an unknown artifact member must not become a 400 the app retries.
func (s *pebbleHTTP) handleCommunityEnqueue(w http.ResponseWriter, r *http.Request) {
	if !s.crawlCommunityReady.Load() || s.crawlCommunityFetch == nil {
		v1.WriteError(w, v1Problem(http.StatusServiceUnavailable, "community_unavailable", "community intake unavailable"))
		return
	}
	var req struct {
		SubmissionID        string                 `json:"submission_id,omitempty"`
		URL                 string                 `json:"url"`
		Artifact            *crawler.LocalArtifact `json:"artifact,omitempty"`
		ApprovedContentHash string                 `json:"approved_content_hash"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			v1.WriteError(w, v1.BodyTooLarge())
			return
		}
		v1.WriteError(w, v1.InvalidBody())
		return
	}
	if !crawler.ValidApprovedContentHash(req.ApprovedContentHash) {
		rule := "pattern"
		if req.ApprovedContentHash == "" {
			rule = "required"
		}
		v1.WriteError(w, v1.InvalidField("approved_content_hash", rule))
		return
	}
	if req.SubmissionID != "" {
		if len(req.SubmissionID) < 16 || len(req.SubmissionID) > 64 || strings.IndexFunc(req.SubmissionID, func(r rune) bool {
			return !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-')
		}) >= 0 {
			v1.WriteError(w, v1Problem(http.StatusBadRequest, "invalid_submission_id", "invalid submission id"))
			return
		}
	}
	u, err := community.NormalizeURL(req.URL)
	if err != nil {
		v1.WriteError(w, v1Problem(http.StatusBadRequest, "invalid_url", "invalid url"))
		return
	}
	liftWriteDeadline(w)
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Minute)
	defer cancel()
	receipt, err := s.fetchCommunityReceipt(ctx, req.SubmissionID, u, req.Artifact, req.ApprovedContentHash)
	if err != nil {
		switch {
		case errors.Is(err, errCommunityReceiptConflict):
			v1.WriteError(w, v1Problem(http.StatusConflict, "submission_conflict", "submission id belongs to another payload"))
		case errors.Is(err, crawler.ErrContributionRejected):
			v1.WriteError(w, v1Problem(http.StatusUnprocessableEntity, "contribution_rejected", "webpage or local artifact does not meet index validation policy"))
		default:
			v1.WriteError(w, v1Problem(http.StatusBadGateway, "fetch_failed", "webpage could not be indexed"))
		}
		return
	}
	if len(receipt.FinalURL) > maxReceiptFinalURL {
		receipt.FinalURL = ""
	}
	v1.WriteJSON(w, http.StatusOK, receipt)
}

// handleCommunityModerate answers with exactly {decision, category}: the app decodes it with DisallowUnknownFields.
func (s *pebbleHTTP) handleCommunityModerate(w http.ResponseWriter, r *http.Request) {
	var doc community.ModerationDocument
	if !v1.Decode(w, r, &doc, v1ModerateBodyLimit) {
		return
	}
	if field, ok := moderationDocumentBound(doc); !ok {
		e := v1Problem(http.StatusBadRequest, "invalid_document", "invalid document")
		e.Field, e.Rule = field, "length"
		v1.WriteError(w, e)
		return
	}
	liftWriteDeadline(w)
	verdict, err := s.moderateDocument(r.Context(), doc, s.reserveCommunityModeration)
	var ceiling *ceilingError
	switch {
	case err == nil:
		s.communityModerations.ok.Add(1)
		v1.WriteJSON(w, http.StatusOK, verdict)
	case errors.As(err, &ceiling):
		s.communityModerations.ceiling.Add(1)
		e := v1Problem(http.StatusServiceUnavailable, "moderation_unavailable", "daily moderation ceiling reached")
		e.Ceiling, e.RetryAfter = true, ceiling.retry
		v1.WriteError(w, e)
	case errors.Is(err, errModerationInvalidURL):
		s.communityModerations.errs.Add(1)
		v1.WriteError(w, v1Problem(http.StatusBadRequest, "invalid_url", "invalid url"))
	default:
		s.communityModerations.errs.Add(1)
		v1.WriteError(w, v1Problem(http.StatusServiceUnavailable, "moderation_unavailable", "content safety service unavailable"))
	}
}

// moderationDocumentBound names the first member of doc outside its size bound.
func moderationDocumentBound(doc community.ModerationDocument) (string, bool) {
	switch {
	case len(doc.Text) < 80 || len(doc.Text) > 32000:
		return "text", false
	case len(doc.Title) > 1000:
		return "title", false
	case len(doc.Signals) > 4000:
		return "signals", false
	}
	return "", true
}
