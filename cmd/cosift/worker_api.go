package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const workReplyLimit = 1 << 20

type workLease struct {
	LeaseID        string `json:"lease_id"`
	URL            string `json:"url"`
	Source         string `json:"source"`
	ExpiresAt      string `json:"expires_at"`
	ExtractVersion int    `json:"extract_version"`
}

type claimReply struct {
	Leases      []workLease `json:"leases"`
	RetryAfterS int         `json:"retry_after_s"`
	Ceiling     bool        `json:"moderation_ceiling"`
}

type submitReply struct {
	Verdict       string `json:"verdict"`
	AgreeingCount int    `json:"agreeing_count"`
	Indexed       bool   `json:"indexed"`
	Reason        string `json:"reason"`
	CreditsEarned *int   `json:"credits_earned"`
}

// workAPIError is a refusal from the community app's work routes; it carries codes only, never the response text.
type workAPIError struct {
	Status         int
	Code           string
	Field, Rule    string
	RetryAfter     time.Duration
	ExtractVersion int
}

func (e *workAPIError) Error() string {
	msg := fmt.Sprintf("HTTP %d %s", e.Status, e.Code)
	if e.Field != "" {
		msg += " (" + e.Field + ": " + e.Rule + ")"
	}
	return msg
}

// workAPI calls /api/work/* with the bearer token; the token appears nowhere else.
type workAPI struct {
	base   string
	token  string
	client *http.Client
}

func newWorkAPI(base, token string) *workAPI {
	return &workAPI{
		base:   strings.TrimRight(base, "/"),
		token:  token,
		client: &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
	}
}

func (a *workAPI) claim(ctx context.Context, max int) (claimReply, error) {
	var out claimReply
	return out, a.post(ctx, "claim", map[string]int{"max": max}, &out)
}

func (a *workAPI) submit(ctx context.Context, body map[string]any) (submitReply, error) {
	var out submitReply
	return out, a.post(ctx, "submit", body, &out)
}

func (a *workAPI) post(ctx context.Context, route string, body, out any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.base+"/api/work/"+route, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Cosift-Client", "community")
	req.Header.Set("Authorization", "Bearer "+a.token)
	res, err := a.client.Do(req)
	if err != nil {
		return fmt.Errorf("work %s: %w", route, err)
	}
	defer res.Body.Close()
	data, err := io.ReadAll(io.LimitReader(res.Body, workReplyLimit))
	if err != nil {
		return fmt.Errorf("work %s: %w", route, err)
	}
	if res.StatusCode != http.StatusOK {
		return parseWorkError(res, data)
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("work %s: unreadable answer", route)
	}
	return nil
}

func parseWorkError(res *http.Response, data []byte) *workAPIError {
	var e struct {
		Code           string `json:"code"`
		Field          string `json:"field"`
		Rule           string `json:"rule"`
		RetryAfterSecs int    `json:"retry_after_seconds"`
		ExtractVersion int    `json:"extract_version"`
	}
	_ = json.Unmarshal(data, &e)
	out := &workAPIError{Status: res.StatusCode, Code: safeToken(e.Code), Field: safeToken(e.Field), Rule: safeToken(e.Rule), ExtractVersion: e.ExtractVersion}
	secs := e.RetryAfterSecs
	if h, err := strconv.Atoi(res.Header.Get("Retry-After")); err == nil && h > 0 {
		secs = h
	}
	if secs > 0 {
		out.RetryAfter = time.Duration(secs) * time.Second
	}
	return out
}

// safeToken keeps a server-supplied code printable: lower-case words only, short.
func safeToken(s string) string {
	if len(s) == 0 || len(s) > 40 {
		return ""
	}
	for _, c := range s {
		if !(c >= 'a' && c <= 'z' || c == '_') {
			return ""
		}
	}
	return s
}
