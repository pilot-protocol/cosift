package wiki

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"time"
)

var (
	slugPattern   = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)
	idPattern     = regexp.MustCompile(`^[0-9A-HJKMNP-TV-Z]{26}$`)
	cursorPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,1000}$`)
	reserved      = map[string]bool{"v": true, "index": true, "sitemap": true, "search": true, "api": true, "admin": true, "random": true, "new": true}
	verticals     = map[string]string{"dev-docs": "Developer docs", "technology-news": "Technology news", "research": "Research", "other": "Other"}
	verticalOrder = []string{"dev-docs", "technology-news", "research", "other"}
	tiers         = map[string]string{"strong": "Strong", "ok": "OK", "thin": "Thin"}
)

// pageSlug is AR §4's slug pattern (≤ 80 bytes) minus the reserved words.
func pageSlug(s string) bool { return len(s) <= 80 && slugPattern.MatchString(s) && !reserved[s] }

const (
	bySlugTimeout = 3 * time.Second
	listTimeout   = 10 * time.Second
	bySlugLimit   = 1 << 20
	listLimit     = 4 << 20
)

type outcome int

const (
	outFail outcome = iota
	outArticle
	outStub
	outRedirect
	outGone
	outNotFound
)

type engineCitation struct {
	N     int    `json:"n"`
	URL   string `json:"url"`
	Title string `json:"title"`
	Host  string `json:"host"`
	Quote string `json:"quote"`
}

// engineRecord holds only public and stub projection fields, plus prelive
// to detect an over-scoped key.
type engineRecord struct {
	ID          string           `json:"id"`
	Slug        string           `json:"slug"`
	Title       string           `json:"title"`
	Lead        string           `json:"lead"`
	BodyMD      string           `json:"body_md"`
	Citations   []engineCitation `json:"citations"`
	QualityTier string           `json:"quality_tier"`
	Vertical    string           `json:"vertical"`
	Status      string           `json:"status"`
	AIGenerated bool             `json:"ai_generated"`
	CreatedAt   string           `json:"created_at"`
	UpdatedAt   string           `json:"updated_at"`
	Prelive     json.RawMessage  `json:"prelive"`
}

type engineItem struct {
	ID          string          `json:"id"`
	Slug        string          `json:"slug"`
	Title       string          `json:"title"`
	Vertical    string          `json:"vertical"`
	QualityTier string          `json:"quality_tier"`
	Promoted    bool            `json:"promoted"`
	Status      string          `json:"status"`
	UpdatedAt   string          `json:"updated_at"`
	Prelive     json.RawMessage `json:"prelive"`
}

type engineList struct {
	Items      []engineItem `json:"items"`
	NextCursor *string      `json:"next_cursor"`
}

type result struct {
	kind   outcome
	rec    *engineRecord
	target string
	code   string
}

func failed(code string) result { return result{kind: outFail, code: code} }

// engine calls only GET by-slug and GET list on the /v1 listener. The key
// goes on those two requests and nowhere else.
type engine struct {
	base        string
	key         string
	http        *http.Client
	slugTimeout time.Duration
	listTimeout time.Duration
}

func newEngine(base, key string) *engine {
	tr := &http.Transport{
		Proxy:                 nil,
		DialContext:           (&net.Dialer{Timeout: 2 * time.Second}).DialContext,
		ResponseHeaderTimeout: 5 * time.Second,
		MaxIdleConnsPerHost:   8,
		IdleConnTimeout:       60 * time.Second,
		DisableCompression:    true,
	}
	return &engine{base: base, key: key, slugTimeout: bySlugTimeout, listTimeout: listTimeout, http: &http.Client{
		Transport:     tr,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}
}

// get returns the status and a JSON body, or a failure class.
func (e *engine) get(ctx context.Context, path string, limit int64) (int, []byte, string) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, e.base+path, nil)
	if err != nil {
		return 0, nil, "engine_transport"
	}
	req.Header.Set("Authorization", "Bearer "+e.key)
	req.Header.Set("Accept", "application/json")
	res, err := e.http.Do(req)
	if err != nil {
		return 0, nil, transportClass(err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(io.LimitReader(res.Body, limit+1))
	if err != nil {
		return 0, nil, transportClass(err)
	}
	switch {
	case res.StatusCode == http.StatusTooManyRequests:
		return res.StatusCode, nil, "engine_429"
	case res.StatusCode >= 500:
		return res.StatusCode, nil, "engine_5xx"
	case res.StatusCode == http.StatusUnauthorized || res.StatusCode == http.StatusForbidden:
		return res.StatusCode, nil, "engine_auth"
	case res.StatusCode >= 300 && res.StatusCode < 400:
		return res.StatusCode, nil, "engine_3xx"
	}
	kind, _, _ := mime.ParseMediaType(res.Header.Get("Content-Type"))
	if int64(len(body)) > limit || kind != "application/json" {
		return res.StatusCode, nil, "engine_body"
	}
	return res.StatusCode, body, ""
}

func transportClass(err error) string {
	var ne net.Error
	if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &ne) && ne.Timeout()) {
		return "engine_timeout"
	}
	return "engine_transport"
}

// errorCode reads the code of an engine error body.
func errorCode(body []byte) string {
	var e struct {
		Code string `json:"code"`
	}
	if json.Unmarshal(body, &e) != nil {
		return ""
	}
	return e.Code
}

func (e *engine) bySlug(ctx context.Context, slug string) result {
	ctx, cancel := context.WithTimeout(ctx, e.slugTimeout)
	defer cancel()
	status, body, code := e.get(ctx, "/v1/articles/by-slug/"+slug, bySlugLimit)
	if code != "" {
		return failed(code)
	}
	switch status {
	case http.StatusOK:
		return dispatch(slug, body)
	case http.StatusNotFound:
		if errorCode(body) == "not_found" {
			return result{kind: outNotFound}
		}
	case http.StatusGone:
		if errorCode(body) == "gone" {
			return result{kind: outGone}
		}
	}
	return failed("engine_4xx")
}

// dispatch decides a 200 by-slug body on its decoded status.
func dispatch(slug string, body []byte) result {
	var top map[string]json.RawMessage
	if json.Unmarshal(body, &top) != nil {
		return failed("engine_body")
	}
	if raw, ok := top["redirect_slug"]; ok && len(top) == 1 {
		var target string
		if json.Unmarshal(raw, &target) != nil || !pageSlug(target) || target == slug {
			return failed("engine_dispatch")
		}
		return result{kind: outRedirect, target: target}
	}
	var rec engineRecord
	if json.Unmarshal(body, &rec) != nil {
		return failed("engine_body")
	}
	if len(rec.Prelive) > 0 {
		return failed("engine_overscoped")
	}
	if !idPattern.MatchString(rec.ID) || rec.Slug != slug || !pageSlug(rec.Slug) || verticals[rec.Vertical] == "" || cleanLine(rec.Title) == "" || !validTime(rec.CreatedAt) {
		return failed("engine_dispatch")
	}
	switch rec.Status {
	case "published":
		if !rec.AIGenerated || tiers[rec.QualityTier] == "" || !validTime(rec.UpdatedAt) || len(rec.Citations) > 30 {
			return failed("engine_dispatch")
		}
		for i, c := range rec.Citations {
			if c.N != i+1 {
				return failed("engine_dispatch")
			}
		}
		return result{kind: outArticle, rec: &rec}
	case "pending":
		return result{kind: outStub, rec: &rec}
	}
	return failed("engine_dispatch")
}

func validTime(s string) bool {
	_, err := time.Parse(time.RFC3339, s)
	return err == nil
}

// list fetches one page of GET /v1/articles?order=slug&limit=500.
func (e *engine) list(ctx context.Context, cursor string) (engineList, string) {
	path := "/v1/articles?order=slug&limit=500"
	if cursor != "" {
		if !cursorPattern.MatchString(cursor) {
			return engineList{}, "engine_dispatch"
		}
		path += "&cursor=" + url.QueryEscape(cursor)
	}
	ctx, cancel := context.WithTimeout(ctx, e.listTimeout)
	defer cancel()
	status, body, code := e.get(ctx, path, listLimit)
	if code != "" {
		return engineList{}, code
	}
	if status != http.StatusOK {
		return engineList{}, "engine_4xx"
	}
	var page engineList
	dec := json.NewDecoder(bytes.NewReader(body))
	if dec.Decode(&page) != nil || page.Items == nil {
		return engineList{}, "engine_body"
	}
	for _, it := range page.Items {
		if len(it.Prelive) > 0 {
			return engineList{}, "engine_overscoped"
		}
	}
	return page, ""
}
