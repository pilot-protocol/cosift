package crawler

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// FetchResult is a parsed page from a single fetch. Same shape as what the
// crawler's worker pipeline produces, but exposed for callers that only need
// "give me this one URL" — /contents on-demand fetch, the CLI `cosift fetch`
// command (not yet wired), etc.
type FetchResult struct {
	URL   string
	Title string
	Text  string
	Lang  string
	Links []string
}

// FetchOne fetches and parses a single URL. No frontier, no robots cache, no
// rate limiter. The caller is responsible for politeness if it's calling
// FetchOne in a loop.
//
// On non-2xx responses, the returned error includes the status code.
// Content-Type must be HTML/XML or empty; binary types are rejected (the
// extractor would produce garbage anyway).
func FetchOne(ctx context.Context, client *http.Client, userAgent, rawURL string, maxBodyBytes int64) (*FetchResult, error) {
	if client == nil {
		client = defaultHTTPClient()
	}
	if userAgent == "" {
		userAgent = "CosiftBot/0.0 (+https://github.com/pilot-protocol/cosift)"
	}
	if maxBodyBytes <= 0 {
		maxBodyBytes = 5 << 20
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/pdf")
	// Iter 141: no manual Accept-Encoding — see crawler.go for full rationale.

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("http %d", resp.StatusCode)
	}
	ct := resp.Header.Get("Content-Type")
	if ct != "" && !strings.Contains(ct, "html") && !strings.Contains(ct, "xml") && !strings.Contains(ct, "pdf") {
		return nil, fmt.Errorf("non-html/xml/pdf content-type: %s", ct)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
	if err != nil {
		return nil, err
	}
	finalURL := resp.Request.URL.String()
	// Iter 73: dispatch parser by content-type. HTML default; PDF when ct contains "pdf".
	var parsed *ParsedDoc
	if strings.Contains(ct, "pdf") {
		parsed, err = ParsePDF(body, finalURL)
	} else {
		parsed, err = Parse(body, finalURL)
	}
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(parsed.Text) == "" {
		return nil, errors.New("empty content")
	}
	return &FetchResult{
		URL:   finalURL,
		Title: parsed.Title,
		Text:  parsed.Text,
		Lang:  parsed.Lang,
		Links: parsed.Links,
	}, nil
}

// defaultHTTPClient mirrors the crawler worker's transport tuning.
func defaultHTTPClient() *http.Client {
	return &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:          50,
			MaxConnsPerHost:       2,
			IdleConnTimeout:       90 * time.Second,
			ForceAttemptHTTP2:     true,
			ResponseHeaderTimeout: 15 * time.Second,
		},
	}
}
