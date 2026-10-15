package crawler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/pilot-protocol/cosift/internal/netguard"
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

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, http.NoBody)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/pdf")
	// no manual Accept-Encoding — see crawler.go for full rationale.

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("http %d", resp.StatusCode)
	}
	if err := checkContentType(resp.Header.Get("Content-Type")); err != nil {
		return nil, err
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
	if err != nil {
		return nil, err
	}
	finalURL := resp.Request.URL.String()
	parsed, err := ExtractBody(resp.Header.Get("Content-Type"), body, finalURL)
	if err != nil {
		return nil, err
	}
	return &FetchResult{
		URL:   finalURL,
		Title: parsed.Title,
		Text:  parsed.Text,
		Lang:  parsed.Lang,
		Links: parsed.Links,
	}, nil
}

// ExtractBody turns a fetched body into the parsed page FetchOne returns; binary content types are refused.
func ExtractBody(contentType string, body []byte, finalURL string) (*ParsedDoc, error) {
	return ExtractBodyWith(contentType, body, finalURL, ParsePDF)
}

// ExtractBodyWith is ExtractBody with the PDF parser supplied by the caller, so a PDF can be parsed out of process.
func ExtractBodyWith(contentType string, body []byte, finalURL string, parsePDF func([]byte, string) (*ParsedDoc, error)) (*ParsedDoc, error) {
	if err := checkContentType(contentType); err != nil {
		return nil, err
	}
	var parsed *ParsedDoc
	var err error
	if strings.Contains(contentType, "pdf") {
		parsed, err = parsePDF(body, finalURL)
	} else {
		parsed, err = Parse(body, finalURL)
	}
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(parsed.Text) == "" {
		return nil, errors.New("empty content")
	}
	return parsed, nil
}

func checkContentType(ct string) error {
	if ct != "" && !strings.Contains(ct, "html") && !strings.Contains(ct, "xml") && !strings.Contains(ct, "pdf") {
		return fmt.Errorf("non-html/xml/pdf content-type: %s", ct)
	}
	return nil
}

// ContentHash is the lower-case hex SHA-256 of text, the content_hash the server recomputes.
func ContentHash(text string) string {
	sum := sha256.Sum256([]byte(text))
	return hex.EncodeToString(sum[:])
}

// defaultHTTPClient mirrors the crawler worker's transport tuning.
func defaultHTTPClient() *http.Client {
	return &http.Client{
		Timeout: 30 * time.Second,
		Transport: netguard.Protect(&http.Transport{
			MaxIdleConns:          50,
			MaxConnsPerHost:       2,
			IdleConnTimeout:       90 * time.Second,
			ForceAttemptHTTP2:     true,
			ResponseHeaderTimeout: 15 * time.Second,
		}, true),
	}
}
