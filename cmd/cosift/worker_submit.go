package main

import (
	"net/http"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/pilot-protocol/cosift/internal/crawler"
)

const (
	minTextBytes  = 80
	maxTextBytes  = 512 << 10
	maxTitleBytes = 1000
	maxLangBytes  = 35
)

var langTag = regexp.MustCompile(`^[A-Za-z]{2,3}(-[A-Za-z0-9]{1,8})*$`)

// clampTitle cuts a title to the server's byte bound on a character boundary.
func clampTitle(s string) string {
	if !utf8.ValidString(s) {
		s = strings.ToValidUTF8(s, "")
	}
	if len(s) <= maxTitleBytes {
		return s
	}
	s = s[:maxTitleBytes]
	for !utf8.ValidString(s) {
		s = s[:len(s)-1]
	}
	return s
}

// submissionFor builds the attestation for a lease, or says why the page cannot be submitted.
func submissionFor(leaseID string, p *extracted) (map[string]any, string) {
	switch {
	case len(p.Text) < minTextBytes:
		return nil, "too_short"
	case len(p.Text) > maxTextBytes:
		return nil, "too_long"
	case !utf8.ValidString(p.Text) || strings.ContainsRune(p.Text, 0):
		return nil, "bad_text"
	case len(p.FinalURL) > 2048:
		return nil, "bad_url"
	}
	lang := p.Lang
	if len(lang) > maxLangBytes || !langTag.MatchString(lang) {
		lang = ""
	}
	return map[string]any{
		"lease_id": leaseID, "final_url": p.FinalURL, "http_status": http.StatusOK, "title": clampTitle(p.Title),
		"text": p.Text, "lang": lang, "content_hash": p.ContentHash, "simhash": p.Simhash, "extract_version": crawler.ExtractVersion,
	}, ""
}
