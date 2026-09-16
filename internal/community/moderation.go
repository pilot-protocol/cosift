package community

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/pilot-protocol/cosift/internal/adultfilter"
	"github.com/pilot-protocol/cosift/internal/crawler"
	"golang.org/x/net/html"
)

// ModerationDocument is public page data, never contributor identity or session
// data. Treat every field as untrusted content in the classifier's prompt.
type ModerationDocument struct {
	URL     string `json:"url"`
	Title   string `json:"title"`
	Text    string `json:"text"`
	Signals string `json:"signals"`
}
type ModerationVerdict struct {
	Decision string `json:"decision"`
	Category string `json:"category"`
}

func ValidVerdict(v ModerationVerdict) bool {
	if v.Decision == "allow" {
		return v.Category == "safe"
	}
	if v.Decision == "uncertain" {
		return v.Category == "unverified"
	}
	if v.Decision != "reject" {
		return false
	}
	switch v.Category {
	case "adult", "malware", "phishing", "graphic_violence", "extremist_promotion", "illegal_harm", "spam", "low_quality":
		return true
	}
	return false
}

func newModerationClient() *http.Client {
	client := crawler.PublicHTTPClient(15 * time.Second)
	client.CheckRedirect = func(r *http.Request, via []*http.Request) error {
		if len(via) >= 5 {
			return fmt.Errorf("too many redirects")
		}
		_, err := NormalizeURL(r.URL.String())
		return err
	}
	return client
}

// prevalidate runs before delivery. Only an explicit allow decision may enter
// the crawl queue. Transient failures retain pending work; unreadable content
// is unverified, and a positive policy match is rejected.
func (s *Server) prevalidate(ctx context.Context, raw string) (status, reason string) {
	if _, err := NormalizeURL(raw); err != nil {
		return "rejected", "URL is not an eligible public webpage."
	}
	allowed, delay, err := s.moderationRobots.Allowed(ctx, raw)
	if err != nil {
		return "pending", "Waiting to check the webpage."
	}
	if !allowed {
		return "unverified", "The website does not permit automated page checks."
	}
	if delay > 0 {
		if delay > 15*time.Second {
			return "unverified", "The website requires a longer crawl delay than validation supports."
		}
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return "pending", "Validation interrupted."
		case <-timer.C:
		}
	}
	req, err := http.NewRequestWithContext(ctx, "GET", raw, nil)
	if err != nil {
		return "unverified", "The webpage could not be checked."
	}
	req.Header.Set("User-Agent", "Cosift-Community/1.0")
	req.Header.Set("Accept", "text/html, application/xhtml+xml, text/plain")
	res, err := s.pageClient.Do(req)
	if err != nil {
		return "pending", "The webpage could not be reached for validation."
	}
	defer res.Body.Close()
	if res.StatusCode == 429 || res.StatusCode >= 500 {
		return "pending", "The website is temporarily unavailable for validation."
	}
	if res.StatusCode != 200 {
		return "unverified", "The webpage is unavailable or requires a login."
	}
	finalURL := res.Request.URL.String()
	if _, err := NormalizeURL(finalURL); err != nil {
		return "rejected", "The destination URL is not eligible."
	}
	body, err := io.ReadAll(io.LimitReader(res.Body, (2<<20)+1))
	if err != nil {
		return "pending", "The webpage could not be read."
	}
	if len(body) > 2<<20 {
		return "unverified", "The webpage exceeds the validation size limit."
	}
	kind, _, _ := mime.ParseMediaType(res.Header.Get("Content-Type"))
	if kind == "" {
		kind, _, _ = mime.ParseMediaType(http.DetectContentType(body))
	}
	doc := ModerationDocument{URL: finalURL}
	switch kind {
	case "text/html", "application/xhtml+xml":
		parsed, err := crawler.Parse(body, finalURL)
		if err != nil {
			return "unverified", "The webpage could not be interpreted."
		}
		doc.Title = parsed.Title
		doc.Text = parsed.Text
		doc.Signals = pageSignals(body)
	case "text/plain":
		doc.Text = string(body)
	default:
		return "unverified", "Only readable webpages can be checked; media and downloads are not accepted."
	}
	if adultfilter.IsAdult(doc.Title, doc.Text+" "+doc.Signals, finalURL) || strings.Contains(doc.Signals, "COSIFT_EXPLICIT_RATING") {
		return "rejected", "Explicit adult content is not accepted."
	}
	if len(strings.TrimSpace(doc.Text)) < 80 {
		return "unverified", "Not enough readable text to validate this webpage."
	}
	if len(doc.Text) > 32000 || len(doc.Title) > 1000 || len(doc.Signals) > 4000 {
		return "unverified", "The webpage contains more content than can be fully checked in one validation."
	}
	if status, reason := ObviousQualityProblem(doc); status != "" {
		return status, reason
	}
	b, _ := json.Marshal(doc)
	checkReq, _ := http.NewRequestWithContext(ctx, "POST", s.cfg.Backend+"/admin/community-moderate", bytes.NewReader(b))
	checkReq.Header.Set("Content-Type", "application/json")
	checkReq.Header.Set("Authorization", "Bearer "+s.cfg.AdminToken)
	client := *s.client
	client.Timeout = 60 * time.Second
	checkRes, err := client.Do(checkReq)
	if err != nil {
		return "pending", "Waiting for content safety checks."
	}
	defer checkRes.Body.Close()
	var verdict ModerationVerdict
	decision := json.NewDecoder(io.LimitReader(checkRes.Body, 4096))
	decision.DisallowUnknownFields()
	if checkRes.StatusCode != 200 || decision.Decode(&verdict) != nil || decision.Decode(new(any)) != io.EOF || !ValidVerdict(verdict) {
		return "pending", "Waiting for a valid content safety decision."
	}
	switch verdict.Decision {
	case "allow":
		return "allowed", "Content checks passed."
	case "uncertain":
		return "unverified", "This webpage could not be confidently validated."
	default:
		labels := map[string]string{"adult": "Explicit adult content", "malware": "Malware distribution or malicious instructions", "phishing": "Phishing or credential theft", "graphic_violence": "Graphic violence or violent abuse", "extremist_promotion": "Extremist promotion or recruitment", "illegal_harm": "Promotion of illegal harm", "spam": "Spam or search manipulation", "low_quality": "Garbage or content without useful information"}
		return "rejected", labels[verdict.Category] + " is not accepted."
	}
}

// Include descriptions and image alt text that the normal article parser may
// omit. This is textual screening, not image/video classification.
func pageSignals(body []byte) string {
	z := html.NewTokenizer(bytes.NewReader(body))
	var out strings.Builder
	for {
		tt := z.Next()
		if tt == html.ErrorToken {
			break
		}
		if tt != html.StartTagToken && tt != html.SelfClosingTagToken {
			continue
		}
		t := z.Token()
		attrs := map[string]string{}
		for _, a := range t.Attr {
			attrs[strings.ToLower(a.Key)] = a.Val
		}
		if t.Data == "img" {
			out.WriteString(attrs["alt"] + " " + attrs["title"] + "\n")
		}
		if t.Data == "meta" {
			key := strings.ToLower(attrs["name"] + attrs["property"])
			value := attrs["content"]
			if strings.Contains(key, "rating") && (strings.EqualFold(value, "adult") || strings.Contains(strings.ToLower(value), "rta-5042")) {
				out.WriteString("COSIFT_EXPLICIT_RATING ")
			}
			if strings.Contains(key, "description") || strings.Contains(key, "keywords") || strings.Contains(key, "title") {
				out.WriteString(value + "\n")
			}
		}
		if out.Len() > 4000 {
			break
		}
	}
	return out.String()
}
