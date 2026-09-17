package community

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestContributionModerationMustAllowBeforeEnqueue(t *testing.T) {
	cases := []struct {
		name, html, verdict, want string
		status                    int
		enqueue                   bool
	}{
		{"safe", `<html><title>Defensive security research</title><article>This educational research explains how phishing is detected and how organizations can protect their employees through defensive software and training.</article></html>`, `{"decision":"allow","category":"safe"}`, "queued", 200, true},
		{"spam", `<html><article>This page contains enough readable material to classify, but the classifier identifies deceptive search manipulation and promotional keyword stuffing.</article></html>`, `{"decision":"reject","category":"spam"}`, "rejected", 200, false},
		{"low-quality", `<html><article>This page contains enough readable material to classify, but the classifier identifies incoherent scraped fragments without useful information.</article></html>`, `{"decision":"reject","category":"low_quality"}`, "rejected", 200, false},
		{"phishing", `<html><title>Account page</title><article>A page claiming to be a bank and requesting users to send account credentials and authentication codes to an unrelated form endpoint for collection.</article></html>`, `{"decision":"reject","category":"phishing"}`, "rejected", 200, false},
		{"adult", `<html><title>Free porn videos</title><article>Explicit pornographic gallery with unlimited clips and much more content provided through the site's video collection and premium membership service.</article></html>`, "", "rejected", 200, false},
		{"rated-adult", `<html><meta name="rating" content="adult"><article>This webpage has a sufficient amount of readable content but self declares an explicit adult rating in its own metadata and must be rejected before queueing.</article></html>`, "", "rejected", 200, false},
		{"image-only", `<html><title>Gallery</title><img src="photo.jpg"></html>`, "", "unverified", 200, false},
		{"uncertain", `<html><article>This is a generic page containing enough readable text for evaluation, but its meaning and intent are unclear and the safety classifier cannot reliably determine the category.</article></html>`, `{"decision":"uncertain","category":"unverified"}`, "unverified", 200, false},
		{"outage", `<html><article>This educational page explains programming tools, reliable systems, and defensive software engineering. It contains useful public documentation for developers.</article></html>`, "", "pending", 503, false},
		{"invalid-verdict", `<html><article>This educational page explains programming tools, reliable systems, and defensive software engineering. It contains useful public documentation for developers.</article></html>`, `{"decision":"allow","category":"malware"}`, "pending", 200, false},
		{"trailing-verdict", `<html><article>This educational page explains programming tools, reliable systems, and defensive software engineering. It contains useful public documentation for developers.</article></html>`, `{"decision":"allow","category":"safe"}{"decision":"reject","category":"malware"}`, "pending", 200, false},
		{"extra-field", `<html><article>This educational page explains programming tools, reliable systems, and defensive software engineering. It contains useful public documentation for developers.</article></html>`, `{"decision":"allow","category":"safe","override":true}`, "pending", 200, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			queued := 0
			s := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/admin/community-moderate":
					w.WriteHeader(tc.status)
					io.WriteString(w, tc.verdict)
				case "/admin/community-enqueue":
					queued++
					io.WriteString(w, `{"queued":"https://example.com/page"}`)
				default:
					t.Errorf("unexpected endpoint %s", r.URL.Path)
				}
			}))
			setTestPage(s, tc.html)
			cookie := account(t, s, "moderation@example.com")
			expect(t, request(t, s, "POST", "/api/submissions", map[string]any{"urls": []string{"https://example.com/page"}}, cookie), 202)
			if err := s.dispatch(context.Background()); err != nil {
				t.Fatal(err)
			}
			var status, reason string
			s.db.QueryRow(`SELECT status,reason FROM submissions`).Scan(&status, &reason)
			if status != tc.want || reason == "" || (queued == 1) != tc.enqueue {
				t.Fatalf("state=%s reason=%s queued=%d", status, reason, queued)
			}
		})
	}
}

func TestKnownAdultOrInstallerURLsRejectedBeforeQueueing(t *testing.T) {
	s := testServer(t, nil)
	cookie := account(t, s, "invalid@example.com")
	for _, raw := range []string{"https://www.pornhub.com/view", "https://example.xxx/page", "https://example.com/download.exe"} {
		expect(t, request(t, s, "POST", "/api/submissions", map[string]any{"urls": []string{"https://example.com/good", raw}}, cookie), 400)
	}
	var count int
	s.db.QueryRow(`SELECT count(*) FROM guest_usage`).Scan(&count)
	if count != 0 {
		t.Fatal("rejected URLs consumed quota")
	}
	s.db.QueryRow(`SELECT count(*) FROM submissions`).Scan(&count)
	if count != 0 {
		t.Fatal("partial batch persisted")
	}
}

func TestModerationRedirectAndMetadataChecks(t *testing.T) {
	client := newModerationClient()
	req, _ := http.NewRequest("GET", "https://www.pornhub.com/", nil)
	if client.CheckRedirect(req, []*http.Request{req}) == nil {
		t.Fatal("adult redirect accepted")
	}
	signals := pageSignals([]byte(`<meta name="rating" content="RTA-5042-1996-1400-1577-RTA"><img alt="descriptive image text"><meta name="description" content="page summary">`))
	for _, part := range []string{"COSIFT_EXPLICIT_RATING", "descriptive image text", "page summary"} {
		if !strings.Contains(signals, part) {
			t.Errorf("missing %s", part)
		}
	}
}
