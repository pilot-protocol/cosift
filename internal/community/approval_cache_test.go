package community

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/pilot-protocol/cosift/internal/crawler"
)

type approvalBackend struct {
	moderations, deliveries atomic.Int32
	moderateStatus          atomic.Int32
	failDeliveries          atomic.Int32
	hashes                  []string
}

func (b *approvalBackend) handler(t *testing.T) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/community/moderate":
			b.moderations.Add(1)
			if code := int(b.moderateStatus.Load()); code != 0 {
				w.WriteHeader(code)
				return
			}
			io.WriteString(w, `{"decision":"allow","category":"safe"}`)
		case "/v1/community/enqueue":
			b.deliveries.Add(1)
			var in struct {
				Hash string `json:"approved_content_hash"`
			}
			json.NewDecoder(r.Body).Decode(&in)
			b.hashes = append(b.hashes, in.Hash)
			if b.failDeliveries.Add(-1) >= 0 {
				w.WriteHeader(502)
				return
			}
			io.WriteString(w, `{"indexed":true,"novel":false,"content_hash":"`+strings.Repeat("a", 64)+`"}`)
		}
	})
}

func TestDeliveryRetriesDoNotRemoderateUnchangedContent(t *testing.T) {
	captureLog(t)
	b := &approvalBackend{}
	b.failDeliveries.Store(2)
	s := testServer(t, b.handler(t))
	cookie := account(t, s, "member@example.com")
	expect(t, request(t, s, "POST", "/api/submissions", map[string]any{"urls": []string{"https://example.com/guide"}}, cookie), 202)
	exhaust(t, s, 3)
	if got := listSubmissions(t, s, cookie); got[0].Status != "indexed" {
		t.Fatalf("status %s", got[0].Status)
	}
	if b.moderations.Load() != 1 || b.deliveries.Load() != 3 {
		t.Fatalf("%d moderations and %d deliveries, want 1 and 3", b.moderations.Load(), b.deliveries.Load())
	}
	if len(b.hashes) != 3 || len(b.hashes[0]) != 64 || b.hashes[0] != b.hashes[1] || b.hashes[1] != b.hashes[2] {
		t.Fatalf("deliveries did not carry the one approved hash: %v", b.hashes)
	}
}

func TestChangedContentIsModeratedAgain(t *testing.T) {
	captureLog(t)
	b := &approvalBackend{}
	b.failDeliveries.Store(100)
	s := testServer(t, b.handler(t))
	cookie := account(t, s, "member@example.com")
	expect(t, request(t, s, "POST", "/api/submissions", map[string]any{"urls": []string{"https://example.com/guide"}}, cookie), 202)
	exhaust(t, s, 2)
	setTestPage(s, `<html><title>Another guide</title><body><article>This revised educational reference describes compiler ownership checks, reliable memory management, and defensive programming techniques for developers building software.</article></body></html>`)
	exhaust(t, s, 2)
	if b.moderations.Load() != 2 {
		t.Fatalf("%d moderations, want one per distinct content", b.moderations.Load())
	}
	if len(b.hashes) != 4 || b.hashes[0] != b.hashes[1] || b.hashes[2] != b.hashes[3] || b.hashes[1] == b.hashes[2] {
		t.Fatalf("deliveries did not follow the content: %v", b.hashes)
	}
}

func TestOnlyAllowDecisionsAreCached(t *testing.T) {
	captureLog(t)
	b := &approvalBackend{}
	b.moderateStatus.Store(500)
	s := testServer(t, b.handler(t))
	cookie := account(t, s, "member@example.com")
	expect(t, request(t, s, "POST", "/api/submissions", map[string]any{"urls": []string{"https://example.com/guide"}}, cookie), 202)
	exhaust(t, s, 2)
	b.moderateStatus.Store(0)
	exhaust(t, s, 1)
	if b.moderations.Load() != 3 || b.deliveries.Load() != 1 {
		t.Fatalf("%d moderations and %d deliveries, want 3 and 1", b.moderations.Load(), b.deliveries.Load())
	}
}

func TestApprovalsAreNotSharedBetweenSubmissions(t *testing.T) {
	b := &approvalBackend{}
	s := testServer(t, b.handler(t))
	for _, email := range []string{"first@example.com", "second@example.com"} {
		cookie := account(t, s, email)
		expect(t, request(t, s, "POST", "/api/submissions", map[string]any{"urls": []string{"https://example.com/guide"}}, cookie), 202)
	}
	if err := s.dispatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	if b.moderations.Load() != 2 {
		t.Fatalf("%d moderations for two submissions of one URL, want 2", b.moderations.Load())
	}
}

func TestCachedApprovalStillRunsTheSafetyChecks(t *testing.T) {
	page := func(extra string) string {
		return `<html><title>Technical documentation</title><body><article>This educational page explains programming tools, reliable systems, and defensive software engineering. It contains useful public documentation for developers.</article>` + extra + `</body></html>`
	}
	approved := func(html string) string {
		doc, err := crawler.Parse([]byte(html), "https://example.com/guide")
		if err != nil {
			t.Fatal(err)
		}
		return crawler.ApprovedContentHash(doc.Title, doc.Text)
	}
	for _, tc := range []struct{ name, extra, status string }{
		{"explicit rating", `<meta name="rating" content="adult">`, "rejected"},
		{"oversized signals", `<img alt="` + strings.Repeat("docs ", 900) + `">`, "unverified"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			captureLog(t)
			if approved(page("")) != approved(page(tc.extra)) {
				t.Fatal("the fixture changes the approved hash, so it would not reach the cache")
			}
			b := &approvalBackend{}
			b.failDeliveries.Store(1)
			s := testServer(t, b.handler(t))
			cookie := account(t, s, "member@example.com")
			expect(t, request(t, s, "POST", "/api/submissions", map[string]any{"urls": []string{"https://example.com/guide"}}, cookie), 202)
			exhaust(t, s, 1)
			if b.moderations.Load() != 1 || b.deliveries.Load() != 1 {
				t.Fatalf("%d moderations and %d deliveries before the page changed, want 1 and 1", b.moderations.Load(), b.deliveries.Load())
			}
			setTestPage(s, page(tc.extra))
			exhaust(t, s, 1)
			if got := listSubmissions(t, s, cookie); got[0].Status != tc.status {
				t.Fatalf("status %s, want %s: an earlier approval must not skip the page checks", got[0].Status, tc.status)
			}
			if b.deliveries.Load() != 1 {
				t.Fatalf("a page that fails the checks was delivered %d times", b.deliveries.Load())
			}
		})
	}
}
