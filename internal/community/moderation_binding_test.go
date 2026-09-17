package community

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/pilot-protocol/cosift/internal/crawler"
)

func TestDispatchBindsFreshModerationToEachDelivery(t *testing.T) {
	for _, local := range []bool{false, true} {
		name := "url"
		if local {
			name = "local artifact"
		}
		t.Run(name, func(t *testing.T) {
			var approved []string
			deliveries := 0
			s := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/admin/community-moderate":
					var doc ModerationDocument
					if err := json.NewDecoder(r.Body).Decode(&doc); err != nil {
						t.Error(err)
					}
					approved = append(approved, crawler.ApprovedContentHash(doc.Title, doc.Text))
					io.WriteString(w, `{"decision":"allow","category":"safe"}`)
				case "/admin/community-enqueue":
					deliveries++
					var payload struct {
						ApprovedContentHash string                 `json:"approved_content_hash"`
						Artifact            *crawler.LocalArtifact `json:"artifact"`
					}
					if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
						t.Error(err)
					}
					if len(approved) != deliveries || payload.ApprovedContentHash != approved[deliveries-1] || len(payload.ApprovedContentHash) != 64 {
						t.Errorf("delivery does not bind latest moderation: %+v", payload)
					}
					if (payload.Artifact != nil) != local {
						t.Error("artifact delivery changed")
					}
					if deliveries == 1 {
						w.WriteHeader(503)
					} else {
						w.WriteHeader(422)
					}
				default:
					t.Errorf("unexpected backend request %s", r.URL.Path)
				}
			}))
			cookie := account(t, s, "binding@example.com")
			first := `<html><title>First technical guide</title><article>This educational page explains programming tools, reliable systems, and defensive software engineering. It contains useful public documentation for developers.</article></html>`
			second := `<html><title>Updated technical guide</title><article>This revised educational reference describes compiler ownership checks, reliable memory management, and defensive programming techniques for developers building software.</article></html>`
			setTestPage(s, first)
			body := map[string]any{"urls": []string{"https://example.com/guide"}}
			if local {
				parsed, err := crawler.Parse([]byte(first), "https://example.com/guide")
				if err != nil {
					t.Fatal(err)
				}
				body = map[string]any{"artifacts": []any{map[string]any{"url": "https://example.com/guide", "title": parsed.Title, "text": parsed.Text, "model": "test", "chunks": []any{map[string]any{"text": parsed.Text, "embedding": []float32{1, 2}}}}}}
			}
			expect(t, request(t, s, "POST", "/api/submissions", body, cookie), 202)
			if err := s.dispatch(context.Background()); err != nil {
				t.Fatal(err)
			}
			setTestPage(s, second)
			if _, err := s.db.Exec(`UPDATE submissions SET next_attempt=0`); err != nil {
				t.Fatal(err)
			}
			if err := s.dispatch(context.Background()); err != nil {
				t.Fatal(err)
			}
			if len(approved) != 2 || approved[0] == approved[1] {
				t.Fatalf("approval was reused across changed content: %v", approved)
			}
			var status string
			if err := s.db.QueryRow(`SELECT status FROM submissions`).Scan(&status); err != nil || status != "unverified" {
				t.Fatalf("rejected approval remains retryable: %s %v", status, err)
			}
			if err := s.dispatch(context.Background()); err != nil {
				t.Fatal(err)
			}
			if deliveries != 2 {
				t.Fatal("permanent mismatch retried")
			}
			var credits int
			if err := s.db.QueryRow(`SELECT count(*) FROM credit_ledger`).Scan(&credits); err != nil || credits != 0 {
				t.Fatalf("failed content earned credits: %d %v", credits, err)
			}
		})
	}
}

func TestPlainTextContributionIsUnverified(t *testing.T) {
	s := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unsupported content reached backend %s", r.URL.Path)
	}))
	s.pageClient = &http.Client{Transport: pageTransport(func(r *http.Request) (*http.Response, error) {
		body := "This plain text is readable and educational, but the guarded contribution crawler only supports HTML documents. It must not enter an endless delivery retry loop."
		if r.URL.Path == "/robots.txt" {
			body = "User-agent: *\nAllow: /\n"
		}
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/plain"}}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
	})}
	s.moderationRobots = crawler.NewRobots(s.pageClient, "Cosift-Community/1.0")
	status, reason, hash := s.prevalidate(context.Background(), "https://example.com/readme")
	if status != "unverified" || !strings.Contains(reason, "HTML") || hash != "" {
		t.Fatalf("unsupported plaintext approved: %s %s %s", status, reason, hash)
	}
}
