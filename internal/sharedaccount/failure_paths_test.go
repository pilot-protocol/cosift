package sharedaccount

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"golang.org/x/oauth2"
)

func TestRemoteRejectsInvalidAuthResponses(t *testing.T) {
	const token = "ck_1_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	for _, tc := range []struct {
		name  string
		body  any
		start bool
	}{
		{"missing challenge", Challenge{ExpiresAt: time.Now().Add(time.Minute)}, true},
		{"expired challenge", Challenge{"fixture-request", time.Now().Add(-time.Minute)}, true},
		{"malformed token", Issued{"invalid", "0123456789abcdef"}, false},
		{"malformed account", Issued{token, "different-account"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewEncoder(w).Encode(tc.body)
			}))
			defer srv.Close()
			r, err := newRemote(context.Background(), srv.URL, "", false)
			if err != nil {
				t.Fatal(err)
			}
			c := Client{auth: r}
			if tc.start {
				_, err = c.Start(context.Background(), "fixture@example.com")
			} else {
				_, err = c.Finish(context.Background(), "fixture-request", "123456")
			}
			if !errors.Is(err, ErrUnavailable) {
				t.Fatalf("invalid identity response must fail closed: %v", err)
			}
		})
	}
}

func TestMCPRejectsErrorsAndUnexpectedEnvelopes(t *testing.T) {
	for _, tc := range []struct {
		name, response string
		want           error
	}{
		{"protocol", `{"jsonrpc":"1.0","id":1,"result":{}}`, ErrUnavailable},
		{"response ID", `{"jsonrpc":"2.0","id":2,"result":{}}`, ErrUnavailable},
		{"RPC error", `{"jsonrpc":"2.0","id":1,"error":{"code":-32603}}`, ErrUnavailable},
		{"tool error", `{"jsonrpc":"2.0","id":1,"result":{"isError":true}}`, ErrUnavailable},
		{"no content", `{"jsonrpc":"2.0","id":1,"result":{"content":[]}}`, ErrUnavailable},
		{"image content", `{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"image","text":"{}"}]}}`, ErrUnavailable},
		{"null payload", `{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"null"}]}}`, ErrUnavailable},
		{"invalid payload", `{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"not JSON"}]}}`, ErrUnavailable},
		{"upstream outage", `{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"{\"unavailable\":true}"}]}}`, ErrUnavailable},
		{"quota exceeded", `{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"{\"status\":\"unavailable\",\"reason\":\"quota_exceeded\"}"}]}}`, ErrLimited},
		{"invalid arguments", `{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"{\"status\":\"invalid\"}"}]}}`, ErrInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("X-Forwarded-For") != "" {
					t.Error("auth proxy IP leaked to MCP")
				}
				_, _ = io.WriteString(w, tc.response)
			}))
			defer srv.Close()
			r, err := newRemote(context.Background(), srv.URL+"/v1/mcp", "", true)
			if err != nil {
				t.Fatal(err)
			}
			c := Client{mcp: r}
			out, err := c.Call(WithClientIP(context.Background(), "203.0.113.7"), "fixture", "cosift_lookup", map[string]any{"topic": "fixture"})
			if !errors.Is(err, tc.want) || out != nil {
				t.Fatalf("response should not become article data: out=%v err=%v", out, err)
			}
		})
	}
}

type failingTokenSource struct{}

func (failingTokenSource) Token() (*oauth2.Token, error) {
	return nil, errors.New("fixture identity service unavailable")
}

func TestRemoteCannotSendWithoutInfrastructureIdentity(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("request reached private service without its infrastructure identity")
	}))
	defer srv.Close()
	r, err := newRemote(context.Background(), srv.URL, "", false)
	if err != nil {
		t.Fatal(err)
	}
	r.identity = failingTokenSource{}
	if err := r.call(context.Background(), "/auth/start", "", nil, &map[string]any{}); !errors.Is(err, ErrUnavailable) {
		t.Fatal(err)
	}
}

func TestRemoteTransportAndEncodingFailures(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "broken JSON")
	}))
	r, err := newRemote(context.Background(), srv.URL, "", false)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	var out map[string]any
	if err = r.call(context.Background(), "", "", nil, &out); !errors.Is(err, ErrUnavailable) {
		t.Fatal("invalid JSON accepted", err)
	}
	if err = r.call(context.Background(), "", "", make(chan int), &out); !errors.Is(err, ErrInvalid) {
		t.Fatal("non-JSON arguments sent", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err = r.call(ctx, "", "", nil, &out); !errors.Is(err, ErrUnavailable) {
		t.Fatal("canceled request succeeded", err)
	}
}

func TestVerifierRejectsUnavailablePepperAndCorruptAccounts(t *testing.T) {
	const token = "ck_1_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	for _, tc := range []struct {
		name     string
		key      []byte
		keyErr   error
		record   tokenRecord
		identity Identity
		want     error
	}{
		{name: "unknown pepper", keyErr: ErrUnauthorized, want: ErrUnauthorized},
		{name: "pepper outage", keyErr: ErrUnavailable, want: ErrUnavailable},
		{name: "short pepper", key: make([]byte, 31), want: ErrUnavailable},
		{name: "invalid account path", key: make([]byte, 32), record: tokenRecord{UID: "other"}, want: ErrUnauthorized},
		{name: "different account", key: make([]byte, 32), record: tokenRecord{UID: "0123456789abcdef"}, identity: Identity{"fedcba9876543210", "fixture@example.com"}, want: ErrUnavailable},
		{name: "missing email", key: make([]byte, 32), record: tokenRecord{UID: "0123456789abcdef"}, identity: Identity{UID: "0123456789abcdef"}, want: ErrUnavailable},
		{name: "invalid email", key: make([]byte, 32), record: tokenRecord{UID: "0123456789abcdef"}, identity: Identity{"0123456789abcdef", "missing-domain"}, want: ErrUnavailable},
		{name: "oversize email", key: make([]byte, 32), record: tokenRecord{UID: "0123456789abcdef"}, identity: Identity{"0123456789abcdef", strings.Repeat("a", 250) + "@example.com"}, want: ErrUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &memoryIdentityStore{rec: tc.record, identity: tc.identity}
			v := verifier{store: s, pepper: func(context.Context, string) ([]byte, error) { return tc.key, tc.keyErr }}
			if _, err := v.verify(context.Background(), token); !errors.Is(err, tc.want) {
				t.Fatalf("expected %v, got %v", tc.want, err)
			}
			if s.touches != 0 {
				t.Error("rejected identity updated token activity")
			}
		})
	}
}

func TestClientIPRejectsUntrustedHeaderSyntax(t *testing.T) {
	ctx := context.Background()
	for _, raw := range []string{"203.0.113.7, 127.0.0.1", "203.0.113.7\r\nX-Evil: yes", "example.com", ""} {
		if got := WithClientIP(ctx, raw); got != ctx {
			t.Errorf("accepted invalid resolved IP %q", raw)
		}
	}
	if got := WithClientIP(ctx, "2001:0db8::1").Value(clientIPKey{}); got != "2001:db8::1" {
		t.Fatalf("IPv6 was not normalized: %v", got)
	}
}
