package sharedaccount

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"golang.org/x/oauth2"
)

func TestRemoteContractsAndCredentialSeparation(t *testing.T) {
	const token = "ck_1_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Serverless-Authorization") != "Bearer fake-google-identity" {
			t.Error("missing infrastructure credential")
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if r.URL.Path == "/auth/start" {
			if r.Header.Get("X-Forwarded-For") != "203.0.113.7" {
				t.Error("lost gateway client IP")
			}
			if r.Header.Get("Authorization") != "" || body["email"] != "test@example.com" {
				t.Error("start contract")
			}
			_ = json.NewEncoder(w).Encode(Challenge{"01ARZ3NDEKTSV4RRFFQ69G5FAV", time.Now().Add(time.Minute)})
			return
		}
		if r.URL.Path == "/auth/verify" {
			if body["code"] != "123456" {
				t.Error("verify contract")
			}
			_ = json.NewEncoder(w).Encode(Issued{token, "0123456789abcdef"})
			return
		}
		if r.Header.Get("Authorization") != "Bearer "+token {
			t.Error("account credential lost")
		}
		if r.URL.Path == "/auth/revoke" {
			_, _ = w.Write([]byte(`{"revoked":true}`))
			return
		}
		if body["method"] != "tools/call" || r.Header.Get("MCP-Protocol-Version") == "" {
			t.Error("MCP contract")
		}
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"error":null,"result":{"isError":false,"content":[{"type":"text","text":"{\"action\":\"list\",\"topics\":[]}"}]}}`))
	}))
	defer upstream.Close()
	remote, err := newRemote(context.Background(), upstream.URL, "", false)
	if err != nil {
		t.Fatal(err)
	}
	remote.identity = oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "fake-google-identity"})
	c := Client{auth: remote, mcp: remote}
	if _, err = c.Start(WithClientIP(context.Background(), "203.0.113.7"), "test@example.com"); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Finish(context.Background(), "01ARZ3NDEKTSV4RRFFQ69G5FAV", "123456"); err != nil {
		t.Fatal(err)
	}
	if err = c.Revoke(context.Background(), token); err != nil {
		t.Fatal(err)
	}
	if out, err := c.Call(context.Background(), token, "cosift_topics", map[string]any{"action": "list"}); err != nil || out["action"] != "list" {
		t.Fatal(out, err)
	}
	if _, err := c.Call(context.Background(), token, "arbitrary_tool", nil); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
}
func TestRemoteRefusesRedirectsOversizeAndUnsafeOrigins(t *testing.T) {
	for _, raw := range []string{"http://example.com", "https://user:pass@example.com", "https://example.com?token=secret", "https://example.com/path"} {
		if _, err := newRemote(context.Background(), raw, "", false); err == nil {
			t.Error("accepted", raw)
		}
	}
	status := 302
	body := ""
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Location", "/stolen")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()
	r, _ := newRemote(context.Background(), srv.URL, "", false)
	var out map[string]any
	if err := r.call(context.Background(), "", "sensitive-token", nil, &out); !errors.Is(err, ErrUnavailable) || calls != 1 {
		t.Fatal("followed credential redirect", err, calls)
	}
	status = 200
	body = strings.Repeat("x", (1<<20)+1)
	if err := r.call(context.Background(), "", "", nil, &out); !errors.Is(err, ErrUnavailable) {
		t.Fatal(err)
	}
	body = `{}`
	for code, want := range map[int]error{401: ErrUnavailable, 403: ErrUnavailable, 429: ErrLimited, 500: ErrUnavailable} {
		status = code
		if err := r.call(context.Background(), "", "", nil, &out); !errors.Is(err, want) {
			t.Fatal(code, err)
		}
	}
}

// Exercise real FastMCP envelopes, topic arguments, and the stateless protocol.
func TestMCPRemoteWireContract(t *testing.T) {
	raw := os.Getenv("COSIFT_MCP_CONTRACT_URL")
	if raw == "" {
		t.Skip("start integrations/cosift-mcp/remote_contract_server.py and set COSIFT_MCP_CONTRACT_URL")
	}
	if raw != "http://127.0.0.1:17981/v1/mcp" {
		t.Fatal("use the dedicated local MCP fixture")
	}
	r, err := newRemote(context.Background(), raw, "", true)
	if err != nil {
		t.Fatal(err)
	}
	c := Client{mcp: r}
	token := "ck_1_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	if _, err = c.Call(context.Background(), token, "cosift_topics", map[string]any{"action": "add", "topics": []string{"Rust contract test"}}); err != nil {
		t.Fatal(err)
	}
	out, err := c.Call(context.Background(), token, "cosift_topics", map[string]any{"action": "list"})
	if err != nil {
		t.Fatal(err)
	}
	if len(out["topics"].([]any)) != 1 {
		t.Fatal("topic not shared", out)
	}
	out, err = c.Call(context.Background(), token, "cosift_request", map[string]any{"topic": "Rust contract test"})
	if err != nil || out["status"] != "requested" {
		t.Fatal("request contract", out, err)
	}
	out, err = c.Call(context.Background(), token, "cosift_request", map[string]any{"topic": "Rust contract test"})
	if err != nil || out["status"] != "already_requested" {
		t.Fatal("idempotency contract", out, err)
	}
	out, err = c.Call(context.Background(), token, "cosift_lookup", map[string]any{"topic": "Rust contract test"})
	if err != nil || out["coverage"] != "none" || out["requested"] != true {
		t.Fatal("lookup coverage contract", out, err)
	}
	if _, err = c.Call(context.Background(), token, "cosift_topics", map[string]any{"action": "remove", "topics": []string{"Rust contract test"}}); err != nil {
		t.Fatal(err)
	}
}
