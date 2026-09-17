package community

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

type browserSharedFixture struct {
	fakeShared
	endpoint string
}

func (f *browserSharedFixture) Call(ctx context.Context, token, tool string, args map[string]any) (map[string]any, error) {
	data, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": map[string]any{"name": tool, "arguments": args}})
	req, _ := http.NewRequestWithContext(ctx, "POST", f.endpoint, bytes.NewReader(data))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	var envelope struct {
		Result struct{ Content []struct{ Text string } }
	}
	if err = json.NewDecoder(res.Body).Decode(&envelope); err != nil {
		return nil, err
	}
	if len(envelope.Result.Content) != 1 {
		return nil, fmt.Errorf("fixture MCP response")
	}
	var payload map[string]any
	err = json.Unmarshal([]byte(envelope.Result.Content[0].Text), &payload)
	return payload, err
}

// Manual local browser fixture, never compiled into the shipped binary. Uses
// fabricated auth and the real MCP protocol/tools with in-memory topic storage.
func TestSharedBrowserFixture(t *testing.T) {
	endpoint := os.Getenv("COSIFT_BROWSER_MCP_FIXTURE")
	if endpoint == "" {
		t.Skip("manual browser fixture")
	}
	if endpoint != "http://127.0.0.1:17981/v1/mcp" {
		t.Fatal("fixture must be the dedicated loopback MCP endpoint")
	}
	s := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"query":"Rust","hits":[{"title":"Rust documentation","url":"https://doc.rust-lang.org/book/","excerpt":"A local fixture result for browser verification."}]}`))
	}))
	s.cfg.Shared = &browserSharedFixture{endpoint: endpoint}
	srv := httptest.NewServer(s)
	defer srv.Close()
	s.cfg.PublicURL = srv.URL
	fmt.Printf("SHARED_BROWSER_FIXTURE=%s (fabricated email auth; any six-digit test code)\n", srv.URL)
	time.Sleep(8 * time.Minute)
}
