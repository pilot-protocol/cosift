package community

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// Optional cross-repository check; ordinary tests never download Python or use GCP.
func TestMCPGatewayContract(t *testing.T) {
	checkout := os.Getenv("COSIFT_MCP_CHECKOUT")
	if checkout == "" {
		t.Skip("set COSIFT_MCP_CHECKOUT to a pinned, patched checkout with uv sync --frozen")
	}
	var hits atomic.Int32
	s := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if r.URL.Query().Get("k") != "20" || r.URL.Query().Get("retriever") != "bm25" {
			t.Error("lost MCP ranking parameters")
		}
		if r.Header.Get("Authorization") != "" {
			t.Error("credential reached engine")
		}
		_, _ = w.Write([]byte(`{"query":"rust","retriever":"bm25","hits":[]}`))
	}))
	s.cfg.Shared = &fakeShared{}
	s.cfg.SearchRPM = 1
	gateway := httptest.NewServer(s)
	defer gateway.Close()
	script, err := filepath.Abs("../../integrations/cosift-mcp/search_contract.py")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, filepath.Join(checkout, ".venv/bin/python"), script)
	cmd.Dir = checkout
	cmd.Env = append(os.Environ(), "COSIFT_CONTRACT_GATEWAY="+gateway.URL, "PYTHONPATH="+checkout)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("MCP contract: %v\n%s", err, out)
	}
	if hits.Load() != 2 {
		t.Fatalf("engine received %d requests, want two accounts once", hits.Load())
	}
	var balance int
	if err := s.db.QueryRow(`SELECT sum(delta) FROM credit_ledger`).Scan(&balance); err != nil || balance != 2*monthlyFreeCredits-2 {
		t.Fatalf("MCP requests did not debit both accounts: %d %v", balance, err)
	}
	t.Log(string(out))
}
