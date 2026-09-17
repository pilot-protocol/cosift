package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCommunityCreditsCLIIncludesModeCosts(t *testing.T) {
	session := isolateInstalledCommunitySession(t)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/credits" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		w.Write([]byte(`{"balance":1000,"monthly_free_credits":1000,"request_credit_costs":{"search":1,"answer":2,"research":3}}`))
	}))
	defer backend.Close()
	writeInstalledCommunitySession(t, session, backend.URL, "installed-token", time.Now().Add(time.Hour))
	output, err := os.Create(filepath.Join(t.TempDir(), "output.json"))
	if err != nil {
		t.Fatal(err)
	}
	defer output.Close()
	previous := os.Stdout
	os.Stdout = output
	defer func() { os.Stdout = previous }()
	if err := runContribute(context.Background(), []string{"-credits"}); err != nil {
		t.Fatal(err)
	}
	if _, err := output.Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	var got struct {
		Balance int
		Costs   map[string]int `json:"request_credit_costs"`
	}
	if err := json.NewDecoder(output).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got.Balance != 1000 || got.Costs["search"] != 1 || got.Costs["answer"] != 2 || got.Costs["research"] != 3 {
		t.Fatalf("CLI omitted mode cost metadata: %+v", got)
	}
}
