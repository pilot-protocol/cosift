package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/store"
	"github.com/pilot-protocol/cosift/internal/svcauth/svcauthtest"
)

// adminMux is the operator routes behind the admin gate, over a fixture's store.
func adminMux(t *testing.T, srv *pebbleHTTP) *http.ServeMux {
	t.Helper()
	srv.cluster.PeerAuthToken = "admin-secret"
	mux := http.NewServeMux()
	srv.registerWorkAdmin(mux, func(h http.HandlerFunc) http.HandlerFunc { return srv.requireAdmin(h) })
	return mux
}

func adminCall(mux *http.ServeMux, path, token, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("POST", path, strings.NewReader(body))
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)
	return w
}

func TestWorkPurgeRemovesAnAttestedDocumentAndKeepsACrawledOne(t *testing.T) {
	j, logs := workJoint(t)
	ctx := context.Background()
	mux := adminMux(t, j.srv)
	for _, d := range []store.Document{
		{URL: "https://forged.example.org/page", Source: "attested", VerifiedBy: "quorum"},
		{URL: "https://real.example.org/page", Source: "crawl"},
	} {
		d.Title, d.Text, d.FetchedAt = "T", "body text of the page", time.Now()
		if _, err := j.ps.UpsertDocument(ctx, &d); err != nil {
			t.Fatal(err)
		}
	}
	call := func(url string, apply bool) map[string]any {
		body, _ := json.Marshal(map[string]any{"url": url, "apply": apply})
		w := adminCall(mux, "/admin/work-purge", "admin-secret", string(body))
		if w.Code != 200 {
			t.Fatalf("%d %s", w.Code, w.Body)
		}
		var out map[string]any
		_ = json.Unmarshal(w.Body.Bytes(), &out)
		return out
	}
	dry := call("https://forged.example.org/page", false)
	if dry["documents_deleted"] != float64(1) || dry["documents_kept"] != float64(0) || dry["parked_until"] == nil {
		t.Fatalf("%v", dry)
	}
	if doc, err := j.ps.GetDocByURL(ctx, "https://forged.example.org/page"); err != nil || doc == nil {
		t.Fatal("a dry run deleted the document")
	}
	if strings.Contains(logs.String(), "work_purge") {
		t.Fatal("a dry run logged a purge")
	}
	done := call("https://forged.example.org/page", true)
	if done["documents_deleted"] != float64(1) {
		t.Fatalf("%v", done)
	}
	if _, err := j.ps.GetDocByURL(ctx, "https://forged.example.org/page"); err == nil {
		t.Fatal("the attested document survived")
	}
	kept := call("https://real.example.org/page", true)
	if kept["documents_deleted"] != float64(0) || kept["documents_kept"] != float64(1) {
		t.Fatalf("%v", kept)
	}
	if doc, err := j.ps.GetDocByURL(ctx, "https://real.example.org/page"); err != nil || doc.Source != "crawl" {
		t.Fatal("a crawled document was deleted")
	}
	out := logs.String()
	if !strings.Contains(out, "work_purge documents_deleted=1") {
		t.Fatalf("no fixed-code line: %s", out)
	}
	for _, leak := range []string{"forged.example.org", "real.example.org", "admin-secret"} {
		if strings.Contains(out, leak) {
			t.Errorf("the log carries %q", leak)
		}
	}
}

func TestWorkVoidAnswersItsCounts(t *testing.T) {
	j, logs := workJoint(t)
	mux := adminMux(t, j.srv)
	worker := strings.Repeat("a", 32)
	w := adminCall(mux, "/admin/work-void", "admin-secret", `{"worker":"`+worker+`","apply":true}`)
	if w.Code != 200 || strings.TrimSpace(w.Body.String()) != `{"records_changed":0,"attestations_dropped":0,"records_deleted":0,"urls_parked":0}` {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	if !strings.Contains(logs.String(), "work_void records_changed=0") || strings.Contains(logs.String(), worker) {
		t.Fatalf("%s", logs.String())
	}
}

func TestWorkAdminRoutesAreStrictAndGated(t *testing.T) {
	j, logs := workJoint(t)
	mux := adminMux(t, j.srv)
	for _, path := range []string{"/admin/work-purge", "/admin/work-void"} {
		for _, token := range []string{"", "wrong", j.keys["community-broker"], "old-community-admin"} {
			if w := adminCall(mux, path, token, `{}`); w.Code != 401 {
				t.Errorf("%s with token %q: %d", path, token, w.Code)
			}
		}
		for name, body := range map[string]string{
			"unknown member": `{"url":"https://a.example.org/x","worker":"` + strings.Repeat("a", 32) + `","extra":1}`,
			"trailing data":  `{"url":"https://a.example.org/x"} {}`,
			"not an object":  `[]`,
			"empty":          ``,
		} {
			if w := adminCall(mux, path, "admin-secret", body); w.Code != 400 {
				t.Errorf("%s %s: %d", path, name, w.Code)
			}
		}
		if w := adminCall(mux, path, "admin-secret", `{"url":"`+strings.Repeat("a", 5000)+`"}`); w.Code != 413 {
			t.Errorf("%s over 4 KiB: %d", path, w.Code)
		}
	}
	if w := adminCall(mux, "/admin/work-purge", "admin-secret", `{"url":"http://127.0.0.1/x"}`); w.Code != 400 {
		t.Errorf("an unusable URL: %d", w.Code)
	}
	if w := adminCall(mux, "/admin/work-void", "admin-secret", `{"worker":"nothex"}`); w.Code != 422 {
		t.Errorf("a bad worker id: %d", w.Code)
	}
	out := logs.String()
	if !strings.Contains(out, "work_admin_refused status=401") || !strings.Contains(out, "work_admin_refused status=400") || strings.Contains(out, "127.0.0.1/x") {
		t.Fatalf("%s", out)
	}
	j.srv.work = nil
	if w := adminCall(mux, "/admin/work-void", "admin-secret", `{"worker":"`+strings.Repeat("a", 32)+`"}`); w.Code != 503 || !strings.Contains(logs.String(), "work_admin_failed status=503") {
		t.Errorf("no service: %d", w.Code)
	}
}

// The operator routes live on :7777 only, and they work whether or not fetch work is on.
func TestWorkAdminRoutesAreOnThePrivateListenerOnly(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "service-auth.json")
	setV1Globals(t, cfgPath, filepath.Join(dir, "articles.json"), svcauthtest.NewSigner("k"))
	keys := jointKeys(t)
	v1Addr := freeAddr(t)
	cfgJSON, _ := json.Marshal(jointConfig(v1Addr, keys))
	if err := writeFile0640(cfgPath, cfgJSON); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{Cluster: config.Cluster{PeerAuthToken: "peer-secret"}, Server: config.Server{AdminToken: "admin-secret"}}
	mainAddr, _ := serveProcess(t, cfg, filepath.Join(dir, "pebble"))
	post := func(addr, path, token, body string) (int, string) {
		req, _ := http.NewRequest("POST", "http://"+addr+path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		b, _ := io.ReadAll(res.Body)
		return res.StatusCode, string(b)
	}
	body := `{"url":"https://a.example.org/x"}`
	for _, path := range []string{"/admin/work-purge", "/admin/work-void"} {
		if path == "/admin/work-void" {
			body = `{"worker":"` + strings.Repeat("a", 32) + `"}`
		}
		for _, token := range []string{"", "admin-secret", keys["community-broker"], keys["dash-prod"]} {
			if code, _ := post(mainAddr, path, token, body); code != 401 {
				t.Errorf("%s with %.6q on :7777: %d", path, token, code)
			}
		}
		if code, out := post(mainAddr, path, "peer-secret", body); code != 200 {
			t.Errorf("%s with the admin gate's token while work is off: %d %s", path, code, out)
		}
		for _, token := range []string{keys["community-broker"], keys["dash-prod"], "peer-secret", "admin-secret"} {
			if code, _ := post(v1Addr, path, token, body); code != 404 {
				t.Errorf("%s on the /v1 listener with %.6q: %d", path, token, code)
			}
		}
	}
}

func writeFile0640(path string, b []byte) error {
	if err := os.WriteFile(path, b, 0o640); err != nil {
		return err
	}
	return os.Chmod(path, 0o640)
}
