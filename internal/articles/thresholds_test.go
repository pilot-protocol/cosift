package articles

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble"

	v1 "github.com/pilot-protocol/cosift/internal/v1"
)

func TestMain(m *testing.M) {
	thresholdsOwner = uint32(os.Getuid())
	os.Exit(m.Run())
}

func writeThresholds(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o640); err != nil {
		t.Fatal(err)
	}
}

func TestLoadThresholds(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "articles.json")
	if got, err := LoadThresholds(path); err != nil || got != DefaultThresholds {
		t.Fatalf("absent = %+v, %v", got, err)
	}
	writeThresholds(t, path, `{"schema_version": 1, "theta_covered": 0.8, "theta_related": 0.7, "measured": true}`)
	if got, err := LoadThresholds(path); err != nil || got != (Thresholds{0.8, 0.7, true}) {
		t.Fatalf("valid = %+v, %v", got, err)
	}
	bad := []string{
		`{"schema_version": 2, "theta_covered": 0.8, "theta_related": 0.7}`,
		`{"theta_covered": 0.8, "theta_related": 0.7}`,
		`{"schema_version": 1, "theta_covered": 0.8}`,
		`{"schema_version": 1, "theta_covered": 0.7, "theta_related": 0.7}`,
		`{"schema_version": 1, "theta_covered": 1.1, "theta_related": 0.7}`,
		`{"schema_version": 1, "theta_covered": 0.8, "theta_related": 0}`,
		`{"schema_version": 1, "theta_covered": 0.8, "theta_related": 0.7, "extra": 1}`,
		`{"schema_version": 1, "theta_covered": 0.8, "theta_related": 0.7} {}`,
		`not json`,
	}
	for _, b := range bad {
		writeThresholds(t, path, b)
		if _, err := LoadThresholds(path); err == nil {
			t.Errorf("accepted %s", b)
		}
	}
	writeThresholds(t, path, `{"schema_version": 1, "theta_covered": 1, "theta_related": 0.5}`)
	if _, err := LoadThresholds(path); err != nil {
		t.Errorf("covered = 1 refused: %v", err)
	}
	for _, mode := range []os.FileMode{0o660, 0o644, 0o641} {
		if err := os.Chmod(path, mode); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadThresholds(path); err == nil {
			t.Errorf("mode %o accepted", mode)
		}
	}
	_ = os.Chmod(path, 0o600)
	saved := thresholdsOwner
	thresholdsOwner = saved + 1
	_, err := LoadThresholds(path)
	thresholdsOwner = saved
	if err == nil {
		t.Error("file of another owner accepted")
	}
}

func TestReloadKeepsPreviousOnInvalid(t *testing.T) {
	h := newHarness(t)
	path := h.dir + "/articles.json"
	writeThresholds(t, path, `{"schema_version": 1, "theta_covered": 0.9, "theta_related": 0.8, "measured": true}`)
	if err := h.s.ReloadThresholds(); err != nil {
		t.Fatal(err)
	}
	writeThresholds(t, path, `{"schema_version": 1, "theta_covered": 0.5, "theta_related": 0.8}`)
	if err := h.s.ReloadThresholds(); err == nil {
		t.Fatal("invalid file reloaded")
	}
	if got := h.s.th(); got != (Thresholds{0.9, 0.8, true}) {
		t.Errorf("thresholds after an invalid reload = %+v", got)
	}
	stats := h.call(dashProd, "GET", "/v1/articles/stats", nil, 200)
	if th := stats["thresholds"].(map[string]any); th["covered"] != 0.9 || th["related"] != 0.8 {
		t.Errorf("stats thresholds = %v", th)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := h.s.ReloadThresholds(); err != nil {
		t.Fatalf("reload of an absent file: %v", err)
	}
	if got := h.s.th(); got != DefaultThresholds {
		t.Errorf("thresholds after the file was removed = %+v", got)
	}
}

func TestInvalidThresholdsAtStartupLogError(t *testing.T) {
	db, err := pebble.Open(t.TempDir(), &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	path := filepath.Join(t.TempDir(), "articles.json")
	open := func() ([]string, *Store) {
		var lines []string
		s, err := Open(Options{DB: db, Policy: v1.NewFakePolicy(), ThresholdsPath: path, Logf: func(f string, a ...any) { lines = append(lines, fmt.Sprintf(f, a...)) }})
		if err != nil {
			t.Fatal(err)
		}
		return lines, s
	}
	if lines, s := open(); len(lines) != 0 || s.th() != DefaultThresholds {
		t.Errorf("absent file: %q, %+v", lines, s.th())
	}
	writeThresholds(t, path, `{"schema_version": 1, "theta_covered": 0.7, "theta_related": 0.8}`)
	lines, s := open()
	if len(lines) != 1 || !strings.Contains(lines[0], "ERROR") || !strings.Contains(lines[0], path) || !strings.Contains(lines[0], "theta_related < theta_covered") {
		t.Errorf("log = %q", lines)
	}
	if s.th() != DefaultThresholds {
		t.Errorf("thresholds = %+v", s.th())
	}
}
