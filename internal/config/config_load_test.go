package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDefaultHasSensibleValues(t *testing.T) {
	c := Default()
	if c == nil {
		t.Fatal("Default returned nil")
	}
	if c.DataDir == "" {
		t.Errorf("DataDir empty")
	}
	if c.Server.Addr == "" {
		t.Errorf("Server.Addr empty")
	}
	if c.Crawler.UserAgent == "" {
		t.Errorf("Crawler.UserAgent empty")
	}
	if c.Crawler.MaxConcurrent <= 0 {
		t.Errorf("MaxConcurrent should be > 0, got %d", c.Crawler.MaxConcurrent)
	}
	if c.Crawler.MaxBodyBytes <= 0 {
		t.Errorf("MaxBodyBytes should be > 0, got %d", c.Crawler.MaxBodyBytes)
	}
	if !c.Crawler.RespectRobots {
		t.Errorf("RespectRobots should default true")
	}
}

func TestLoadMissingFileReturnsDefaults(t *testing.T) {
	// Save & restore env vars we touch.
	for _, k := range []string{"PORT", "COSIFT_LISTEN", "COSIFT_DATA_DIR"} {
		orig, had := os.LookupEnv(k)
		os.Unsetenv(k)
		defer func(k, v string, had bool) {
			if had {
				os.Setenv(k, v)
			} else {
				os.Unsetenv(k)
			}
		}(k, orig, had)
	}

	path := filepath.Join(t.TempDir(), "does-not-exist.json")
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load missing file: %v", err)
	}
	if cfg.DataDir != Default().DataDir {
		t.Errorf("DataDir not default: %q", cfg.DataDir)
	}
	if cfg.Server.Addr != Default().Server.Addr {
		t.Errorf("Server.Addr not default: %q", cfg.Server.Addr)
	}
}

func TestLoadFromFile(t *testing.T) {
	for _, k := range []string{"PORT", "COSIFT_LISTEN", "COSIFT_DATA_DIR"} {
		orig, had := os.LookupEnv(k)
		os.Unsetenv(k)
		defer func(k, v string, had bool) {
			if had {
				os.Setenv(k, v)
			} else {
				os.Unsetenv(k)
			}
		}(k, orig, had)
	}

	body := []byte(`{
		"data_dir": "/tmp/cosift-test",
		"server": {"addr": "127.0.0.1:9999"},
		"crawler": {"max_concurrent": 16}
	}`)
	dir := t.TempDir()
	path := filepath.Join(dir, "cosift.json")
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.DataDir != "/tmp/cosift-test" {
		t.Errorf("DataDir: got %q", cfg.DataDir)
	}
	if cfg.Server.Addr != "127.0.0.1:9999" {
		t.Errorf("Addr: got %q", cfg.Server.Addr)
	}
	if cfg.Crawler.MaxConcurrent != 16 {
		t.Errorf("MaxConcurrent: got %d", cfg.Crawler.MaxConcurrent)
	}
}

func TestLoadBadJSONErrors(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(path, []byte("{not json"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := Load(path); err == nil {
		t.Errorf("expected error for malformed JSON, got nil")
	}
}

func TestLoadEmptyDataDirFallsBackToDefault(t *testing.T) {
	for _, k := range []string{"PORT", "COSIFT_LISTEN", "COSIFT_DATA_DIR"} {
		orig, had := os.LookupEnv(k)
		os.Unsetenv(k)
		defer func(k, v string, had bool) {
			if had {
				os.Setenv(k, v)
			} else {
				os.Unsetenv(k)
			}
		}(k, orig, had)
	}

	body := []byte(`{"data_dir": "", "server": {"addr": "x:1"}}`)
	dir := t.TempDir()
	path := filepath.Join(dir, "c.json")
	_ = os.WriteFile(path, body, 0o644)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.DataDir == "" {
		t.Errorf("empty DataDir should fall back to Default")
	}
	if cfg.DataDir != Default().DataDir {
		t.Errorf("DataDir: got %q want %q", cfg.DataDir, Default().DataDir)
	}
}

func TestApplyEnvOverridesPort(t *testing.T) {
	orig, had := os.LookupEnv("PORT")
	os.Setenv("PORT", "8765")
	t.Cleanup(func() {
		if had {
			os.Setenv("PORT", orig)
		} else {
			os.Unsetenv("PORT")
		}
	})

	cfg := Default()
	applyEnvOverrides(cfg)
	if cfg.Server.Addr != "0.0.0.0:8765" {
		t.Errorf("PORT override: got %q want 0.0.0.0:8765", cfg.Server.Addr)
	}
}

func TestApplyEnvOverridesCosiftListen(t *testing.T) {
	orig, had := os.LookupEnv("COSIFT_LISTEN")
	os.Setenv("COSIFT_LISTEN", "10.0.0.1:1234")
	t.Cleanup(func() {
		if had {
			os.Setenv("COSIFT_LISTEN", orig)
		} else {
			os.Unsetenv("COSIFT_LISTEN")
		}
	})

	cfg := Default()
	applyEnvOverrides(cfg)
	if cfg.Server.Addr != "10.0.0.1:1234" {
		t.Errorf("COSIFT_LISTEN override: got %q", cfg.Server.Addr)
	}
}

func TestApplyEnvOverridesDataDir(t *testing.T) {
	orig, had := os.LookupEnv("COSIFT_DATA_DIR")
	os.Setenv("COSIFT_DATA_DIR", "/custom/path")
	t.Cleanup(func() {
		if had {
			os.Setenv("COSIFT_DATA_DIR", orig)
		} else {
			os.Unsetenv("COSIFT_DATA_DIR")
		}
	})

	cfg := Default()
	applyEnvOverrides(cfg)
	if cfg.DataDir != "/custom/path" {
		t.Errorf("COSIFT_DATA_DIR override: got %q", cfg.DataDir)
	}
}

func TestApplyEnvOverridesEmptyIgnored(t *testing.T) {
	orig, had := os.LookupEnv("PORT")
	os.Setenv("PORT", "   ")
	t.Cleanup(func() {
		if had {
			os.Setenv("PORT", orig)
		} else {
			os.Unsetenv("PORT")
		}
	})

	cfg := Default()
	addr := cfg.Server.Addr
	applyEnvOverrides(cfg)
	if cfg.Server.Addr != addr {
		t.Errorf("whitespace-only PORT should be ignored, addr changed from %q to %q", addr, cfg.Server.Addr)
	}
}

func TestLoadDotEnvMissingFileNoError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nope.env")
	if err := LoadDotEnv(path); err != nil {
		t.Errorf("missing .env: %v", err)
	}
}

func TestLoadDotEnvBasic(t *testing.T) {
	const key = "COSIFT_DOTENV_TEST_KEY_1"
	const val = "hello-world"
	os.Unsetenv(key)
	t.Cleanup(func() { os.Unsetenv(key) })

	body := []byte(key + "=" + val + "\n")
	path := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := LoadDotEnv(path); err != nil {
		t.Fatalf("LoadDotEnv: %v", err)
	}
	if got := os.Getenv(key); got != val {
		t.Errorf("env value: got %q want %q", got, val)
	}
}

func TestLoadDotEnvQuotedAndComments(t *testing.T) {
	keys := map[string]string{
		"COSIFT_DOTENV_DOUBLE": "double-quoted-value",
		"COSIFT_DOTENV_SINGLE": "single-quoted-value",
		"COSIFT_DOTENV_BARE":   "bare",
	}
	for k := range keys {
		os.Unsetenv(k)
		k := k
		t.Cleanup(func() { os.Unsetenv(k) })
	}
	body := []byte(`# a comment line
COSIFT_DOTENV_DOUBLE="double-quoted-value"
COSIFT_DOTENV_SINGLE='single-quoted-value'
COSIFT_DOTENV_BARE=bare

# blank line above and below comment

malformed_line_no_equals
`)
	path := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := LoadDotEnv(path); err != nil {
		t.Fatalf("LoadDotEnv: %v", err)
	}
	for k, want := range keys {
		if got := os.Getenv(k); got != want {
			t.Errorf("%s: got %q want %q", k, got, want)
		}
	}
}

func TestLoadDotEnvDoesNotOverrideExisting(t *testing.T) {
	const key = "COSIFT_DOTENV_EXISTS"
	const preset = "preset-wins"
	os.Setenv(key, preset)
	t.Cleanup(func() { os.Unsetenv(key) })

	body := []byte(key + "=from-dotenv\n")
	path := filepath.Join(t.TempDir(), ".env")
	_ = os.WriteFile(path, body, 0o644)

	if err := LoadDotEnv(path); err != nil {
		t.Fatalf("LoadDotEnv: %v", err)
	}
	if got := os.Getenv(key); got != preset {
		t.Errorf("existing env was overridden: got %q want %q", got, preset)
	}
}
