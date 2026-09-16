package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestIsCredentialKey(t *testing.T) {
	yes := []string{"OPENAI", "OPENAI_API_KEY", "COSIFT_CHAT_API_KEY", "COHERE_APIKEY", "COSIFT_ADMIN_TOKEN", "DD_SECRET", "PGPASSWORD"}
	no := []string{"COSIFT_DATA_DIR", "PORT", "COSIFT_RATELIMIT_RPM", "OPENAI_BASE_URL"}
	for _, k := range yes {
		if !isCredentialKey(k) {
			t.Errorf("isCredentialKey(%q) = false, want true", k)
		}
	}
	for _, k := range no {
		if isCredentialKey(k) {
			t.Errorf("isCredentialKey(%q) = true, want false", k)
		}
	}
}

// loadDotEnv must report credential-shaped keys even when the value is already
// in the environment (the hygiene problem is the file, not the assignment).
func TestLoadDotEnvReportsCredentialKeys(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".env")
	body := "# comment\nCOSIFT_DOTENV_HYGIENE_TEST_API_KEY=sk-proj-xxx\nCOSIFT_DOTENV_HYGIENE_TEST_DIR=/tmp\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	t.Setenv("COSIFT_DOTENV_HYGIENE_TEST_API_KEY", "already-set")

	got, err := loadDotEnv(path)
	if err != nil {
		t.Fatalf("loadDotEnv: %v", err)
	}
	if len(got) != 1 || got[0] != "COSIFT_DOTENV_HYGIENE_TEST_API_KEY" {
		t.Errorf("secrets = %v, want just the API-key entry", got)
	}
}

func TestLoadDotEnvNoCredentialKeys(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(path, []byte("COSIFT_DOTENV_PLAIN_TEST=1\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	t.Cleanup(func() { os.Unsetenv("COSIFT_DOTENV_PLAIN_TEST") })
	got, err := loadDotEnv(path)
	if err != nil {
		t.Fatalf("loadDotEnv: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("secrets = %v, want none", got)
	}
}
