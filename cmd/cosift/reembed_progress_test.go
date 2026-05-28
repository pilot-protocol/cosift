package main

import (
	"context"
	"strings"
	"testing"

	"github.com/pilot-protocol/cosift/internal/config"
)

// TestRunReembedAcceptsProgressFlag verifies the flag-wiring without
// actually calling the embedder API. Reembed has no other test coverage (it
// requires OpenAI API), so this is the minimum smoke test for the structural
// change: -progress parses without error, and execution proceeds to the API
// key check (where it errors with the expected message).
func TestRunReembedAcceptsProgressFlag(t *testing.T) {
	cfg := config.Default()
	cfg.Embeddings.Model = "text-embedding-3-small" // bypass the "model required" check
	// Force no API key so we hit the API-key check, not a real embedder call.
	t.Setenv("OPENAI_API_KEY", "")
	t.Setenv("OPENAI", "")

	err := runReembed(context.Background(), cfg, []string{"-progress", "1ms"})
	if err == nil {
		t.Fatal("expected error from missing API key")
	}
	// The error must be the API-key one, NOT a flag-parsing error.
	if strings.Contains(err.Error(), "flag provided but not defined") {
		t.Fatalf("flag wiring broken: %v", err)
	}
	if !strings.Contains(err.Error(), "OPENAI") {
		t.Errorf("expected API-key error, got: %v", err)
	}
}

func TestRunReembedRejectsInvalidProgressDuration(t *testing.T) {
	cfg := config.Default()
	cfg.Embeddings.Model = "text-embedding-3-small"
	// "5" without a unit is invalid for time.Duration. flag.ExitOnError would
	// normally os.Exit; in tests we get the err back because flag uses CommandLine
	// shape... actually `flag.NewFlagSet(..., flag.ExitOnError)` DOES call os.Exit.
	// So this test would crash the test process. Better: confirm valid syntax
	// works, and trust go's time.ParseDuration tests for the parser.
	t.Skip("flag.ExitOnError would terminate the test process; valid-duration paths covered by other tests")
}

func TestRunReembedDisabledProgress(t *testing.T) {
	// -progress 0 should still parse correctly.
	cfg := config.Default()
	cfg.Embeddings.Model = "text-embedding-3-small"
	t.Setenv("OPENAI_API_KEY", "")
	t.Setenv("OPENAI", "")
	err := runReembed(context.Background(), cfg, []string{"-progress", "0"})
	if err == nil {
		t.Fatal("expected API-key error")
	}
	if strings.Contains(err.Error(), "flag provided but not defined") {
		t.Errorf("flag wiring broken: %v", err)
	}
}
