package main

import (
	"context"
	"strings"
	"testing"
)

func runCommunityOnce(t *testing.T) error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return runCommunity(ctx, []string{"-addr", "127.0.0.1:0", "-data-dir", t.TempDir()})
}

func TestCommunityRefusesToStartWhileTheOldAdminTokenIsSet(t *testing.T) {
	t.Setenv("COSIFT_COMMUNITY_ADMIN_TOKEN", "peer-secret-value")
	err := runCommunityOnce(t)
	if err == nil || !strings.Contains(err.Error(), "COSIFT_COMMUNITY_ADMIN_TOKEN") || strings.Contains(err.Error(), "peer-secret-value") {
		t.Fatalf("got %v", err)
	}
}

func TestCommunityBrokerEnvIsValidatedAtStart(t *testing.T) {
	t.Setenv("COSIFT_COMMUNITY_ADMIN_TOKEN", "")
	t.Setenv("COSIFT_COMMUNITY_BROKER_KEY", "not-an-engine-key-value")
	err := runCommunityOnce(t)
	if err == nil || !strings.Contains(err.Error(), "COSIFT_COMMUNITY_BROKER_KEY") || strings.Contains(err.Error(), "not-an-engine-key-value") {
		t.Fatalf("key: got %v", err)
	}
	t.Setenv("COSIFT_COMMUNITY_BROKER_KEY", "")
	t.Setenv("COSIFT_COMMUNITY_ENGINE_URL", "https://example.com")
	if err := runCommunityOnce(t); err == nil || !strings.Contains(err.Error(), "COSIFT_COMMUNITY_ENGINE_URL") {
		t.Fatalf("url: got %v", err)
	}
}

func TestCommunityStartsWithoutABrokerKey(t *testing.T) {
	t.Setenv("COSIFT_COMMUNITY_ADMIN_TOKEN", "")
	t.Setenv("COSIFT_COMMUNITY_BROKER_KEY", "")
	t.Setenv("COSIFT_COMMUNITY_ENGINE_URL", "")
	t.Setenv("COSIFT_WIKI_PUBLIC", "0")
	if err := runCommunityOnce(t); err != nil {
		t.Fatalf("got %v", err)
	}
}
