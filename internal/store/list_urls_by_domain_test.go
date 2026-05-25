package store

import (
	"context"
	"sort"
	"testing"
	"time"
)

// seedDocsByDomain inserts docs spanning multiple domains and subdomains so
// the iter-79 suffix-on-dot-boundary semantics get exercised.
func seedDocsByDomain(t *testing.T) *Store {
	t.Helper()
	// Iter 134: OpenMemory.
	s, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	docs := []Document{
		{URL: "https://example.com/a", Domain: "example.com", Title: "A", Text: "a", Source: "test", FetchedAt: time.Now()},
		{URL: "https://blog.example.com/b", Domain: "blog.example.com", Title: "B", Text: "b", Source: "test", FetchedAt: time.Now()},
		{URL: "https://deep.blog.example.com/c", Domain: "deep.blog.example.com", Title: "C", Text: "c", Source: "test", FetchedAt: time.Now()},
		{URL: "https://evilexample.com/d", Domain: "evilexample.com", Title: "D", Text: "d", Source: "test", FetchedAt: time.Now()},
		{URL: "https://otherdomain.org/e", Domain: "otherdomain.org", Title: "E", Text: "e", Source: "test", FetchedAt: time.Now()},
	}
	for i := range docs {
		if _, err := s.UpsertDocument(context.Background(), &docs[i]); err != nil {
			t.Fatalf("upsert %d: %v", i, err)
		}
	}
	return s
}

func TestListURLsByDomainExactAndSubdomains(t *testing.T) {
	s := seedDocsByDomain(t)
	got, err := s.ListURLsByDomain(context.Background(), "example.com")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	sort.Strings(got)
	want := []string{
		"https://blog.example.com/b",
		"https://deep.blog.example.com/c",
		"https://example.com/a",
	}
	if len(got) != len(want) {
		t.Fatalf("want %d urls, got %d: %+v", len(want), len(got), got)
	}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("url %d: got %q, want %q", i, got[i], w)
		}
	}
}

func TestListURLsByDomainRejectsAdversarialMatch(t *testing.T) {
	// `example.com` should NOT match `evilexample.com` (the classic
	// suffix-without-boundary adversarial case from iter 79).
	s := seedDocsByDomain(t)
	got, err := s.ListURLsByDomain(context.Background(), "example.com")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	for _, u := range got {
		if u == "https://evilexample.com/d" {
			t.Errorf("evilexample.com leaked through: %s", u)
		}
	}
}

func TestListURLsByDomainCaseInsensitive(t *testing.T) {
	s := seedDocsByDomain(t)
	got, err := s.ListURLsByDomain(context.Background(), "EXAMPLE.COM")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(got) == 0 {
		t.Errorf("uppercase pattern should still match: got %+v", got)
	}
}

func TestListURLsByDomainEmpty(t *testing.T) {
	s := seedDocsByDomain(t)
	_, err := s.ListURLsByDomain(context.Background(), "")
	if err == nil {
		t.Fatal("empty pattern should error")
	}
}

func TestListURLsByDomainUnknown(t *testing.T) {
	// Pattern matches nothing in the corpus → empty slice, no error.
	s := seedDocsByDomain(t)
	got, err := s.ListURLsByDomain(context.Background(), "nonexistent.io")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("unmatched pattern should return empty, got %+v", got)
	}
}
