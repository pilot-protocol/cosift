package articles

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestTitleVectors(t *testing.T) {
	b, err := os.ReadFile("testdata/title-vectors-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var vectors []struct {
		Input  string `json:"input"`
		Field  string `json:"field"`
		Accept bool   `json:"accept"`
		Rule   string `json:"rule"`
		Slug   string `json:"slug"`
	}
	if err := json.Unmarshal(b, &vectors); err != nil {
		t.Fatal(err)
	}
	if len(vectors) != 106 {
		t.Fatalf("%d vectors, want 106", len(vectors))
	}
	for _, v := range vectors {
		got := CheckTitle(v.Input, v.Field == "alias")
		want := v.Rule
		if v.Accept {
			want = ""
		}
		if got != want {
			t.Errorf("CheckTitle(%q) = %q, want %q", v.Input, got, want)
		}
		if v.Accept && v.Field != "alias" {
			if s := Slugify(v.Input); s != v.Slug {
				t.Errorf("Slugify(%q) = %q, want %q", v.Input, s, v.Slug)
			}
		}
	}
}

func TestAssignSlug(t *testing.T) {
	a80 := strings.Repeat("a", 80)
	a77bb := strings.Repeat("a", 77) + "-bb"
	fox := "the-quick-brown-fox-jumps-over-the-lazy-dog-while-the-lazy-dog-sleeps-under-a"
	cases := []struct {
		base  string
		taken []string
		want  string
	}{
		{"rust-async-runtimes", nil, "rust-async-runtimes"},
		{"rust-async-runtimes", []string{"rust-async-runtimes"}, "rust-async-runtimes-2"},
		{"rust-async-runtimes", []string{"rust-async-runtimes", "rust-async-runtimes-2"}, "rust-async-runtimes-3"},
		{"index", nil, "index-2"},
		{a80, []string{a80}, strings.Repeat("a", 78) + "-2"},
		{a77bb, []string{a77bb}, strings.Repeat("a", 77) + "-2"},
		{fox, []string{fox}, fox + "-2"},
	}
	for _, c := range cases {
		taken := set(c.taken...)
		if got := assignSlug(c.base, func(s string) bool { return taken[s] }); got != c.want {
			t.Errorf("assignSlug(%q, %v) = %q, want %q", c.base, c.taken, got, c.want)
		}
	}
	for s := range reservedSlugs {
		if got := assignSlug(s, func(string) bool { return false }); got != s+"-2" {
			t.Errorf("reserved %q assigned %q", s, got)
		}
	}
}

func TestFullSlugSkipsCut(t *testing.T) {
	long := "The quick brown fox jumps over the lazy dog while the lazy dog sleeps under a very tall tree in the old park"
	if Slugify(long) == fullSlug(long) {
		t.Fatal("the cut did not apply")
	}
	if !strings.HasPrefix(fullSlug(long), Slugify(long)) || !strings.HasSuffix(fullSlug(long), "-old-park") {
		t.Errorf("fullSlug = %q", fullSlug(long))
	}
}
