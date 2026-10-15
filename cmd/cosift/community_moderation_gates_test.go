package main

import (
	"context"
	"strings"
	"testing"

	"github.com/pilot-protocol/cosift/internal/community"
)

func TestCommunityModerationDecidesObviousCasesWithoutTheModel(t *testing.T) {
	long := strings.Repeat("A neutral paragraph about reliable software systems. ", 5)
	for name, tc := range map[string]struct {
		doc  community.ModerationDocument
		want community.ModerationVerdict
	}{
		"adult host": {community.ModerationDocument{URL: "https://www.pornhub.com/view", Title: "Some Video", Text: long},
			community.ModerationVerdict{Decision: "reject", Category: "adult"}},
		"adult content on a neutral host": {community.ModerationDocument{URL: "https://generic.example.net/g", Title: "Gallery", Text: "creampie and gangbang scenes"},
			community.ModerationVerdict{Decision: "reject", Category: "adult"}},
		"bot-check page": {community.ModerationDocument{URL: "https://example.com/check", Title: "Just a moment", Text: long},
			community.ModerationVerdict{Decision: "uncertain", Category: "unverified"}},
		"parked domain": {community.ModerationDocument{URL: "https://example.com/parked", Title: "Domain for sale", Text: long},
			community.ModerationVerdict{Decision: "reject", Category: "low_quality"}},
	} {
		t.Run(name, func(t *testing.T) {
			s := &pebbleHTTP{}
			reserved := false
			got, err := s.moderateDocument(context.Background(), tc.doc, func() error { reserved = true; return nil })
			if err != nil || got != tc.want {
				t.Fatalf("verdict %+v, %v; want %+v", got, err, tc.want)
			}
			if reserved {
				t.Fatal("a document decided without the model still took from its budget")
			}
		})
	}
}
