package crawler

import (
	"context"
	"errors"
	"testing"
)

func TestContributionNoveltyWaitsForCompleteIndex(t *testing.T) {
	page := pagePlain("fresh article nobody has indexed")
	c, ps, _ := pebbleContributor(t, map[string]string{"https://news.example/a": page, "https://news.example/b": pagePlain("a second fresh article")})
	before := contribute(t, c, "https://news.example/a", page)
	if before.Novel || before.ContentIndexComplete {
		t.Fatalf("incomplete index must not vouch for novelty: %+v", before)
	}
	completeContentIndex(t, ps)
	after := contribute(t, c, "https://news.example/b", pagePlain("a second fresh article"))
	if !after.Novel || !after.ContentIndexComplete {
		t.Fatalf("complete index: %+v", after)
	}
}

func TestContributionNoveltyIsRecordedBeforeTheWrite(t *testing.T) {
	page := pagePlain("text for the recorder")
	c, ps, _ := pebbleContributor(t, map[string]string{"https://news.example/rec": page})
	completeContentIndex(t, ps)
	var got []NoveltyDecision
	ctx := WithNoveltyRecorder(context.Background(), func(d NoveltyDecision) error {
		if doc, err := ps.GetDocByURL(context.Background(), "https://news.example/rec"); err == nil && doc != nil {
			t.Error("verdict recorded after the document was written")
		}
		got = append(got, d)
		return nil
	})
	r, err := c.FetchContribution(ctx, "https://news.example/rec", nil, approvedForHTML(t, "https://news.example/rec", page))
	if err != nil || !r.Novel || len(got) != 1 || !got[0].Novel || got[0].FinalURL != "https://news.example/rec" {
		t.Fatalf("receipt %+v err %v decisions %+v", r, err, got)
	}
}

func TestContributionRecorderFailureAbortsTheWrite(t *testing.T) {
	page := pagePlain("text for the failing recorder")
	c, ps, _ := pebbleContributor(t, map[string]string{"https://news.example/rec": page})
	boom := errors.New("journal unavailable")
	ctx := WithNoveltyRecorder(context.Background(), func(NoveltyDecision) error { return boom })
	if _, err := c.FetchContribution(ctx, "https://news.example/rec", nil, approvedForHTML(t, "https://news.example/rec", page)); !errors.Is(err, boom) {
		t.Fatalf("want the recorder error, got %v", err)
	}
	if doc, err := ps.GetDocByURL(context.Background(), "https://news.example/rec"); err == nil && doc != nil {
		t.Fatal("document written although the verdict could not be recorded")
	}
}

func TestContributionWithoutContentIndexIsNeverNovel(t *testing.T) {
	page := sampleHTML
	routes := map[string]string{"https://news.example/sqlite": page}
	c, _, _ := redirectFixture(t, routes, nil)
	r, err := c.FetchContribution(context.Background(), "https://news.example/sqlite", nil, approvedFor(t, "https://news.example/sqlite"))
	if err != nil || !r.Indexed || r.Novel || r.ContentIndexComplete {
		t.Fatalf("store without a content index: %+v %v", r, err)
	}
}
