package wiki

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	stubQuery = "status=pending&order=updated_at&limit=200"
	thinQuery = "status=published&tier=thin&order=updated_at&limit=200"
)

func stubItem(slug, title string) map[string]any {
	return map[string]any{"id": fixtureID(), "slug": slug, "title": title, "vertical": "other", "promoted": false, "status": "pending", "version": 0,
		"updated_at": "2026-10-07T08:00:00Z", "readers_7d": 0}
}

func thinItem(slug, title string) map[string]any {
	return map[string]any{"id": fixtureID(), "slug": slug, "title": title, "vertical": "dev-docs", "quality_tier": "thin", "promoted": false, "status": "published",
		"version": 2, "updated_at": "2026-10-06T10:00:00Z", "readers_7d": 3}
}

func boardReply(next any, items ...map[string]any) reply {
	if items == nil {
		items = []map[string]any{}
	}
	return jsonReply(200, map[string]any{"items": items, "next_cursor": next})
}

func wantedHarness(t *testing.T) *harness {
	h := newHarness(t)
	h.v1.setQuery(stubQuery, boardReply(nil, stubItem("tidal-power", "Tidal power"), stubItem("volcano-monitoring", "Volcano monitoring")))
	h.v1.setQuery(thinQuery, boardReply(nil, thinItem("rust-async-runtimes", "Rust async runtimes")))
	return h
}

func TestWantedBoardListsStubsByTitleAndLowArticlesWithALink(t *testing.T) {
	h := wantedHarness(t)
	rec := h.get("/wiki/wanted")
	wantPage(t, rec, 200, public(300))
	doc := structure(t, "wanted", rec.Body.String())
	main := find(doc, tag("main"))[0]
	lists := find(main, tag("ul"))
	if len(lists) != 2 {
		t.Fatalf("%d lists", len(lists))
	}
	stubs := find(lists[0], tag("li"))
	if len(stubs) != 2 || strings.TrimSpace(textOf(stubs[0])) != "Tidal power Other" || len(find(lists[0], tag("a"))) != 0 {
		t.Fatalf("stubs %q", textOf(lists[0]))
	}
	thin := find(lists[1], tag("a"))
	if len(thin) != 1 || attr(thin[0], "href") != "/wiki/rust-async-runtimes" || textOf(thin[0]) != "Rust async runtimes" {
		t.Fatal("the Low article is not linked")
	}
	body := rec.Body.String()
	for _, secret := range []string{"tidal-power", "volcano-monitoring", "readers"} {
		if strings.Contains(body, secret) {
			t.Fatalf("a stub's %q reached the page", secret)
		}
	}
	if h.v1.queryCalls(stubQuery) != 1 || h.v1.queryCalls(thinQuery) != 1 {
		t.Fatalf("engine queries %v", h.v1.allCalls())
	}
}

func TestWantedBoardDropsItemsThatFailTheirListsTest(t *testing.T) {
	h := newHarness(t)
	published := with(thinItem("not-a-stub", "Published in the stub list"), "status", "published")
	badSlug := with(stubItem("Bad_Slug", "Bad slug"), "slug", "Bad_Slug")
	noTitle := with(stubItem("no-title", "   "), "title", "  ‮ ")
	badID := with(stubItem("bad-id", "Bad id"), "id", "not-a-ulid")
	h.v1.setQuery(stubQuery, boardReply(nil, published, badSlug, noTitle, badID, stubItem("kept-stub", "Kept stub")))
	strong := with(thinItem("strong-one", "Strong in the thin list"), "quality_tier", "strong")
	pending := with(stubItem("pending-one", "Pending in the thin list"), "quality_tier", "thin")
	h.v1.setQuery(thinQuery, boardReply(nil, strong, pending, thinItem("kept-thin", "Kept thin")))
	body := h.get("/wiki/wanted").Body.String()
	for _, gone := range []string{"Published in the stub list", "Bad slug", "Bad id", "Strong in the thin list", "Pending in the thin list"} {
		if strings.Contains(body, gone) {
			t.Fatalf("%q was shown", gone)
		}
	}
	for _, kept := range []string{"Kept stub", "Kept thin"} {
		if !strings.Contains(body, kept) {
			t.Fatalf("%q was dropped", kept)
		}
	}
}

func TestWantedBoardEscapesTitlesAndStripsControls(t *testing.T) {
	h := newHarness(t)
	h.v1.setQuery(stubQuery, boardReply(nil, stubItem("x-one", "<script>alert(1)</script> ‮RTL​")))
	h.v1.setQuery(thinQuery, boardReply(nil, thinItem("x-two", `"><img src=x onerror=alert(1)>`)))
	body := h.get("/wiki/wanted").Body.String()
	structure(t, "wanted", body)
	if strings.Contains(body, "<script>alert") || strings.Contains(body, "<img") || strings.ContainsAny(body, "‮​") || !strings.Contains(body, "&lt;script&gt;alert(1)&lt;/script&gt; RTL") {
		t.Fatalf("unsafe title in\n%s", body)
	}
}

func TestWantedBoardIsEmptyAndCappedNotes(t *testing.T) {
	h := newHarness(t)
	h.v1.setQuery(stubQuery, boardReply(nil))
	h.v1.setQuery(thinQuery, boardReply("next"))
	body := h.get("/wiki/wanted").Body.String()
	if strings.Count(body, "None right now.") != 2 || strings.Count(body, "Showing the newest 200.") != 1 {
		t.Fatalf("empty and cap notes wrong in\n%s", body)
	}
	if doc := structure(t, "wanted", body); len(find(doc, tag("h1"))) != 1 {
		t.Fatal("heading")
	}
}

func TestWantedBoardIsCachedForFiveMinutesAndSharedByConcurrentMisses(t *testing.T) {
	h := wantedHarness(t)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if rec := h.get("/wiki/wanted", "RemoteAddr", fmt.Sprintf("192.0.2.%d:1", i+1)); rec.Code != 200 {
				t.Errorf("status %d", rec.Code)
			}
		}(i)
	}
	wg.Wait()
	if h.v1.queryCalls(stubQuery) != 1 || h.v1.queryCalls(thinQuery) != 1 {
		t.Fatalf("%d and %d engine calls", h.v1.queryCalls(stubQuery), h.v1.queryCalls(thinQuery))
	}
	h.clock.Add(100 * time.Second)
	wantPage(t, h.get("/wiki/wanted"), 200, public(200))
	h.clock.Add(200 * time.Second)
	wantPage(t, h.get("/wiki/wanted"), 200, public(300))
	if h.v1.queryCalls(stubQuery) != 2 {
		t.Fatalf("%d engine calls after the bound", h.v1.queryCalls(stubQuery))
	}
}

func TestWantedBoardFailsClosedAndRecovers(t *testing.T) {
	h := wantedHarness(t)
	h.v1.setQuery(thinQuery, errReply(500, "internal"))
	wantPage(t, h.get("/wiki/wanted"), 503, "no-store")
	h.clock.Add(time.Second)
	h.v1.setQuery(thinQuery, jsonReply(200, map[string]any{"items": []any{with(thinItem("x-three", "Over scoped"), "prelive", map[string]any{"environment": "staging"})}, "next_cursor": nil}))
	wantPage(t, h.get("/wiki/wanted"), 503, "no-store")
	h.v1.setQuery(thinQuery, boardReply(nil))
	h.clock.Add(time.Second)
	wantPage(t, h.get("/wiki/wanted"), 200, public(300))
}

func TestWantedBoardServesNoStaleBoardPastItsBound(t *testing.T) {
	h := wantedHarness(t)
	h.get("/wiki/wanted")
	h.v1.setQuery(stubQuery, errReply(500, "internal"))
	h.clock.Add(boardBound)
	wantPage(t, h.get("/wiki/wanted"), 503, "no-store")
}

func TestWantedBoardMissesAreChargedToTheClient(t *testing.T) {
	h := wantedHarness(t)
	h.clock.Add(0)
	for i := 0; i < ipLimit; i++ {
		h.w.ips.allow(clientKey("192.0.2.9"), h.clock.Now())
	}
	rec := h.get("/wiki/wanted", "RemoteAddr", "192.0.2.9:1")
	if rec.Code != 429 || h.v1.count() != 0 {
		t.Fatalf("%d with %d engine calls", rec.Code, h.v1.count())
	}
	if h.get("/wiki/wanted", "RemoteAddr", "192.0.2.10:1").Code != 200 {
		t.Fatal("another client was refused")
	}
	h.get("/wiki/wanted", "RemoteAddr", "192.0.2.9:1")
}

func TestGoldenWantedPage(t *testing.T) {
	checkGolden(t, "wanted.html", wantedHarness(t).get("/wiki/wanted").Body.String())
}
