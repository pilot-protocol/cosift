package articles

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

// addRedirect stores a non-canonical slug, as a later rename route would.
func addRedirect(h *harness, slug, id string) {
	h.t.Helper()
	v, _ := json.Marshal(slugEntry{ID: id, Canonical: false})
	if err := h.ps.DB().Set(slugKey(slug), v, pebble.Sync); err != nil {
		h.t.Fatal(err)
	}
}

func TestBySlugResolution(t *testing.T) {
	h := newHarness(t)
	h.put(synthProd, ulid(1), articleBody("Published article title"), http.StatusCreated)
	h.put(synthProd, ulid(2), with(articleBody("Held article title"), "status", "held"), http.StatusCreated)
	h.put(synthProd, ulid(3), articleBody("Rejected article title"), http.StatusCreated)
	h.moderate(dashProd, ulid(3), "reject", "quality", http.StatusOK)
	h.put(synthProd, ulid(4), articleBody("Tombstoned article title"), http.StatusCreated)
	h.moderate(dashProd, ulid(4), "tombstone", "privacy", http.StatusOK)
	h.put(synthStaging, ulid(5), articleBody("Prelive article title"), http.StatusCreated)
	h.moderate(dashStaging, ulid(5), "tombstone", "privacy", http.StatusOK)
	h.call(resolverStaging, http.MethodPut, "/v1/articles/"+ulid(6), stubBodyFor("Prelive stub title", topic(6)), http.StatusCreated)
	addRedirect(h, "old-published", ulid(1))
	addRedirect(h, "old-held", ulid(2))
	addRedirect(h, "old-tombstoned", ulid(4))
	h.call(resolverProd, http.MethodPut, "/v1/articles/"+ulid(7), stubBodyFor("Prod stub title", topic(7)), http.StatusCreated)
	addRedirect(h, "old-prod-stub", ulid(7))
	addRedirect(h, "old-prelive-stub", ulid(6))
	h.restart()

	type want struct {
		prod, staging int
	}
	cases := map[string]want{
		"published-article-title":  {200, 200},
		"held-article-title":       {404, 404},
		"rejected-article-title":   {404, 404},
		"tombstoned-article-title": {410, 410},
		"prelive-article-title":    {404, 410},
		"prelive-stub-title":       {404, 200},
		"old-published":            {200, 200},
		"old-held":                 {404, 404},
		"old-tombstoned":           {200, 200},
		"old-prod-stub":            {200, 200},
		"old-prelive-stub":         {404, 200},
		"never-existed":            {404, 404},
	}
	for slug, w := range cases {
		if rec := h.do(wiki, http.MethodGet, "/v1/articles/by-slug/"+slug, nil); rec.Code != w.prod {
			t.Errorf("prod %s: %d", slug, rec.Code)
		}
		if rec := h.do(mcpStaging, http.MethodGet, "/v1/articles/by-slug/"+slug, nil); rec.Code != w.staging {
			t.Errorf("staging %s: %d", slug, rec.Code)
		}
		if slug != "never-existed" {
			if rec := h.do(dashProd, http.MethodGet, "/v1/articles/by-slug/"+slug, nil); rec.Code != 200 {
				t.Errorf("read_all %s: %d", slug, rec.Code)
			}
		}
	}
	if out := h.call(wiki, http.MethodGet, "/v1/articles/by-slug/old-published", nil, http.StatusOK); len(out) != 1 || out["redirect_slug"] != "published-article-title" {
		t.Errorf("redirect = %v", out)
	}
	if out := h.call(dashProd, http.MethodGet, "/v1/articles/by-slug/held-article-title", nil, http.StatusOK); out["status"] != "held" || out["readers_7d"] == nil {
		t.Errorf("read_all held = %v", out)
	}
	for _, bad := range []string{"Upper", "a--b", "-a", "a_b", "x%2e"} {
		if rec := h.do(wiki, http.MethodGet, "/v1/articles/by-slug/"+bad, nil); rec.Code != 404 {
			t.Errorf("slug %q: %d", bad, rec.Code)
		}
	}
	for _, bad := range []string{"01J8ZC2Q7W4X9M3K5N6P8R000I", "01j8zc2q7w4x9m3k5n6p8r0001", "short"} {
		h.expectError(dashProd, http.MethodGet, "/v1/articles/"+bad, nil, http.StatusNotFound, "not_found")
	}
}

func TestListFiltersAndCursor(t *testing.T) {
	h := newHarness(t)
	verticals := []string{"dev-docs", "research", "other"}
	h.emb.set("Listed article c", axis(1))
	h.emb.set("reader q", mix(1, 0, 0.95))
	for i := range 7 {
		h.clock.advance(time.Minute)
		tier := "strong"
		if i%2 == 1 {
			tier = "ok"
		}
		h.put(synthProd, ulid(i+1), with(articleBody(fmt.Sprintf("Listed article %c", 'b'+i)), "vertical", verticals[i%3], "quality_tier", tier), http.StatusCreated)
	}
	h.put(synthProd, ulid(20), with(articleBody("Held listed article"), "status", "held"), http.StatusCreated)

	var slugs []string
	cursor := ""
	for page := 0; ; page++ {
		path := "/v1/articles?limit=3"
		if cursor != "" {
			path += "&cursor=" + url.QueryEscape(cursor)
		}
		out := h.call(wiki, http.MethodGet, path, nil, http.StatusOK)
		for _, it := range out["items"].([]any) {
			m := it.(map[string]any)
			if _, ok := m["prelive"]; ok {
				t.Error("read items carry prelive")
			}
			slugs = append(slugs, m["slug"].(string))
		}
		next, _ := out["next_cursor"].(string)
		if next == "" {
			break
		}
		cursor = next
		if page > 5 {
			t.Fatal("paging does not end")
		}
	}
	if len(slugs) != 7 || slugs[0] != "listed-article-b" || slugs[6] != "listed-article-h" {
		t.Errorf("slug pages = %v", slugs)
	}
	upd := h.call(wiki, http.MethodGet, "/v1/articles?order=updated_at&limit=2", nil, http.StatusOK)
	if items := upd["items"].([]any); items[0].(map[string]any)["slug"] != "listed-article-h" {
		t.Errorf("updated_at order = %v", items)
	}
	h.expectError(wiki, http.MethodGet, "/v1/articles?order=slug&cursor="+url.QueryEscape(upd["next_cursor"].(string)), nil, http.StatusBadRequest, "invalid_cursor")
	h.expectError(wiki, http.MethodGet, "/v1/articles?cursor=garbage", nil, http.StatusBadRequest, "invalid_cursor")
	if items := h.call(wiki, http.MethodGet, "/v1/articles?promoted=true&vertical=dev-docs", nil, http.StatusOK)["items"].([]any); len(items) != 3 {
		t.Errorf("promoted dev-docs = %v", items)
	}
	since := h.clock.Now().Add(-90 * time.Second).UTC().Format(time.RFC3339)
	if items := h.call(wiki, http.MethodGet, "/v1/articles?updated_since="+url.QueryEscape(since), nil, http.StatusOK)["items"].([]any); len(items) != 2 {
		t.Errorf("updated_since = %v", items)
	}
	for _, q := range []string{"status=held", "status=published,held", "prelive=true", "limit=0", "limit=501", "order=title", "promoted=yes", "vertical=news", "updated_since=yesterday", "bogus=1"} {
		h.expectError(wiki, http.MethodGet, "/v1/articles?"+q, nil, http.StatusBadRequest, "invalid_param")
	}
	h.call(wiki, http.MethodGet, "/v1/articles?status=published", nil, http.StatusOK)
	if items := h.call(dashProd, http.MethodGet, "/v1/articles?status=held", nil, http.StatusOK)["items"].([]any); len(items) != 1 || items[0].(map[string]any)["prelive"] != false {
		t.Errorf("read_all held = %v", items)
	}

	for _, r := range []string{"a", "b"} {
		h.match(mcpProd, map[string]any{"q": "reader q", "reader": strings.Repeat(r, 32)}, http.StatusOK)
	}
	top := h.call(wiki, http.MethodGet, "/v1/articles?order=readers_7d&limit=2", nil, http.StatusOK)["items"].([]any)
	if top[0].(map[string]any)["slug"] != "listed-article-c" || top[0].(map[string]any)["readers_7d"] != 2.0 || top[1].(map[string]any)["slug"] != "listed-article-b" {
		t.Errorf("readers order = %v", top)
	}
}

func TestReadScopes(t *testing.T) {
	h := newHarness(t)
	h.put(synthProd, ulid(1), articleBody("Rust async runtimes"), http.StatusCreated)
	for _, path := range []string{"/v1/articles/" + ulid(1), "/v1/articles/by-slug/rust-async-runtimes", "/v1/articles"} {
		if out := h.expectError(goliveAdmin, http.MethodGet, path, nil, http.StatusForbidden, "missing_scope"); out["detail"] != "requires scope articles:read or articles:read_all" {
			t.Errorf("%s: %v", path, out)
		}
	}
	if out := h.expectError(dashProd, http.MethodPost, "/v1/articles/match", map[string]any{"q": "Rust async runtimes"}, http.StatusForbidden, "missing_scope"); out["detail"] != "requires scope articles:read" {
		t.Errorf("read_all granted match: %v", out)
	}
	h.expectError(wiki, http.MethodGet, "/v1/articles/stats", nil, http.StatusForbidden, "missing_scope")
}

func TestStatsOrphanKeys(t *testing.T) {
	h := newHarness(t)
	h.put(synthProd, ulid(1), with(articleBody("Rust async runtimes"), "topic_ids", []string{topic(1)}), http.StatusCreated)
	orphans := func() float64 {
		return h.call(dashProd, http.MethodGet, "/v1/articles/stats", nil, http.StatusOK)["orphan_keys"].(float64)
	}
	if n := orphans(); n != 0 {
		t.Fatalf("orphan_keys = %v", n)
	}
	db := h.ps.DB()
	_ = db.Set(topicKey(topic(9)), []byte(ulid(9)), pebble.Sync)
	_ = db.Set(countersKey(ulid(9)), []byte(`{"v":1,"days":{}}`), pebble.Sync)
	if n := orphans(); n != 2 {
		t.Errorf("two orphan keys counted as %v", n)
	}
	b := db.NewBatch()
	rec := &Record{Schema: 1, ID: ulid(8), Slug: "committed-not-indexed", BaseSlug: "committed-not-indexed", Title: "Committed not indexed", Status: StatusPending, TopicIDs: []string{topic(8)}}
	v, _ := rec.MarshalJSON()
	_ = b.Set(recordKey(ulid(8)), v, nil)
	e, _ := json.Marshal(slugEntry{ID: ulid(8), Canonical: true})
	_ = b.Set(slugKey("committed-not-indexed"), e, nil)
	_ = b.Set(topicKey(topic(8)), []byte(ulid(8)), nil)
	if err := b.Commit(pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if n := orphans(); n != 2 {
		t.Errorf("a committed record the index has not applied yet counted as orphans: %v", n)
	}
}

func TestStatsDuringWritesReportsNoOrphans(t *testing.T) {
	h := newHarness(t)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := range 4 {
		wg.Go(func() {
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				id := ulid(1000*(w+1) + i)
				h.do(resolverProd, http.MethodPut, "/v1/articles/"+id, stubBodyFor(fmt.Sprintf("Concurrent stub %d", 1000*(w+1)+i), topic(1000*(w+1)+i)))
				if i%2 == 0 {
					h.do(resolverProd, http.MethodDelete, "/v1/articles/"+id, nil)
				}
			}
		})
	}
	for range 150 {
		if n := h.call(dashProd, http.MethodGet, "/v1/articles/stats", nil, http.StatusOK)["orphan_keys"]; n != 0.0 {
			t.Errorf("orphan_keys = %v during writes", n)
			break
		}
	}
	close(stop)
	wg.Wait()
}

func TestListUpdatedSinceIsInclusive(t *testing.T) {
	h := newHarness(t)
	a := article(h.put(synthProd, ulid(1), articleBody("Rust async runtimes"), http.StatusCreated))
	since := url.QueryEscape(a["updated_at"].(string))
	if items := h.call(wiki, http.MethodGet, "/v1/articles?updated_since="+since, nil, http.StatusOK)["items"].([]any); len(items) != 1 {
		t.Errorf("updated_since equal to updated_at: %v", items)
	}
}
