package articles

import (
	"net/http"
	"path/filepath"
	"testing"
)

func TestPromotionMinTierSetting(t *testing.T) {
	path := filepath.Join(t.TempDir(), "articles.json")
	for body, want := range map[string]string{
		`{"schema_version": 1, "theta_covered": 0.8, "theta_related": 0.7}`:                                 "ok",
		`{"schema_version": 1, "theta_covered": 0.8, "theta_related": 0.7, "promotion_min_tier": "ok"}`:     "ok",
		`{"schema_version": 1, "theta_covered": 0.8, "theta_related": 0.7, "promotion_min_tier": "strong"}`: "strong",
	} {
		writeThresholds(t, path, body)
		if got, err := LoadThresholds(path); err != nil || got.PromotionMinTier != want {
			t.Fatalf("%s: %+v, %v", body, got, err)
		}
	}
	for _, bad := range []string{`"thin"`, `""`, `"STRONG"`, `1`} {
		writeThresholds(t, path, `{"schema_version": 1, "theta_covered": 0.8, "theta_related": 0.7, "promotion_min_tier": `+bad+`}`)
		if _, err := LoadThresholds(path); err == nil {
			t.Fatalf("promotion_min_tier %s accepted", bad)
		}
	}
	if DefaultThresholds.PromotionMinTier != "ok" {
		t.Fatalf("default %q", DefaultThresholds.PromotionMinTier)
	}
}

func TestPromotionFollowsAReload(t *testing.T) {
	h := newHarness(t)
	h.put(synthProd, ulid(1), with(articleBody("Strong promoted article"), "quality_tier", "strong"), http.StatusCreated)
	h.put(synthProd, ulid(2), with(articleBody("Ok tier article"), "quality_tier", "ok"), http.StatusCreated)
	h.put(synthProd, ulid(3), with(articleBody("Thin tier article"), "quality_tier", "thin"), http.StatusCreated)
	h.put(synthProd, ulid(4), with(articleBody("Held strong article"), "quality_tier", "strong", "status", "held"), http.StatusCreated)
	check := func(minTier string, want map[string]bool) {
		t.Helper()
		listed := map[string]bool{}
		for _, it := range h.call(wiki, http.MethodGet, "/v1/articles?promoted=true", nil, http.StatusOK)["items"].([]any) {
			listed[it.(map[string]any)["id"].(string)] = true
		}
		for id, promoted := range want {
			if listed[id] != promoted {
				t.Fatalf("%s: %s listed as promoted = %v", minTier, id, listed[id])
			}
			if id == ulid(4) {
				continue
			}
			if got := h.call(wiki, http.MethodGet, "/v1/articles/"+id, nil, http.StatusOK)["promoted"]; got != promoted {
				t.Fatalf("%s: %s public promoted = %v", minTier, id, got)
			}
			if got := h.call(dashProd, http.MethodGet, "/v1/articles/"+id, nil, http.StatusOK)["promoted"]; got != promoted {
				t.Fatalf("%s: %s full promoted = %v", minTier, id, got)
			}
		}
		if got := h.call(dashProd, http.MethodGet, "/v1/articles/"+ulid(4), nil, http.StatusOK)["promoted"]; got != false {
			t.Fatalf("%s: a held article is promoted", minTier)
		}
	}
	check("default", map[string]bool{ulid(1): true, ulid(2): true, ulid(3): false, ulid(4): false})
	writeThresholds(t, h.dir+"/articles.json", `{"schema_version": 1, "theta_covered": 0.78, "theta_related": 0.74, "promotion_min_tier": "strong"}`)
	if err := h.s.ReloadThresholds(); err != nil {
		t.Fatal(err)
	}
	check("strong", map[string]bool{ulid(1): true, ulid(2): false, ulid(3): false, ulid(4): false})
	writeThresholds(t, h.dir+"/articles.json", `{"schema_version": 1, "theta_covered": 0.78, "theta_related": 0.74, "promotion_min_tier": "ok"}`)
	if err := h.s.ReloadThresholds(); err != nil {
		t.Fatal(err)
	}
	check("ok", map[string]bool{ulid(1): true, ulid(2): true, ulid(3): false, ulid(4): false})
}
