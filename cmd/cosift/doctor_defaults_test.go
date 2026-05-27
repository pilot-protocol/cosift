package main

import (
	"strings"
	"testing"

	"github.com/pilot-protocol/cosift/internal/config"
)

// findCheck returns the first check matching name, or zero value if absent.
func findCheck(checks []doctorCheck, name string) doctorCheck {
	for _, c := range checks {
		if c.Name == name {
			return c
		}
	}
	return doctorCheck{}
}

func TestDoctorDefaultsAllZero(t *testing.T) {
	cks := doctorDefaultsChecks(config.Defaults{}, true, true)
	if len(cks) != 1 {
		t.Fatalf("zero defaults: want 1 check (just summary), got %d: %+v", len(cks), cks)
	}
	if cks[0].Status != "PASS" || cks[0].Name != "defaults" {
		t.Errorf("zero defaults summary: %+v", cks[0])
	}
}

func TestDoctorDefaultsAllSetCapabilitiesOK(t *testing.T) {
	d := config.Defaults{
		Retriever:        "hybrid",
		Expand:           true,
		ResearchStrategy: "paraphrase",
	}
	cks := doctorDefaultsChecks(d, true, true) // both capabilities wired
	if len(cks) != 1 {
		t.Errorf("all-set + capable: want only the PASS summary, got %d: %+v", len(cks), cks)
	}
	summary := findCheck(cks, "defaults")
	if summary.Status != "PASS" {
		t.Errorf("summary status: %+v", summary)
	}
	if summary.Detail == "" || !strings.Contains(summary.Detail, "hybrid") || !strings.Contains(summary.Detail, "paraphrase") {
		t.Errorf("summary detail should mention configured defaults: %q", summary.Detail)
	}
}

// Each WARN path is triggered ONLY when its capability is missing.
func TestDoctorDefaultsWarnsOnMissingEmbed(t *testing.T) {
	d := config.Defaults{Retriever: "hybrid"}
	cks := doctorDefaultsChecks(d, false /* no embed */, true)
	w := findCheck(cks, "defaults.retriever")
	if w.Status != "WARN" {
		t.Errorf("hybrid w/o embedder should WARN; got %+v", w)
	}
	if !strings.Contains(w.Detail, "embeddings model") {
		t.Errorf("WARN detail should mention embeddings: %q", w.Detail)
	}
}

func TestDoctorDefaultsWarnsOnMissingChatForExpand(t *testing.T) {
	d := config.Defaults{Expand: true}
	cks := doctorDefaultsChecks(d, true, false /* no chat */)
	w := findCheck(cks, "defaults.expand")
	if w.Status != "WARN" {
		t.Errorf("expand=true w/o chat should WARN; got %+v", w)
	}
}

func TestDoctorDefaultsWarnsOnMissingChatForParaphrase(t *testing.T) {
	d := config.Defaults{ResearchStrategy: "paraphrase"}
	cks := doctorDefaultsChecks(d, true, false /* no chat */)
	w := findCheck(cks, "defaults.research_strategy")
	if w.Status != "WARN" {
		t.Errorf("paraphrase w/o chat should WARN; got %+v", w)
	}
}

// FAIL paths block deploy via non-zero exit. Unknown retriever and strategy
// values are the main risks (typo in cosift.json).
func TestDoctorDefaultsFailsOnUnknownRetriever(t *testing.T) {
	d := config.Defaults{Retriever: "elasticsearch-but-not-really"}
	cks := doctorDefaultsChecks(d, true, true)
	f := findCheck(cks, "defaults.retriever")
	if f.Status != "FAIL" {
		t.Errorf("unknown retriever should FAIL; got %+v", f)
	}
	if !strings.Contains(f.Detail, "valid: bm25, dense, hybrid") {
		t.Errorf("FAIL detail should list valid values: %q", f.Detail)
	}
}

func TestDoctorDefaultsFailsOnUnknownStrategy(t *testing.T) {
	d := config.Defaults{ResearchStrategy: "tree-of-thought-or-something"}
	cks := doctorDefaultsChecks(d, true, true)
	f := findCheck(cks, "defaults.research_strategy")
	if f.Status != "FAIL" {
		t.Errorf("unknown strategy should FAIL; got %+v", f)
	}
	if !strings.Contains(f.Detail, "valid: planner, paraphrase") {
		t.Errorf("FAIL detail should list valid values: %q", f.Detail)
	}
}

// bm25 explicitly named (not just empty) should NOT warn — it doesn't need
// embeddings. Locks the historic-default behavior in.
func TestDoctorDefaultsExplicitBM25NoWarn(t *testing.T) {
	d := config.Defaults{Retriever: "bm25"}
	cks := doctorDefaultsChecks(d, false /* no embed */, true)
	if len(cks) != 1 {
		t.Errorf("bm25 should not WARN on missing embedder; got %+v", cks)
	}
}

// iter-62 ResearchSynthK: zero is the "use default" sentinel and must NOT
// trigger the all-zero summary path (we want it to be a no-op zero, not
// "ignore this field"). PASS+detail with synth_k=0 is the right behavior.
func TestDoctorDefaultsSynthKZero(t *testing.T) {
	d := config.Defaults{ResearchSynthK: 0, Retriever: "hybrid"}
	cks := doctorDefaultsChecks(d, true, true)
	// summary should include synth_k in detail
	if !strings.Contains(findCheck(cks, "defaults").Detail, "research_synth_k=0") {
		t.Errorf("summary should include research_synth_k=0; got %q", findCheck(cks, "defaults").Detail)
	}
}

func TestDoctorDefaultsSynthKPositive(t *testing.T) {
	d := config.Defaults{ResearchSynthK: 5}
	cks := doctorDefaultsChecks(d, true, true)
	if !strings.Contains(findCheck(cks, "defaults").Detail, "research_synth_k=5") {
		t.Errorf("summary should include synth_k=5; got %q", findCheck(cks, "defaults").Detail)
	}
	// no WARN/FAIL expected for a valid positive value
	if findCheck(cks, "defaults.research_synth_k").Status != "" {
		t.Errorf("synth_k=5 should not WARN or FAIL: %+v", findCheck(cks, "defaults.research_synth_k"))
	}
}

func TestDoctorDefaultsSynthKNegative(t *testing.T) {
	d := config.Defaults{ResearchSynthK: -3}
	cks := doctorDefaultsChecks(d, true, true)
	if findCheck(cks, "defaults.research_synth_k").Status != "FAIL" {
		t.Errorf("negative synth_k should FAIL: %+v", findCheck(cks, "defaults.research_synth_k"))
	}
}

// Multiple WARN paths can co-occur — both capability checks should fire.
func TestDoctorDefaultsMultipleWarns(t *testing.T) {
	d := config.Defaults{
		Retriever:        "dense",
		Expand:           true,
		ResearchStrategy: "paraphrase",
	}
	cks := doctorDefaultsChecks(d, false, false) // neither capability wired
	if len(cks) != 4 {
		t.Errorf("want 1 summary + 3 WARNs; got %d: %+v", len(cks), cks)
	}
	for _, name := range []string{"defaults.retriever", "defaults.expand", "defaults.research_strategy"} {
		if findCheck(cks, name).Status != "WARN" {
			t.Errorf("%s should WARN; got %+v", name, findCheck(cks, name))
		}
	}
}

