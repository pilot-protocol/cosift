package main

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"
)

const jointCid = "0f1e2d3c4b5a6978"

func jointContribution(title, cid string, kv ...any) map[string]any {
	b := jointArticle(title)
	b["contributor"] = cid
	for i := 0; i+1 < len(kv); i += 2 {
		b[kv[i].(string)] = kv[i+1]
	}
	return b
}

func corpusCounts(t *testing.T, j *joint) map[string]any {
	t.Helper()
	body, err := j.srv.buildStatsBody(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var all map[string]any
	if err := json.Unmarshal(body, &all); err != nil {
		t.Fatal(err)
	}
	out := map[string]any{}
	for _, k := range []string{"documents", "indexed_docs", "sum_doc_len", "avg_doc_len", "terms"} {
		out[k] = all[k]
	}
	return out
}

func TestJointContributionAndRevert(t *testing.T) {
	j := newJoint(t, nil, nil)
	counts := corpusCounts(t, j)
	id := jointID(1)

	j.want(j.do("intake-prod", "PUT", "/v1/articles/"+id, jointContribution("Raft consensus explained", jointCid)), 201, "")
	j.want(j.do("intake-prod", "PUT", "/v1/articles/"+id, jointContribution("Raft consensus explained", jointCid)), 200, "")
	r := j.do("intake-prod", "PUT", "/v1/articles/"+id, jointContribution("Raft consensus explained", "ffffffffffffffff", "lead", "A revised lead. It differs.", "expected_version", 1))
	j.want(r, 200, "")
	if r.body["result"] != "updated" {
		t.Fatalf("update = %s", r.raw)
	}

	full := j.do("dash-prod", "GET", "/v1/articles/"+id, nil)
	j.want(full, 200, "")
	if full.body["origin"] != "community" || full.body["version_contributor"] != "ffffffffffffffff" || len(full.body["contributors"].([]any)) != 2 {
		t.Fatalf("full = %s", full.raw)
	}
	pub := j.do("community-wiki", "GET", "/v1/articles/"+id, nil)
	j.want(pub, 200, "")
	if pub.body["origin"] != "community" || strings.Contains(pub.raw, "version_contributor") || strings.Contains(pub.raw, "reverts_to") {
		t.Fatalf("public = %s", pub.raw)
	}

	j.want(j.do("synth-prod", "PUT", "/v1/articles/"+id, jointContribution("Raft consensus explained", jointCid)), 422, "invalid_field")
	j.want(j.do("mcp-prod", "PUT", "/v1/articles/"+id, jointContribution("Raft consensus explained", jointCid)), 403, "missing_scope")
	j.want(j.do("intake-prod", "PUT", "/v1/articles/"+jointID(2), jointStub("Contributors write no stubs", "8624225e00000001")), 403, "missing_scope")
	j.want(j.do("intake-prod", "POST", "/v1/articles/"+id+"/revert", map[string]any{"to_version": 1, "reason_code": "quality", "by": "x"}), 403, "missing_scope")
	j.want(j.do("community-wiki", "POST", "/v1/articles/"+id+"/revert", map[string]any{"to_version": 1, "reason_code": "quality", "by": "x"}), 403, "missing_scope")
	j.want(j.do("intake-prod", "POST", "/v1/articles/"+id+"/status", moderation("hold")), 403, "missing_scope")
	j.want(j.do("intake-prod", "POST", "/v1/articles/match", map[string]any{"q": "Raft consensus explained", "purpose": "build", "topic_ids": []string{"8624225e00000002"}}), 200, "")

	rv := j.do("dash-prod", "POST", "/v1/articles/"+id+"/revert", map[string]any{"to_version": 1, "reason_code": "quality", "by": "operator", "request_id": id + "_01J9D6T1F7G8H9J0K1M2N3P4Q5"})
	j.want(rv, 200, "")
	if rv.body["result"] != "reverted" || !slices.Equal(anyStrings(rv.body["reverted_contributors"]), []string{"ffffffffffffffff"}) {
		t.Fatalf("revert = %s", rv.raw)
	}
	tomb := j.do("dash-prod", "POST", "/v1/articles/"+id+"/status", moderation("tombstone"))
	j.want(tomb, 200, "")
	if !slices.Equal(anyStrings(tomb.body["contributors"]), []string{jointCid, "ffffffffffffffff"}) {
		t.Fatalf("tombstone = %s", tomb.raw)
	}

	if got := corpusCounts(t, j); !equalJSON(got, counts) {
		t.Fatalf("/stats counts moved: %v → %v", counts, got)
	}
	if v := j.metric(`cosift_v1_writes_total{principal="intake-prod"}`); v != "3" {
		t.Fatalf("intake writes charged = %s", v)
	}
}

func TestJointWantedListFilters(t *testing.T) {
	j := newJoint(t, nil, nil)
	j.want(j.do("resolver-prod", "PUT", "/v1/articles/"+jointID(1), jointStub("Wanted stub title", "8624225e00000001")), 201, "")
	j.want(j.do("synth-prod", "PUT", "/v1/articles/"+jointID(2), jointArticle("Published strong title")), 201, "")
	thin := jointArticle("Published thin title")
	thin["quality_tier"] = "thin"
	j.want(j.do("synth-prod", "PUT", "/v1/articles/"+jointID(3), thin), 201, "")
	for _, c := range []struct {
		who, query string
		want       int
	}{
		{"community-wiki", "status=pending", 1},
		{"mcp-prod", "status=published&tier=thin", 1},
		{"mcp-prod", "tier=strong,thin", 2},
		{"mcp-prod", "", 2},
	} {
		r := j.do(c.who, "GET", "/v1/articles?"+c.query, nil)
		j.want(r, 200, "")
		if n := len(r.body["items"].([]any)); n != c.want {
			t.Errorf("%s ?%s: %d items, want %d", c.who, c.query, n, c.want)
		}
	}
	j.want(j.do("community-wiki", "GET", "/v1/articles?status=held", nil), 400, "invalid_param")
}

func anyStrings(v any) []string {
	var out []string
	for _, x := range v.([]any) {
		out = append(out, x.(string))
	}
	return out
}

func equalJSON(a, b any) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return string(x) == string(y)
}
