package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

func TestStatsAndMetricsExposeRuntimeMemory(t *testing.T) {
	f := populatedPebbleStore(t)
	srv := f.makeServer(nil)
	runtime.GC()

	body, err := srv.buildStatsBody(context.Background())
	if err != nil {
		t.Fatalf("buildStatsBody: %v", err)
	}
	var out struct {
		Runtime map[string]float64 `json:"runtime"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	rt := out.Runtime
	for _, k := range []string{"heap_objects_bytes", "heap_live_bytes", "heap_goal_bytes", "mem_limit_bytes", "mem_total_bytes", "gc_cycles", "goroutines"} {
		if _, ok := rt[k]; !ok {
			t.Errorf("/stats runtime missing %q", k)
		}
	}
	if !(rt["heap_goal_bytes"] >= rt["heap_live_bytes"] && rt["heap_live_bytes"] > 0) {
		t.Errorf("want heap_goal >= heap_live > 0, got goal=%v live=%v", rt["heap_goal_bytes"], rt["heap_live_bytes"])
	}
	if rt["mem_total_bytes"] < rt["heap_objects_bytes"] || rt["goroutines"] < 1 || rt["gc_cycles"] < 1 {
		t.Errorf("implausible runtime stats: %v", rt)
	}
	if rt["mem_limit_bytes"] != -1 && rt["mem_limit_bytes"] <= 0 {
		t.Errorf("mem_limit_bytes: got %v", rt["mem_limit_bytes"])
	}

	rec := httptest.NewRecorder()
	srv.handleMetrics(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics: code %d", rec.Code)
	}
	vals := map[string]float64{}
	for _, line := range strings.Split(rec.Body.String(), "\n") {
		if strings.HasPrefix(line, "cosift_go_") {
			name, v, _ := strings.Cut(line, " ")
			n, err := strconv.ParseFloat(v, 64)
			if err != nil {
				t.Errorf("parse %q: %v", line, err)
			}
			vals[name] = n
		}
	}
	for _, k := range []string{"cosift_go_heap_objects_bytes", "cosift_go_heap_live_bytes", "cosift_go_heap_goal_bytes", "cosift_go_mem_limit_bytes", "cosift_go_mem_total_bytes", "cosift_go_gc_cycles_total", "cosift_go_goroutines"} {
		if _, ok := vals[k]; !ok {
			t.Errorf("/metrics missing %q", k)
		}
	}
	if !(vals["cosift_go_heap_goal_bytes"] >= vals["cosift_go_heap_live_bytes"] && vals["cosift_go_heap_live_bytes"] > 0) {
		t.Errorf("want heap_goal >= heap_live > 0, got %v", vals)
	}
}
