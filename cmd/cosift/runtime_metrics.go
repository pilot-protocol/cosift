package main

import (
	"math"
	"runtime/metrics"
)

type runtimeStats struct {
	HeapObjects, HeapLive, HeapGoal, MemLimit, MemTotal, GCCycles, Goroutines int64
}

// readRuntimeStats samples runtime/metrics (no STW). MemLimit is -1 when no
// GOMEMLIMIT is set.
func readRuntimeStats() runtimeStats {
	samples := []metrics.Sample{
		{Name: "/memory/classes/heap/objects:bytes"},
		{Name: "/gc/heap/live:bytes"},
		{Name: "/gc/heap/goal:bytes"},
		{Name: "/gc/memory/limit:bytes"},
		{Name: "/memory/classes/total:bytes"},
		{Name: "/gc/cycles/total:gc-cycles"},
		{Name: "/sched/goroutines:goroutines"},
	}
	metrics.Read(samples)
	u := func(i int) int64 {
		if samples[i].Value.Kind() != metrics.KindUint64 {
			return 0
		}
		return int64(samples[i].Value.Uint64())
	}
	rs := runtimeStats{
		HeapObjects: u(0),
		HeapLive:    u(1),
		HeapGoal:    u(2),
		MemLimit:    u(3),
		MemTotal:    u(4),
		GCCycles:    u(5),
		Goroutines:  u(6),
	}
	if rs.MemLimit <= 0 || rs.MemLimit == math.MaxInt64 {
		rs.MemLimit = -1
	}
	return rs
}

func (rs runtimeStats) statsMap() map[string]any {
	return map[string]any{
		"heap_objects_bytes": rs.HeapObjects,
		"heap_live_bytes":    rs.HeapLive,
		"heap_goal_bytes":    rs.HeapGoal,
		"mem_limit_bytes":    rs.MemLimit,
		"mem_total_bytes":    rs.MemTotal,
		"gc_cycles":          rs.GCCycles,
		"goroutines":         rs.Goroutines,
	}
}
