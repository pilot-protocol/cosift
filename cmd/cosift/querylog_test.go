package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTailLines(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "q.jsonl")
	var sb strings.Builder
	for i := 0; i < 500; i++ {
		sb.WriteString("line")
		sb.WriteByte(byte('0' + i%10))
		sb.WriteByte('\n')
	}
	if err := os.WriteFile(p, []byte(sb.String()), 0644); err != nil {
		t.Fatal(err)
	}
	got := tailLines(p, 3)
	if len(got) != 3 {
		t.Fatalf("want 3 lines, got %d", len(got))
	}
	// Last three lines are i=497,498,499 -> line7,line8,line9
	want := []string{"line7", "line8", "line9"}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("line %d: got %q want %q", i, got[i], want[i])
		}
	}
	// n larger than file → all lines, no partials/blanks.
	all := tailLines(p, 10000)
	if len(all) != 500 {
		t.Errorf("want 500 lines, got %d", len(all))
	}
	for _, l := range all {
		if l == "" {
			t.Error("blank line leaked into output")
		}
	}
}

func TestTailLinesMissingFile(t *testing.T) {
	if got := tailLines("/nonexistent/path/q.jsonl", 5); got != nil {
		t.Errorf("missing file should return nil, got %v", got)
	}
}
