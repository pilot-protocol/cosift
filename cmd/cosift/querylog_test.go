package main

import (
	"net/http"
	"net/http/httptest"
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

func TestQlogNoLogHeader(t *testing.T) {
	const body = "ok"
	cases := []struct {
		name     string
		token    string // s.qlogNoLogToken
		header   string // X-Cosift-No-Log value ("" = header absent)
		wantLine bool
	}{
		{"no header", "secret", "", true},
		{"wrong secret", "secret", "wrong", true},
		{"correct secret", "secret", "secret", false},
		{"token unset ignores header", "", "secret", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "q.jsonl")
			f, err := os.OpenFile(p, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			s := &pebbleHTTP{qlogFile: f, qlogNoLogToken: tc.token}
			ran := false
			h := s.qlog(func(w http.ResponseWriter, r *http.Request) {
				ran = true
				_, _ = w.Write([]byte(body))
			})
			req := httptest.NewRequest(http.MethodGet, "/search?q=test", nil)
			if tc.header != "" {
				req.Header.Set("X-Cosift-No-Log", tc.header)
			}
			rec := httptest.NewRecorder()
			h(rec, req)

			if !ran {
				t.Fatal("wrapped handler did not run")
			}
			if got := rec.Body.String(); got != body {
				t.Errorf("response body = %q, want %q", got, body)
			}
			data, err := os.ReadFile(p)
			if err != nil {
				t.Fatal(err)
			}
			logged := len(strings.TrimSpace(string(data))) > 0
			if logged != tc.wantLine {
				t.Errorf("logged = %v, want %v (qlog=%q)", logged, tc.wantLine, data)
			}
		})
	}
}
