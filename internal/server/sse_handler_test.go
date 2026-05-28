package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// nonFlusher wraps a ResponseWriter but does NOT implement http.Flusher.
// Used to exercise the sseHandler "ok=false" fallback path.
type nonFlusher struct {
	http.ResponseWriter
}

func TestSSEHandlerSetsHeadersAndEmit(t *testing.T) {
	rec := httptest.NewRecorder()
	emit, bail, ok := sseHandler(rec)
	if !ok {
		t.Fatal("httptest.ResponseRecorder should support flushing")
	}
	if emit == nil || bail == nil {
		t.Fatal("emit/bail should be non-nil when ok")
	}

	// Headers locked in by the helper.
	if ct := rec.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("Content-Type: got %q", ct)
	}
	if cc := rec.Header().Get("Cache-Control"); cc != "no-cache" {
		t.Errorf("Cache-Control: got %q", cc)
	}
	if rec.Code != http.StatusOK {
		t.Errorf("status: got %d", rec.Code)
	}

	// Emit produces standard SSE framing.
	emit("plan", map[string]string{"strategy": "planner"})
	body := rec.Body.String()
	if !strings.Contains(body, "event: plan\n") {
		t.Errorf("emit didn't write event line: %q", body)
	}
	if !strings.Contains(body, `data: {"strategy":"planner"}`) {
		t.Errorf("emit didn't write data line: %q", body)
	}
	if !strings.HasSuffix(body, "\n\n") {
		t.Errorf("event block should end with blank line: %q", body)
	}
}

func TestSSEHandlerBailEmitsErrorEvent(t *testing.T) {
	rec := httptest.NewRecorder()
	_, bail, ok := sseHandler(rec)
	if !ok {
		t.Fatal("recorder should support flushing")
	}
	bail("things went wrong")
	body := rec.Body.String()
	if !strings.Contains(body, "event: error\n") {
		t.Errorf("bail should emit error event: %q", body)
	}
	if !strings.Contains(body, `"detail":"things went wrong"`) {
		t.Errorf("bail should include detail: %q", body)
	}
}

func TestSSEHandlerNoFlusherWritesProblem(t *testing.T) {
	// Wrap httptest's recorder in a struct that strips the Flusher capability.
	rec := httptest.NewRecorder()
	w := nonFlusher{ResponseWriter: rec}

	emit, bail, ok := sseHandler(w)
	if ok {
		t.Fatal("non-flushing writer should return ok=false")
	}
	if emit != nil || bail != nil {
		t.Error("emit/bail should be nil when ok=false")
	}
	// Helper has already written a 500 problem response.
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 written by helper, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "streaming unsupported") {
		t.Errorf("problem detail missing: %q", rec.Body.String())
	}
}

func TestSSEHandlerEmitJSONErrorTolerated(t *testing.T) {
	// json.Marshal can fail on values like channels — emit should not panic.
	rec := httptest.NewRecorder()
	emit, _, _ := sseHandler(rec)
	emit("x", make(chan int)) // chan is unmarshalable
	// Body shouldn't contain a half-written event (no `event: x` since marshal failed pre-write).
	body := rec.Body.String()
	if strings.Contains(body, "event: x") {
		t.Errorf("emit should bail before writing on marshal failure: %q", body)
	}
}
