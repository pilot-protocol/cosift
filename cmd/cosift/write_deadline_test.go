package main

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestLiftWriteDeadlineThroughMiddleware(t *testing.T) {
	srv := &pebbleHTTP{rl: newRateLimiterFromEnv()}
	h := srv.count(srv.rateLimit(func(w http.ResponseWriter, r *http.Request) {
		liftWriteDeadline(w)
		time.Sleep(500 * time.Millisecond)
		_, _ = w.Write([]byte("slow-ok"))
	}))
	ts := httptest.NewUnstartedServer(h)
	ts.Config.WriteTimeout = 200 * time.Millisecond
	ts.Start()
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/slow")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if string(body) != "slow-ok" {
		t.Fatalf("body: got %q want %q", body, "slow-ok")
	}
}

type noUnwrapWriter struct{ http.ResponseWriter }

func TestWrappedWritersSupportSetWriteDeadline(t *testing.T) {
	var scErr, rwErr, bareErr error
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		scErr = http.NewResponseController(&statusCapturingWriter{ResponseWriter: w}).SetWriteDeadline(time.Time{})
		rwErr = http.NewResponseController(&recordingWriter{ResponseWriter: w}).SetWriteDeadline(time.Time{})
		bareErr = http.NewResponseController(&noUnwrapWriter{w}).SetWriteDeadline(time.Time{})
	}))
	defer ts.Close()
	resp, err := http.Get(ts.URL)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	resp.Body.Close()
	if scErr != nil {
		t.Errorf("statusCapturingWriter: %v", scErr)
	}
	if rwErr != nil {
		t.Errorf("recordingWriter: %v", rwErr)
	}
	if !errors.Is(bareErr, http.ErrNotSupported) {
		t.Errorf("wrapper without Unwrap: got %v want ErrNotSupported", bareErr)
	}
}
