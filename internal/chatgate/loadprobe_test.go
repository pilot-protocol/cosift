package chatgate

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestParseVLLMMetrics(t *testing.T) {
	body := `# HELP vllm:num_requests_running ...
# TYPE vllm:num_requests_running gauge
vllm:num_requests_running{engine="0",model_name="qwen3.5:9b-fp8"} 0.0
vllm:num_requests_waiting{engine="0",model_name="qwen3.5:9b-fp8"} 3.0
vllm:num_requests_waiting_by_reason{engine="0",reason="capacity"} 1.0
`
	w, r := parseVLLMMetrics(body)
	if w != 3 {
		t.Errorf("waiting: got %d want 3", w)
	}
	if r != 0 {
		t.Errorf("running: got %d want 0", r)
	}
}

func TestLoadProbeRunDetectsLoad(t *testing.T) {
	body := `vllm:num_requests_running{engine="0"} 1.0
vllm:num_requests_waiting{engine="0"} 7.0
`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	p := NewLoadProbe(srv.URL, 5, 10*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Run(ctx)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if p.Loaded() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !p.Loaded() {
		t.Fatalf("probe never observed load; stats=%+v", p.Stats())
	}
	st := p.Stats()
	if st.Waiting != 7 {
		t.Errorf("waiting: got %d want 7", st.Waiting)
	}
}

func TestLoadProbeNilSafe(t *testing.T) {
	var p *LoadProbe
	if p.Loaded() {
		t.Errorf("nil probe loaded")
	}
	if (p.Stats() != ProbeStats{}) {
		t.Errorf("nil probe stats not zero")
	}
}

func TestVLLMMetricsURLFrom(t *testing.T) {
	cases := []struct{ in, want string }{
		{"http://h:8000/v1/chat/completions", "http://h:8000/metrics"},
		{"https://h/v1", "https://h/metrics"},
		{"", ""},
		{"::not-a-url", ""},
	}
	for _, c := range cases {
		if got := VLLMMetricsURLFrom(c.in); got != c.want {
			t.Errorf("%q: got %q want %q", c.in, got, c.want)
		}
	}
}
