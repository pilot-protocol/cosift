package community

import (
	"regexp"
	"strconv"
	"testing"
	"time"
)

func TestBrowserTimeoutsOutlastTheBackendCalls(t *testing.T) {
	js, err := assets.ReadFile("web/app.js")
	if err != nil {
		t.Fatal(err)
	}
	table := regexp.MustCompile(`REQUEST_TIMEOUT_MS = \{([^}]*)\}`).FindSubmatch(js)
	if table == nil {
		t.Fatal("app.js has no REQUEST_TIMEOUT_MS table")
	}
	browser := map[string]time.Duration{}
	for _, m := range regexp.MustCompile(`(\w+):\s*(\d+)`).FindAllSubmatch(table[1], -1) {
		ms, _ := strconv.Atoi(string(m[2]))
		browser[string(m[1])] = time.Duration(ms) * time.Millisecond
	}
	s := testServer(t, nil)
	for mode, backend := range map[string]time.Duration{"search": s.client.Timeout, "answer": retrievalTimeout, "research": retrievalTimeout} {
		if browser[mode] <= backend {
			t.Errorf("%s: the browser gives up after %s, which must exceed the backend call's %s so a slow backend is refunded", mode, browser[mode], backend)
		}
	}
}
