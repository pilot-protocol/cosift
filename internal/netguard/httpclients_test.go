package netguard

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

var (
	outboundClient   = regexp.MustCompile(`&?http\.(Client|Transport)\{|http\.(Get|Post|PostForm|Head)\(|http\.Default(Client|Transport)\b`)
	clientLiteral    = regexp.MustCompile(`&?http\.Client\{`)
	transportLiteral = regexp.MustCompile(`&?http\.Transport\{`)
)

// Files allowed to build an HTTP client outside netguard: each dials an
// operator-controlled endpoint, or uses an independently tested stricter dialer.
var unguardedByDesign = map[string]string{
	"cmd/cosift/community.go":         "CLI talks only to its explicitly configured server; redirects disabled",
	"internal/community/server.go":    "operator-configured backend origin only; redirects disabled; contributed URLs use crawler.PublicHTTPClient",
	"internal/crawler/public_dial.go": "stricter public-only DNS-pinned dialer, denies all private answers, non-web ports and proxies, cannot be disabled by environment; covered by public_dial_test.go",
	"internal/embed/client.go":        "embeddings endpoint — box-local vLLM in prod",
	"internal/embed/chat.go":          "chat endpoint — box-local vLLM in prod",
	"internal/rerank/http.go":         "rerank endpoint — box-local in prod",
	"internal/chatgate/loadprobe.go":  "polls the local vLLM /metrics",
	"cmd/cosift/serve_search.go":      "peer shard forward + gateway, both operator-configured hosts",
	"cmd/cosift/serve_helpers.go":     "operator-configured gateway",
	"cmd/cosift/find.go":              "CLI talking to its own server",
	"cmd/cosift/cmd_admin.go":         "CLI talking to its own server",
	"cmd/cosift/cmd_query.go":         "CLI talking to its own server",
	"cmd/cosift/cmd_maintenance.go":   "CLI talking to its own server",
	"cmd/cosift/cmd_eval.go":          "CLI talking to its own server and the judge model",
}

func TestOutboundClientsRouteThroughNetguard(t *testing.T) {
	root := moduleRoot(t)
	var offenders, stale []string
	seen := map[string]bool{}

	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if name := d.Name(); name == ".git" || name == "testdata" || name == "vendor" {
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if strings.HasPrefix(rel, "internal/netguard/") {
			return nil
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		lines := strings.Split(string(src), "\n")
		for _, i := range unguardedLines(lines) {
			seen[rel] = true
			if _, ok := unguardedByDesign[rel]; ok {
				continue
			}
			offenders = append(offenders, rel+":"+strconv.Itoa(i+1)+": "+strings.TrimSpace(lines[i]))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	for rel := range unguardedByDesign {
		if !seen[rel] {
			stale = append(stale, rel)
		}
	}
	if len(offenders) > 0 {
		t.Errorf("HTTP clients built outside netguard — guard them or add them to unguardedByDesign with a reason:\n  %s",
			strings.Join(offenders, "\n  "))
	}
	if len(stale) > 0 {
		t.Errorf("unguardedByDesign lists files that no longer build an unguarded HTTP client:\n  %s",
			strings.Join(stale, "\n  "))
	}
}

// unguardedLines returns the indices of lines that start an outbound HTTP
// client which is not routed through netguard.
func unguardedLines(lines []string) []int {
	var hits []int
	for i, line := range lines {
		if !outboundClient.MatchString(line) {
			continue
		}
		block := literalBlock(lines, i)
		if strings.Contains(block, "netguard.") {
			continue
		}
		// A client whose Transport is built elsewhere is judged there; one
		// that inlines its own transport literal is judged here.
		if clientLiteral.MatchString(line) && strings.Contains(block, "Transport:") && !transportLiteral.MatchString(block) {
			continue
		}
		hits = append(hits, i)
	}
	return hits
}

func TestUnguardedLinesMatchesEveryEgressForm(t *testing.T) {
	for _, tc := range []struct {
		name string
		src  string
		want bool
	}{
		{"client literal", "var c = &http.Client{Timeout: t}", true},
		{"transport literal", "var t = &http.Transport{}", true},
		{"package helper", "resp, err := http.Get(u)", true},
		{"post helper", "resp, err := http.Post(u, ct, body)", true},
		{"default client", "resp, err := http.DefaultClient.Do(req)", true},
		{"default transport", "tr := http.DefaultTransport", true},
		{"client inlining a transport", "var c = &http.Client{Transport: &http.Transport{}}", true},
		{"multiline client inlining a transport", "c := &http.Client{\n\tTransport: &http.Transport{\n\t\tMaxIdleConns: 2,\n\t},\n}", true},
		{"guarded client", "var c = netguard.Client(20 * time.Second)", false},
		{"guarded transport", "tr := netguard.Protect(&http.Transport{}, true)", false},
		{"client over a transport from elsewhere", "c := &http.Client{\n\tTimeout:   d,\n\tTransport: rt,\n}", false},
		{"no egress", "func f() error { return nil }", false},
	} {
		got := len(unguardedLines(strings.Split(tc.src, "\n"))) > 0
		if got != tc.want {
			t.Errorf("%s: unguarded = %v, want %v (src %q)", tc.name, got, tc.want, tc.src)
		}
	}
}

// literalBlock returns the composite literal opened on line i: everything up to
// the line where its braces balance again (capped, so a match outside a literal
// still yields its own line).
func literalBlock(lines []string, i int) string {
	depth := 0
	end := i
	for ; end < len(lines) && end < i+30; end++ {
		depth += strings.Count(lines[end], "{") - strings.Count(lines[end], "}")
		if depth <= 0 {
			break
		}
	}
	if end >= len(lines) {
		end = len(lines) - 1
	}
	return strings.Join(lines[i:end+1], "\n")
}

func moduleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("no go.mod above the test's working directory")
		}
		dir = parent
	}
}
