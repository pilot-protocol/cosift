package community

import (
	"context"
	"net/http"
	"strings"
	"testing"
)

func TestQualityStopsGarbageBeforeModelIndexingAndRewards(t *testing.T) {
	for _, tc := range []struct{ title, text, status string }{
		{"Domain for sale", strings.Repeat("Contact the owner to purchase this website. ", 4), "rejected"},
		{"Just a moment...", strings.Repeat("Please verify you are a human before continuing. ", 4), "unverified"},
		{"Best deals", strings.Repeat("cheap best discount sale offer free money today ", 50), "rejected"},
	} {
		t.Run(tc.title, func(t *testing.T) {
			s := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Errorf("garbage reached backend %s", r.URL.Path) }))
			setTestPage(s, "<html><title>"+tc.title+"</title><article>"+tc.text+"</article></html>")
			cookie := account(t, s, "junk@example.com")
			expect(t, request(t, s, "POST", "/api/submissions", map[string]any{"urls": []string{"https://example.com/junk"}}, cookie), 202)
			if err := s.dispatch(context.Background()); err != nil {
				t.Fatal(err)
			}
			var status string
			s.db.QueryRow(`SELECT status FROM submissions`).Scan(&status)
			if status != tc.status {
				t.Fatalf("status=%s", status)
			}
			var credits int
			s.db.QueryRow(`SELECT count(*) FROM credit_ledger`).Scan(&credits)
			if credits != 0 {
				t.Fatal("garbage earned credits")
			}
		})
	}
}

func TestQualityPreservesUsefulContent(t *testing.T) {
	for _, doc := range []ModerationDocument{
		{Title: "Understanding access denied errors", Text: "A technical article explaining authentication failures, response codes, and how to diagnose configuration issues."},
		{Title: "Cercetare medicală", Text: "Acest articol explică rezultatele cercetării și prezintă metodologia, limitările și concluziile studiului."},
		{Title: "Reference", Text: "func main() {\n fmt.Println(\"Hello, world!\")\n}\nThis example prints a greeting and exits successfully."},
		{Title: "参考资料", Text: "本研究介绍了实验的方法和结果，并讨论了相关工作与未来研究方向。"},
	} {
		if status, reason := ObviousQualityProblem(doc); status != "" {
			t.Fatalf("rejected useful page %q: %s", doc.Title, reason)
		}
	}
	for _, category := range []string{"spam", "low_quality"} {
		if !ValidVerdict(ModerationVerdict{Decision: "reject", Category: category}) {
			t.Fatal("invalid junk verdict")
		}
		if ValidVerdict(ModerationVerdict{Decision: "allow", Category: category}) {
			t.Fatal("junk allowed")
		}
	}
}
