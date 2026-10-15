package community

import (
	"encoding/json"
	"strings"
	"testing"
)

func longFinalURLReceipt(pairs int) func(string) receiptFields {
	return func(url string) receiptFields {
		r := novelReceipt(true)(url)
		r["final_url"] = "https://example.com/" + strings.Repeat("a&", pairs) + "end"
		return r
	}
}

func submissionStatus(t *testing.T, s *Server) string {
	t.Helper()
	var status string
	if err := s.db.QueryRow(`SELECT status FROM submissions`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	return status
}

func TestReceiptWithALongFinalURLIsAccepted(t *testing.T) {
	if b, _ := json.Marshal(longFinalURLReceipt(800)("u")); len(b) <= 4096 {
		t.Fatalf("fixture receipt is only %d bytes", len(b))
	}
	s := rewardServer(t, nil, longFinalURLReceipt(800))
	submitAll(t, s, "member@example.com", "https://example.com/guide")
	if got := submissionStatus(t, s); got != "indexed" {
		t.Fatalf("an indexed page with a long final URL is %s", got)
	}
}

func TestGrosslyOversizedReceiptIsRefused(t *testing.T) {
	captureLog(t)
	s := rewardServer(t, nil, longFinalURLReceipt(40000))
	submitAll(t, s, "member@example.com", "https://example.com/guide")
	if got := submissionStatus(t, s); got != "pending" {
		t.Fatalf("an 80 KB receipt was accepted: %s", got)
	}
}
