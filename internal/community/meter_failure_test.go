package community

import (
	"strings"
	"testing"
)

func TestMeterAnswers500WithoutChargingWhenTheLedgerRefusesTheDebit(t *testing.T) {
	s, uid := meterServer(t, 10)
	logs := captureLog(t)
	if _, err := s.db.Exec(`CREATE TRIGGER refuse_debit BEFORE INSERT ON credit_ledger WHEN NEW.reason='article_read' BEGIN SELECT RAISE(ABORT, 'ledger offline'); END`); err != nil {
		t.Fatal(err)
	}
	w := meterCall(s, sharedTestToken, meterBody("refused-request-01"), nil)
	expect(t, w, 500)
	if a := decodeMeter(t, w); a.Code != "credits_unavailable" || a.Charged != 0 {
		t.Fatalf("%+v", a)
	}
	if strings.Contains(w.Body.String(), "ledger offline") || strings.Contains(logs.String(), "ledger offline") {
		t.Fatalf("the storage error leaked: body %q log %q", w.Body.String(), logs.String())
	}
	if !strings.Contains(logs.String(), "credits_unavailable") || meterRows(t, s) != 0 || balanceOf(t, s, uid) != 10 {
		t.Fatalf("rows %d balance %d log %q", meterRows(t, s), balanceOf(t, s, uid), logs.String())
	}
}

func TestMeterAnswers500WhenTheMonthlyGrantCannotBeWritten(t *testing.T) {
	s := testServer(t, nil)
	s.cfg.Shared = &fakeShared{}
	expect(t, bearerRequest(s, "/api/me", sharedTestToken), 200)
	if _, err := s.db.Exec(`CREATE TRIGGER refuse_grant BEFORE INSERT ON credit_ledger WHEN NEW.reason='monthly_free' BEGIN SELECT RAISE(ABORT, 'ledger offline'); END`); err != nil {
		t.Fatal(err)
	}
	w := meterCall(s, sharedTestToken, meterBody("ungranted-request1"), nil)
	expect(t, w, 500)
	if a := decodeMeter(t, w); a.Code != "credits_unavailable" || meterRows(t, s) != 0 {
		t.Fatalf("%+v rows %d", a, meterRows(t, s))
	}
}

func balanceOf(t *testing.T, s *Server, userID string) int {
	t.Helper()
	var b int
	if err := s.db.QueryRow(`SELECT COALESCE(sum(delta),0) FROM credit_ledger WHERE user_id=?`, userID).Scan(&b); err != nil {
		t.Fatal(err)
	}
	return b
}
