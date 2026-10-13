package community

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
)

var opaqueID = regexp.MustCompile(`^[0-9a-f]{32}$`)

func workServer(t *testing.T) *Server {
	t.Helper()
	s := testServer(t, nil)
	if err := s.loadWorkSalts(); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestNetHashesTheIPv4SlashTwentyFourAndTheIPv6SlashFortyEight(t *testing.T) {
	s := workServer(t)
	same := [][2]string{
		{"203.0.113.5", "203.0.113.250"},
		{"2001:db8:1:1::1", "2001:db8:1:2::9"},
		{"2001:db8:1:0:aaaa::1", "2001:db8:1:ffff::1"},
		{"203.0.113.5", "::ffff:203.0.113.77"},
	}
	for _, p := range same {
		a, okA := s.netID(p[0])
		b, okB := s.netID(p[1])
		if !okA || !okB || a != b {
			t.Errorf("%s and %s: %q %q, want one net", p[0], p[1], a, b)
		}
	}
	apart := [][2]string{
		{"203.0.113.5", "203.0.114.5"},
		{"203.0.113.5", "203.1.113.5"},
		{"2001:db8:1::1", "2001:db8:2::1"},
		{"2001:db8:1:ffff::1", "2001:db8:2:0::1"},
	}
	for _, p := range apart {
		a, _ := s.netID(p[0])
		b, _ := s.netID(p[1])
		if a == b {
			t.Errorf("%s and %s share a net", p[0], p[1])
		}
	}
}

func TestNetRefusesAnAddressThatDoesNotParse(t *testing.T) {
	s := workServer(t)
	for _, ip := range []string{"", "unknown", "203.0.113", "300.1.1.1", "::g"} {
		if got, ok := s.netID(ip); ok || got != "" {
			t.Errorf("%q gave %q", ip, got)
		}
	}
}

func TestWorkerAndNetAreOpaqueSaltedAndKeepTheAddressOut(t *testing.T) {
	s := workServer(t)
	worker, net := s.workerID("user-a"), mustNet(t, s, "198.51.100.23")
	for _, id := range []string{worker, net} {
		if !opaqueID.MatchString(id) {
			t.Fatalf("%q is not 32 lower-case hex characters", id)
		}
	}
	if worker == s.workerID("user-b") || worker != s.workerID("user-a") {
		t.Fatal("a worker id must be stable per account and differ between accounts")
	}
	if strings.Contains(net, "198") || worker == net {
		t.Fatal("the net id leaks its input")
	}
	other := workServer(t)
	if other.workerID("user-a") == worker || mustNet(t, other, "198.51.100.23") == net {
		t.Fatal("two databases derive the same ids: the salts are not per database")
	}
}

func mustNet(t *testing.T, s *Server, ip string) string {
	t.Helper()
	id, ok := s.netID(ip)
	if !ok {
		t.Fatalf("%s refused", ip)
	}
	return id
}

func TestWorkSaltsArePersistedAndSeparate(t *testing.T) {
	dir := t.TempDir()
	open := func() *Server {
		s, err := Open(Config{DataDir: dir, Backend: "http://127.0.0.1:1", PublicURL: "http://localhost:7780"})
		if err != nil {
			t.Fatal(err)
		}
		if err := s.loadWorkSalts(); err != nil {
			t.Fatal(err)
		}
		return s
	}
	first := open()
	worker, net := first.workerID("user-a"), mustNet(t, first, "198.51.100.23")
	var a, b string
	first.db.QueryRow(`SELECT value FROM settings WHERE key='work_worker_salt'`).Scan(&a)
	first.db.QueryRow(`SELECT value FROM settings WHERE key='work_net_salt'`).Scan(&b)
	first.Close()
	if len(a) != 64 || len(b) != 64 || a == b {
		t.Fatalf("salts %q %q: want two distinct 32-byte values", a, b)
	}
	second := open()
	defer second.Close()
	if second.workerID("user-a") != worker || mustNet(t, second, "198.51.100.23") != net {
		t.Fatal("ids changed across a restart")
	}
}

func TestWorkSchemaAddsTheTablesAndMigratesFailClass(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Config{DataDir: dir, Backend: "http://127.0.0.1:1", PublicURL: "http://localhost:7780"})
	if err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"work_workers", "work_pushed"} {
		var n int
		if err := s.db.QueryRow(fmt.Sprintf(`SELECT count(*) FROM %s`, table)).Scan(&n); err != nil {
			t.Errorf("%s: %v", table, err)
		}
	}
	if _, err := s.db.Exec(`ALTER TABLE submissions DROP COLUMN fail_class`); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, err = Open(Config{DataDir: dir, Backend: "http://127.0.0.1:1", PublicURL: "http://localhost:7780"})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var class string
	s.db.Exec(`INSERT INTO users(id,email,name,salt,password_hash,created_at) VALUES('u','u@example.com','u','s','',0)`)
	s.db.Exec(`INSERT INTO submissions(id,user_id,url,created_at) VALUES('s','u','https://example.com/',0)`)
	if err := s.db.QueryRow(`SELECT fail_class FROM submissions WHERE id='s'`).Scan(&class); err != nil || class != "" {
		t.Fatalf("fail_class %q %v", class, err)
	}
}
