package community

import (
	"strings"
	"testing"
	"time"
)

func TestLinkingAnExistingPasswordAccountIsAllOrNothing(t *testing.T) {
	for name, trigger := range map[string]string{
		"identity row":    `CREATE TRIGGER refuse_link BEFORE INSERT ON shared_identities BEGIN SELECT RAISE(ABORT, 'store offline'); END`,
		"session revoke":  `CREATE TRIGGER refuse_link BEFORE DELETE ON sessions BEGIN SELECT RAISE(ABORT, 'store offline'); END`,
		"password revoke": `CREATE TRIGGER refuse_link BEFORE UPDATE OF password_hash ON users BEGIN SELECT RAISE(ABORT, 'store offline'); END`,
	} {
		t.Run(name, func(t *testing.T) {
			r := newAwardRig(t)
			a := r.articleAward(1, 1, 1000)
			account(t, r.s, emailN(1))
			if _, err := r.s.db.Exec(trigger); err != nil {
				t.Fatal(err)
			}
			r.tick(t)
			var links, sessions int
			var hash string
			r.s.db.QueryRow(`SELECT count(*) FROM shared_identities`).Scan(&links)
			r.s.db.QueryRow(`SELECT count(*) FROM sessions`).Scan(&sessions)
			r.s.db.QueryRow(`SELECT password_hash FROM users WHERE email=?`, emailN(1)).Scan(&hash)
			if links != 0 || sessions != 1 || hash == "" {
				t.Fatalf("a failed link was partly applied: links %d sessions %d password kept %v", links, sessions, hash != "")
			}
			if r.src.result(a.ID) != nil || r.ledgerRows(t) != 0 || !strings.Contains(r.logs.String(), "class=link") {
				t.Fatalf("result %+v rows %d log %q", r.src.result(a.ID), r.ledgerRows(t), r.logs.String())
			}
			r.s.db.Exec(`DROP TRIGGER refuse_link`)
			r.now = r.now.Add(2 * time.Hour)
			r.tick(t)
			if _, found := r.ledger(t, a.ID); !found {
				t.Fatal("the award was not applied once the store recovered")
			}
		})
	}
}
