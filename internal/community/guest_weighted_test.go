package community

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestGuestWeightedCooldownSharesModesAliasesAndPersists(t *testing.T) {
	for mode, cost := range requestCreditCosts() {
		t.Run(mode, func(t *testing.T) {
			calls := 0
			s := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; _, _ = w.Write([]byte(`{}`)) }))
			before := time.Now().Unix()
			expect(t, request(t, s, "GET", "/api/"+mode+"?q=first", nil, nil), 200)
			var until int64
			var reservation string
			if err := s.db.QueryRow(`SELECT expires_at,reservation FROM guest_usage`).Scan(&until, &reservation); err != nil {
				t.Fatal(err)
			}
			interval := int64(1800 * cost)
			if until < before+interval || until > time.Now().Unix()+interval || !strings.HasPrefix(reservation, mode+":") {
				t.Fatalf("incorrect %s reservation: %d %s", mode, until-before, reservation)
			}
			for _, next := range []string{"search", "answer", "research"} {
				for _, prefix := range []string{"/", "/api/"} {
					r := httptest.NewRequest("GET", prefix+next+"?q=blocked", nil)
					r.Header.Set("X-Forwarded-For", "203.0.113.123")
					w := httptest.NewRecorder()
					s.ServeHTTP(w, r)
					expect(t, w, 429)
					var response struct {
						RetryAt    int64 `json:"retry_at"`
						RetryAfter int64 `json:"retry_after_seconds"`
					}
					if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil || response.RetryAt != until || (response.RetryAfter < until-time.Now().Unix() || response.RetryAfter > until-time.Now().Unix()+1) || w.Header().Get("Retry-After") != strconv.FormatInt(response.RetryAfter, 10) {
						t.Fatal("incorrect retry guidance", w.Body.String(), err)
					}
				}
			}
			var status struct {
				Available bool
				RetryAt   int64          `json:"retry_at"`
				Intervals map[string]int `json:"mode_intervals_seconds"`
			}
			if err := json.Unmarshal(request(t, s, "GET", "/api/guest", nil, nil).Body.Bytes(), &status); err != nil || status.Available || status.RetryAt != until || status.Intervals["search"] != 1800 || status.Intervals["answer"] != 3600 || status.Intervals["research"] != 5400 {
				t.Fatalf("wrong status %+v %v", status, err)
			}
			policy := s.limitPolicy()["guest"].(map[string]any)
			if policy[mode].(modeLimit).WindowSeconds != int(interval) {
				t.Fatal("wrong public mode interval")
			}
			reopened, err := Open(s.cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			expect(t, request(t, reopened, "GET", "/search?q=restart", nil, nil), 429)
			if calls != 1 {
				t.Fatal("cooldown bypass reached backend", calls)
			}
			var ledger int
			if err = s.db.QueryRow(`SELECT count(*) FROM credit_ledger`).Scan(&ledger); err != nil || ledger != 0 {
				t.Fatal("guest created credit activity", err)
			}
		})
	}
}

func TestGuestWeightedFailureReleasesOnlyItsOwnReservation(t *testing.T) {
	for mode := range requestCreditCosts() {
		t.Run(mode, func(t *testing.T) {
			fail := true
			s := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if fail {
					w.WriteHeader(503)
					return
				}
				_, _ = w.Write([]byte(`{}`))
			}))
			expect(t, request(t, s, "GET", "/api/"+mode+"?q=failed", nil, nil), 502)
			var rows int
			if err := s.db.QueryRow(`SELECT count(*) FROM guest_usage`).Scan(&rows); err != nil || rows != 0 {
				t.Fatal("failed guest retained cooldown", err)
			}
			fail = false
			expect(t, request(t, s, "GET", "/"+mode+"?q=retry", nil, nil), 200)
		})
	}
	s := testServer(t, nil)
	r := httptest.NewRequest("GET", "/search?q=test", nil)
	old, ok := s.reserveGuest(httptest.NewRecorder(), r, "search")
	if !ok {
		t.Fatal("first reservation denied")
	}
	old(false)
	next, ok := s.reserveGuest(httptest.NewRecorder(), r, "research")
	if !ok {
		t.Fatal("retry denied")
	}
	next(true)
	old(false)
	expect(t, request(t, s, "GET", "/api/answer?q=blocked", nil, nil), 429)
}

func TestGuestWeightedPolicyMigrationPreservesOriginalRequestTime(t *testing.T) {
	s := testServer(t, nil)
	used := time.Now().Unix() - 10
	if _, err := s.db.Exec(`UPDATE settings SET value='60' WHERE key='guest_interval_seconds'`); err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct {
		key, token string
		weight     int64
	}{{"legacy", "oldopaque", 1}, {"search", "search:opaque", 1}, {"answer", "answer:opaque", 2}, {"research", "research:opaque", 3}} {
		if _, err := s.db.Exec(`INSERT INTO guest_usage(ip_hash,expires_at,reservation) VALUES(?,?,?)`, item.key, used+60*item.weight, item.token); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		if err := s.migrateGuestInterval(); err != nil {
			t.Fatal(err)
		}
		for _, item := range []struct {
			key    string
			weight int64
		}{{"legacy", 1}, {"search", 1}, {"answer", 2}, {"research", 3}} {
			var until int64
			if err := s.db.QueryRow(`SELECT expires_at FROM guest_usage WHERE ip_hash=?`, item.key).Scan(&until); err != nil || until != used+1800*item.weight {
				t.Fatalf("%s migration changed request time: %d %v", item.key, until, err)
			}
		}
	}
}

func TestGuestLoweredIntervalMigratesModeCapsAndMatchesAvailability(t *testing.T) {
	s := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{}`)) }))
	expect(t, request(t, s, "GET", "/research?q=initial", nil, nil), 200)
	// Move the successful research request four minutes into the past. Lowering
	// the base from30minutes to1minute makes its new3-minute cooldown expire.
	if _, err := s.db.Exec(`UPDATE guest_usage SET expires_at=expires_at-240`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE retrieval_usage SET expires_at=expires_at-240 WHERE identity LIKE 'guest:%'`); err != nil {
		t.Fatal(err)
	}
	memberUntil := time.Now().Unix() + 600
	if _, err := s.db.Exec(`INSERT INTO retrieval_usage(identity,mode,count,expires_at) VALUES('member:unchanged','research',1,?)`, memberUntil); err != nil {
		t.Fatal(err)
	}
	s.cfg.GuestInterval = time.Minute
	for range 2 {
		if err := s.migrateGuestInterval(); err != nil {
			t.Fatal(err)
		}
	}
	var sharedUntil, modeUntil, stillMember int64
	if err := s.db.QueryRow(`SELECT expires_at FROM guest_usage`).Scan(&sharedUntil); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow(`SELECT expires_at FROM retrieval_usage WHERE identity LIKE 'guest:%'`).Scan(&modeUntil); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow(`SELECT expires_at FROM retrieval_usage WHERE identity='member:unchanged'`).Scan(&stillMember); err != nil {
		t.Fatal(err)
	}
	if sharedUntil != modeUntil || modeUntil > time.Now().Unix() || stillMember != memberUntil {
		t.Fatalf("migration mismatch shared=%d mode=%d member=%d", sharedUntil, modeUntil, stillMember)
	}
	var status struct{ Available bool }
	if err := json.Unmarshal(request(t, s, "GET", "/api/guest", nil, nil).Body.Bytes(), &status); err != nil || !status.Available {
		t.Fatal("expired cooldown not available", err)
	}
	expect(t, request(t, s, "GET", "/api/research?q=after-migration", nil, nil), 200)
}

func TestGuestLegacyModeExpiryMigrationMatchesSharedReservation(t *testing.T) {
	s := testServer(t, nil)
	used := time.Now().Unix() - 10
	if _, err := s.db.Exec(`UPDATE settings SET value='60' WHERE key='guest_interval_seconds'`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO guest_usage(ip_hash,expires_at,reservation) VALUES('legacy',?,'oldopaque')`, used+60); err != nil {
		t.Fatal(err)
	}
	for mode, window := range map[string]int64{"search": 60, "answer": 300, "research": 1800} {
		if _, err := s.db.Exec(`INSERT INTO retrieval_usage(identity,mode,count,expires_at) VALUES('guest:legacy',?,1,?)`, mode, used+window); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.db.Exec(`INSERT INTO retrieval_usage(identity,mode,count,expires_at) VALUES('guest:orphan','research',1,?)`, used+1800); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := s.migrateGuestInterval(); err != nil {
			t.Fatal(err)
		}
		for mode := range requestCreditCosts() {
			var until int64
			if err := s.db.QueryRow(`SELECT expires_at FROM retrieval_usage WHERE identity='guest:legacy' AND mode=?`, mode).Scan(&until); err != nil || until != used+1800 {
				t.Fatalf("legacy %s expiry=%d want%d: %v", mode, until, used+1800, err)
			}
		}
		var orphanUntil int64
		if err := s.db.QueryRow(`SELECT expires_at FROM retrieval_usage WHERE identity='guest:orphan'`).Scan(&orphanUntil); err != nil || orphanUntil != 0 {
			t.Fatal("orphan quota remained active", orphanUntil, err)
		}
	}
}
