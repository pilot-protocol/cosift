package community

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/netip"

	"github.com/pilot-protocol/cosift/internal/sharedaccount"
)

const (
	workerSaltKey = "work_worker_salt"
	netSaltKey    = "work_net_salt"
)

type workSalts struct{ worker, net []byte }

func (s *Server) initWork() error {
	s.work.joins = newJoinCache(joinCacheKeys)
	return s.loadWorkSalts()
}

func (s *Server) loadWorkSalts() error {
	var err error
	if s.work.salts.worker, err = s.saltSetting(workerSaltKey); err != nil {
		return err
	}
	s.work.salts.net, err = s.saltSetting(netSaltKey)
	return err
}

func (s *Server) saltSetting(key string) ([]byte, error) {
	fresh := make([]byte, 32)
	rand.Read(fresh)
	if _, err := s.db.Exec(`INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO NOTHING`, key, hex.EncodeToString(fresh)); err != nil {
		return nil, err
	}
	var stored string
	if err := s.db.QueryRow(`SELECT value FROM settings WHERE key=?`, key).Scan(&stored); err != nil {
		return nil, err
	}
	salt, err := hex.DecodeString(stored)
	if err != nil || len(salt) != 32 {
		return nil, fmt.Errorf("setting %s is not a 32-byte salt", key)
	}
	return salt, nil
}

func deriveID(salt []byte, domain, input string) string {
	mac := hmac.New(sha256.New, salt)
	mac.Write([]byte(domain + "\x00" + input))
	return hex.EncodeToString(mac.Sum(nil)[:16])
}

func (s *Server) workerID(userID string) string { return deriveID(s.work.salts.worker, "w1", userID) }

// netID hashes the client's IPv4 /24 or IPv6 /48; the quorum treats workers behind one prefix as one network.
func (s *Server) netID(ip string) (string, bool) {
	a, err := netip.ParseAddr(ip)
	if err != nil {
		return "", false
	}
	a = a.WithZone("").Unmap()
	bits := 48
	if a.Is4() {
		bits = 24
	}
	return deriveID(s.work.salts.net, "n1", netip.PrefixFrom(a, bits).Masked().String()), true
}

type workRuntime struct {
	salts  workSalts
	source sharedaccount.WorkSource
	joins  *joinCache

	counters fetchCounters
	alerts   alertQueue
}

// workSourceFor checks what the work routes and loops stand on: shared accounts, the broker key and a known environment.
func workSourceFor(cfg Config) (sharedaccount.WorkSource, error) {
	if !cfg.Work {
		return nil, nil
	}
	src, ok := cfg.Shared.(sharedaccount.WorkSource)
	switch {
	case !ok:
		return nil, fmt.Errorf("COSIFT_COMMUNITY_WORK requires COSIFT_AUTH_MODE=shared")
	case cfg.BrokerKey == "":
		return nil, fmt.Errorf("COSIFT_COMMUNITY_WORK requires COSIFT_COMMUNITY_BROKER_KEY")
	case cfg.Environment != "prod" && cfg.Environment != "staging":
		return nil, fmt.Errorf("COSIFT_ENV must be staging or prod")
	}
	return src, nil
}
