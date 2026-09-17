package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

// Explicit opt-in only: store a revocable session, never an account password.
// The exact origin binding prevents accidentally using one server's credentials
// with another server. Cookies are always reconstructed as host-only cookies.
type communitySession struct {
	Origin  string    `json:"origin"`
	Token   string    `json:"token"`
	Expires time.Time `json:"expires"`
}

func (s communitySession) cookie() *http.Cookie {
	return &http.Cookie{Name: "cosift_session", Value: s.Token, Path: "/", HttpOnly: true, Secure: len(s.Origin) >= 6 && s.Origin[:6] == "https:", Expires: s.Expires}
}

func readCommunitySession(path, origin string, allowExpired bool) (communitySession, error) {
	s, err := readCommunitySessionFile(path, allowExpired)
	if err != nil {
		return s, err
	}
	if s.Origin != origin {
		return s, fmt.Errorf("session belongs to a different server")
	}
	return s, nil
}

// Only the installer's documented session path participates in discovery. The
// origin still passes runContribute's HTTPS/origin checks before any request.
func installedCommunitySession(allowExpired bool) (string, communitySession, error) {
	dir := os.Getenv("XDG_CONFIG_HOME")
	if dir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			// A process without a home has no implicit session location.
			return "", communitySession{}, nil
		}
		dir = filepath.Join(home, ".config")
	}
	path := filepath.Join(dir, "cosift", "community-session.json")
	saved, err := readCommunitySessionFile(path, allowExpired)
	if os.IsNotExist(err) {
		return "", communitySession{}, nil
	}
	if err != nil {
		return "", communitySession{}, fmt.Errorf("installed CLI session: %w", err)
	}
	return path, saved, nil
}

func readCommunitySessionFile(path string, allowExpired bool) (communitySession, error) {
	var s communitySession
	info, err := os.Lstat(path)
	if err != nil {
		return s, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return s, fmt.Errorf("session file must be a private regular file (chmod 600)")
	}
	f, err := os.Open(path)
	if err != nil {
		return s, err
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil {
		return s, err
	}
	if !os.SameFile(info, opened) {
		return s, fmt.Errorf("session file changed while opening")
	}
	data, err := io.ReadAll(io.LimitReader(f, 8193))
	if err != nil {
		return s, err
	}
	if len(data) > 8192 || json.Unmarshal(data, &s) != nil {
		return s, fmt.Errorf("invalid session file; log in again")
	}
	if s.Token == "" || s.cookie().Valid() != nil || s.Expires.IsZero() {
		return s, fmt.Errorf("invalid session file; log in again")
	}
	if !allowExpired && !s.Expires.After(time.Now()) {
		return s, fmt.Errorf("CLI session expired; log out and log in again")
	}
	return s, nil
}

type communityHTTPError struct {
	path    string
	status  int
	message string
}

func (e *communityHTTPError) Error() string {
	return fmt.Sprintf("community %s: HTTP %d: %s", e.path, e.status, e.message)
}
