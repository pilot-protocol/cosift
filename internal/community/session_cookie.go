package community

import (
	"net/http"
	"strings"
	"time"
)

const hostCookieName = "__Host-" + cookieName

func (s *Server) secureCookies() bool { return strings.HasPrefix(s.cfg.PublicURL, "https:") }

// sessionCookieName is __Host-prefixed whenever the cookie can be Secure; plain-HTTP localhost keeps the legacy name.
func (s *Server) sessionCookieName() string {
	if s.secureCookies() {
		return hostCookieName
	}
	return cookieName
}

// sessionCookies returns the request's session cookies, current name first; the legacy name is read for one release.
func sessionCookies(r *http.Request) []*http.Cookie {
	var out []*http.Cookie
	for _, name := range []string{hostCookieName, cookieName} {
		if c, err := r.Cookie(name); err == nil {
			out = append(out, c)
		}
	}
	return out
}

func requestSessionCookie(r *http.Request) (*http.Cookie, error) {
	if c, err := r.Cookie(hostCookieName); err == nil {
		return c, nil
	}
	return r.Cookie(cookieName)
}

func (s *Server) newSessionCookie(name, token string, age int) *http.Cookie {
	return &http.Cookie{Name: name, Value: token, Path: "/", HttpOnly: true, Secure: s.secureCookies(), SameSite: http.SameSiteLaxMode, MaxAge: age, Expires: time.Now().Add(time.Duration(age) * time.Second)}
}

func (s *Server) setSession(w http.ResponseWriter, token string) {
	name := s.sessionCookieName()
	http.SetCookie(w, s.newSessionCookie(name, token, int(sessionAge.Seconds())))
	if name != cookieName {
		http.SetCookie(w, s.newSessionCookie(cookieName, "", -1))
	}
}

// clearSession expires the session cookies the request carried, or the current one when it carried none.
func (s *Server) clearSession(w http.ResponseWriter, r *http.Request) {
	carried := sessionCookies(r)
	if len(carried) == 0 {
		http.SetCookie(w, s.newSessionCookie(s.sessionCookieName(), "", -1))
		return
	}
	for _, c := range carried {
		http.SetCookie(w, s.newSessionCookie(c.Name, "", -1))
	}
}
