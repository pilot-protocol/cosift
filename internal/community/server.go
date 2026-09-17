package community

import (
	"bytes"
	"context"
	"crypto/pbkdf2"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"net/mail"
	"net/netip"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pilot-protocol/cosift/internal/crawler"
	"github.com/pilot-protocol/cosift/internal/sharedaccount"
)

//go:embed web/*
var assets embed.FS

const cookieName = "cosift_session"
const sessionAge = 30 * 24 * time.Hour
const dailyContributionLimit = 1000

type Config struct {
	Shared                      sharedaccount.Provider
	SharedPasswordEnabled       bool // Enable only after the upstream password service is deployed.
	DataDir                     string
	Backend                     string
	PublicURL                   string
	AdminToken                  string // Only used for crawl-enqueue, never forwarded with searches.
	TrustedProxies              []string
	GuestInterval               time.Duration
	MemberFreeRPM               int // Deprecated and ignored: all authenticated retrievals cost credits.
	SearchRPM                   int
	AnswerRPM                   int
	ResearchPer10Min            int
	StripeSecretKey             string
	StripeWebhookSecret         string
	AllowTestPayments           bool // Explicit opt-in for an isolated QA ledger only.
	StripePortalConfigurationID string
	GAMeasurementID             string
}

type bucket struct {
	count int
	until time.Time
}
type Server struct {
	db               *sql.DB
	cfg              Config
	client           *http.Client
	paymentClient    *http.Client
	handler          http.Handler
	mu               sync.Mutex
	billingMu        sync.Mutex // Serializes provider-state refreshes in this single gateway.
	limits           map[string]bucket
	hashSlots        chan struct{}
	trustedProxies   []netip.Prefix
	guestSalt        string
	pageClient       *http.Client
	moderationRobots *crawler.Robots
}

func Open(cfg Config) (*Server, error) {
	if cfg.GAMeasurementID != "" && !regexp.MustCompile(`^G-[A-Z0-9]+$`).MatchString(cfg.GAMeasurementID) {
		return nil, fmt.Errorf("COSIFT_GA_MEASUREMENT_ID must match G-[A-Z0-9]+")
	}
	if err := cfg.defaultLimits(); err != nil {
		return nil, err
	}
	for name, raw := range map[string]string{"backend": cfg.Backend, "public URL": cfg.PublicURL} {
		u, err := url.Parse(raw)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
			return nil, fmt.Errorf("%s must be an http(s) origin without a path or credentials", name)
		}
		if name == "public URL" && u.Scheme != "https" && u.Hostname() != "localhost" && u.Hostname() != "127.0.0.1" && u.Hostname() != "::1" {
			return nil, fmt.Errorf("public URL must use HTTPS except on localhost")
		}
	}
	if cfg.AdminToken == "" {
		return nil, fmt.Errorf("COSIFT_COMMUNITY_ADMIN_TOKEN is required for contribution delivery")
	}
	cfg.Backend = strings.TrimRight(cfg.Backend, "/")
	cfg.PublicURL = strings.TrimRight(cfg.PublicURL, "/")
	db, err := openDB(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	s := &Server{db: db, cfg: cfg, limits: map[string]bucket{}, hashSlots: make(chan struct{}, 4), client: &http.Client{
		Timeout:       20 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}
	s.paymentClient = &http.Client{Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	s.pageClient = newModerationClient()
	s.moderationRobots = crawler.NewRobots(s.pageClient, "Cosift-Community/1.0")
	for _, raw := range cfg.TrustedProxies {
		prefix, err := netip.ParsePrefix(strings.TrimSpace(raw))
		if err != nil {
			db.Close()
			return nil, fmt.Errorf("invalid trusted proxy CIDR %q", raw)
		}
		s.trustedProxies = append(s.trustedProxies, prefix)
	}
	if _, err = db.Exec(`INSERT INTO settings(key,value) VALUES('guest_salt',?) ON CONFLICT(key) DO NOTHING`, randomID()); err != nil {
		db.Close()
		return nil, err
	}
	if err = db.QueryRow(`SELECT value FROM settings WHERE key='guest_salt'`).Scan(&s.guestSalt); err != nil {
		db.Close()
		return nil, err
	}
	if err := s.migrateGuestInterval(); err != nil {
		db.Close()
		return nil, err
	}
	if err := s.bindSharedNamespace(); err != nil {
		db.Close()
		return nil, err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/auth/config", func(w http.ResponseWriter, r *http.Request) {
		respond(w, 200, map[string]bool{"shared": s.cfg.Shared != nil, "supports_password": s.sharedPasswordProvider() != nil})
	})
	mux.HandleFunc("POST /api/auth/start", s.sharedStart)
	mux.HandleFunc("POST /api/auth/verify", s.sharedFinish)
	mux.HandleFunc("POST /api/auth/password", s.sharedPassword)
	mux.HandleFunc("POST /api/shared", s.auth(s.sharedTool))
	mux.HandleFunc("GET /{$}", s.asset("index.html", "text/html; charset=utf-8"))
	mux.HandleFunc("GET /login", s.asset("index.html", "text/html; charset=utf-8"))
	mux.HandleFunc("GET /signup", s.asset("index.html", "text/html; charset=utf-8"))
	mux.HandleFunc("GET /app.js", s.asset("app.js", "text/javascript; charset=utf-8"))
	mux.HandleFunc("GET /style.css", s.asset("style.css", "text/css; charset=utf-8"))
	mux.HandleFunc("GET /sample.csv", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Disposition", `attachment; filename="cosift-sample.csv"`)
		s.asset("sample.csv", "text/csv; charset=utf-8")(w, r)
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { respond(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("POST /api/register", s.credentials(true))
	mux.HandleFunc("POST /api/login", s.credentials(false))
	mux.HandleFunc("POST /api/logout", s.auth(s.logout))
	mux.HandleFunc("GET /api/me", s.auth(func(w http.ResponseWriter, r *http.Request, u User) { respond(w, 200, u) }))
	mux.HandleFunc("PUT /api/interests", s.auth(s.interests))
	for _, mode := range []string{"search", "answer", "research"} {
		handler := s.optionalAuth(func(w http.ResponseWriter, r *http.Request, u User) { s.retrieve(w, r, u, mode) })
		mux.HandleFunc("GET /api/"+mode, handler)
		mux.HandleFunc("GET /"+mode, handler)
	}
	mux.HandleFunc("GET /api/guest", s.guestStatus)
	mux.HandleFunc("GET /api/limits", func(w http.ResponseWriter, r *http.Request) { respond(w, 200, s.limitPolicy()) })
	mux.HandleFunc("GET /api/credits", s.auth(s.credits))
	mux.HandleFunc("GET /api/analytics", func(w http.ResponseWriter, r *http.Request) {
		respond(w, 200, map[string]string{"measurement_id": s.cfg.GAMeasurementID})
	})
	mux.HandleFunc("POST /api/payments/checkout", s.auth(s.checkout))
	mux.HandleFunc("POST /api/payments/portal", s.auth(s.portal))
	mux.HandleFunc("POST /api/payments/webhook", s.stripeWebhook)
	mux.HandleFunc("GET /api/saved", s.auth(s.saved))
	mux.HandleFunc("POST /api/saved", s.auth(s.save))
	mux.HandleFunc("DELETE /api/saved/{id}", s.auth(s.unsave))
	mux.HandleFunc("GET /api/submissions", s.auth(s.submissions))
	mux.HandleFunc("POST /api/submissions", s.auth(s.submit))
	s.handler = s.protect(mux)
	return s, nil
}

func (s *Server) Close() error                                     { return s.db.Close() }
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.handler.ServeHTTP(w, r) }

func (s *Server) asset(name, kind string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := assets.ReadFile("web/" + name)
		if err != nil {
			problem(w, 500, "asset unavailable")
			return
		}
		w.Header().Set("Content-Type", kind)
		w.Write(body)
	}
}

func (s *Server) protect(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		csp := "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'"
		if s.cfg.GAMeasurementID != "" {
			csp = strings.Replace(csp, "script-src 'self'", "script-src 'self' https://www.googletagmanager.com/gtag/js", 1)
			csp += "; connect-src 'self' https://www.google-analytics.com/g/collect https://region1.google-analytics.com/g/collect"
		}
		w.Header().Set("Content-Security-Policy", csp)
		// Stripe authenticates the exact webhook body with its signature.
		if r.Method != "GET" && r.Method != "HEAD" && !(r.Method == "POST" && r.URL.Path == "/api/payments/webhook") {
			// The custom header prevents cross-site form posts, including login
			// CSRF. No CORS permissions are granted. CLI requests omit Origin.
			if r.Header.Get("X-Cosift-Client") != "community" || (r.Header.Get("Origin") != "" && r.Header.Get("Origin") != s.cfg.PublicURL) || r.Header.Get("Sec-Fetch-Site") == "cross-site" {
				problem(w, 403, "request must originate from this community app")
				return
			}
		}
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		next.ServeHTTP(w, r)
	})
}

func respond(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func problem(w http.ResponseWriter, status int, msg string) {
	respond(w, status, map[string]string{"error": msg})
}
func decode(r *http.Request, v any) error {
	if kind, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type")); kind != "application/json" {
		return fmt.Errorf("expected application/json")
	}
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return err
	}
	if d.Decode(new(any)) != io.EOF {
		return fmt.Errorf("expected one JSON value")
	}
	return nil
}

func (s *Server) allow(key string, n int, window time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	for k, b := range s.limits {
		if now.After(b.until) {
			delete(s.limits, k)
		}
	}
	b := s.limits[key]
	if b.until.IsZero() {
		if len(s.limits) >= 10000 {
			return false
		}
		b.until = now.Add(window)
	}
	if b.count >= n {
		return false
	}
	b.count++
	s.limits[key] = b
	return true
}

func passwordHash(password, salt string) string {
	b, err := pbkdf2.Key(sha256.New, password, []byte(salt), 600000, 32)
	if err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

func (s *Server) credentials(register bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.cfg.Shared != nil {
			problem(w, 409, "use shared email-code login")
			return
		}
		ip := s.clientIP(r)
		if !s.allow("auth:"+ip, 30, time.Minute) {
			problem(w, 429, "too many attempts; try again in a minute")
			return
		}
		var in struct {
			Email    string `json:"email"`
			Password string `json:"password"`
			Name     string `json:"name"`
		}
		if decode(r, &in) != nil {
			problem(w, 400, "invalid account details")
			return
		}
		in.Email = strings.ToLower(strings.TrimSpace(in.Email))
		in.Name = strings.TrimSpace(in.Name)
		address, err := mail.ParseAddress(in.Email)
		if err != nil || address.Address != in.Email || len(in.Email) > 254 || len(in.Password) < 12 || len(in.Password) > 256 || (register && (in.Name == "" || len(in.Name) > 80)) {
			problem(w, 400, "enter a valid email, a name, and a password of 12–256 characters")
			return
		}
		if !s.allow("email:"+in.Email, 10, time.Minute) {
			problem(w, 429, "too many attempts; try again in a minute")
			return
		}
		select {
		case s.hashSlots <- struct{}{}:
			defer func() { <-s.hashSlots }()
		default:
			problem(w, 503, "please try again shortly")
			return
		}
		var id string
		if register {
			id = randomID()
			salt := randomID()
			_, err = s.db.ExecContext(r.Context(), `INSERT INTO users(id,email,name,salt,password_hash,created_at) VALUES(?,?,?,?,?,?)`, id, in.Email, in.Name, salt, passwordHash(in.Password, salt), time.Now().Unix())
			if err != nil {
				var exists int
				if s.db.QueryRowContext(r.Context(), `SELECT 1 FROM users WHERE email=?`, in.Email).Scan(&exists) == nil {
					problem(w, 409, "unable to create account; try signing in")
				} else {
					problem(w, 500, "unable to create account")
				}
				return
			}
		} else {
			var salt, hash string
			err = s.db.QueryRowContext(r.Context(), `SELECT id,salt,password_hash FROM users WHERE email=?`, in.Email).Scan(&id, &salt, &hash)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				problem(w, 500, "sign in unavailable")
				return
			}
			if salt == "" {
				salt = "cosift-missing-account-timing-salt"
			}
			candidate := passwordHash(in.Password, salt)
			if err != nil || subtle.ConstantTimeCompare([]byte(candidate), []byte(hash)) != 1 {
				problem(w, 401, "email or password is incorrect")
				return
			}
		}
		token := randomID()
		expires := time.Now().Add(sessionAge)
		if old, err := r.Cookie(cookieName); err == nil {
			_, _ = s.db.ExecContext(r.Context(), `DELETE FROM sessions WHERE hash=?`, tokenHash(old.Value))
		}
		_, err = s.db.ExecContext(r.Context(), `INSERT INTO sessions(hash,user_id,expires_at) VALUES(?,?,?)`, tokenHash(token), id, expires.Unix())
		if err != nil {
			problem(w, 500, "could not create session")
			return
		}
		http.SetCookie(w, &http.Cookie{Name: cookieName, Value: token, Path: "/", HttpOnly: true, Secure: strings.HasPrefix(s.cfg.PublicURL, "https:"), SameSite: http.SameSiteLaxMode, Expires: expires, MaxAge: int(sessionAge.Seconds())})
		u, err := scanUser(s.db.QueryRowContext(r.Context(), `SELECT id,email,name,interests,onboarded FROM users WHERE id=?`, id))
		if err != nil {
			problem(w, 500, "could not load account")
			return
		}
		respond(w, 200, u)
	}
}

type userHandler func(http.ResponseWriter, *http.Request, User)

func (s *Server) auth(next userHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.cfg.Shared != nil || r.Header.Get("Authorization") != "" {
			s.sharedAuth(w, r, next, false)
			return
		}
		cookie, err := r.Cookie(cookieName)
		if err != nil {
			problem(w, 401, "sign in to continue")
			return
		}
		u, err := scanUser(s.db.QueryRowContext(r.Context(), `SELECT u.id,u.email,u.name,u.interests,u.onboarded FROM users u JOIN sessions s ON u.id=s.user_id WHERE s.hash=? AND s.expires_at>?`, tokenHash(cookie.Value), time.Now().Unix()))
		if errors.Is(err, sql.ErrNoRows) {
			http.SetCookie(w, &http.Cookie{Name: cookieName, Path: "/", MaxAge: -1, HttpOnly: true, Secure: strings.HasPrefix(s.cfg.PublicURL, "https:"), SameSite: http.SameSiteLaxMode})
			problem(w, 401, "session expired; sign in again")
			return
		}
		if err != nil {
			problem(w, 500, "account unavailable")
			return
		}
		next(w, r, u)
	}
}

func (s *Server) logout(w http.ResponseWriter, r *http.Request, u User) {
	if s.cfg.Shared != nil {
		if err := s.cfg.Shared.Revoke(sharedaccount.WithClientIP(r.Context(), s.clientIP(r)), sharedToken(r)); err != nil {
			sharedProblem(w, err)
			return
		}
		http.SetCookie(w, s.sharedCookie("", -1))
		respond(w, 200, map[string]bool{"ok": true})
		return
	}
	c, _ := r.Cookie(cookieName)
	if _, err := s.db.ExecContext(r.Context(), `DELETE FROM sessions WHERE hash=?`, tokenHash(c.Value)); err != nil {
		problem(w, 500, "could not sign out")
		return
	}
	http.SetCookie(w, &http.Cookie{Name: cookieName, Path: "/", MaxAge: -1, HttpOnly: true, Secure: strings.HasPrefix(s.cfg.PublicURL, "https:"), SameSite: http.SameSiteLaxMode})
	respond(w, 200, map[string]bool{"ok": true})
}

func (s *Server) interests(w http.ResponseWriter, r *http.Request, u User) {
	var in struct {
		Interests []string `json:"interests"`
	}
	if decode(r, &in) != nil || len(in.Interests) > 20 {
		problem(w, 400, "choose up to 20 interests")
		return
	}
	values := []string{}
	seen := map[string]bool{}
	for _, v := range in.Interests {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		if len(v) > 60 {
			problem(w, 400, "keep each interest under 60 characters")
			return
		}
		key := strings.ToLower(v)
		if !seen[key] {
			values = append(values, v)
			seen[key] = true
		}
	}
	if s.cfg.Shared != nil && len(values) > 0 {
		if _, err := s.cfg.Shared.Call(r.Context(), sharedToken(r), "cosift_topics", map[string]any{"action": "add", "topics": values}); err != nil {
			sharedProblem(w, err)
			return
		}
	}
	b, _ := json.Marshal(values)
	if _, err := s.db.ExecContext(r.Context(), `UPDATE users SET interests=?,onboarded=1 WHERE id=?`, string(b), u.ID); err != nil {
		problem(w, 500, "could not save interests")
		return
	}
	u.Interests = values
	u.Onboarded = true
	respond(w, 200, u)
}

func validQuery(q string) bool   { return strings.TrimSpace(q) != "" && len(q) <= 500 }
func validMode(mode string) bool { return mode == "search" || mode == "answer" || mode == "research" }
func (s *Server) retrieve(w http.ResponseWriter, r *http.Request, u User, mode string) {
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	if !validQuery(q) {
		problem(w, 400, "enter a search of 1–500 characters")
		return
	}
	// MCP asks for bounded BM25 results. Validate before reserving any allowance.
	params := url.Values{"q": {q}, "stream": {"false"}}
	if mode == "search" && u.ID != "" {
		if raw := r.URL.Query().Get("k"); raw != "" {
			k, err := strconv.Atoi(raw)
			if err != nil || k < 1 || k > 20 {
				problem(w, 400, "k must be 1–20")
				return
			}
			params.Set("k", strconv.Itoa(k))
		}
		if v := r.URL.Query().Get("retriever"); v != "" {
			if v != "bm25" && v != "dense" && v != "hybrid" {
				problem(w, 400, "invalid retriever")
				return
			}
			params.Set("retriever", v)
		}
	}
	completed := false
	if u.ID == "" {
		finish, ok := s.reserveGuest(w, r, mode)
		if !ok {
			return
		}
		defer func() { finish(completed) }()
	}
	// Hard mode caps apply before charging every authenticated request.
	finishMode, ok := s.allowRetrieval(w, r, u, mode)
	if !ok {
		return
	}
	defer func() { finishMode(completed) }()
	if u.ID != "" {
		finish, ok := s.reserveCredit(w, r, u, mode)
		if !ok {
			return
		}
		defer func() { finish(completed) }()
	}

	// Preserve the backend's normal retrieval, reranking and research defaults.
	req, _ := http.NewRequestWithContext(r.Context(), "GET", s.cfg.Backend+"/"+mode+"?"+params.Encode(), nil)
	client := *s.client
	if mode != "search" {
		client.Timeout = 3 * time.Minute
	}
	res, err := client.Do(req)
	if err != nil {
		problem(w, 502, mode+" is temporarily unavailable; please try again")
		return
	}
	defer res.Body.Close()
	body, err := io.ReadAll(io.LimitReader(res.Body, (4<<20)+1))
	if res.StatusCode == http.StatusNotImplemented && mode != "search" {
		problem(w, 503, strings.ToUpper(mode[:1])+mode[1:]+" requires a configured LLM on the Cosift backend. Search is still available.")
		return
	}
	if err != nil || res.StatusCode != 200 || len(body) > 4<<20 || !json.Valid(body) {
		problem(w, 502, mode+" is temporarily unavailable; please try again")
		return
	}
	completed = true
	w.Header().Set("Content-Type", "application/json")
	w.Write(body)
}

type SavedSearch struct {
	ID        string `json:"id"`
	Query     string `json:"query"`
	Mode      string `json:"mode"`
	CreatedAt int64  `json:"created_at"`
}

func (s *Server) saved(w http.ResponseWriter, r *http.Request, u User) {
	rows, err := s.db.QueryContext(r.Context(), `SELECT id,query,mode,created_at FROM saved_searches WHERE user_id=? ORDER BY created_at DESC,id LIMIT 200`, u.ID)
	if err != nil {
		problem(w, 500, "could not load saved searches")
		return
	}
	defer rows.Close()
	out := []SavedSearch{}
	for rows.Next() {
		var v SavedSearch
		if rows.Scan(&v.ID, &v.Query, &v.Mode, &v.CreatedAt) != nil {
			problem(w, 500, "could not load saved searches")
			return
		}
		out = append(out, v)
	}
	if rows.Err() != nil {
		problem(w, 500, "could not load saved searches")
		return
	}
	respond(w, 200, out)
}
func (s *Server) save(w http.ResponseWriter, r *http.Request, u User) {
	var in struct {
		Query string `json:"query"`
		Mode  string `json:"mode"`
	}
	if decode(r, &in) != nil || !validQuery(in.Query) {
		problem(w, 400, "enter a search of 1–500 characters")
		return
	}
	in.Query = strings.TrimSpace(in.Query)
	if in.Mode == "" {
		in.Mode = "search"
	}
	if !validMode(in.Mode) {
		problem(w, 400, "mode must be search, answer, or research")
		return
	}
	// The INSERT's count predicate enforces the cap atomically.
	_, err := s.db.ExecContext(r.Context(), `INSERT INTO saved_searches(id,user_id,query,mode,created_at) SELECT ?,?,?,?,? WHERE (SELECT count(*) FROM saved_searches WHERE user_id=?)<200 ON CONFLICT(user_id,query,mode) DO NOTHING`, randomID(), u.ID, in.Query, in.Mode, time.Now().Unix(), u.ID)
	if err != nil {
		problem(w, 500, "could not save search")
		return
	}
	var v SavedSearch
	err = s.db.QueryRowContext(r.Context(), `SELECT id,query,mode,created_at FROM saved_searches WHERE user_id=? AND query=? AND mode=?`, u.ID, in.Query, in.Mode).Scan(&v.ID, &v.Query, &v.Mode, &v.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		problem(w, 409, "saved search limit reached; remove a search first")
		return
	}
	if err != nil {
		problem(w, 500, "could not save search")
		return
	}
	respond(w, 200, v)
}
func (s *Server) unsave(w http.ResponseWriter, r *http.Request, u User) {
	res, err := s.db.ExecContext(r.Context(), `DELETE FROM saved_searches WHERE id=? AND user_id=?`, r.PathValue("id"), u.ID)
	if err != nil {
		problem(w, 500, "could not remove search")
		return
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		problem(w, 404, "saved search not found")
		return
	}
	respond(w, 200, map[string]bool{"ok": true})
}

type Submission struct {
	ID        string `json:"id"`
	URL       string `json:"url"`
	Status    string `json:"status"`
	Reason    string `json:"reason"`
	CreatedAt int64  `json:"created_at"`
}

func (s *Server) submissions(w http.ResponseWriter, r *http.Request, u User) {
	rows, err := s.db.QueryContext(r.Context(), `SELECT id,url,status,reason,created_at FROM submissions WHERE user_id=? ORDER BY created_at DESC,id LIMIT 200`, u.ID)
	if err != nil {
		problem(w, 500, "could not load contributions")
		return
	}
	defer rows.Close()
	out := []Submission{}
	for rows.Next() {
		var v Submission
		if rows.Scan(&v.ID, &v.URL, &v.Status, &v.Reason, &v.CreatedAt) != nil {
			problem(w, 500, "could not load contributions")
			return
		}
		out = append(out, v)
	}
	if rows.Err() != nil {
		problem(w, 500, "could not load contributions")
		return
	}
	respond(w, 200, out)
}
func (s *Server) submit(w http.ResponseWriter, r *http.Request, u User) {
	if u.ID == "" {
		problem(w, http.StatusUnauthorized, "contributions require login")
		return
	}
	if !s.allow("submit:"+u.ID+":"+s.clientIP(r), 20, time.Minute) {
		problem(w, 429, "too many submissions; try again in a minute")
		return
	}
	var values []string
	artifacts := map[string]*crawler.LocalArtifact{}
	var err error
	kind, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if kind == "multipart/form-data" {
		if err = r.ParseMultipartForm(1 << 20); err != nil {
			problem(w, 400, "upload a CSV smaller than 1 MB")
			return
		}
		defer r.MultipartForm.RemoveAll()
		f, _, e := r.FormFile("file")
		if e != nil {
			problem(w, 400, "choose a CSV file")
			return
		}
		defer f.Close()
		values, err = ParseCSV(f)
	} else {
		var in struct {
			URLs      []string                 `json:"urls"`
			Artifacts []*crawler.LocalArtifact `json:"artifacts,omitempty"`
		}
		err = decode(r, &in)
		values = in.URLs
		if len(in.Artifacts) > 0 {
			if len(values) > 0 {
				problem(w, 400, "use URLs or local artifacts")
				return
			}
			for _, a := range in.Artifacts {
				if e := a.Validate(); e != nil {
					problem(w, 400, e.Error())
					return
				}
				canonical, e := NormalizeURL(a.URL)
				if e != nil {
					problem(w, 400, e.Error())
					return
				}
				a.URL = canonical
				values = append(values, canonical)
				artifacts[canonical] = a
			}
		}
	}
	if err != nil {
		problem(w, 400, err.Error())
		return
	}
	values, err = normalizeURLs(values)
	if err != nil {
		problem(w, 400, err.Error())
		return
	}
	tx, err := s.db.BeginTx(r.Context(), nil)
	if err != nil {
		problem(w, 500, "could not save contribution")
		return
	}
	defer tx.Rollback()
	var count int
	now := time.Now().Unix()
	if err = tx.QueryRowContext(r.Context(), `SELECT count(*) FROM submissions WHERE user_id=? AND created_at>?`, u.ID, now-86400).Scan(&count); err != nil {
		problem(w, 500, "could not check contribution limit")
		return
	}
	accepted := 0
	duplicates := 0
	for _, v := range values {
		id := randomID()
		res, e := tx.ExecContext(r.Context(), `INSERT INTO submissions(id,user_id,url,created_at) VALUES(?,?,?,?) ON CONFLICT(user_id,url) DO NOTHING`, id, u.ID, v, now)
		if e != nil {
			problem(w, 500, "could not save contribution")
			return
		}
		n, _ := res.RowsAffected()
		if n == 0 {
			duplicates++
		} else {
			accepted++
			if a := artifacts[v]; a != nil {
				payload, _ := json.Marshal(a)
				if _, e := tx.ExecContext(r.Context(), `INSERT INTO submission_artifacts(submission_id,payload) VALUES(?,?)`, id, string(payload)); e != nil {
					problem(w, 500, "could not save local index")
					return
				}
			}
		}
	}
	if count+accepted > dailyContributionLimit {
		var nextSlot int64
		if err := tx.QueryRowContext(r.Context(), `SELECT created_at+86400 FROM submissions WHERE user_id=? AND created_at>? ORDER BY created_at,id LIMIT 1 OFFSET ?`, u.ID, now-86400, count+accepted-dailyContributionLimit-1).Scan(&nextSlot); err != nil {
			problem(w, 500, "could not check contribution limit")
			return
		}
		retry := max(int64(1), nextSlot-now)
		w.Header().Set("Retry-After", strconv.FormatInt(retry, 10))
		problem(w, 429, fmt.Sprintf("daily limit is %d new webpages; %d remaining in the current 24-hour window. Retry this batch in %d seconds.", dailyContributionLimit, max(0, dailyContributionLimit-count), retry))
		return
	}
	if tx.Commit() != nil {
		problem(w, 500, "could not save contribution")
		return
	}
	respond(w, 202, map[string]any{"accepted": accepted, "duplicates": duplicates, "status": "pending"})
}

// Run dispatches durable submissions to the existing crawl frontier. A crash
// after enqueue but before acknowledgement may resend; frontier enqueue is
// idempotent. Only one Run loop should own a community database.
func (s *Server) Run(ctx context.Context) {
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	for {
		if err := s.dispatch(ctx); err != nil && ctx.Err() == nil {
			log.Printf("community dispatch: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}
func (s *Server) dispatch(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM sessions WHERE expires_at<=?`, time.Now().Unix())
	if err != nil {
		return err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id,url,attempts FROM submissions WHERE status='pending' AND next_attempt<=? ORDER BY next_attempt,created_at,id LIMIT 20`, time.Now().Unix())
	if err != nil {
		return err
	}
	type job struct {
		id, url  string
		attempts int
	}
	jobs := []job{}
	for rows.Next() {
		var j job
		if err := rows.Scan(&j.id, &j.url, &j.attempts); err != nil {
			rows.Close()
			return err
		}
		jobs = append(jobs, j)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, j := range jobs {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		status, reason, approvedContentHash := s.prevalidate(ctx, j.url)
		if status == "allowed" && approvedContentHash == "" {
			status, reason = "pending", "Waiting for a content-bound safety decision."
		}
		if status != "allowed" {
			// Inconclusive/transient checks never fall through to enqueue.
			delay := time.Duration(1<<min(j.attempts, 9)) * 5 * time.Second
			if _, err := s.db.ExecContext(ctx, `UPDATE submissions SET status=?,reason=?,attempts=attempts+1,next_attempt=? WHERE id=?`, status, reason, time.Now().Add(delay).Unix(), j.id); err != nil {
				return err
			}
			continue
		}
		var artifact *crawler.LocalArtifact
		var payload string
		e := s.db.QueryRowContext(ctx, `SELECT payload FROM submission_artifacts WHERE submission_id=?`, j.id).Scan(&payload)
		if e != nil && !errors.Is(e, sql.ErrNoRows) {
			return e
		}
		if payload != "" {
			if e := json.Unmarshal([]byte(payload), &artifact); e != nil {
				return e
			}
		}
		body, _ := json.Marshal(map[string]any{"submission_id": j.id, "url": j.url, "artifact": artifact, "approved_content_hash": approvedContentHash})
		req, _ := http.NewRequestWithContext(ctx, "POST", s.cfg.Backend+"/admin/community-enqueue", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+s.cfg.AdminToken)
		delivery := *s.client
		delivery.Timeout = 150 * time.Second
		res, sendErr := delivery.Do(req)
		ok := false
		indexed := false
		permanent := false
		if sendErr == nil {
			permanent = res.StatusCode == http.StatusUnprocessableEntity || res.StatusCode == http.StatusConflict
			ok = res.StatusCode >= 200 && res.StatusCode < 300
			if !ok {
				log.Printf("community: contribution delivery returned HTTP %d", res.StatusCode)
			}
			var receipt struct {
				Indexed     bool   `json:"indexed"`
				Novel       bool   `json:"novel"`
				ContentHash string `json:"content_hash"`
				Queued      string `json:"queued"` // explicit acknowledgement from older guarded backends
			}
			if ok {
				data, readErr := io.ReadAll(io.LimitReader(res.Body, 4097))
				ok = readErr == nil && len(data) <= 4096 && json.Unmarshal(data, &receipt) == nil
				if ok {
					hash, hashErr := hex.DecodeString(receipt.ContentHash)
					if receipt.Indexed {
						ok = hashErr == nil && len(hash) == sha256.Size
					} else {
						ok = receipt.Queued == j.url && !receipt.Novel
					}
				}
			}
			if ok {
				indexed = receipt.Indexed
				if receipt.Indexed && receipt.Novel && len(receipt.ContentHash) == 64 {
					if err := s.rewardContribution(ctx, j.id, receipt.ContentHash); err != nil {
						res.Body.Close()
						return err
					}
				}
			}
			res.Body.Close()
		} else if ctx.Err() == nil {
			log.Printf("community: contribution backend unavailable; retained for retry")
		}
		status = "pending"
		reason = "Content checks passed; waiting for crawler delivery."
		if permanent {
			status = "unverified"
			reason = "The webpage or local artifact does not meet index validation policy. Check the allowed domain, current page content, and embedding model."
		}
		if ok {
			status = "queued"
			reason = "Content checks passed; delivered to the crawl queue."
			if indexed {
				status = "indexed"
				reason = "Content checks passed and webpage indexed."
			}
		}
		delay := time.Duration(1<<min(j.attempts, 9)) * 5 * time.Second
		_, err = s.db.ExecContext(ctx, `UPDATE submissions SET status=?,reason=?,attempts=attempts+1,next_attempt=? WHERE id=?`, status, reason, time.Now().Add(delay).Unix(), j.id)
		if err != nil {
			return err
		}
	}
	return nil
}
