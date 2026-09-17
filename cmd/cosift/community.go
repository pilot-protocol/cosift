package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/pilot-protocol/cosift/internal/config"
	"io"
	"log"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/pilot-protocol/cosift/internal/community"
)

func runCommunity(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("community", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:7780", "listen address")
	publicURL := fs.String("public-url", "http://127.0.0.1:7780", "browser origin; HTTPS required outside localhost")
	backend := fs.String("backend", "http://127.0.0.1:7777", "Cosift Pebble server origin")
	dir := fs.String("data-dir", "./community-data", "private account database directory")
	proxies := fs.String("trusted-proxies", "", "comma-separated proxy CIDRs allowed to supply X-Forwarded-For")
	guestInterval := fs.Duration("guest-interval", time.Minute, "shared guest allowance interval")
	freeRPM := fs.Int("member-free-rpm", 60, "shared free member requests per minute")
	searchRPM := fs.Int("search-rpm", 120, "member Search hard cap per minute, including credit requests")
	answerRPM := fs.Int("answer-rpm", 20, "member Answer hard cap per minute, including credit requests")
	researchLimit := fs.Int("research-per-10m", 3, "member Research hard cap per ten minutes, including credit requests")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected arguments: %v", fs.Args())
	}
	var trusted []string
	if *proxies != "" {
		trusted = strings.Split(*proxies, ",")
	}
	s, err := community.Open(community.Config{DataDir: *dir, Backend: *backend, PublicURL: *publicURL, AdminToken: os.Getenv("COSIFT_COMMUNITY_ADMIN_TOKEN"), TrustedProxies: trusted, GuestInterval: *guestInterval, MemberFreeRPM: *freeRPM, SearchRPM: *searchRPM, AnswerRPM: *answerRPM, ResearchPer10Min: *researchLimit, StripeSecretKey: os.Getenv("STRIPE_SECRET_KEY"), StripeWebhookSecret: os.Getenv("STRIPE_WEBHOOK_SECRET")})
	if err != nil {
		return err
	}
	defer s.Close()
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); s.Run(workerCtx) }()
	defer func() { cancel(); <-done }()
	srv := &http.Server{Addr: *addr, Handler: s, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 20 * time.Second, WriteTimeout: 4 * time.Minute, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	stopped := make(chan struct{})
	defer close(stopped)
	go func() {
		select {
		case <-ctx.Done():
			shutdownCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
			defer c()
			_ = srv.Shutdown(shutdownCtx)
		case <-stopped:
		}
	}()
	log.Printf("community: listening on %s (public origin %s)", *addr, *publicURL)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func runContribute(ctx context.Context, args []string) error {
	return runContributeConfigured(ctx, nil, args)
}

func runContributeConfigured(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("contribute", flag.ContinueOnError)
	server := fs.String("server", "http://127.0.0.1:7780", "community app origin")
	email := fs.String("email", os.Getenv("COSIFT_EMAIL"), "account email (or COSIFT_EMAIL)")
	file := fs.String("csv", "", "CSV file with webpage URLs; - reads stdin")
	requestMode := fs.Bool("request", false, "perform a community Search, Answer or Research request")
	query := fs.String("query", "", "query for a community request")
	mode := fs.String("mode", "search", "search, answer or research")
	local := fs.Bool("index-locally", false, "fetch, index and embed locally, then contribute verified artifacts (requires login and embedding config)")
	credits := fs.Bool("credits", false, "show the authenticated account credit balance")
	sessionFile := fs.String("session-file", os.Getenv("COSIFT_SESSION_FILE"), "private saved CLI session (or COSIFT_SESSION_FILE)")
	login := fs.Bool("login", false, "save an authenticated CLI session")
	logout := fs.Bool("logout", false, "revoke and delete the saved CLI session")
	guest := fs.Bool("guest", false, "submit without login (server guest limits apply)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	u, err := url.Parse(*server)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") || (u.Scheme != "http" && u.Scheme != "https") {
		return fmt.Errorf("server must be an http(s) origin")
	}
	if u.Scheme != "https" && u.Hostname() != "localhost" && u.Hostname() != "127.0.0.1" && u.Hostname() != "::1" {
		return fmt.Errorf("use HTTPS to protect account credentials")
	}
	password := os.Getenv("COSIFT_PASSWORD")
	if !*guest && (*login || *sessionFile == "") && ((*email == "") != (password == "")) {
		return fmt.Errorf("set both COSIFT_EMAIL and COSIFT_PASSWORD, or use -guest")
	}
	origin := strings.TrimRight(*server, "/")
	values := fs.Args()
	authOnly := *login || *logout
	if authOnly && (*login && *logout || *guest || *requestMode || *local || *credits || len(values) > 0 || *file != "" || *query != "" || *mode != "search") {
		return fmt.Errorf("login/logout cannot be combined with other operations")
	}
	if authOnly && *sessionFile == "" {
		return fmt.Errorf("provide -session-file or COSIFT_SESSION_FILE for login/logout")
	}
	if *login && (*email == "" || password == "") {
		return fmt.Errorf("login requires COSIFT_EMAIL and COSIFT_PASSWORD")
	}
	// Validate intent before login, reading stdin, or touching the local index.
	if *requestMode {
		if *local || *credits || len(values) > 0 || *file != "" {
			return fmt.Errorf("request cannot be combined with contributions or credits")
		}
		*query = strings.TrimSpace(*query)
		if len(*query) == 0 || len(*query) > 500 || (*mode != "search" && *mode != "answer" && *mode != "research") {
			return fmt.Errorf("provide a 1–500 byte -query and a valid -mode")
		}
	} else if *query != "" || *mode != "search" {
		return fmt.Errorf("-query and -mode require request")
	}
	if *credits && (*local || len(values) > 0 || *file != "") {
		return fmt.Errorf("credits cannot be combined with contributions")
	}
	if (*local || *credits) && (*guest || (*email == "" && *sessionFile == "")) {
		return fmt.Errorf("local indexing and credits require login or a saved session")
	}
	if *file != "" {
		if len(values) > 0 {
			return fmt.Errorf("use either -csv or positional URLs")
		}
		var reader io.Reader = os.Stdin
		if *file != "-" {
			f, e := os.Open(*file)
			if e != nil {
				return e
			}
			defer f.Close()
			reader = f
		}
		data, e := io.ReadAll(io.LimitReader(reader, (1<<20)+1))
		if e != nil {
			return e
		}
		if len(data) > 1<<20 {
			return fmt.Errorf("CSV must be smaller than 1 MB")
		}
		values, err = community.ParseCSV(bytes.NewReader(data))
		if err != nil {
			return err
		}
	}
	if !authOnly && !*credits && !*requestMode && (len(values) == 0 || len(values) > community.MaxURLs) {
		return fmt.Errorf("provide 1–100 webpage URLs or -csv FILE")
	}
	for i, v := range values {
		values[i], err = community.NormalizeURL(v)
		if err != nil {
			return fmt.Errorf("URL %d: %w", i+1, err)
		}
	}
	jar, _ := cookiejar.New(nil)
	if !*guest && !*login && *sessionFile != "" {
		saved, err := readCommunitySession(*sessionFile, origin, *logout)
		if err != nil {
			return err
		}
		// Logout must reach the server even when the local expiry has elapsed.
		cookie := saved.cookie()
		if *logout {
			cookie.Expires = time.Time{}
		}
		jar.SetCookies(u, []*http.Cookie{cookie})
	}
	var loginCookie *http.Cookie
	client := &http.Client{Jar: jar, Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	call := func(callCtx context.Context, path string, body any) ([]byte, error) {
		b, _ := json.Marshal(body)
		method := "POST"
		if body == nil {
			method = "GET"
		}
		req, err := http.NewRequestWithContext(callCtx, method, strings.TrimRight(*server, "/")+"/api/"+path, bytes.NewReader(b))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Cosift-Client", "community")
		res, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		defer res.Body.Close()
		if path == "login" {
			for _, c := range res.Cookies() {
				if c.Name == "cosift_session" {
					loginCookie = c
				}
			}
		}
		data, err := io.ReadAll(io.LimitReader(res.Body, (4<<20)+1))
		if err != nil {
			return nil, err
		}
		if len(data) > 4<<20 {
			return nil, fmt.Errorf("community response exceeds 4 MB")
		}
		if res.StatusCode < 200 || res.StatusCode >= 300 {
			return nil, &communityHTTPError{path: path, status: res.StatusCode, message: strings.TrimSpace(string(data))}
		}
		if !json.Valid(data) {
			return nil, fmt.Errorf("community returned invalid JSON")
		}
		return data, nil
	}
	if *logout {
		_, err := call(ctx, "logout", map[string]string{})
		var apiErr *communityHTTPError
		if err != nil && !(errors.As(err, &apiErr) && apiErr.status == http.StatusUnauthorized) {
			return err
		}
		if err := os.Remove(*sessionFile); err != nil {
			return err
		}
		_, err = fmt.Fprintln(os.Stdout, `{"logged_out":true}`)
		return err
	}
	var sessionOut *os.File
	keepSession := false
	if *login {
		// Refuse to overwrite existing sessions or follow symlinks. Logout first.
		sessionOut, err = os.OpenFile(*sessionFile, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return fmt.Errorf("create private session file (log out before replacing): %w", err)
		}
		defer func() {
			sessionOut.Close()
			if !keepSession {
				_ = os.Remove(*sessionFile)
			}
		}()
	}
	if !*guest && *email != "" && (*login || *sessionFile == "") {
		// Revoke temporary sessions, including a login with an unusable response
		// or a session whose persistence failed.
		defer func() {
			if keepSession || len(jar.Cookies(u)) == 0 {
				return
			}
			logoutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, _ = call(logoutCtx, "logout", map[string]string{})
		}()
		if _, err := call(ctx, "login", map[string]string{"email": *email, "password": password}); err != nil {
			return err
		}
	}
	if *login {
		if loginCookie == nil || loginCookie.Value == "" || !loginCookie.Expires.After(time.Now()) || len(jar.Cookies(u)) == 0 {
			return fmt.Errorf("server did not issue a usable session")
		}
		saved := communitySession{Origin: origin, Token: loginCookie.Value, Expires: loginCookie.Expires}
		if err := json.NewEncoder(sessionOut).Encode(saved); err != nil {
			return err
		}
		if err := sessionOut.Sync(); err != nil {
			return err
		}
		if err := sessionOut.Close(); err != nil {
			return err
		}
		keepSession = true
		_, err := fmt.Fprintln(os.Stdout, `{"logged_in":true}`)
		return err
	}
	var body any = map[string]any{"urls": values}
	path := "submissions"
	if *local {
		artifacts, e := indexLocalContributions(ctx, cfg, values)
		if e != nil {
			return e
		}
		body = map[string]any{"artifacts": artifacts}
		encoded, _ := json.Marshal(body)
		if len(encoded) > 1<<20 {
			return fmt.Errorf("local artifacts exceed 1 MB; submit fewer URLs")
		}
	}
	if *requestMode {
		path = *mode + "?q=" + url.QueryEscape(*query)
		body = nil
		client.Timeout = 4 * time.Minute
	}
	if *credits {
		path = "credits"
		body = nil
	}
	result, err := call(ctx, path, body)
	if err != nil {
		return err
	}
	_, err = os.Stdout.Write(result)
	return err
}
