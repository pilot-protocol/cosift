package main

import (
	"bytes"
	"context"
	"encoding/json"
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
	s, err := community.Open(community.Config{DataDir: *dir, Backend: *backend, PublicURL: *publicURL, AdminToken: os.Getenv("COSIFT_COMMUNITY_ADMIN_TOKEN"), TrustedProxies: trusted})
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
	guest := fs.Bool("guest", false, "submit without login (one request per 30 minutes per IP)")
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
	if !*guest && ((*email == "") != (password == "")) {
		return fmt.Errorf("set both COSIFT_EMAIL and COSIFT_PASSWORD, or use -guest")
	}
	values := fs.Args()
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
	if !*credits && !*requestMode && (len(values) == 0 || len(values) > community.MaxURLs) {
		return fmt.Errorf("provide 1–100 webpage URLs or -csv FILE")
	}
	for i, v := range values {
		values[i], err = community.NormalizeURL(v)
		if err != nil {
			return fmt.Errorf("URL %d: %w", i+1, err)
		}
	}
	jar, _ := cookiejar.New(nil)
	client := &http.Client{Jar: jar, Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	call := func(path string, body any) ([]byte, error) {
		b, _ := json.Marshal(body)
		method := "POST"
		if body == nil {
			method = "GET"
		}
		req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(*server, "/")+"/api/"+path, bytes.NewReader(b))
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
		data, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
		if err != nil {
			return nil, err
		}
		if res.StatusCode < 200 || res.StatusCode >= 300 {
			return nil, fmt.Errorf("community %s: HTTP %d: %s", path, res.StatusCode, strings.TrimSpace(string(data)))
		}
		return data, nil
	}
	if !*guest && *email != "" {
		if _, err := call("login", map[string]string{"email": *email, "password": password}); err != nil {
			return err
		}
		// Revoke this CLI session after use; browser sessions are separate.
		defer func() { _, _ = call("logout", map[string]string{}) }()
	}
	var body any = map[string]any{"urls": values}
	path := "submissions"
	if *local || *credits {
		if *guest || *email == "" {
			return fmt.Errorf("local indexing and credits require email/password login")
		}
		if *local && *credits {
			return fmt.Errorf("use -index-locally or -credits")
		}
	}
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
		if *local || *credits || len(values) > 0 || *file != "" {
			return fmt.Errorf("request cannot be combined with contributions or credits")
		}
		if strings.TrimSpace(*query) == "" || (*mode != "search" && *mode != "answer" && *mode != "research") {
			return fmt.Errorf("provide -query and a valid -mode")
		}
		path = *mode + "?q=" + url.QueryEscape(*query)
		body = nil
		client.Timeout = 4 * time.Minute
	}
	if *credits {
		path = "credits"
		body = nil
	}
	result, err := call(path, body)
	if err != nil {
		return err
	}
	_, err = os.Stdout.Write(result)
	return err
}
