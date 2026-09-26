package svcauth

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"google.golang.org/api/idtoken"
	"google.golang.org/api/option"

	v1 "github.com/pilot-protocol/cosift/internal/v1"
)

type Options struct {
	Path     string
	OwnerUID uint32
	// Pepper is COSIFT_SVC_PEPPER; under MinPepperLen bytes key auth is off.
	Pepper         []byte
	PeerAuthToken  string
	AdminToken     string
	MainAddr       string
	ClientIPHeader string
	// Readiness gates every route not marked Ungated; the article store owns it.
	Readiness *v1.Readiness
	// Policy is built before the article store, which takes it; nil makes one.
	Policy     *Policy
	Logger     *log.Logger
	Now        func() time.Time
	CertSource CertSource
	Validator  TokenValidator
}

// Service is the /v1 listener: config, credentials, limits, audit and the
// http.Server that serves the route table.
type Service struct {
	opts    Options
	Policy  *Policy
	Metrics *Metrics
	log     *log.Logger
	now     func() time.Time
	ready   *v1.Readiness
	certs   *CertCache
	oidc    *oidcVerifier
	failed  *failedAuthLimiter
	mux     *http.ServeMux

	mu       sync.Mutex
	srv      *http.Server
	addr     string
	closed   bool
	certsRun bool

	summaryEvery time.Duration
	done         chan struct{}
}

// New builds the service; Mount the route table before Start.
func New(opts Options) (*Service, error) {
	if opts.Path == "" {
		opts.Path = DefaultPath
	}
	if opts.Logger == nil {
		opts.Logger = log.Default()
	}
	opts.ClientIPHeader = strings.TrimSpace(opts.ClientIPHeader)
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Readiness == nil {
		opts.Readiness = &v1.Readiness{}
	}
	if opts.CertSource == nil {
		opts.CertSource = HTTPCertSource(NewCertHTTPClient())
	}
	if opts.Policy == nil {
		opts.Policy = NewPolicy(nil, opts.Now)
	}
	m := opts.Policy.metrics
	s := &Service{
		opts:    opts,
		Policy:  opts.Policy,
		Metrics: m,
		log:     opts.Logger,
		now:     opts.Now,
		ready:   opts.Readiness,

		summaryEvery: 15 * time.Second,
		done:         make(chan struct{}),
	}
	s.certs = NewCertCache(opts.CertSource, m, opts.Now, s.log.Printf)
	s.failed = newFailedAuthLimiter(opts.Now, s.log.Printf)
	validator := opts.Validator
	if validator == nil {
		v, err := idtoken.NewValidator(context.Background(), option.WithHTTPClient(&http.Client{Transport: s.certs}))
		if err != nil {
			return nil, fmt.Errorf("svcauth: validator: %w", err)
		}
		validator = v
	}
	s.oidc = &oidcVerifier{validator: validator, certs: s.certs, now: opts.Now}
	return s, nil
}

// Mount registers the route table on the listener's own mux.
func (s *Service) Mount(routes []v1.Route) error {
	mux, err := s.buildMux(routes)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.mux = mux
	s.mu.Unlock()
	return nil
}

func (s *Service) checks() Checks {
	return Checks{Pepper: s.opts.Pepper, PeerAuthToken: s.opts.PeerAuthToken, AdminToken: s.opts.AdminToken, MainAddr: s.opts.MainAddr}
}

func (s *Service) load() (*Config, error) {
	b, err := ReadFile(s.opts.Path, s.opts.OwnerUID)
	if err != nil {
		return nil, err
	}
	return Parse(b, s.checks())
}

// Start loads the config and, when it is valid, starts the listener. With no
// file or an invalid one the listener stays down and :7777 is unaffected.
func (s *Service) Start(ctx context.Context) {
	if !PepperOK(s.opts.Pepper) {
		s.log.Printf("svcauth: WARN COSIFT_SVC_PEPPER absent or shorter than %d bytes — csk_ key authentication disabled", MinPepperLen)
	}
	cfg, err := s.load()
	switch {
	case errors.Is(err, ErrAbsent):
		s.log.Printf("svcauth: %s absent — /v1 listener disabled", s.opts.Path)
		return
	case err != nil:
		s.log.Printf("svcauth: ERROR %s invalid (%v) — /v1 listener disabled", s.opts.Path, err)
		return
	}
	s.Policy.Install(cfg)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.listenLocked(ctx, cfg)
}

func (s *Service) mounted() *http.ServeMux {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mux
}

var errShuttingDown = errors.New("svcauth: shutting down")

// Reload is one SIGHUP: it re-reads the config, then runs also, the SIGHUP's
// other reloaders, and counts one outcome for them all. An invalid or missing
// file keeps the active config; a valid one is swapped in and starts a
// listener that is down.
func (s *Service) Reload(ctx context.Context, also ...v1.Reloader) error {
	err := s.reloadConfig(ctx)
	if errors.Is(err, errShuttingDown) {
		return err
	}
	for _, f := range also {
		err = errors.Join(err, f())
	}
	s.Metrics.reload(err == nil)
	return err
}

func (s *Service) reloadConfig(ctx context.Context) error {
	cfg, err := s.load()
	if err != nil {
		s.log.Printf("svcauth: ERROR reload of %s refused (%v) — previous config stays active", s.opts.Path, err)
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errShuttingDown
	}
	s.Policy.Install(cfg)
	s.log.Printf("svcauth: reloaded %s (%d principals)", s.opts.Path, len(cfg.Principals))
	if !PepperOK(s.opts.Pepper) && s.Policy.state().hasKeyPrincipals() {
		s.log.Printf("svcauth: WARN COSIFT_SVC_PEPPER absent or shorter than %d bytes — key principals cannot authenticate", MinPepperLen)
	}
	if s.srv != nil {
		if cfg.Listen != s.addr {
			s.log.Printf("svcauth: WARN listen changed to %s — the listener stays on %s until a restart", cfg.Listen, s.addr)
		}
		return nil
	}
	s.listenLocked(ctx, cfg)
	return nil
}

func (s *Service) listenLocked(ctx context.Context, cfg *Config) {
	if s.closed || s.srv != nil {
		return
	}
	if s.mux == nil {
		s.log.Printf("svcauth: ERROR no route table mounted — /v1 listener disabled")
		return
	}
	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		s.log.Printf("svcauth: ERROR cannot listen on %s — /v1 listener disabled", cfg.Listen)
		return
	}
	srv := &http.Server{
		Handler:                      s,
		ReadHeaderTimeout:            5 * time.Second,
		ReadTimeout:                  15 * time.Second,
		WriteTimeout:                 30 * time.Second,
		IdleTimeout:                  60 * time.Second,
		MaxHeaderBytes:               16 << 10,
		ErrorLog:                     log.New(serverErrorWriter{s.Metrics}, "", 0),
		DisableGeneralOptionsHandler: true,
	}
	s.srv, s.addr = srv, ln.Addr().String()
	if !s.certsRun {
		s.certsRun = true
		go s.certs.Run(ctx)
		go s.summaryLoop(ctx)
	}
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.log.Printf("svcauth: ERROR /v1 listener stopped")
		}
	}()
	s.log.Printf("svcauth: /v1 listening on %s", s.addr)
}

// Addr is the bound address, empty while the listener is down.
func (s *Service) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.addr
}

// Shutdown stops the listener for good and waits for in-flight requests, so
// the article store can be flushed and closed after it returns.
func (s *Service) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	wasClosed := s.closed
	s.closed = true
	srv := s.srv
	s.mu.Unlock()
	if !wasClosed {
		close(s.done)
	}
	if srv == nil {
		return nil
	}
	err := srv.Shutdown(ctx)
	if err != nil {
		_ = srv.Close()
	}
	s.failed.flush(s.now(), true)
	return err
}

// summaryLoop writes throttled clients' pending counts when no new failure does.
func (s *Service) summaryLoop(ctx context.Context) {
	t := time.NewTicker(s.summaryEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.done:
			return
		case <-t.C:
			s.failed.flush(s.now(), false)
		}
	}
}
