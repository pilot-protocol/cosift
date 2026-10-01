package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/pilot-protocol/cosift/internal/articles"
	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/embed"
	"github.com/pilot-protocol/cosift/internal/store"
	"github.com/pilot-protocol/cosift/internal/svcauth"
	v1 "github.com/pilot-protocol/cosift/internal/v1"
)

// sighupReloaders is what a SIGHUP runs; see handleSIGHUP.
var sighupReloaders = &v1.Reloaders{}

var (
	svcAuthPath        = svcauth.DefaultPath
	articlesConfigPath = "/etc/cosift/articles.json"
)

// v1CertSource replaces the Google certificate fetch in tests; nil is Google.
var v1CertSource svcauth.CertSource

// articleLayer is the article store as the /v1 listener mounts it.
type articleLayer interface {
	Routes() []v1.Route
	// Readiness is store-owned; it gates every article route.
	Readiness() *v1.Readiness
	WriteLocker() sync.Locker
	Rebuild(ctx context.Context) error
	ReloadThresholds() error
	Close() error
}

var _ articleLayer = (*articles.Store)(nil)

type articleDeps struct {
	db             *store.PebbleStore
	embedder       embed.Embedder
	policy         v1.Policy
	thresholdsPath string
}

// openArticles opens the article store; tests substitute it.
var openArticles = func(d articleDeps) (articleLayer, error) {
	st, err := articles.Open(articles.Options{
		DB:             d.db.DB(),
		Embedder:       d.embedder,
		Policy:         d.policy,
		Corpus:         articles.StoreCorpus{Store: d.db},
		ThresholdsPath: d.thresholdsPath,
		Logf:           log.Printf,
	})
	if err != nil {
		return nil, err
	}
	return st, nil
}

type v1Listener struct {
	svc      *svcauth.Service
	articles articleLayer
	rebuild  sync.WaitGroup
	stopOnce sync.Once
}

// startV1 starts the /v1 listener on its own server and mux. A missing or
// invalid service-auth.json leaves it down; :7777 is never affected.
func (s *pebbleHTTP) startV1(ctx context.Context, cfg *config.Config, rs *v1.Reloaders, mainAddr string) *v1Listener {
	l := &v1Listener{}
	pol := svcauth.NewPolicy(nil, nil)
	routes := s.v1RetrievalRoutes()
	rd := &v1.Readiness{}
	if openArticles != nil {
		a, err := openArticles(articleDeps{db: s.store, embedder: s.v1Embedder, policy: pol, thresholdsPath: articlesConfigPath})
		if err != nil {
			log.Printf("pebble-serve: ERROR article store not opened (%v) — article routes disabled", err)
		} else {
			l.articles, rd = a, a.Readiness()
			routes = append(routes, a.Routes()...)
		}
	}
	svc, err := svcauth.New(svcauth.Options{
		Path:           svcAuthPath,
		OwnerUID:       svcAuthOwnerUID,
		Pepper:         []byte(os.Getenv("COSIFT_SVC_PEPPER")),
		PeerAuthToken:  cfg.Cluster.PeerAuthToken,
		AdminToken:     cfg.Server.AdminToken,
		MainAddr:       mainAddr,
		ClientIPHeader: cfg.Server.ClientIPHeader,
		Readiness:      rd,
		Policy:         pol,
		CertSource:     v1CertSource,
	})
	if err == nil {
		err = svc.Mount(routes)
	}
	if err != nil {
		log.Printf("pebble-serve: ERROR %v — /v1 listener disabled", err)
		return l
	}
	l.svc = svc
	s.v1svc = svc
	if l.articles != nil {
		pol.SetSwapLocker(l.articles.WriteLocker())
	}
	svc.Start(ctx)
	var also []v1.Reloader
	if l.articles != nil {
		also = append(also, func() error {
			if err := l.articles.ReloadThresholds(); err != nil {
				log.Printf("articles: ERROR reload of %s refused (%v) — previous thresholds stay active", articlesConfigPath, err)
				return fmt.Errorf("articles: %w", err)
			}
			return nil
		})
	}
	rs.Add("v1", func() error { return svc.Reload(ctx, also...) })
	if l.articles != nil {
		l.rebuild.Add(1)
		go func() {
			defer l.rebuild.Done()
			if err := l.articles.Rebuild(ctx); err != nil && ctx.Err() == nil {
				log.Printf("pebble-serve: ERROR article index rebuild failed — article routes stay unavailable")
			}
		}()
	}
	return l
}

// v1DrainBudget covers an article PUT's 20 s embed inside the unit's 30 s stop timeout.
const v1DrainBudget = 25 * time.Second

// stop shuts the listener down and waits for its handlers; concurrent callers
// wait for the first.
func (l *v1Listener) stop() {
	l.stopOnce.Do(func() {
		if l.svc == nil {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), v1DrainBudget)
		defer cancel()
		if err := l.svc.Shutdown(ctx); err != nil {
			log.Printf("pebble-serve: WARN /v1 handlers still running after %s", v1DrainBudget)
		}
	})
}

// closeArticles flushes the article store and joins its jobs; it runs after
// stop and before the Pebble store closes.
func (l *v1Listener) closeArticles() {
	if l.articles == nil {
		return
	}
	l.rebuild.Wait()
	if err := l.articles.Close(); err != nil {
		log.Printf("pebble-serve: ERROR article store close failed")
	}
}
