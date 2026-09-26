package main

import (
	"context"
	"log"
	"os"
	"sync"
	"time"

	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/embed"
	"github.com/pilot-protocol/cosift/internal/store"
	"github.com/pilot-protocol/cosift/internal/svcauth"
	v1 "github.com/pilot-protocol/cosift/internal/v1"
)

// sighupReloaders is what a SIGHUP runs; see handleSIGHUP.
var sighupReloaders v1.Reloaders

var svcAuthPath = svcauth.DefaultPath

// articleLayer is the article store as the /v1 listener mounts it.
type articleLayer interface {
	Routes() []v1.Route
	WriteLocker() sync.Locker
	Rebuild(ctx context.Context) error
	ReloadConfig() error
	Close() error
}

type articleDeps struct {
	store     *store.PebbleStore
	embedder  embed.Embedder
	policy    v1.Policy
	readiness *v1.Readiness
}

// openArticles opens the article store; nil until the store is linked in.
var openArticles func(articleDeps) (articleLayer, error)

type v1Listener struct {
	svc      *svcauth.Service
	articles articleLayer
	rebuild  sync.WaitGroup
	stopOnce sync.Once
}

// startV1 starts the /v1 listener on its own server and mux. A missing or
// invalid service-auth.json leaves it down; :7777 is never affected.
func (s *pebbleHTTP) startV1(ctx context.Context, cfg *config.Config) *v1Listener {
	l := &v1Listener{}
	rd := &v1.Readiness{}
	svc, err := svcauth.New(svcauth.Options{
		Path:           svcAuthPath,
		OwnerUID:       svcAuthOwnerUID,
		Pepper:         []byte(os.Getenv("COSIFT_SVC_PEPPER")),
		PeerAuthToken:  cfg.Cluster.PeerAuthToken,
		AdminToken:     cfg.Server.AdminToken,
		ClientIPHeader: cfg.Server.ClientIPHeader,
		Readiness:      rd,
	})
	if err != nil {
		log.Printf("pebble-serve: ERROR %v — /v1 listener disabled", err)
		return l
	}
	routes := s.v1RetrievalRoutes()
	if openArticles != nil {
		a, err := openArticles(articleDeps{store: s.store, embedder: s.v1Embedder, policy: svc.Policy, readiness: rd})
		if err != nil {
			log.Printf("pebble-serve: ERROR article store not opened — article routes disabled")
		} else {
			l.articles = a
			routes = append(routes, a.Routes()...)
		}
	}
	if err := svc.Mount(routes); err != nil {
		log.Printf("pebble-serve: ERROR %v — /v1 listener disabled", err)
		return l
	}
	l.svc = svc
	s.v1svc = svc
	if l.articles != nil {
		svc.Policy.SetSwapLocker(l.articles.WriteLocker())
		sighupReloaders.Add("articles", l.articles.ReloadConfig)
		l.rebuild.Add(1)
		go func() {
			defer l.rebuild.Done()
			if err := l.articles.Rebuild(ctx); err != nil && ctx.Err() == nil {
				log.Printf("pebble-serve: ERROR article index rebuild failed — article routes stay unavailable")
			}
		}()
	}
	svc.Start(ctx)
	sighupReloaders.Add("service-auth", func() error { return svc.Reload(ctx) })
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
