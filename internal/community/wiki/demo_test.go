package wiki_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/community"
)

// TestWikiDemoServer serves the seeded pages on COSIFT_WIKI_DEMO_ADDR until stopped.
func TestWikiDemoServer(t *testing.T) {
	addr := os.Getenv("COSIFT_WIKI_DEMO_ADDR")
	if addr == "" {
		t.Skip("set COSIFT_WIKI_DEMO_ADDR to serve the local demo")
	}
	publicURL := strings.TrimRight(os.Getenv("COSIFT_WIKI_DEMO_PUBLIC_URL"), "/")
	if publicURL == "" {
		publicURL = "http://127.0.0.1:7790"
	}
	public := os.Getenv("COSIFT_WIKI_PUBLIC")
	if public == "" {
		public = "1"
	}
	s := newStack(t)
	s.seed()
	srv := s.pages(t, public == "1", nil, func(c *community.Config) { c.PublicURL = publicURL })
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := srv.Wiki().Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { defer close(done); srv.Run(ctx) }()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	hs := &http.Server{Handler: srv, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := hs.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Error(err)
		}
	}()
	urls := []string{"hub=" + publicURL + "/wiki"}
	for _, f := range fixtures(t) {
		urls = append(urls, f.kind+"="+publicURL+"/wiki/"+f.slug)
	}
	urls = append(urls, "vertical="+publicURL+"/wiki/v/research", "sitemap="+publicURL+"/sitemap.xml", "robots="+publicURL+"/robots.txt")
	fmt.Printf("READY wiki demo (COSIFT_WIKI_PUBLIC=%s) %s\n", public, strings.Join(urls, " "))
	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = hs.Shutdown(shutdown)
	<-done
}
