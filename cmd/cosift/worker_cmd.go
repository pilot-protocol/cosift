package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/pilot-protocol/cosift/internal/crawler"
	"github.com/pilot-protocol/cosift/internal/sharedaccount"
)

const workerBearerAdvice = "worker mode needs your bearer token (ck_...), not a cookie session: set COSIFT_TOKEN to the token cosift-install wrote, or run cosift login with COSIFT_TOKEN set so the saved session holds it"

var errNeedsBearer = errors.New(workerBearerAdvice)

func newWorkerFetchClient() *http.Client { return crawler.BrowserClient(30 * time.Second) }

// workerFetchClient builds the client pages are fetched with; tests replace it.
var workerFetchClient = newWorkerFetchClient

// workerBearer returns the ck_ token worker mode authenticates with, from COSIFT_TOKEN or a saved session; a cookie-only session is refused before any request.
func workerBearer(token, sessionFile, origin string, hasPassword bool) (string, error) {
	switch {
	case token != "":
		return token, nil
	case sessionFile != "":
		saved, err := readCommunitySession(sessionFile, origin, false)
		if err != nil {
			return "", err
		}
		if _, err := sharedaccount.Parse(saved.Token); err != nil {
			return "", errNeedsBearer
		}
		return saved.Token, nil
	case hasPassword:
		return "", errNeedsBearer
	}
	return "", fmt.Errorf("worker mode needs credentials: %s", workerBearerAdvice)
}

func validateWorkerOptions(o workerOptions) error {
	switch {
	case o.MaxPages < 0 || o.Duration < 0:
		return errors.New("-max-pages and -duration cannot be negative")
	case o.Concurrency < 1 || o.Concurrency > maxClaim:
		return fmt.Errorf("-concurrency must be from 1 to %d", maxClaim)
	}
	return nil
}

func runWorkerMode(ctx context.Context, origin, token string, opts workerOptions, stdout, stderr io.Writer) error {
	if err := validateWorkerOptions(opts); err != nil {
		return err
	}
	w := newWorker(newWorkAPI(origin, token), workerFetchClient(), opts, stdout, stderr)
	return w.run(ctx)
}
