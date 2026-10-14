#!/bin/sh
# Renders the wiki's Markdown forms with cmark and cmark-gfm in a container and checks the HTML that comes out.
set -eu
root=$(cd "$(dirname "$0")/.." && pwd)
exec docker run --rm ${CONTAINER:+--name "$CONTAINER"} \
  -v "$root":/src -w /src \
  -v cosift-gomod:/go/pkg/mod -v cosift-gobuild-cache:/root/.cache/go-build \
  -e GOTOOLCHAIN=local \
  golang:1.26-bookworm sh -c \
  'apt-get update -qq >/dev/null && apt-get install -y -qq cmark cmark-gfm >/dev/null && go test -count=1 -tags cmark -run Cmark ./internal/community/wiki'
