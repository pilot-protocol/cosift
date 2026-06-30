# Cosift — convenience targets. Nothing here is required; everything just
# wraps the standard `go` commands so the README's "Quick start" is one line.

BINARY := cosift
PKG    := ./cmd/cosift
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

.PHONY: build check test smoke eval bench docker clean help

help:
	@echo "make build          — build ./$(BINARY) (host platform)"
	@echo "make check          — go build + vet + index/store unit tests (~10s, no network, no LLM)"
	@echo "make test           — go test ./..."
	@echo "make smoke          — real-runner smoke test (build + crawl + serve roundtrip; ~30s; needs network)"
	@echo "make eval           — run the BM25 baseline eval"
	@echo "make eval-dense     — run dense+rerank eval (needs OPENAI in .env)"
	@echo "make bench          — run the latency micro-benchmarks"
	@echo "make docker         — build the Docker image (tag: cosift:$(VERSION))"
	@echo "make clean          — remove built artifacts"

smoke:
	@./scripts/smoke-test.sh

build:
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X main.version=$(VERSION) -X github.com/pilot-protocol/cosift/internal/server.Version=$(VERSION)" -o $(BINARY) $(PKG)

# Light-touch verification: compile + vet + unit tests on packages that
# don't need OPENAI/COHERE keys or live network. Catches latent compile
# bugs without the full ./... test suite's external deps. cmd/cosift
# tests are httptest-based, fast (~20s), and cover the CLI surface.
check:
	go build ./...
	go vet ./cmd/... ./internal/index/... ./internal/store/... ./internal/crawler/...
	go test -count=1 -timeout=90s ./internal/index/... ./internal/store/... ./cmd/cosift/...

test:
	go test ./...

eval: build
	./$(BINARY) eval -retriever bm25 -baseline eval-baseline-v2.json

eval-dense: build
	./$(BINARY) eval -retriever dense -rerank -baseline eval-baseline-v2.json

bench: build
	./$(BINARY) bench -mode both -n 10000 -queries 100

docker:
	docker build -t cosift:$(VERSION) -t cosift:latest .

clean:
	rm -f $(BINARY)
	rm -rf cosift-data
