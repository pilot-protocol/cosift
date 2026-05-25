# Cosift — convenience targets. Nothing here is required; everything just
# wraps the standard `go` commands so the README's "Quick start" is one line.

BINARY := cosift
PKG    := ./cmd/cosift
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

.PHONY: build test smoke eval bench docker clean help

help:
	@echo "make build          — build ./$(BINARY) (host platform)"
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
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X main.version=$(VERSION) -X github.com/calinteodor/cosift/internal/server.Version=$(VERSION)" -o $(BINARY) $(PKG)

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
