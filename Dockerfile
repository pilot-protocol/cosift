# Multi-stage build → ~20 MB final image.
# Stage 1: build the static binary against alpine-pinned Go.
FROM golang:1.25-alpine AS build
WORKDIR /src

# Cache deps separately from source so go.sum changes invalidate less.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# CGO=0 because modernc.org/sqlite is pure Go; -ldflags strips symbols.
RUN CGO_ENABLED=0 GOOS=linux \
    go build -trimpath -ldflags="-s -w" -o /out/cosift ./cmd/cosift

# Stage 2: minimal runtime. Alpine carries ca-certs for outbound HTTPS
# (OpenAI/Cohere/embedding sidecars).
FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata && \
    addgroup -S cosift && adduser -S -G cosift cosift && \
    mkdir -p /var/lib/cosift && chown -R cosift:cosift /var/lib/cosift

COPY --from=build /out/cosift /usr/local/bin/cosift
COPY cosift.json.example /etc/cosift/cosift.json.example

USER cosift
WORKDIR /var/lib/cosift
EXPOSE 7777

# Bind to all interfaces inside the container; user maps the port out.
# Override with -e or mount their own cosift.json.
ENV COSIFT_DATA_DIR=/var/lib/cosift/data \
    COSIFT_LISTEN=0.0.0.0:7777

HEALTHCHECK --interval=30s --timeout=3s --retries=3 \
    CMD wget -qO- "http://127.0.0.1:7777/healthz" || exit 1

ENTRYPOINT ["/usr/local/bin/cosift"]
CMD ["serve"]
