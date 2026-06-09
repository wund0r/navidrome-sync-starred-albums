# syntax=docker/dockerfile:1

FROM golang:1-bookworm AS builder

WORKDIR /src

COPY ./. .

RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/navidrome-starred-albums-sync \
    .

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /out/navidrome-starred-albums-sync /usr/local/bin/navidrome-starred-albums-sync

ENTRYPOINT ["navidrome-starred-albums-sync"]
