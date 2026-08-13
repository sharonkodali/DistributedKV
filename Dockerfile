# --- Build stage ---
# Compiles a static Go binary. Using a separate build stage keeps the final
# image small -- we don't ship the Go toolchain, source code, or build
# cache, just the compiled binary.
FROM golang:1.22-alpine AS build

WORKDIR /src

COPY . .

RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -o /kv-server ./cmd/server

# --- Runtime stage ---
# alpine is a minimal Linux base (~5MB) -- much smaller than shipping the
# full golang build image, which includes the entire compiler toolchain.
FROM alpine:3.20

RUN apk add --no-cache curl # useful for debugging/healthchecks from inside the container

COPY --from=build /kv-server /usr/local/bin/kv-server

ENTRYPOINT ["/usr/local/bin/kv-server"]
