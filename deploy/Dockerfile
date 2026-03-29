FROM golang:1.26-alpine AS builder

WORKDIR /app

# Copy go.mod and go.sum files for dependency caching
COPY go.mod go.sum ./
COPY examples/go.mod examples/go.sum ./examples/

# Download dependencies
RUN cd examples && go mod download

# Copy the full source
COPY . .

# Build the examples binary for the target platform
ARG TARGETOS TARGETARCH
RUN cd examples && CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -o /app/bin/gomr-examples .

FROM alpine:3.21

RUN apk add --no-cache ca-certificates

WORKDIR /app

COPY --from=builder /app/bin/gomr-examples /app/gomr-examples

ENTRYPOINT ["/app/gomr-examples"]
