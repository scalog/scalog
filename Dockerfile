# Start from golang v1.11 base image
FROM golang:1.12 as builder

# Copy everything from the current directory to the PWD(Present Working Directory) inside the container
COPY . /go/src/github.com/scalog/scalog/

# Set the Current Working Directory inside the container
WORKDIR /go/src/github.com/scalog/scalog/

# Download dependencies
RUN set -x && \
    go get github.com/golang/dep/cmd/dep && \
    dep ensure -v

# Build the Go app
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags \
    '-extldflags "-static"' -o scalog .

######## Start a new stage from scratch #######
FROM alpine:latest

RUN apk --no-cache add ca-certificates

# Install health check probe command
RUN GRPC_HEALTH_PROBE_VERSION=v0.2.0 && \
    wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64 && \
    chmod +x /bin/grpc_health_probe

# Copy the Pre-built binary file from the previous stage
COPY --from=builder /go/src/github.com/scalog/scalog/scalog /app/
COPY --from=builder /go/src/github.com/scalog/scalog/.scalog.yaml /root/

WORKDIR /app

EXPOSE 21024
