FROM golang:1.21-bullseye

# Install protobuf compiler
RUN apt-get update && apt-get install -y protobuf-compiler

# Install Go gRPC plugins
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.32.0 \
    && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.3.0

# Ensure the PATH includes the Go bin directory
ENV PATH="$PATH:$(go env GOPATH)/bin"

WORKDIR /app

# Copy go.mod and download dependencies first for caching
COPY go.mod ./
# (If we had go.sum we'd copy it here too)
RUN go mod download

COPY . .

# Default command
CMD ["bash"]
