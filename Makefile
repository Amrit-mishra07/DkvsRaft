.PHONY: proto proto-docker test build

# Compile protobufs locally (requires protoc and go plugins)
proto:
	protoc --go_out=. --go_opt=paths=source_relative \
           --go-grpc_out=. --go-grpc_opt=paths=source_relative \
           proto/raft.proto

# Compile protobufs using Docker
proto-docker:
	docker build -t dkvsraft-dev -f Dockerfile .
	docker run --rm -v $$(pwd):/app dkvsraft-dev make proto

# Run tests
test:
	go test -v ./...

# Run tests using Docker
test-docker:
	docker build -t dkvsraft-dev -f Dockerfile .
	docker run --rm -v $$(pwd):/app dkvsraft-dev make test
