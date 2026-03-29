.PHONY: build run clean test

# Build the server
build:
	CGO_ENABLED=1 go build -o bin/deepzoom-server ./cmd/server

# Run the server
run: build
	./bin/deepzoom-server -addr :8080 -static-dir ./static

# Clean build artifacts
clean:
	rm -rf bin/

# Run tests
test:
	go test -v ./...

# Install dependencies
deps:
	go mod download
	go mod tidy

# Build for Linux (cross-compile)
build-linux:
	GOOS=linux GOARCH=amd64 CGO_ENABLED=1 go build -o bin/deepzoom-server-linux ./cmd/server

# Format code
fmt:
	go fmt ./...

# Lint
lint:
	golangci-lint run
