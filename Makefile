# OpenEndpoint v1.0.0 Makefile

.PHONY: all build test clean install run deps lint fmt help coverage security

BINARY_NAME=openep
VERSION=$(shell git describe --tags --always --dirty 2>/dev/null || echo "v1.0.0")
BUILD_TIME=$(shell date -u '+%Y-%m-%d_%H:%M:%S')
GO_VERSION=$(shell go version | awk '{print $$3}')
LDFLAGS=-ldflags "-s -w -X main.version=${VERSION} -X main.buildTime=${BUILD_TIME}"

# Default target
all: deps lint test build

# Install dependencies
deps:
	go mod download
	go mod tidy

# Build the binary
build:
	@echo "Building ${BINARY_NAME} ${VERSION}..."
	@mkdir -p bin
	CGO_ENABLED=0 go build ${LDFLAGS} -o bin/${BINARY_NAME} ./cmd/openep
	@echo "Build complete: bin/${BINARY_NAME}"

# Build for multiple platforms
build-all:
	@echo "Building for all platforms..."
	@mkdir -p bin
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build ${LDFLAGS} -o bin/${BINARY_NAME}-linux-amd64 ./cmd/openep
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build ${LDFLAGS} -o bin/${BINARY_NAME}-linux-arm64 ./cmd/openep
	GOOS=darwin GOARCH=amd64 CGO_ENABLED=0 go build ${LDFLAGS} -o bin/${BINARY_NAME}-darwin-amd64 ./cmd/openep
	GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build ${LDFLAGS} -o bin/${BINARY_NAME}-darwin-arm64 ./cmd/openep
	GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build ${LDFLAGS} -o bin/${BINARY_NAME}-windows-amd64.exe ./cmd/openep
	@echo "All builds complete"

# Run tests with coverage
test:
	@echo "Running tests..."
	go test -v -race -coverprofile=coverage.out ./...

# Run tests with verbose output
test-verbose:
	go test -v ./...

# Run unit tests only (skip integration)
test-unit:
	go test -v -short ./...

# Run benchmarks
bench:
	@echo "Running benchmarks..."
	go test -bench=. -benchmem ./...

# Generate coverage report
coverage: test
	@echo "Generating coverage report..."
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"
	@go tool cover -func=coverage.out | grep total

# Check coverage threshold (80%)
coverage-check: test
	@COVERAGE=$$(go tool cover -func=coverage.out | grep total | awk '{print $$3}' | sed 's/%//'); \
	echo "Total coverage: $${COVERAGE}%"; \
	if [ $$(echo "$$COVERAGE < 80" | bc) -eq 1 ]; then \
		echo "::warning::Coverage is below 80%"; \
	fi

# Clean build artifacts
clean:
	@echo "Cleaning..."
	rm -rf bin/
	rm -f coverage.out coverage.html

# Install binary to GOPATH
install:
	go install ${LDFLAGS} ./cmd/openep

# Run the server
run:
	go run ./cmd/openep

# Run the server with development config
run-dev:
	go run ./cmd/openep --config config.example.yaml

# Run linter
lint:
	@echo "Running linter..."
	golangci-lint run ./...

# Format code
fmt:
	@echo "Formatting code..."
	go fmt ./...
	gofmt -s -w .

# Run security scanner
security:
	@echo "Running security scan..."
	go install golang.org/x/vuln/cmd/govulncheck@latest
	govulncheck ./...

# Run docker build
docker-build:
	@echo "Building Docker image..."
	docker build -t openendpoint/openendpoint:${VERSION} -f deploy/docker/Dockerfile .
	docker tag openendpoint/openendpoint:${VERSION} openendpoint/openendpoint:latest

# Run docker compose
docker-up:
	docker-compose -f deploy/docker/docker-compose.yml up -d

# Stop docker compose
docker-down:
	docker-compose -f deploy/docker/docker-compose.yml down

# Generate code
generate:
	go generate ./...

# Setup development environment
setup-dev:
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	go install golang.org/x/vuln/cmd/govulncheck@latest
	go mod download

# Create release builds
release: clean build-all
	@echo "Release ${VERSION} builds complete"

# Quick test (fast feedback)
quick: fmt test-unit

# CI pipeline
ci: deps lint security test coverage

# Show help
help:
	@echo "OpenEndpoint V5 Build System"
	@echo ""
	@echo "Go Version: ${GO_VERSION}"
	@echo "Binary: ${BINARY_NAME}"
	@echo "Version: ${VERSION}"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@echo "  all           - Build and test (default)"
	@echo "  deps          - Download dependencies"
	@echo "  build         - Build binary"
	@echo "  build-all     - Build for all platforms"
	@echo "  test          - Run all tests with coverage"
	@echo "  test-unit     - Run unit tests only"
	@echo "  test-verbose  - Run tests with verbose output"
	@echo "  bench         - Run benchmarks"
	@echo "  coverage      - Generate HTML coverage report"
	@echo "  coverage-check- Check coverage threshold (80%)"
	@echo "  clean         - Clean build artifacts"
	@echo "  install       - Install binary to GOPATH"
	@echo "  run           - Run server"
	@echo "  run-dev       - Run server with dev config"
	@echo "  lint          - Run golangci-lint"
	@echo "  fmt           - Format code"
	@echo "  security      - Run vulnerability scan"
	@echo "  docker-build  - Build Docker image"
	@echo "  docker-up     - Start with Docker Compose"
	@echo "  docker-down   - Stop Docker Compose"
	@echo "  generate      - Run go generate"
	@echo "  setup-dev     - Setup development environment"
	@echo "  release       - Create release builds"
	@echo "  quick         - Quick test (format + unit tests)"
	@echo "  ci            - Full CI pipeline"
