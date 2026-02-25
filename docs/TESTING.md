# Testing Guide

This document describes how to test OpenEndpoint in various environments.

## Quick Test

```bash
# Run all tests
go test ./...

# Run with verbose output
go test -v ./...

# Run with coverage
go test ./... -cover

# Run with race detector
go test -race ./...
```

## Test Structure

- **Unit Tests**: `*_test.go` files alongside source code
- **Integration Tests**: `test/integration/` directory
- **Mock Servers**: HTTP test servers for external dependencies

## Coverage Report

```bash
# Generate coverage profile
go test ./... -coverprofile=coverage.out

# View coverage in browser
go tool cover -html=coverage.out

# View function coverage
go tool cover -func=coverage.out
```

## Testing Specific Packages

```bash
# Storage layer
go test -v ./internal/storage/...

# API layer
go test -v ./internal/api/...

# Authentication
go test -v ./internal/auth/...

# Dashboard
go test -v ./internal/dashboard/...
```

## Real Environment Testing

### 1. Build and Run Locally

```bash
# Build
go build -o bin/openep.exe ./cmd/openep

# Create test config
cat > test-config.yaml << 'EOF'
server:
  host: localhost
  port: 9000
storage:
  data_dir: ./test-data
auth:
  enabled: true
  access_key: test-key
  secret_key: test-secret
EOF

# Run server
./bin/openep.exe server -c test-config.yaml
```

### 2. Test with AWS CLI

```bash
# Configure
aws configure set aws_access_key_id test-key
aws configure set aws_secret_access_key test-secret
aws configure set default.region us-east-1

# Test commands
aws --endpoint-url http://localhost:9000 s3 mb s3://test-bucket
aws --endpoint-url http://localhost:9000 s3 cp test.txt s3://test-bucket/
aws --endpoint-url http://localhost:9000 s3 ls s3://test-bucket/
aws --endpoint-url http://localhost:9000 s3 rm s3://test-bucket/test.txt
```

### 3. Test with cURL

```bash
# Health check
curl http://localhost:9000/health

# Metrics
curl http://localhost:9000/metrics

# Management API
curl http://localhost:9000/_mgmt/status
```

### 4. Web Dashboard

Open browser: `http://localhost:9000/_dashboard/`

## Test Data Cleanup

```bash
# Remove test data
rm -rf ./test-data
rm -f test-config.yaml
```

## Continuous Integration

Tests run automatically on:
- Every push to main branch
- Every pull request
- Daily scheduled runs

See `.github/workflows/ci.yml` for CI configuration.
