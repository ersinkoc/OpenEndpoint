# Testing Guide

Complete testing documentation for OpenEndpoint.

## Table of Contents

- [Quick Start](#quick-start)
- [Test Structure](#test-structure)
- [Running Tests](#running-tests)
- [Coverage Reports](#coverage-reports)
- [Real Environment Testing](#real-environment-testing)
- [Integration Testing](#integration-testing)
- [Performance Testing](#performance-testing)
- [Troubleshooting](#troubleshooting)

---

## Quick Start

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

---

## Test Structure

```
.
├── cmd/openep/
│   └── main_test.go              # CLI tests
├── cmd/openep/commands/
│   └── commands_run_test.go      # Command tests
├── internal/
│   ├── api/
│   │   ├── router_test.go        # API routing tests
│   │   ├── router_handlers_test.go # Handler tests
│   │   └── errors_test.go        # Error handling tests
│   ├── auth/
│   │   └── sigv4_test.go         # Signature V4 tests
│   ├── dashboard/
│   │   └── dashboard_test.go     # Dashboard handler tests
│   ├── storage/
│   │   ├── flatfile/
│   │   │   └── flatfile_test.go  # Storage backend tests
│   │   └── packed/
│   │       └── packed_test.go    # Packed storage tests
│   └── ...                       # Other packages
├── pkg/
│   └── ...                       # Package tests
└── test/integration/
    └── integration_test.go       # Integration tests
```

---

## Running Tests

### Basic Commands

```bash
# All tests
go test ./...

# Specific package
go test ./internal/api/...
go test ./internal/storage/flatfile/...

# Verbose mode
go test -v ./internal/auth/...

# With timeout
go test -timeout 60s ./...

# Parallel execution
go test -parallel 4 ./...
```

### Test Selection

```bash
# Run specific test
go test -run TestNewRouter ./internal/api/...

# Run tests matching pattern
go test -run "TestHandle.*" ./internal/api/...

# Skip specific tests
go test -skip TestSlowOperation ./...
```

### Race Detection

```bash
# Detect race conditions
go test -race ./...

# Race detection with timeout
go test -race -timeout 120s ./...
```

---

## Coverage Reports

### Generate Coverage

```bash
# Generate coverage profile
go test ./... -coverprofile=coverage.out

# View coverage in terminal
go tool cover -func=coverage.out

# Open in browser
go tool cover -html=coverage.out

# Coverage for specific package
go test ./internal/api/... -coverprofile=api_coverage.out
```

### Coverage by Package

```bash
# Get coverage summary
go test ./... -cover | grep -E "(ok|coverage)"

# Detailed coverage
go tool cover -func=coverage.out | grep -v "100.0%" | head -20
```

### Current Coverage Status

| Package | Coverage | Notes |
|---------|----------|-------|
| internal/api | ~72% | Error paths need more tests |
| internal/auth | ~90% | Well covered |
| internal/dashboard | ~91% | Good coverage with mock servers |
| internal/mgmt | ~84% | Management API handlers |
| internal/storage/flatfile | ~85% | Core storage tested |
| cmd/openep | ~67% | CLI commands, main() not testable |
| cmd/openep/commands | ~67% | HTTP client calls need mock |

---

## Real Environment Testing

### 1. Build Application

```bash
# Build binary
go build -o bin/openep.exe ./cmd/openep

# Verify build
./bin/openep.exe version
```

### 2. Create Test Configuration

```bash
cat > test-config.yaml << 'EOF'
server:
  host: localhost
  port: 9000
  read_timeout: 30
  write_timeout: 30
  idle_timeout: 120

storage:
  data_dir: ./test-data
  backend: flatfile

auth:
  enabled: true
  access_key: test-key
  secret_key: test-secret

logging:
  level: info
  format: json

cluster:
  enabled: false
EOF
```

### 3. Start Server

```bash
# Terminal 1: Start server
./bin/openep.exe server -c test-config.yaml

# Should see:
# {"level":"info",...,"msg":"starting OpenEndpoint server"}
# {"level":"info",...,"msg":"server listening"}
```

### 4. Test with cURL

```bash
# Health check
curl http://localhost:9000/health
# Output: OK

# Readiness check
curl http://localhost:9000/ready
# Output: READY

# Management status
curl http://localhost:9000/_mgmt/status

# Prometheus metrics
curl http://localhost:9000/metrics

# Web dashboard
curl http://localhost:9000/_dashboard/
```

### 5. Test with AWS CLI

```bash
# Configure AWS CLI
aws configure set aws_access_key_id test-key
aws configure set aws_secret_access_key test-secret
aws configure set default.region us-east-1

# Create bucket
aws --endpoint-url http://localhost:9000 s3 mb s3://test-bucket

# List buckets
aws --endpoint-url http://localhost:9000 s3 ls

# Upload file
echo "Hello World" > test.txt
aws --endpoint-url http://localhost:9000 s3 cp test.txt s3://test-bucket/

# List objects
aws --endpoint-url http://localhost:9000 s3 ls s3://test-bucket/

# Download file
aws --endpoint-url http://localhost:9000 s3 cp s3://test-bucket/test.txt downloaded.txt

# Delete object
aws --endpoint-url http://localhost:9000 s3 rm s3://test-bucket/test.txt

# Delete bucket
aws --endpoint-url http://localhost:9000 s3 rb s3://test-bucket
```

### 6. Test with Python (boto3)

```python
import boto3

# Create client
s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='test-key',
    aws_secret_access_key='test-secret',
    region_name='us-east-1'
)

# Create bucket
s3.create_bucket(Bucket='my-bucket')

# Upload file
s3.put_object(
    Bucket='my-bucket',
    Key='hello.txt',
    Body=b'Hello, OpenEndpoint!'
)

# List objects
response = s3.list_objects_v2(Bucket='my-bucket')
for obj in response.get('Contents', []):
    print(f"Object: {obj['Key']}")

# Download file
response = s3.get_object(Bucket='my-bucket', Key='hello.txt')
print(response['Body'].read().decode())

# Delete object
s3.delete_object(Bucket='my-bucket', Key='hello.txt')

# Delete bucket
s3.delete_bucket(Bucket='my-bucket')
```

### 7. Test CLI Commands

```bash
# Using the CLI
./bin/openep.exe bucket create test-bucket
./bin/openep.exe bucket list
./bin/openep.exe bucket info test-bucket

# Object operations
./bin/openep.exe object put test-bucket/test.txt ./test.txt
./bin/openep.exe object list test-bucket
./bin/openep.exe object get test-bucket/test.txt ./downloaded.txt
./bin/openep.exe object delete test-bucket/test.txt

# Monitoring
./bin/openep.exe monitor status
./bin/openep.exe monitor health
./bin/openep.exe monitor metrics
```

---

## Integration Testing

### Run Integration Tests

```bash
# All integration tests
go test ./test/integration/...

# With verbose output
go test -v ./test/integration/...

# With coverage
go test ./test/integration/... -cover
```

### Test Scenarios

Integration tests cover:
- Full object lifecycle (create, read, update, delete)
- Multipart uploads
- Bucket operations
- Authentication flows
- Error handling

---

## Performance Testing

### Benchmarks

```bash
# Run all benchmarks
go test -bench=. ./...

# Run specific benchmark
go test -bench=BenchmarkPutObject ./internal/engine/...

# Run with memory profiling
go test -bench=. -memprofile=mem.out ./...

# Run with CPU profiling
go test -bench=. -cpuprofile=cpu.out ./...
```

### Load Testing

```bash
# Using wrk
wrk -t12 -c400 -d30s http://localhost:9000/health

# Using ab (Apache Bench)
ab -n 10000 -c 100 http://localhost:9000/health
```

---

## Troubleshooting

### Common Issues

#### Test Timeouts

```bash
# Increase timeout
go test -timeout 120s ./...

# Run specific test with timeout
go test -timeout 60s -run TestSlow ./...
```

#### Port Already in Use

```bash
# Find process using port 9000
lsof -i :9000
# or
netstat -tlnp | grep 9000

# Kill process
kill -9 <PID>
```

#### Permission Denied

```bash
# Fix permissions
chmod +x bin/openep.exe

# Check data directory permissions
ls -la ./test-data
```

#### Module Issues

```bash
# Download dependencies
go mod download

# Verify modules
go mod verify

# Tidy modules
go mod tidy
```

### Debug Mode

```bash
# Run with debug logging
./bin/openep.exe server -c config.yaml --log-level debug

# Enable Go debug
go test -v -debug ./...
```

---

## CI/CD Testing

### GitHub Actions

Tests run automatically on:
- Every push to main branch
- Every pull request
- Daily scheduled runs

### Local CI Simulation

```bash
# Run full test suite
go test -race -cover ./...

# Build all platforms
GOOS=linux GOARCH=amd64 go build -o bin/openep-linux ./cmd/openep
GOOS=darwin GOARCH=amd64 go build -o bin/openep-darwin ./cmd/openep
GOOS=windows GOARCH=amd64 go build -o bin/openep.exe ./cmd/openep
```

---

## Test Data Cleanup

```bash
# Remove test data
rm -rf ./test-data
rm -f test-config.yaml
rm -f test.txt downloaded.txt

# Remove coverage files
rm -f coverage.out *.cov
```

---

## Best Practices

1. **Always run tests before committing**
   ```bash
   go test ./...
   ```

2. **Check race conditions**
   ```bash
   go test -race ./...
   ```

3. **Maintain coverage**
   ```bash
   go test ./... -cover
   ```

4. **Test with real data**
   - Use the real environment testing steps
   - Test with various file sizes
   - Test error scenarios

5. **Clean up after testing**
   - Remove test data
   - Stop running servers
   - Clean up temporary files

---

## Additional Resources

- [Go Testing Documentation](https://golang.org/pkg/testing/)
- [Go Coverage Tool](https://golang.org/cmd/cover/)
- [AWS S3 API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html)
- [OpenEndpoint README](../README.md)
