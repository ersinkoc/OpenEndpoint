# Contributing to OpenEndpoint

First off, thank you for considering contributing to OpenEndpoint! It's people like you that make OpenEndpoint such a great tool.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Environment](#development-environment)
- [Project Structure](#project-structure)
- [Coding Standards](#coding-standards)
- [Testing Guidelines](#testing-guidelines)
- [Commit Message Convention](#commit-message-convention)
- [Pull Request Process](#pull-request-process)
- [Release Process](#release-process)
- [Security](#security)
- [Community](#community)

## Code of Conduct

This project and everyone participating in it is governed by our [Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code.

## Getting Started

### Fork and Clone

1. **Fork** the repository on GitHub
2. **Clone** your fork locally:
   ```bash
   git clone https://github.com/YOUR_USERNAME/openendpoint.git
   cd openendpoint
   ```
3. **Add upstream** remote:
   ```bash
   git remote add upstream https://github.com/openendpoint/openendpoint.git
   ```
4. **Create a branch** for your feature:
   ```bash
   git checkout -b feature/amazing-feature
   ```

### Sync with Upstream

Before starting work, ensure you're up to date:

```bash
git fetch upstream
git checkout main
git merge upstream/main
```

## Development Environment

### Prerequisites

- **Go** 1.22 or higher ([download](https://go.dev/dl/))
- **Make** (optional but recommended)
- **Docker** 20.10+ (for integration tests)
- **Git** 2.30+

### Quick Setup

```bash
# Install dependencies
make deps

# Or manually:
go mod download
go mod tidy

# Verify installation
make test-unit
```

### Available Make Commands

```bash
make help          # Show all available commands
make deps          # Download and tidy dependencies
make build         # Build binary for current platform
make build-all     # Build for all platforms
make test          # Run all tests with race detector
make test-unit     # Run unit tests only (faster)
make coverage      # Generate HTML coverage report
make coverage-check # Check coverage threshold (80%)
make lint          # Run golangci-lint
make fmt           # Format all Go files
make security      # Run govulncheck security scan
make bench         # Run benchmarks
make docker-build  # Build Docker image
make ci            # Run full CI pipeline locally
```

## Project Structure

```
openendpoint/
├── cmd/openep/           # Main application entry point
│   └── main.go
├── internal/             # Private packages
│   ├── api/              # HTTP handlers and routing
│   ├── auth/             # AWS Signature V2/V4 authentication
│   ├── storage/          # Storage backends (flatfile, packed)
│   ├── metadata/         # Metadata stores (pebble, bbolt)
│   ├── cluster/          # Multi-node clustering
│   ├── replication/      # Data replication
│   ├── federation/       # Multi-region federation
│   ├── encryption/       # Server-side encryption
│   ├── lifecycle/        # Object lifecycle management
│   ├── iam/              # Identity and access management
│   ├── quota/            # Storage quotas
│   ├── ratelimit/        # Rate limiting
│   ├── audit/            # Audit logging
│   ├── events/           # Event system
│   ├── telemetry/        # Metrics and monitoring
│   └── ...               # Additional packages
├── pkg/                  # Public API packages
│   ├── checksum/         # Checksum utilities
│   ├── byteutil/         # Byte manipulation
│   ├── s3types/          # S3 type definitions
│   └── client/           # Go client SDK
├── test/                 # Test utilities and integration tests
├── deploy/               # Deployment configurations
│   ├── docker/
│   ├── k8s/
│   └── systemd/
├── docs/                 # Documentation
└── files/                # Static assets
```

## Coding Standards

### Go Style Guide

We follow [Effective Go](https://golang.org/doc/effective_go) and [Google Go Style Guide](https://google.github.io/styleguide/go/):

- **Formatting**: All code must pass `gofmt -s`
- **Linting**: All code must pass `golangci-lint run`
- **Vetting**: Run `go vet ./...` to catch common mistakes
- **Imports**: Group imports: stdlib, third-party, internal
- **Documentation**: Export all public symbols with proper documentation

### Code Organization

```go
// Package doc comment
package example

import (
    // Standard library
    "context"
    "fmt"

    // Third-party
    "github.com/some/package"

    // Internal
    "github.com/openendpoint/openendpoint/internal/..."
)

// Constants
const (
    DefaultTimeout = 30 * time.Second
)

// Variables
var (
    ErrNotFound = errors.New("not found")
)

// Interface definitions
// ...

// Type definitions
// ...

// Constructor functions
// ...

// Method implementations
// ...
```

### Error Handling

- Use wrapped errors with context: `fmt.Errorf("context: %w", err)`
- Define sentinel errors for common cases
- Check errors immediately after assignment

### Logging

- Use structured logging via `internal/logging`
- Log at appropriate levels (Debug, Info, Warn, Error)
- Include relevant context in log fields

## Testing Guidelines

### Test Requirements

- **Coverage**: Maintain minimum 80% coverage
- **All new code** must have tests
- **Table-driven tests** preferred
- **Race-free**: All tests must pass with `-race`

### Running Tests

```bash
# Unit tests only (fast)
go test -short ./...

# All tests with race detector
go test -race ./...

# Specific package
go test -v ./internal/storage/flatfile/...

# With coverage
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

### Writing Tests

```go
func TestNewStore(t *testing.T) {
    tests := []struct {
        name    string
        config  Config
        wantErr bool
    }{
        {
            name: "valid config",
            config: Config{DataDir: t.TempDir()},
            wantErr: false,
        },
        {
            name: "missing data dir",
            config: Config{},
            wantErr: true,
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            got, err := NewStore(tt.config)
            if (err != nil) != tt.wantErr {
                t.Errorf("NewStore() error = %v, wantErr %v", err, tt.wantErr)
                return
            }
            if !tt.wantErr && got == nil {
                t.Error("NewStore() returned nil without error")
            }
        })
    }
}
```

### Benchmarks

```go
func BenchmarkPutObject(b *testing.B) {
    store, _ := NewStore(Config{DataDir: b.TempDir()})
    defer store.Close()

    data := make([]byte, 1024*1024) // 1MB

    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        store.PutObject(fmt.Sprintf("key-%d", i), data)
    }
}
```

## Commit Message Convention

We follow [Conventional Commits](https://www.conventionalcommits.org/):

### Format

```
<type>(<scope>): <subject>

<body>

<footer>
```

### Types

- **feat**: New feature
- **fix**: Bug fix
- **docs**: Documentation only
- **style**: Code style (formatting, semicolons, etc.)
- **refactor**: Code refactoring
- **perf**: Performance improvement
- **test**: Adding or fixing tests
- **chore**: Build process or auxiliary tool changes
- **ci**: CI/CD changes
- **security**: Security-related changes

### Examples

```
feat(storage): add compression support

Implement gzip compression for object storage.
Reduces storage usage by ~60% for text files.

Closes #123
```

```
fix(auth): resolve signature validation timing attack

Use constant-time comparison for HMAC verification
to prevent timing side-channel attacks.

Fixes #456
```

```
docs(api): update bucket policy documentation

Add examples for common policy configurations.
```

## Pull Request Process

### Before Submitting

1. **Sync with upstream**: `git fetch upstream && git rebase upstream/main`
2. **Run tests**: `make test` (all must pass)
3. **Check coverage**: `make coverage` (must be ≥80%)
4. **Lint code**: `make lint` (must pass)
5. **Format code**: `make fmt`
6. **Security scan**: `make security`

### PR Template

Your PR description should include:

```markdown
## Description
Brief description of changes

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Breaking change
- [ ] Documentation update

## Testing
- [ ] Tests added/updated
- [ ] All tests pass
- [ ] Coverage maintained

## Checklist
- [ ] Code follows style guidelines
- [ ] Self-review completed
- [ ] Documentation updated
- [ ] No new warnings
```

### Review Process

1. All PRs require at least **one review** from a maintainer
2. All **CI checks must pass**
3. **Conflicts must be resolved** before merge
4. We use **squash merge** to keep history clean

## Release Process

We follow [Semantic Versioning](https://semver.org/):

- **MAJOR**: Incompatible API changes
- **MINOR**: New functionality (backward compatible)
- **PATCH**: Bug fixes (backward compatible)

### Creating a Release

1. Update CHANGELOG.md
2. Create a signed tag:
   ```bash
   git tag -s v1.0.0 -m "Release v1.0.0"
   git push origin v1.0.0
   ```
3. GitHub Actions automatically:
   - Builds binaries for all platforms
   - Creates Docker images
   - Generates SBOM
   - Creates GitHub Release
   - Updates Homebrew formula

## Security

### Reporting Vulnerabilities

**DO NOT** report security vulnerabilities through public GitHub issues.

Instead, please:
- Email: **security@openendpoint.com**
- Include: Description, impact, reproduction steps
- Allow 48 hours for initial response
- Allow 90 days before public disclosure

### Security Best Practices

- Never commit secrets or credentials
- Use `internal/audit` for sensitive operations
- Follow OWASP guidelines for web security
- Run `make security` before submitting PRs

## Community

### Communication Channels

- **GitHub Issues**: Bug reports, feature requests
- **GitHub Discussions**: Questions, ideas, general discussion
- **Security**: security@openendpoint.com

### Recognition

Contributors will be:
- Listed in [CONTRIBUTORS.md](CONTRIBUTORS.md)
- Mentioned in release notes
- Added to the "Contributors" section on our website

## Questions?

Feel free to:
- Open an issue for questions
- Start a discussion
- Email: maintainers@openendpoint.com

---

**Thank you for contributing to OpenEndpoint!** 🚀
