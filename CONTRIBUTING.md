# Contributing to OpenEndpoint

Thank you for your interest in contributing to OpenEndpoint!

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Project Structure](#project-structure)
- [Coding Standards](#coding-standards)
- [Testing](#testing)
- [Pull Request Process](#pull-request-process)
- [Security](#security)

## Code of Conduct

This project follows the [Contributor Covenant Code of Conduct](https://www.contributor-covenant.org/version/2/1/code_of_conduct/). By participating, you are expected to uphold this code.

## Getting Started

1. Fork the repository
2. Clone your fork: `git clone https://github.com/YOUR_USERNAME/openendpoint.git`
3. Create a feature branch: `git checkout -b feature/my-feature`
4. Make your changes
5. Submit a pull request

## Development Setup

### Prerequisites

- Go 1.22 or higher
- Make (optional)
- Docker (optional)

### Quick Start

```bash
# Install dependencies
make deps

# Run tests
make test

# Build binary
make build

# Run locally
make run-dev
```

### Available Commands

```bash
make help          # Show all commands
make deps          # Download dependencies
make build         # Build binary
make test          # Run all tests
make test-unit     # Run unit tests only
make coverage      # Generate coverage report
make lint          # Run linter
make fmt           # Format code
make security      # Run security scan
make docker-build  # Build Docker image
```

## Project Structure

```
openendpoint/
├── cmd/openep/           # Main application
├── internal/             # Private packages (36 packages)
│   ├── api/              # HTTP handlers
│   ├── auth/             # AWS Signature V2/V4
│   ├── storage/          # Storage backends
│   ├── cluster/          # Multi-node clustering
│   ├── federation/       # Multi-region support
│   └── ...               # 30+ more packages
├── pkg/                  # Public packages
├── test/                 # Test utilities
├── files/                # Documentation
└── deploy/               # Deployment configs
```

## Coding Standards

### Go Style

- Follow [Effective Go](https://golang.org/doc/effective_go)
- Run `gofmt` before committing
- Run `go vet` to catch issues
- Ensure `golangci-lint` passes

### Commit Messages

- Use present tense ("Add feature" not "Added feature")
- Limit first line to 72 characters
- Reference issues: "Fixes #123"

## Testing

### Running Tests

```bash
make test        # All 527 tests
make coverage    # Coverage report
```

### Requirements

- All new code must have tests
- Maintain 80%+ coverage
- Use table-driven tests

## Pull Request Process

1. Create feature branch from `main`
2. Make changes following guidelines
3. Add/update tests
4. Update documentation
5. Ensure all checks pass
6. Submit pull request

### Checklist

- [ ] Code follows guidelines
- [ ] Tests added/updated
- [ ] Documentation updated
- [ ] All tests pass

## Security

**Do not report security issues publicly.**

Email: security@openendpoint.com

## License

By contributing, you agree that your contributions will be licensed under the Apache 2.0 License.
