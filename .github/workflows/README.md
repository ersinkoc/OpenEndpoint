# GitHub Actions Workflows

This directory contains comprehensive CI/CD workflows for the OpenEndpoint project.

## Workflows Overview

### 1. CI Workflow (`ci.yml`)
**Triggers:** Push to main/develop, Pull requests

**Jobs:**
- **Lint:** Code formatting, golangci-lint, go vet
- **Security Scan:** Govulncheck, Gosec, Trivy with SARIF upload
- **Test:** Matrix testing (Ubuntu/macOS/Windows × Go 1.21/1.22)
- **Integration Tests:** Tests against MinIO service container
- **Benchmarks:** Performance tracking with GitHub benchmark action
- **Build:** Multi-platform binary builds (5 platforms)
- **Docker:** Multi-arch Docker builds with SBOM generation

### 2. Release Workflow (`release.yml`)
**Triggers:** Tag push (v*), Manual workflow dispatch

**Jobs:**
- **Metadata:** Version detection, automated changelog generation
- **Build Binaries:** Native builds on respective platforms
- **Docker:** Multi-registry push (GHCR + Docker Hub)
- **Security Scan:** Trivy image scanning
- **Release:** GitHub Release creation with signed checksums
- **Homebrew:** Automatic formula updates
- **Notify:** Discord notifications

**Features:**
- Automatic pre-release detection (alpha, beta, rc tags)
- Categorized changelog (Features, Fixes, Performance, etc.)
- GPG-signed checksums
- SBOM generation
- Build attestation

### 3. Changelog Workflow (`changelog.yml`)
**Triggers:** PR merge, Manual dispatch

**Jobs:**
- **Update Changelog:** Auto-adds PR entries to CHANGELOG.md
- **Prepare Release:** Creates release PR with version bump
- **Validate:** Checks CHANGELOG.md format

**Features:**
- Emoji categorization based on PR title
- Automatic section organization
- PR-based release preparation

### 4. Deploy Workflow (`deploy.yml`)
**Triggers:** Manual dispatch, Release completion

**Jobs:**
- **Validate:** Image verification, environment checks
- **Deploy Staging:** EKS deployment with smoke tests
- **Deploy Production:** Canary deployment with rollback
- **Docker Compose:** Direct server deployment
- **Update Docs:** Version update in documentation

**Features:**
- Environment protection rules
- Canary deployments
- Automatic rollback on failure
- Pre/post deployment verification

### 5. Dependencies Workflow (`dependencies.yml`)
**Triggers:** Weekly schedule, Manual dispatch

**Jobs:**
- **Check Go Deps:** Automated dependency updates via PR
- **Security Audit:** Govulncheck with issue creation
- **Docker Images:** Base image vulnerability scanning
- **License Check:** Compliance verification

### 6. Code Quality Workflow (`code-quality.yml`)
**Triggers:** Push, PR, Nightly schedule

**Jobs:**
- **Coverage:** Detailed coverage analysis with PR comments
- **Static Analysis:** go vet, staticcheck, ineffassign, misspell
- **Complexity:** Cyclomatic complexity tracking
- **Dead Code:** Unused code detection
- **Race Detector:** Concurrent access detection
- **Benchmarks:** Performance regression tracking

## Required Secrets

See `secrets-example.env` for the full list of required secrets.

### Essential Secrets
- `GITHUB_TOKEN` (auto-provided)
- `DOCKERHUB_USERNAME` / `DOCKERHUB_TOKEN`
- `CODECOV_TOKEN`

### Deployment Secrets (Optional)
- `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`
- `EKS_CLUSTER_NAME`
- `SSH_PRIVATE_KEY` / `DEPLOY_SERVER`

### Notification Secrets (Optional)
- `DISCORD_WEBHOOK_URL`
- `GPG_PRIVATE_KEY` / `GPG_PASSPHRASE`

## Usage

### Creating a Release

1. **Automatic (Recommended):**
   ```bash
   git tag v1.0.0
   git push origin v1.0.0
   ```

2. **Manual via GitHub:**
   - Go to Actions → Release → Run workflow
   - Enter version (e.g., v1.0.0)
   - Check "pre-release" if applicable

3. **Via Changelog Workflow:**
   - Merge PRs with conventional commits
   - Run Changelog workflow → Prepare Release
   - Merge the created PR
   - Push the tag

### Deploying

1. **Staging:**
   - Automatically deploys on release completion
   - Or manual: Actions → Deploy → staging

2. **Production:**
   - Actions → Deploy → production
   - Must confirm deployment
   - Requires manual approval (environment protection)

## Workflow Dependencies

```
ci.yml
    ↓ (on tag push)
release.yml
    ↓ (on success)
deploy.yml (staging)
    ↓ (manual approval)
deploy.yml (production)
```

## Badges

Add these to your README.md:

```markdown
![CI](https://github.com/openendpoint/openendpoint/workflows/CI/badge.svg)
![Release](https://github.com/openendpoint/openendpoint/workflows/Release/badge.svg)
![Code Quality](https://github.com/openendpoint/openendpoint/workflows/Code%20Quality/badge.svg)
```

## Troubleshooting

### Common Issues

1. **Docker login fails:**
   - Check DOCKERHUB_TOKEN has push permissions
   - Verify token is not expired

2. **EKS deployment fails:**
   - Verify AWS credentials
   - Check kubectl context
   - Ensure cluster is accessible

3. **GPG signing fails:**
   - Verify GPG key format (armored)
   - Check passphrase is correct

4. **Coverage upload fails:**
   - Verify CODECOV_TOKEN
   - Check file paths in coverage.out

### Debug Mode

Add `ACTIONS_STEP_DEBUG=true` to repository secrets for verbose logging.
