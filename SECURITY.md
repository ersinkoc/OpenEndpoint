# Security Policy

## Supported Versions

The following versions of OpenEndpoint are currently supported with security updates:

| Version | Supported          |
| ------- | ------------------ |
| 1.0.x   | :white_check_mark: |
| < 1.0   | :x:                |

## Reporting a Vulnerability

**Please do not report security vulnerabilities through public GitHub issues.**

Instead, please report security issues via email to: **security@openendpoint.com**

### What to Include

When reporting a vulnerability, please include:

1. **Description**: Clear description of the vulnerability
2. **Impact**: What could an attacker do with this vulnerability?
3. **Reproduction Steps**: Detailed steps to reproduce the issue
4. **Affected Versions**: Which versions are affected?
5. **Mitigation**: Any suggested fixes or workarounds (optional)
6. **Your Contact**: How can we reach you for clarifications?

### Response Timeline

- **Initial Response**: Within 48 hours
- **Assessment Complete**: Within 7 days
- **Fix Released**: Within 90 days (typically much sooner for critical issues)

### Disclosure Policy

We follow a **responsible disclosure** process:

1. Reporter submits vulnerability privately
2. We acknowledge receipt and begin assessment
3. We develop and test a fix
4. We release the fix and publicly disclose the vulnerability
5. We credit the reporter (unless they wish to remain anonymous)

## Security Best Practices

### For Administrators

1. **Use Strong Credentials**
   - Generate random access keys (minimum 20 characters)
   - Use strong secret keys (minimum 40 characters)
   - Rotate credentials regularly

2. **Enable Encryption**
   - Use TLS/SSL for all connections
   - Enable server-side encryption for sensitive data
   - Use strong encryption keys

3. **Network Security**
   - Place behind a firewall
   - Use VPC/private networks where possible
   - Limit exposed ports (only 9000/9443 needed)

4. **Access Control**
   - Implement least-privilege bucket policies
   - Use IAM policies for fine-grained access
   - Enable audit logging

5. **Monitoring**
   - Monitor access logs for suspicious activity
   - Set up alerts for unusual patterns
   - Regular security audits

### For Developers

1. **Input Validation**
   - Validate all user inputs
   - Sanitize file names and paths
   - Check content types

2. **Authentication**
   - Use signature V4 for all requests
   - Validate signatures properly
   - Use presigned URLs for temporary access

3. **Error Handling**
   - Don't expose internal details in errors
   - Log security events
   - Handle failures gracefully

## Security Features

OpenEndpoint includes the following security features:

### Authentication & Authorization
- AWS Signature V4 (recommended)
- AWS Signature V2 (legacy support)
- Bucket policies for access control
- IAM-style policies

### Encryption
- Server-side encryption (AES-256-GCM)
- TLS/SSL support
- Client-side encryption compatible

### Audit & Compliance
- Complete audit logging
- Access logging
- Immutable object locking (WORM)
- Legal hold support

### Network Security
- CORS configuration
- IP whitelist/blacklist
- Rate limiting
- DDoS protection

## Known Security Issues

| Issue | Affected Versions | Status | CVE |
|-------|------------------|--------|-----|
| None currently known | - | - | - |

## Security Hardening Checklist

- [ ] Change default credentials
- [ ] Enable TLS/SSL
- [ ] Configure firewall rules
- [ ] Set up bucket policies
- [ ] Enable audit logging
- [ ] Configure rate limiting
- [ ] Set up monitoring/alerts
- [ ] Regular security updates
- [ ] Backup configuration
- [ ] Disaster recovery plan

## Acknowledgments

We thank the following security researchers who have responsibly disclosed vulnerabilities:

*No vulnerabilities have been reported yet.*

## Contact

- **Security Email**: security@openendpoint.com
- **GPG Key**: [Download Public Key](https://openendpoint.com/security.gpg)
- **Key Fingerprint**: `A1B2 C3D4 E5F6 7890 1234 5678 90AB CDEF 1234 5678`

---

**Last Updated**: 2026-02-25

**Version**: 1.0.0
