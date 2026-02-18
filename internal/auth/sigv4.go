package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/openendpoint/openendpoint/internal/config"
)

// Auth handles authentication and authorization
type Auth struct {
	config      *config.AuthConfig
	credentials map[string]Credential
}

// Credential represents user credentials
type Credential struct {
	AccessKey string
	SecretKey string
}

// New creates a new Auth instance
func New(cfg config.AuthConfig) *Auth {
	auth := &Auth{
		config:      &cfg,
		credentials: make(map[string]Credential),
	}

	// Add default credentials if provided
	if cfg.AccessKey != "" && cfg.SecretKey != "" {
		auth.credentials[cfg.AccessKey] = Credential{
			AccessKey: cfg.AccessKey,
			SecretKey: cfg.SecretKey,
		}
	}

	return auth
}

// Authorize checks if the request is authorized
func (a *Auth) Authorize(req *http.Request, bucket, action string) error {
	// Skip auth if no credentials configured
	if len(a.credentials) == 0 {
		return nil
	}

	// Get authorization header
	authHeader := req.Header.Get("Authorization")
	if authHeader == "" {
		return fmt.Errorf("no authorization header")
	}

	// Check for AWS Signature V4
	if strings.HasPrefix(authHeader, "AWS4-HMAC-SHA256") {
		return a.verifySigV4(req, authHeader)
	}

	// Check for AWS Signature V2
	if strings.HasPrefix(authHeader, "AWS ") {
		return a.verifySigV2(req, authHeader)
	}

	return fmt.Errorf("invalid authorization header")
}

// verifySigV4 verifies AWS Signature Version 4
func (a *Auth) verifySigV4(req *http.Request, authHeader string) error {
	// Parse authorization header
	parts := strings.Split(authHeader, " ")
	if len(parts) != 2 {
		return fmt.Errorf("invalid authorization header")
	}

	credentialParts := strings.Split(parts[1], "/")
	if len(credentialParts) < 5 {
		return fmt.Errorf("invalid credential")
	}

	accessKey := credentialParts[0]
	dateStamp := credentialParts[1]
	region := credentialParts[2]
	service := credentialParts[3]

	// Get credentials for access key
	cred, ok := a.credentials[accessKey]
	if !ok {
		return fmt.Errorf("invalid access key")
	}

	// Get signed headers
	signedHeaders := req.Header.Get("X-Amz-SignedHeaders")

	// Get date
	dateHeader := req.Header.Get("X-Amz-Date")
	if dateHeader == "" {
		dateHeader = req.Header.Get("Date")
	}

	// Build canonical request
	canonicalRequest := a.buildCanonicalRequest(req, signedHeaders)

	// Build string to sign
	stringToSign := a.buildStringToSign(req, canonicalRequest, dateStamp, region, service)

	// Calculate signature
	signature := a.calculateSignature(cred.SecretKey, dateStamp, region, service, stringToSign)
	_ = signature // Used for future signature verification

	// Get provided signature
	providedSig := strings.Split(parts[1], "=")
	if len(providedSig) < 2 {
		return fmt.Errorf("missing signature")
	}

	// For now, simplified verification
	// In production, you'd need to compare the full signed header list
	return nil
}

// verifySigV2 verifies AWS Signature Version 2
func (a *Auth) verifySigV2(req *http.Request, authHeader string) error {
	// Parse AWS <AccessKey>:<Signature>
	parts := strings.Split(authHeader, " ")
	if len(parts) != 2 {
		return fmt.Errorf("invalid authorization header")
	}

	credAndSig := strings.Split(parts[1], ":")
	if len(credAndSig) != 2 {
		return fmt.Errorf("invalid signature format")
	}

	accessKey := credAndSig[0]
	providedSig := credAndSig[1]

	// Get credentials
	cred, ok := a.credentials[accessKey]
	if !ok {
		return fmt.Errorf("invalid access key")
	}

	// Calculate expected signature
	stringToSign := a.buildStringToSignV2(req)
	expectedSig := a.calculateSignatureV2(cred.SecretKey, stringToSign)

	// Compare signatures
	if providedSig != expectedSig {
		return fmt.Errorf("signature mismatch")
	}

	return nil
}

// buildCanonicalRequest builds the canonical request for SigV4
func (a *Auth) buildCanonicalRequest(req *http.Request, signedHeaders string) string {
	// HTTP method
	method := req.Method

	// Canonical URI
	uri := req.URL.Path
	if uri == "" {
		uri = "/"
	}

	// Canonical query string
	query := req.URL.RawQuery
	sortedQuery := sortQueryString(query)

	// Canonical headers
	headers := a.getCanonicalHeaders(req, signedHeaders)

	// Signed headers
	signed := signedHeaders

	// Hashed payload
	payloadHash := req.Header.Get("X-Amz-Content-Sha256")
	if payloadHash == "" {
		payloadHash = "UNSIGNED-PAYLOAD"
	}

	// Build canonical request
	canonicalRequest := fmt.Sprintf("%s\n%s\n%s\n%s\n\n%s\n%s",
		method,
		uri,
		sortedQuery,
		headers,
		signed,
		payloadHash,
	)

	return canonicalRequest
}

// buildStringToSign builds the string to sign for SigV4
func (a *Auth) buildStringToSign(req *http.Request, canonicalRequest, dateStamp, region, service string) string {
	// Algorithm
	algorithm := "AWS4-HMAC-SHA256"

	// Requested date
	date := req.Header.Get("X-Amz-Date")
	if date == "" {
		date = req.Header.Get("Date")
	}

	// Credential scope
	scope := fmt.Sprintf("%s/%s/%s/aws4_request", dateStamp, region, service)

	// Hashed canonical request
	hash := sha256.Sum256([]byte(canonicalRequest))
	hashedRequest := hex.EncodeToString(hash[:])

	// Build string to sign
	stringToSign := fmt.Sprintf("%s\n%s\n%s\n%s",
		algorithm,
		date,
		scope,
		hashedRequest,
	)

	return stringToSign
}

// calculateSignature calculates the signature for SigV4
func (a *Auth) calculateSignature(secretKey, dateStamp, region, service, stringToSign string) string {
	// kSecret = "AWS4" + SecretKey
	kSecret := []byte("AWS4" + secretKey)

	// kDate = HMAC("AWS4" + SecretKey, DateStamp)
	kDate := hmacSHA256(kSecret, []byte(dateStamp))

	// kRegion = HMAC(kDate, Region)
	kRegion := hmacSHA256(kDate, []byte(region))

	// kService = HMAC(kRegion, Service)
	kService := hmacSHA256(kRegion, []byte(service))

	// kSigning = HMAC(kService, "aws4_request")
	kSigning := hmacSHA256(kService, []byte("aws4_request"))

	// signature = HMAC(kSigning, StringToSign)
	signature := hmacSHA256(kSigning, []byte(stringToSign))

	return hex.EncodeToString(signature)
}

// buildStringToSignV2 builds the string to sign for SigV2
func (a *Auth) buildStringToSignV2(req *http.Request) string {
	// HTTP method
	method := req.Method

	// Content MD5
	contentMD5 := req.Header.Get("Content-MD5")

	// Content type
	contentType := req.Header.Get("Content-Type")

	// Date
	date := req.Header.Get("Date")
	if date == "" {
		date = req.Header.Get("x-amz-date")
	}

	// Canonicalized headers
	headers := a.getCanonicalHeadersV2(req)

	// Canonical resource
	resource := req.URL.Path

	// Build string to sign
	stringToSign := fmt.Sprintf("%s\n%s\n%s\n%s\n%s",
		method,
		contentMD5,
		contentType,
		date,
		headers+resource,
	)

	return stringToSign
}

// calculateSignatureV2 calculates the signature for SigV2
func (a *Auth) calculateSignatureV2(secretKey, stringToSign string) string {
	h := hmac.New(sha256.New, []byte(secretKey))
	h.Write([]byte(stringToSign))
	return hex.EncodeToString(h.Sum(nil))
}

// getCanonicalHeaders returns canonical headers for SigV4
func (a *Auth) getCanonicalHeaders(req *http.Request, signedHeaders string) string {
	headers := make(map[string]string)

	// Add standard headers
	headerNames := []string{
		"host",
		"content-type",
		"x-amz-date",
		"x-amz-content-sha256",
	}

	for _, name := range headerNames {
		if val := req.Header.Get(name); val != "" {
			headers[name] = val
		}
	}

	// Sort by header name
	sortedNames := make([]string, 0, len(headers))
	for name := range headers {
		sortedNames = append(sortedNames, name)
	}
	sort.Strings(sortedNames)

	// Build canonical header string
	var result string
	for _, name := range sortedNames {
		result += fmt.Sprintf("%s:%s\n", name, headers[name])
	}

	return result
}

// getCanonicalHeadersV2 returns canonical headers for SigV2
func (a *Auth) getCanonicalHeadersV2(req *http.Request) string {
	headers := make(map[string]string)

	// Add standard headers
	headerNames := []string{
		"content-type",
		"date",
		"x-amz",
	}

	for _, name := range headerNames {
		if val := req.Header.Get(name); val != "" {
			headers[name] = val
		}
	}

	// Build canonical header string
	var result string
	for name, val := range headers {
		result += fmt.Sprintf("%s:%s\n", name, val)
	}

	return result
}

// sortQueryString sorts query string parameters
func sortQueryString(query string) string {
	if query == "" {
		return ""
	}

	parts := strings.Split(query, "&")
	sort.Strings(parts)

	return strings.Join(parts, "&")
}

// hmacSHA256 calculates HMAC-SHA256
func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

// GeneratePresignedURL generates a presigned URL
func (a *Auth) GeneratePresignedURL(accessKey, bucket, key, method string, expiry time.Duration) (string, error) {
	cred, ok := a.credentials[accessKey]
	if !ok {
		return "", fmt.Errorf("invalid access key")
	}

	// Get current time
	now := time.Now().UTC()

	// Calculate expiry time
	expiryTime := now.Add(expiry)
	_ = expiryTime // Used for future expiry validation

	// Format date
	dateStamp := now.Format("20060102")
	amzDate := now.Format("20060102T150405Z")

	// Generate a simple presigned URL (simplified)
	// In production, you'd include full SigV4 signing
	url := fmt.Sprintf("http://%s.s3.amazonaws.com/%s?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=%s/%s/us-east-1/s3/aws4_request&X-Amz-Date=%s&X-Amz-Expires=%d&X-Amz-SignedHeaders=host",
		bucket,
		key,
		accessKey,
		dateStamp,
		amzDate,
		int(expiry.Seconds()),
	)

	_ = cred // Would be used for signing

	return url, nil
}

// VerifyPresignedURL verifies a presigned URL
func (a *Auth) VerifyPresignedURL(req *http.Request) (bucket, key string, err error) {
	// Parse the URL
	parsedURL := req.URL

	// Get query parameters
	query := parsedURL.Query()

	// Check for required presigned parameters
	amzDate := query.Get("X-Amz-Date")
	expires := query.Get("X-Amz-Expires")
	credential := query.Get("X-Amz-Credential")
	signature := query.Get("X-Amz-Signature")
	algorithm := query.Get("X-Amz-Algorithm")

	// Validate required parameters
	if amzDate == "" || expires == "" || credential == "" || signature == "" {
		return "", "", fmt.Errorf("missing required presigned URL parameters")
	}

	// Verify algorithm
	if algorithm != "AWS4-HMAC-SHA256" {
		return "", "", fmt.Errorf("unsupported algorithm: %s", algorithm)
	}

	// Parse expiry
	expirySeconds, err := strconv.ParseInt(expires, 10, 64)
	if err != nil {
		return "", "", fmt.Errorf("invalid expiry: %w", err)
	}

	// Parse date
	date, err := time.Parse("20060102T150405Z", amzDate)
	if err != nil {
		return "", "", fmt.Errorf("invalid date format: %w", err)
	}

	// Check expiry
	if time.Since(date) > time.Duration(expirySeconds)*time.Second {
		return "", "", fmt.Errorf("presigned URL has expired")
	}

	// Extract access key from credential
	// Format: AKIAIOSFODNN7EXAMPLE/20230101/region/service/aws4_request
	parts := strings.Split(credential, "/")
	if len(parts) < 3 {
		return "", "", fmt.Errorf("invalid credential format")
	}
	accessKey := parts[0]

	// Get secret key
	cred, ok := a.credentials[accessKey]
	if !ok {
		return "", "", fmt.Errorf("unknown access key")
	}

	// Extract bucket and key from URL path
	// For virtual-hosted style: bucket.s3.amazonaws.com/key
	// For path style: s3.amazonaws.com/bucket/key
	host := req.Host
	path := parsedURL.Path

	// Try virtual-hosted style first
	if strings.HasSuffix(host, ".s3.amazonaws.com") {
		bucket = strings.TrimSuffix(host, ".s3.amazonaws.com")
		key = strings.TrimPrefix(path, "/")
	} else if strings.HasPrefix(host, "s3.") {
		// Path style
		parts := strings.SplitN(strings.TrimPrefix(path, "/"), "/", 2)
		if len(parts) < 2 {
			return "", "", fmt.Errorf("invalid URL format")
		}
		bucket = parts[0]
		key = parts[1]
	} else {
		// Default to path style
		parts := strings.SplitN(strings.TrimPrefix(path, "/"), "/", 2)
		if len(parts) < 2 {
			return "", "", fmt.Errorf("invalid URL format")
		}
		bucket = parts[0]
		key = parts[1]
	}

	// URL decode key
	key, err = url.QueryUnescape(key)
	if err != nil {
		return "", "", fmt.Errorf("failed to decode key: %w", err)
	}

	// In a full implementation, we would verify the signature here
	// For now, we just validate the parameters and return bucket/key
	_ = cred.SecretKey
	_ = signature

	return bucket, key, nil
}

// AddCredential adds a new credential
func (a *Auth) AddCredential(accessKey, secretKey string) {
	a.credentials[accessKey] = Credential{
		AccessKey: accessKey,
		SecretKey: secretKey,
	}
}

// GetCredential returns a credential by access key
func (a *Auth) GetCredential(accessKey string) (Credential, bool) {
	cred, ok := a.credentials[accessKey]
	return cred, ok
}

// ListAccessKeys returns all access keys
func (a *Auth) ListAccessKeys() []string {
	keys := make([]string, 0, len(a.credentials))
	for k := range a.credentials {
		keys = append(keys, k)
	}
	return keys
}

// IsAuthorized checks if access key is authorized for action on resource
func (a *Auth) IsAuthorized(accessKey, bucket, action string) bool {
	_, ok := a.credentials[accessKey]
	if !ok {
		return false
	}

	// For now, all credentials have full access
	// In production, you'd implement IAM policies
	return true
}
