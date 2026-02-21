package auth

import (
	"crypto/hmac"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/openendpoint/openendpoint/internal/config"
)

func TestNew(t *testing.T) {
	cfg := config.AuthConfig{
		AccessKey: "test-access-key",
		SecretKey: "test-secret-key-123",
	}

	auth := New(cfg)
	if auth == nil {
		t.Fatal("Auth should not be nil")
	}

	if len(auth.credentials) != 1 {
		t.Errorf("Expected 1 credential, got %d", len(auth.credentials))
	}

	cred, ok := auth.credentials["test-access-key"]
	if !ok {
		t.Fatal("Credential not found")
	}

	if cred.SecretKey != "test-secret-key-123" {
		t.Errorf("SecretKey = %s, want test-secret-key-123", cred.SecretKey)
	}
}

func TestAuthorize_NoCredentials(t *testing.T) {
	cfg := config.AuthConfig{}
	auth := New(cfg)

	req, _ := http.NewRequest("GET", "/test", nil)

	// Should pass when no credentials configured
	err := auth.Authorize(req, "test-bucket", "GetObject")
	if err != nil {
		t.Errorf("Expected no error with no credentials, got: %v", err)
	}
}

func TestAuthorize_MissingHeader(t *testing.T) {
	cfg := config.AuthConfig{
		AccessKey: "test-access-key",
		SecretKey: "test-secret-key-123",
	}
	auth := New(cfg)

	req, _ := http.NewRequest("GET", "/test", nil)

	err := auth.Authorize(req, "test-bucket", "GetObject")
	if err == nil {
		t.Error("Expected error when authorization header is missing")
	}
}

func TestAuthorize_InvalidHeader(t *testing.T) {
	cfg := config.AuthConfig{
		AccessKey: "test-access-key",
		SecretKey: "test-secret-key-123",
	}
	auth := New(cfg)

	req, _ := http.NewRequest("GET", "/test", nil)
	req.Header.Set("Authorization", "Invalid header")

	err := auth.Authorize(req, "test-bucket", "GetObject")
	if err == nil {
		t.Error("Expected error when authorization header is invalid")
	}
}

func TestAddCredential(t *testing.T) {
	cfg := config.AuthConfig{}
	auth := New(cfg)

	auth.AddCredential("new-key", "new-secret")

	if len(auth.credentials) != 1 {
		t.Errorf("Expected 1 credential, got %d", len(auth.credentials))
	}

	cred, ok := auth.credentials["new-key"]
	if !ok {
		t.Fatal("Credential not found")
	}

	if cred.SecretKey != "new-secret" {
		t.Errorf("SecretKey = %s, want new-secret", cred.SecretKey)
	}
}

func TestGetCredential(t *testing.T) {
	cfg := config.AuthConfig{
		AccessKey: "test-access-key",
		SecretKey: "test-secret-key-123",
	}
	auth := New(cfg)

	cred, ok := auth.GetCredential("test-access-key")
	if !ok {
		t.Fatal("Credential not found")
	}

	if cred.AccessKey != "test-access-key" {
		t.Errorf("AccessKey = %s, want test-access-key", cred.AccessKey)
	}

	_, ok = auth.GetCredential("non-existent")
	if ok {
		t.Error("Expected credential not to exist")
	}
}

func TestListAccessKeys(t *testing.T) {
	cfg := config.AuthConfig{}
	auth := New(cfg)

	auth.AddCredential("key1", "secret1")
	auth.AddCredential("key2", "secret2")
	auth.AddCredential("key3", "secret3")

	keys := auth.ListAccessKeys()
	if len(keys) != 3 {
		t.Errorf("Expected 3 keys, got %d", len(keys))
	}
}

func TestCalculateSignature(t *testing.T) {
	cfg := config.AuthConfig{
		AccessKey: "AKIAIOSFODNN7EXAMPLE",
		SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
	}
	auth := New(cfg)

	// Test that signature calculation returns a non-empty string
	signature := auth.calculateSignature(
		"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		"20130524",
		"us-east-1",
		"s3",
		"test-string-to-sign",
	)

	if signature == "" {
		t.Error("Signature should not be empty")
	}

	// Signature should be a valid hex string
	for _, c := range signature {
		if !isHexChar(c) {
			t.Errorf("Signature contains non-hex character: %c", c)
			break
		}
	}
}

func isHexChar(c rune) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

func TestBuildCanonicalRequest(t *testing.T) {
	cfg := config.AuthConfig{
		AccessKey: "test-key",
		SecretKey: "test-secret",
	}
	auth := New(cfg)

	req, _ := http.NewRequest("GET", "/test-bucket/test-key?param=value", nil)
	req.Host = "s3.amazonaws.com"
	req.Header.Set("Host", "s3.amazonaws.com")
	req.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")

	canonical := auth.buildCanonicalRequest(req, "host;x-amz-content-sha256")

	if canonical == "" {
		t.Error("Canonical request should not be empty")
	}

	// Should contain HTTP method
	if !strings.Contains(canonical, "GET") {
		t.Error("Canonical request should contain GET method")
	}
}

func TestBuildStringToSign(t *testing.T) {
	cfg := config.AuthConfig{
		AccessKey: "test-key",
		SecretKey: "test-secret",
	}
	auth := New(cfg)

	req, _ := http.NewRequest("GET", "/test", nil)
	req.Header.Set("X-Amz-Date", "20130524T000000Z")

	stringToSign := auth.buildStringToSign(req, "canonical-request", "20130524", "us-east-1", "s3")

	if stringToSign == "" {
		t.Error("String to sign should not be empty")
	}

	// Should contain algorithm
	if !strings.Contains(stringToSign, "AWS4-HMAC-SHA256") {
		t.Error("String to sign should contain algorithm")
	}
}

func TestVerifyPresignedURL_MissingParameters(t *testing.T) {
	cfg := config.AuthConfig{
		AccessKey: "test-key",
		SecretKey: "test-secret",
	}
	auth := New(cfg)

	req, _ := http.NewRequest("GET", "/bucket/key", nil)

	_, _, err := auth.VerifyPresignedURL(req)
	if err == nil {
		t.Error("Expected error when presigned URL parameters are missing")
	}
}

func TestVerifyPresignedURL_Expired(t *testing.T) {
	cfg := config.AuthConfig{
		AccessKey: "AKIAIOSFODNN7EXAMPLE",
		SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
	}
	auth := New(cfg)

	// Create expired URL
	req, _ := http.NewRequest("GET", "/bucket/key", nil)
	query := url.Values{}
	query.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	query.Set("X-Amz-Credential", "AKIAIOSFODNN7EXAMPLE/20200101/us-east-1/s3/aws4_request")
	query.Set("X-Amz-Date", "20200101T000000Z") // Old date
	query.Set("X-Amz-Expires", "3600")
	query.Set("X-Amz-SignedHeaders", "host")
	query.Set("X-Amz-Signature", "abc123")
	req.URL.RawQuery = query.Encode()

	_, _, err := auth.VerifyPresignedURL(req)
	if err == nil {
		t.Error("Expected error when presigned URL is expired")
	}
}

func TestVerifyPresignedURL_InvalidAlgorithm(t *testing.T) {
	cfg := config.AuthConfig{
		AccessKey: "test-key",
		SecretKey: "test-secret",
	}
	auth := New(cfg)

	req, _ := http.NewRequest("GET", "/bucket/key", nil)
	query := url.Values{}
	query.Set("X-Amz-Algorithm", "INVALID")
	query.Set("X-Amz-Credential", "test-key/20230101/us-east-1/s3/aws4_request")
	query.Set("X-Amz-Date", time.Now().Format("20060102T150405Z"))
	query.Set("X-Amz-Expires", "3600")
	query.Set("X-Amz-SignedHeaders", "host")
	query.Set("X-Amz-Signature", "abc123")
	req.URL.RawQuery = query.Encode()

	_, _, err := auth.VerifyPresignedURL(req)
	if err == nil {
		t.Error("Expected error when algorithm is invalid")
	}
}

func TestVerifyPresignedURL_InvalidCredential(t *testing.T) {
	cfg := config.AuthConfig{
		AccessKey: "test-key",
		SecretKey: "test-secret",
	}
	auth := New(cfg)

	req, _ := http.NewRequest("GET", "/bucket/key", nil)
	query := url.Values{}
	query.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	query.Set("X-Amz-Credential", "invalid") // Invalid credential format
	query.Set("X-Amz-Date", time.Now().Format("20060102T150405Z"))
	query.Set("X-Amz-Expires", "3600")
	query.Set("X-Amz-SignedHeaders", "host")
	query.Set("X-Amz-Signature", "abc123")
	req.URL.RawQuery = query.Encode()

	_, _, err := auth.VerifyPresignedURL(req)
	if err == nil {
		t.Error("Expected error when credential format is invalid")
	}
}

func TestVerifyPresignedURL_UnknownAccessKey(t *testing.T) {
	cfg := config.AuthConfig{
		AccessKey: "known-key",
		SecretKey: "test-secret",
	}
	auth := New(cfg)

	req, _ := http.NewRequest("GET", "/bucket/key", nil)
	query := url.Values{}
	query.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	query.Set("X-Amz-Credential", "unknown-key/20230101/us-east-1/s3/aws4_request")
	query.Set("X-Amz-Date", time.Now().Format("20060102T150405Z"))
	query.Set("X-Amz-Expires", "3600")
	query.Set("X-Amz-SignedHeaders", "host")
	query.Set("X-Amz-Signature", "abc123")
	req.URL.RawQuery = query.Encode()

	_, _, err := auth.VerifyPresignedURL(req)
	if err == nil {
		t.Error("Expected error when access key is unknown")
	}
}

func TestHMACSHA256(t *testing.T) {
	key := []byte("test-key")
	data := []byte("test-data")

	result := hmacSHA256(key, data)

	if len(result) == 0 {
		t.Error("HMAC result should not be empty")
	}

	// Verify consistency
	result2 := hmacSHA256(key, data)
	if !hmac.Equal(result, result2) {
		t.Error("HMAC should produce consistent results")
	}

	// Verify different keys produce different results
	result3 := hmacSHA256([]byte("different-key"), data)
	if hmac.Equal(result, result3) {
		t.Error("Different keys should produce different HMACs")
	}
}

func TestSortQueryString(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"c=3&a=1&b=2", "a=1&b=2&c=3"},
		{"", ""},
		{"single=value", "single=value"},
	}

	for _, test := range tests {
		result := sortQueryString(test.input)
		if result != test.expected {
			t.Errorf("sortQueryString(%s) = %s, want %s", test.input, result, test.expected)
		}
	}
}

func TestGetCanonicalHeaders(t *testing.T) {
	cfg := config.AuthConfig{
		AccessKey: "test-key",
		SecretKey: "test-secret",
	}
	auth := New(cfg)

	req, _ := http.NewRequest("GET", "/test", nil)
	req.Header.Set("Host", "example.com")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Amz-Date", "20130524T000000Z")

	headers := auth.getCanonicalHeaders(req, "host;content-type;x-amz-date")

	if headers == "" {
		t.Error("Canonical headers should not be empty")
	}

	// Should contain host header
	if !strings.Contains(headers, "host:") {
		t.Error("Canonical headers should contain host")
	}
}

func TestVerifySigV2(t *testing.T) {
	cfg := config.AuthConfig{
		AccessKey: "test-key",
		SecretKey: "test-secret",
	}
	auth := New(cfg)

	// Test with invalid format
	req, _ := http.NewRequest("GET", "/test", nil)
	req.Header.Set("Authorization", "AWS invalid-format")

	err := auth.verifySigV2(req, "AWS invalid-format")
	if err == nil {
		t.Error("Expected error with invalid SigV2 format")
	}
}

func TestVerifySigV2_InvalidSignature(t *testing.T) {
	cfg := config.AuthConfig{
		AccessKey: "test-key",
		SecretKey: "test-secret",
	}
	auth := New(cfg)

	req, _ := http.NewRequest("GET", "/test", nil)
	req.Header.Set("Authorization", "AWS test-key:invalid-signature")
	req.Header.Set("Date", time.Now().Format(http.TimeFormat))

	err := auth.verifySigV2(req, "AWS test-key:invalid-signature")
	if err == nil {
		t.Error("Expected error with invalid signature")
	}
}

func TestVerifySigV4_InvalidCredential(t *testing.T) {
	cfg := config.AuthConfig{
		AccessKey: "test-key",
		SecretKey: "test-secret",
	}
	auth := New(cfg)

	req, _ := http.NewRequest("GET", "/test", nil)
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=invalid")

	err := auth.verifySigV4(req, "AWS4-HMAC-SHA256 Credential=invalid")
	if err == nil {
		t.Error("Expected error with invalid credential")
	}
}

func TestVerifySigV4_UnknownAccessKey(t *testing.T) {
	cfg := config.AuthConfig{
		AccessKey: "known-key",
		SecretKey: "test-secret",
	}
	auth := New(cfg)

	req, _ := http.NewRequest("GET", "/test", nil)
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=unknown-key/20230101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=abc123")

	err := auth.verifySigV4(req, "AWS4-HMAC-SHA256 Credential=unknown-key/20230101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=abc123")
	if err == nil {
		t.Error("Expected error with unknown access key")
	}
}
