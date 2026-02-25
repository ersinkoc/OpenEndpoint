package auth

import (
	"crypto/hmac"
	"fmt"
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

	err := auth.verifySigV4(req, "AWS4-HMAC-SHA256 invalid")
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
	req.Header.Set("X-Amz-SignedHeaders", "host")

	err := auth.verifySigV4(req, "AWS4-HMAC-SHA256 unknown-key/20230101/us-east-1/s3/aws4_request=abc123")
	if err == nil {
		t.Error("Expected error with unknown access key")
	}
}

func TestIsAuthorized(t *testing.T) {
	cfg := config.AuthConfig{
		AccessKey: "test-key",
		SecretKey: "test-secret",
	}
	auth := New(cfg)

	if !auth.IsAuthorized("test-key", "bucket", "GetObject") {
		t.Error("Should be authorized with valid key")
	}

	if auth.IsAuthorized("unknown-key", "bucket", "GetObject") {
		t.Error("Should not be authorized with unknown key")
	}
}

func TestGeneratePresignedURL(t *testing.T) {
	cfg := config.AuthConfig{
		AccessKey: "test-key",
		SecretKey: "test-secret",
	}
	auth := New(cfg)

	url, err := auth.GeneratePresignedURL("test-key", "test-bucket", "test-key", "GET", time.Hour)
	if err != nil {
		t.Fatalf("GeneratePresignedURL failed: %v", err)
	}

	if url == "" {
		t.Error("URL should not be empty")
	}

	if !strings.Contains(url, "test-bucket") {
		t.Error("URL should contain bucket name")
	}

	if !strings.Contains(url, "X-Amz-Algorithm=AWS4-HMAC-SHA256") {
		t.Error("URL should contain algorithm")
	}
}

func TestGeneratePresignedURL_InvalidKey(t *testing.T) {
	cfg := config.AuthConfig{
		AccessKey: "test-key",
		SecretKey: "test-secret",
	}
	auth := New(cfg)

	_, err := auth.GeneratePresignedURL("unknown-key", "bucket", "key", "GET", time.Hour)
	if err == nil {
		t.Error("Expected error with unknown access key")
	}
}

func TestCalculateSignatureV2(t *testing.T) {
	cfg := config.AuthConfig{
		AccessKey: "test-key",
		SecretKey: "test-secret",
	}
	auth := New(cfg)

	sig := auth.calculateSignatureV2("secret-key", "string-to-sign")

	if sig == "" {
		t.Error("Signature should not be empty")
	}

	// Should be hex string
	for _, c := range sig {
		if !isHexChar(c) {
			t.Errorf("Signature contains non-hex character: %c", c)
			break
		}
	}
}

func TestBuildStringToSignV2(t *testing.T) {
	cfg := config.AuthConfig{
		AccessKey: "test-key",
		SecretKey: "test-secret",
	}
	auth := New(cfg)

	req, _ := http.NewRequest("GET", "/bucket/key", nil)
	req.Header.Set("Content-MD5", "d41d8cd98f00b204e9800998ecf8427e")
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("Date", "Thu, 21 Feb 2019 22:28:04 +0000")

	stringToSign := auth.buildStringToSignV2(req)

	if stringToSign == "" {
		t.Error("String to sign should not be empty")
	}

	if !strings.Contains(stringToSign, "GET") {
		t.Error("String to sign should contain method")
	}
}

func TestGetCanonicalHeadersV2(t *testing.T) {
	cfg := config.AuthConfig{
		AccessKey: "test-key",
		SecretKey: "test-secret",
	}
	auth := New(cfg)

	req, _ := http.NewRequest("GET", "/test", nil)
	req.Header.Set("Date", "Thu, 21 Feb 2019 22:28:04 +0000")
	req.Header.Set("Content-Type", "text/plain")

	headers := auth.getCanonicalHeadersV2(req)

	if headers == "" {
		t.Error("Canonical headers V2 should not be empty")
	}
}

func TestVerifySigV2_MissingColon(t *testing.T) {
	cfg := config.AuthConfig{
		AccessKey: "test-key",
		SecretKey: "test-secret",
	}
	auth := New(cfg)

	req, _ := http.NewRequest("GET", "/test", nil)
	req.Header.Set("Authorization", "AWS test-key")

	err := auth.verifySigV2(req, "AWS test-key")
	if err == nil {
		t.Error("Expected error with missing colon in signature")
	}
}

func TestVerifySigV2_InvalidAccessKey(t *testing.T) {
	cfg := config.AuthConfig{
		AccessKey: "test-key",
		SecretKey: "test-secret",
	}
	auth := New(cfg)

	req, _ := http.NewRequest("GET", "/test", nil)
	req.Header.Set("Authorization", "AWS unknown-key:sig")

	err := auth.verifySigV2(req, "AWS unknown-key:sig")
	if err == nil {
		t.Error("Expected error with unknown access key")
	}
}

func TestVerifySigV4_MissingSignature(t *testing.T) {
	cfg := config.AuthConfig{
		AccessKey: "test-key",
		SecretKey: "test-secret",
	}
	auth := New(cfg)

	req, _ := http.NewRequest("GET", "/test", nil)
	req.Header.Set("X-Amz-SignedHeaders", "host")

	err := auth.verifySigV4(req, "AWS4-HMAC-SHA256 test-key/20230101/us-east-1/s3/aws4_request")
	if err == nil {
		t.Error("Expected error with missing signature")
	}
}

func TestVerifySigV4_SignatureMismatch(t *testing.T) {
	cfg := config.AuthConfig{
		AccessKey: "test-key",
		SecretKey: "test-secret",
	}
	auth := New(cfg)

	req, _ := http.NewRequest("GET", "/test", nil)
	req.Header.Set("X-Amz-SignedHeaders", "host")
	req.Header.Set("X-Amz-Date", "20230101T000000Z")

	err := auth.verifySigV4(req, "AWS4-HMAC-SHA256 test-key/20230101/us-east-1/s3/aws4_request=invalidsignature")
	if err == nil {
		t.Error("Expected error with signature mismatch")
	}
}

func TestVerifyPresignedURL_InvalidExpiry(t *testing.T) {
	cfg := config.AuthConfig{
		AccessKey: "test-key",
		SecretKey: "test-secret",
	}
	auth := New(cfg)

	req, _ := http.NewRequest("GET", "/bucket/key", nil)
	query := url.Values{}
	query.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	query.Set("X-Amz-Credential", "test-key/20230101/us-east-1/s3/aws4_request")
	query.Set("X-Amz-Date", time.Now().Format("20060102T150405Z"))
	query.Set("X-Amz-Expires", "invalid")
	query.Set("X-Amz-SignedHeaders", "host")
	query.Set("X-Amz-Signature", "abc123")
	req.URL.RawQuery = query.Encode()

	_, _, err := auth.VerifyPresignedURL(req)
	if err == nil {
		t.Error("Expected error with invalid expiry")
	}
}

func TestVerifyPresignedURL_InvalidDateFormat(t *testing.T) {
	cfg := config.AuthConfig{
		AccessKey: "test-key",
		SecretKey: "test-secret",
	}
	auth := New(cfg)

	req, _ := http.NewRequest("GET", "/bucket/key", nil)
	query := url.Values{}
	query.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	query.Set("X-Amz-Credential", "test-key/20230101/us-east-1/s3/aws4_request")
	query.Set("X-Amz-Date", "invalid-date")
	query.Set("X-Amz-Expires", "3600")
	query.Set("X-Amz-SignedHeaders", "host")
	query.Set("X-Amz-Signature", "abc123")
	req.URL.RawQuery = query.Encode()

	_, _, err := auth.VerifyPresignedURL(req)
	if err == nil {
		t.Error("Expected error with invalid date format")
	}
}

func TestVerifyPresignedURL_VirtualHostedStyle(t *testing.T) {
	cfg := config.AuthConfig{
		AccessKey: "test-key",
		SecretKey: "test-secret",
	}
	auth := New(cfg)

	req, _ := http.NewRequest("GET", "/mykey", nil)
	req.Host = "mybucket.s3.amazonaws.com"
	query := url.Values{}
	query.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	query.Set("X-Amz-Credential", "test-key/20230101/us-east-1/s3/aws4_request")
	query.Set("X-Amz-Date", time.Now().Format("20060102T150405Z"))
	query.Set("X-Amz-Expires", "3600")
	query.Set("X-Amz-SignedHeaders", "host")
	query.Set("X-Amz-Signature", "abc123")
	req.URL.RawQuery = query.Encode()

	_, _, err := auth.VerifyPresignedURL(req)
	if err == nil {
		t.Error("Expected error with signature mismatch")
	}
}

func TestVerifyPresignedURL_PathStyle(t *testing.T) {
	cfg := config.AuthConfig{
		AccessKey: "test-key",
		SecretKey: "test-secret",
	}
	auth := New(cfg)

	req, _ := http.NewRequest("GET", "/mybucket/mykey", nil)
	req.Host = "s3.amazonaws.com"
	query := url.Values{}
	query.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	query.Set("X-Amz-Credential", "test-key/20230101/us-east-1/s3/aws4_request")
	query.Set("X-Amz-Date", time.Now().Format("20060102T150405Z"))
	query.Set("X-Amz-Expires", "3600")
	query.Set("X-Amz-SignedHeaders", "host")
	query.Set("X-Amz-Signature", "abc123")
	req.URL.RawQuery = query.Encode()

	_, _, err := auth.VerifyPresignedURL(req)
	if err == nil {
		t.Error("Expected error with signature mismatch")
	}
}

func TestAuthorize_SigV2(t *testing.T) {
	cfg := config.AuthConfig{
		AccessKey: "test-key",
		SecretKey: "test-secret",
	}
	auth := New(cfg)

	req, _ := http.NewRequest("GET", "/test", nil)
	req.Header.Set("Authorization", "AWS test-key:invalid-signature")
	req.Header.Set("Date", time.Now().Format(http.TimeFormat))

	err := auth.Authorize(req, "bucket", "GetObject")
	if err == nil {
		t.Error("Expected error with invalid SigV2 signature")
	}
}

func TestAuthorize_SigV4(t *testing.T) {
	cfg := config.AuthConfig{
		AccessKey: "test-key",
		SecretKey: "test-secret",
	}
	auth := New(cfg)

	req, _ := http.NewRequest("GET", "/test", nil)
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test-key/20230101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=invalid")
	req.Header.Set("X-Amz-Date", "20230101T000000Z")

	err := auth.Authorize(req, "bucket", "GetObject")
	if err == nil {
		t.Error("Expected error with invalid SigV4 signature")
	}
}

func TestVerifySigV4_NoSpace(t *testing.T) {
	cfg := config.AuthConfig{
		AccessKey: "test-key",
		SecretKey: "test-secret",
	}
	auth := New(cfg)

	req, _ := http.NewRequest("GET", "/test", nil)
	err := auth.verifySigV4(req, "AWS4-HMAC-SHA256")
	if err == nil {
		t.Error("Expected error with authorization header without space")
	}
}

func TestVerifySigV4_CredentialTooFewParts(t *testing.T) {
	cfg := config.AuthConfig{
		AccessKey: "test-key",
		SecretKey: "test-secret",
	}
	auth := New(cfg)

	req, _ := http.NewRequest("GET", "/test", nil)
	err := auth.verifySigV4(req, "AWS4-HMAC-SHA256 test-key/20230101")
	if err == nil {
		t.Error("Expected error with credential having too few parts")
	}
}

func TestVerifySigV2_NoSpace(t *testing.T) {
	cfg := config.AuthConfig{
		AccessKey: "test-key",
		SecretKey: "test-secret",
	}
	auth := New(cfg)

	req, _ := http.NewRequest("GET", "/test", nil)
	err := auth.verifySigV2(req, "AWS")
	if err == nil {
		t.Error("Expected error with authorization header without space")
	}
}

func TestBuildCanonicalRequest_EmptyPath(t *testing.T) {
	cfg := config.AuthConfig{
		AccessKey: "test-key",
		SecretKey: "test-secret",
	}
	auth := New(cfg)

	req, _ := http.NewRequest("GET", "", nil)
	req.URL.Path = ""
	req.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")

	canonical := auth.buildCanonicalRequest(req, "host")
	if !strings.Contains(canonical, "/") {
		t.Error("Canonical request should contain / for empty path")
	}
}

func TestBuildStringToSignV2_NoDateHeader(t *testing.T) {
	cfg := config.AuthConfig{
		AccessKey: "test-key",
		SecretKey: "test-secret",
	}
	auth := New(cfg)

	req, _ := http.NewRequest("GET", "/bucket/key", nil)
	req.Header.Set("x-amz-date", "20130524T000000Z")
	req.Header.Del("Date")

	stringToSign := auth.buildStringToSignV2(req)
	if stringToSign == "" {
		t.Error("String to sign should not be empty")
	}
}

func TestVerifyPresignedURL_DefaultPathStyle(t *testing.T) {
	cfg := config.AuthConfig{
		AccessKey: "test-key",
		SecretKey: "test-secret",
	}
	auth := New(cfg)

	req, _ := http.NewRequest("GET", "/mybucket/mykey", nil)
	req.Host = "custom-host.com"
	query := url.Values{}
	query.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	query.Set("X-Amz-Credential", "test-key/20230101/us-east-1/s3/aws4_request")
	query.Set("X-Amz-Date", time.Now().Format("20060102T150405Z"))
	query.Set("X-Amz-Expires", "3600")
	query.Set("X-Amz-SignedHeaders", "host")
	query.Set("X-Amz-Signature", "abc123")
	req.URL.RawQuery = query.Encode()

	_, _, err := auth.VerifyPresignedURL(req)
	if err == nil {
		t.Error("Expected error with signature mismatch")
	}
}

func TestVerifyPresignedURL_PathStyleShortPath(t *testing.T) {
	cfg := config.AuthConfig{
		AccessKey: "test-key",
		SecretKey: "test-secret",
	}
	auth := New(cfg)

	req, _ := http.NewRequest("GET", "/bucketonly", nil)
	req.Host = "s3.amazonaws.com"
	query := url.Values{}
	query.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	query.Set("X-Amz-Credential", "test-key/20230101/us-east-1/s3/aws4_request")
	query.Set("X-Amz-Date", time.Now().Format("20060102T150405Z"))
	query.Set("X-Amz-Expires", "3600")
	query.Set("X-Amz-SignedHeaders", "host")
	query.Set("X-Amz-Signature", "abc123")
	req.URL.RawQuery = query.Encode()

	_, _, err := auth.VerifyPresignedURL(req)
	if err == nil || !strings.Contains(err.Error(), "invalid URL format") {
		t.Errorf("Expected invalid URL format error, got: %v", err)
	}
}

func TestVerifyPresignedURL_DefaultPathStyleShortPath(t *testing.T) {
	cfg := config.AuthConfig{
		AccessKey: "test-key",
		SecretKey: "test-secret",
	}
	auth := New(cfg)

	req, _ := http.NewRequest("GET", "/bucketonly", nil)
	req.Host = "custom-host.com"
	query := url.Values{}
	query.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	query.Set("X-Amz-Credential", "test-key/20230101/us-east-1/s3/aws4_request")
	query.Set("X-Amz-Date", time.Now().Format("20060102T150405Z"))
	query.Set("X-Amz-Expires", "3600")
	query.Set("X-Amz-SignedHeaders", "host")
	query.Set("X-Amz-Signature", "abc123")
	req.URL.RawQuery = query.Encode()

	_, _, err := auth.VerifyPresignedURL(req)
	if err == nil || !strings.Contains(err.Error(), "invalid URL format") {
		t.Errorf("Expected invalid URL format error, got: %v", err)
	}
}

func TestVerifyPresignedURL_URLEncodedKey(t *testing.T) {
	cfg := config.AuthConfig{
		AccessKey: "test-key",
		SecretKey: "test-secret",
	}
	auth := New(cfg)

	req, _ := http.NewRequest("GET", "/mybucket/my%20key", nil)
	req.Host = "s3.amazonaws.com"
	query := url.Values{}
	query.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	query.Set("X-Amz-Credential", "test-key/20230101/us-east-1/s3/aws4_request")
	query.Set("X-Amz-Date", time.Now().Format("20060102T150405Z"))
	query.Set("X-Amz-Expires", "3600")
	query.Set("X-Amz-SignedHeaders", "host")
	query.Set("X-Amz-Signature", "abc123")
	req.URL.RawQuery = query.Encode()

	_, _, err := auth.VerifyPresignedURL(req)
	if err == nil {
		t.Error("Expected error with signature mismatch")
	}
}

func TestBuildCanonicalRequest_NoPayloadHash(t *testing.T) {
	cfg := config.AuthConfig{
		AccessKey: "test-key",
		SecretKey: "test-secret",
	}
	auth := New(cfg)

	req, _ := http.NewRequest("GET", "/test-bucket/test-key", nil)
	req.Header.Del("X-Amz-Content-Sha256")

	canonical := auth.buildCanonicalRequest(req, "host")
	if !strings.Contains(canonical, "UNSIGNED-PAYLOAD") {
		t.Error("Canonical request should contain UNSIGNED-PAYLOAD when no hash provided")
	}
}

func TestBuildStringToSign_DateFallback(t *testing.T) {
	cfg := config.AuthConfig{
		AccessKey: "test-key",
		SecretKey: "test-secret",
	}
	auth := New(cfg)

	req, _ := http.NewRequest("GET", "/test", nil)
	req.Header.Del("X-Amz-Date")
	req.Header.Set("Date", "Tue, 21 Feb 2023 00:00:00 GMT")

	stringToSign := auth.buildStringToSign(req, "canonical-request", "20230221", "us-east-1", "s3")
	if stringToSign == "" {
		t.Error("String to sign should not be empty")
	}
}

func TestVerifySigV4_CompleteFlow(t *testing.T) {
	cfg := config.AuthConfig{
		AccessKey: "AKIAIOSFODNN7EXAMPLE",
		SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
	}
	auth := New(cfg)

	req, _ := http.NewRequest("GET", "/test-bucket/test-key", nil)
	req.Host = "example.s3.amazonaws.com"
	req.Header.Set("Host", "example.s3.amazonaws.com")
	req.Header.Set("X-Amz-Date", "20130524T000000Z")
	req.Header.Set("X-Amz-SignedHeaders", "host")
	req.Header.Set("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")

	dateStamp := "20130524"
	region := "us-east-1"
	service := "s3"

	canonicalReq := auth.buildCanonicalRequest(req, "host")
	stringToSign := auth.buildStringToSign(req, canonicalReq, dateStamp, region, service)
	signature := auth.calculateSignature(cfg.SecretKey, dateStamp, region, service, stringToSign)

	credentialStr := fmt.Sprintf("%s/%s/%s/%s/aws4_request", cfg.AccessKey, dateStamp, region, service)
	authHeader := fmt.Sprintf("AWS4-HMAC-SHA256 %s=%s", credentialStr, signature)

	err := auth.verifySigV4(req, authHeader)
	if err != nil {
		t.Errorf("Expected no error with valid signature, got: %v", err)
	}
}

func TestVerifySigV4_DateFallback(t *testing.T) {
	cfg := config.AuthConfig{
		AccessKey: "AKIAIOSFODNN7EXAMPLE",
		SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
	}
	auth := New(cfg)

	req, _ := http.NewRequest("GET", "/test-bucket/test-key", nil)
	req.Host = "example.s3.amazonaws.com"
	req.Header.Set("Host", "example.s3.amazonaws.com")
	req.Header.Set("Date", "Fri, 24 May 2013 00:00:00 GMT")
	req.Header.Del("X-Amz-Date")
	req.Header.Set("X-Amz-SignedHeaders", "host")
	req.Header.Set("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")

	dateStamp := "20130524"
	region := "us-east-1"
	service := "s3"

	canonicalReq := auth.buildCanonicalRequest(req, "host")
	stringToSign := auth.buildStringToSign(req, canonicalReq, dateStamp, region, service)
	signature := auth.calculateSignature(cfg.SecretKey, dateStamp, region, service, stringToSign)

	credentialStr := fmt.Sprintf("%s/%s/%s/%s/aws4_request", cfg.AccessKey, dateStamp, region, service)
	authHeader := fmt.Sprintf("AWS4-HMAC-SHA256 %s=%s", credentialStr, signature)

	err := auth.verifySigV4(req, authHeader)
	if err != nil {
		t.Errorf("Expected no error with valid signature, got: %v", err)
	}
}

func TestVerifySigV2_CompleteFlow(t *testing.T) {
	cfg := config.AuthConfig{
		AccessKey: "AKIAIOSFODNN7EXAMPLE",
		SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
	}
	auth := New(cfg)

	req, _ := http.NewRequest("GET", "/test-bucket/test-key", nil)
	req.Header.Set("Content-MD5", "")
	req.Header.Set("Content-Type", "")
	req.Header.Set("Date", "Fri, 24 May 2013 00:00:00 GMT")

	stringToSign := auth.buildStringToSignV2(req)
	signature := auth.calculateSignatureV2(cfg.SecretKey, stringToSign)

	authHeader := fmt.Sprintf("AWS %s:%s", cfg.AccessKey, signature)

	err := auth.verifySigV2(req, authHeader)
	if err != nil {
		t.Errorf("Expected no error with valid SigV2 signature, got: %v", err)
	}
}

func TestAuthorize_SigV4_Valid(t *testing.T) {
	cfg := config.AuthConfig{
		AccessKey: "AKIAIOSFODNN7EXAMPLE",
		SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
	}
	auth := New(cfg)

	req, _ := http.NewRequest("GET", "/test-bucket/test-key", nil)
	req.Host = "example.s3.amazonaws.com"
	req.Header.Set("Host", "example.s3.amazonaws.com")
	req.Header.Set("X-Amz-Date", "20130524T000000Z")
	req.Header.Set("X-Amz-SignedHeaders", "host")
	req.Header.Set("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")

	dateStamp := "20130524"
	region := "us-east-1"
	service := "s3"

	canonicalReq := auth.buildCanonicalRequest(req, "host")
	stringToSign := auth.buildStringToSign(req, canonicalReq, dateStamp, region, service)
	signature := auth.calculateSignature(cfg.SecretKey, dateStamp, region, service, stringToSign)

	credentialStr := fmt.Sprintf("%s/%s/%s/%s/aws4_request", cfg.AccessKey, dateStamp, region, service)
	authHeader := fmt.Sprintf("AWS4-HMAC-SHA256 %s=%s", credentialStr, signature)
	req.Header.Set("Authorization", authHeader)

	err := auth.Authorize(req, "test-bucket", "GetObject")
	if err != nil {
		t.Errorf("Expected no error with valid SigV4, got: %v", err)
	}
}

func TestAuthorize_SigV2_Valid(t *testing.T) {
	cfg := config.AuthConfig{
		AccessKey: "AKIAIOSFODNN7EXAMPLE",
		SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
	}
	auth := New(cfg)

	req, _ := http.NewRequest("GET", "/test-bucket/test-key", nil)
	req.Header.Set("Content-MD5", "")
	req.Header.Set("Content-Type", "")
	req.Header.Set("Date", "Fri, 24 May 2013 00:00:00 GMT")

	stringToSign := auth.buildStringToSignV2(req)
	signature := auth.calculateSignatureV2(cfg.SecretKey, stringToSign)

	authHeader := fmt.Sprintf("AWS %s:%s", cfg.AccessKey, signature)
	req.Header.Set("Authorization", authHeader)

	err := auth.Authorize(req, "test-bucket", "GetObject")
	if err != nil {
		t.Errorf("Expected no error with valid SigV2, got: %v", err)
	}
}

func TestVerifyPresignedURL_S3Prefix(t *testing.T) {
	cfg := config.AuthConfig{
		AccessKey: "test-key",
		SecretKey: "test-secret",
	}
	auth := New(cfg)

	req, _ := http.NewRequest("GET", "/mybucket/mykey", nil)
	req.Host = "s3.us-east-1.amazonaws.com"
	query := url.Values{}
	query.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	query.Set("X-Amz-Credential", "test-key/20230101/us-east-1/s3/aws4_request")
	query.Set("X-Amz-Date", time.Now().Format("20060102T150405Z"))
	query.Set("X-Amz-Expires", "3600")
	query.Set("X-Amz-SignedHeaders", "host")
	query.Set("X-Amz-Signature", "abc123")
	req.URL.RawQuery = query.Encode()

	_, _, err := auth.VerifyPresignedURL(req)
	if err == nil {
		t.Error("Expected error with signature mismatch")
	}
}

func TestVerifyPresignedURL_S3PrefixShortPath(t *testing.T) {
	cfg := config.AuthConfig{
		AccessKey: "test-key",
		SecretKey: "test-secret",
	}
	auth := New(cfg)

	req, _ := http.NewRequest("GET", "/bucketonly", nil)
	req.Host = "s3.us-east-1.amazonaws.com"
	query := url.Values{}
	query.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	query.Set("X-Amz-Credential", "test-key/20230101/us-east-1/s3/aws4_request")
	query.Set("X-Amz-Date", time.Now().Format("20060102T150405Z"))
	query.Set("X-Amz-Expires", "3600")
	query.Set("X-Amz-SignedHeaders", "host")
	query.Set("X-Amz-Signature", "abc123")
	req.URL.RawQuery = query.Encode()

	_, _, err := auth.VerifyPresignedURL(req)
	if err == nil || !strings.Contains(err.Error(), "invalid URL format") {
		t.Errorf("Expected invalid URL format error, got: %v", err)
	}
}

func TestVerifyPresignedURL_SignatureMismatch(t *testing.T) {
	cfg := config.AuthConfig{
		AccessKey: "AKIAIOSFODNN7EXAMPLE",
		SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
	}
	auth := New(cfg)

	now := time.Now().UTC()
	dateStamp := now.Format("20060102")
	amzDate := now.Format("20060102T150405Z")
	region := "us-east-1"
	service := "s3"

	tests := []struct {
		name        string
		host        string
		path        string
		expectError bool
	}{
		{"VirtualHostedStyle", "mybucket.s3.amazonaws.com", "/test-key", true},
		{"PathStyle", "s3.amazonaws.com", "/mybucket/test-key", true},
		{"DefaultPathStyle", "custom-host.com", "/mybucket/test-key", true},
		{"S3Prefix", "s3.us-east-1.amazonaws.com", "/mybucket/test-key", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, _ := http.NewRequest("GET", tt.path, nil)
			req.Host = tt.host
			req.Header.Set("Host", tt.host)
			req.Header.Set("X-Amz-Date", amzDate)
			req.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")

			query := url.Values{}
			query.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
			query.Set("X-Amz-Credential", fmt.Sprintf("%s/%s/%s/%s/aws4_request", cfg.AccessKey, dateStamp, region, service))
			query.Set("X-Amz-Date", amzDate)
			query.Set("X-Amz-Expires", "3600")
			query.Set("X-Amz-SignedHeaders", "host")
			query.Set("X-Amz-Signature", "invalidsignature")
			req.URL.RawQuery = query.Encode()

			_, _, err := auth.VerifyPresignedURL(req)
			if tt.expectError && err == nil {
				t.Error("Expected error with signature mismatch")
			}
		})
	}
}

func TestVerifyPresignedURL_InvalidURLEncoding(t *testing.T) {
	cfg := config.AuthConfig{
		AccessKey: "test-key",
		SecretKey: "test-secret",
	}
	auth := New(cfg)

	req, _ := http.NewRequest("GET", "/mybucket/test", nil)
	req.Host = "s3.amazonaws.com"

	query := url.Values{}
	query.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	query.Set("X-Amz-Credential", "test-key/20230101/us-east-1/s3/aws4_request")
	query.Set("X-Amz-Date", time.Now().Format("20060102T150405Z"))
	query.Set("X-Amz-Expires", "3600")
	query.Set("X-Amz-SignedHeaders", "host")
	query.Set("X-Amz-Signature", "abc123")
	req.URL.RawQuery = query.Encode()

	req.URL.Path = "/mybucket/%zz"
	req.URL.RawPath = "/mybucket/%zz"

	_, _, err := auth.VerifyPresignedURL(req)
	if err == nil || !strings.Contains(err.Error(), "failed to decode key") {
		t.Errorf("Expected 'failed to decode key' error, got: %v", err)
	}
}

func TestVerifySigV4_AllPaths(t *testing.T) {
	cfg := config.AuthConfig{
		AccessKey: "AKIAIOSFODNN7EXAMPLE",
		SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
	}
	auth := New(cfg)

	t.Run("SuccessWithXAmzDate", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/test-bucket/test-key", nil)
		req.Host = "example.s3.amazonaws.com"
		req.Header.Set("Host", "example.s3.amazonaws.com")
		req.Header.Set("X-Amz-Date", "20130524T000000Z")
		req.Header.Set("X-Amz-SignedHeaders", "host")
		req.Header.Set("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")

		dateStamp := "20130524"
		region := "us-east-1"
		service := "s3"

		canonicalReq := auth.buildCanonicalRequest(req, "host")
		stringToSign := auth.buildStringToSign(req, canonicalReq, dateStamp, region, service)
		signature := auth.calculateSignature(cfg.SecretKey, dateStamp, region, service, stringToSign)

		credentialStr := fmt.Sprintf("%s/%s/%s/%s/aws4_request", cfg.AccessKey, dateStamp, region, service)
		authHeader := fmt.Sprintf("AWS4-HMAC-SHA256 %s=%s", credentialStr, signature)

		err := auth.verifySigV4(req, authHeader)
		if err != nil {
			t.Errorf("Expected no error, got: %v", err)
		}
	})

	t.Run("SuccessWithDateFallback", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/test-bucket/test-key", nil)
		req.Host = "example.s3.amazonaws.com"
		req.Header.Set("Host", "example.s3.amazonaws.com")
		req.Header.Set("Date", "Fri, 24 May 2013 00:00:00 GMT")
		req.Header.Del("X-Amz-Date")
		req.Header.Set("X-Amz-SignedHeaders", "host")
		req.Header.Set("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")

		dateStamp := "20130524"
		region := "us-east-1"
		service := "s3"

		canonicalReq := auth.buildCanonicalRequest(req, "host")
		stringToSign := auth.buildStringToSign(req, canonicalReq, dateStamp, region, service)
		signature := auth.calculateSignature(cfg.SecretKey, dateStamp, region, service, stringToSign)

		credentialStr := fmt.Sprintf("%s/%s/%s/%s/aws4_request", cfg.AccessKey, dateStamp, region, service)
		authHeader := fmt.Sprintf("AWS4-HMAC-SHA256 %s=%s", credentialStr, signature)

		err := auth.verifySigV4(req, authHeader)
		if err != nil {
			t.Errorf("Expected no error, got: %v", err)
		}
	})
}
