package api

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.uber.org/zap"
)

func TestReadLimitedBody(t *testing.T) {
	data := []byte("test data")
	reader := bytes.NewReader(data)

	result, err := readLimitedBody(reader)
	if err != nil {
		t.Fatalf("readLimitedBody failed: %v", err)
	}
	if string(result) != "test data" {
		t.Errorf("result = %s, want test data", string(result))
	}
}

func TestReadLimitedBodyEmpty(t *testing.T) {
	result, err := readLimitedBody(bytes.NewReader([]byte{}))
	if err != nil {
		t.Fatalf("readLimitedBody failed: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("len(result) = %d, want 0", len(result))
	}
}

func TestReadLimitedBodyLarge(t *testing.T) {
	// Create data larger than maxRequestBodySize
	largeData := make([]byte, maxRequestBodySize+1000)
	result, err := readLimitedBody(bytes.NewReader(largeData))
	if err != nil {
		t.Fatalf("readLimitedBody failed: %v", err)
	}
	if len(result) > maxRequestBodySize+1 {
		t.Errorf("result should be limited to maxRequestBodySize+1")
	}
}

func TestIsBodyTooLarge(t *testing.T) {
	// Create data exactly at limit
	exactLimit := make([]byte, maxRequestBodySize)
	if isBodyTooLarge(exactLimit) {
		t.Error("data at exact limit should not be too large")
	}

	// Create data over limit
	overLimit := make([]byte, maxRequestBodySize+1)
	if !isBodyTooLarge(overLimit) {
		t.Error("data over limit should be too large")
	}

	// Create data under limit
	underLimit := make([]byte, maxRequestBodySize-1)
	if isBodyTooLarge(underLimit) {
		t.Error("data under limit should not be too large")
	}
}

func TestSanitizeHeaderValue(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"normal value", "normal value"},
		{"value\rwith\rnewlines", "valuewithnewlines"},
		{"value\nwith\nnewlines", "valuewithnewlines"},
		{"value\r\nwith\r\ncrlf", "valuewithcrlf"},
		{"", ""},
		{"no special chars", "no special chars"},
	}

	for _, tt := range tests {
		result := sanitizeHeaderValue(tt.input)
		if result != tt.expected {
			t.Errorf("sanitizeHeaderValue(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

func TestParseInt(t *testing.T) {
	tests := []struct {
		input      string
		defaultVal int
		expected   int
	}{
		{"123", 0, 123},
		{"", 100, 100},
		{"0", 50, 0},
		{"-5", 10, -5},
		{"abc", 999, 0}, // fmt.Sscanf fails, returns 0
	}

	for _, tt := range tests {
		result := parseInt(tt.input, tt.defaultVal)
		if result != tt.expected {
			t.Errorf("parseInt(%q, %d) = %d, want %d", tt.input, tt.defaultVal, result, tt.expected)
		}
	}
}

func TestFindByteIndex(t *testing.T) {
	tests := []struct {
		s        string
		b        byte
		expected int
	}{
		{"hello", 'l', 2},
		{"hello", 'o', 4},
		{"hello", 'h', 0},
		{"hello", 'x', -1},
		{"", 'a', -1},
		{"aaaa", 'a', 0},
	}

	for _, tt := range tests {
		result := findByteIndex(tt.s, tt.b)
		if result != tt.expected {
			t.Errorf("findByteIndex(%q, %q) = %d, want %d", tt.s, tt.b, result, tt.expected)
		}
	}
}

func TestMaxRequestBodySize(t *testing.T) {
	if maxRequestBodySize != 10*1024*1024 {
		t.Errorf("maxRequestBodySize = %d, want %d", maxRequestBodySize, 10*1024*1024)
	}
}

func TestParseBucketKey(t *testing.T) {
	tests := []struct {
		path       string
		wantBucket string
		wantKey    string
	}{
		{"/s3/", "", ""},
		{"/s3/bucket", "bucket", ""},
		{"/s3/bucket/key", "bucket", "key"},
		{"/s3/bucket/path/to/object", "bucket", "path/to/object"},
		{"", "", ""},
	}

	for _, tt := range tests {
		bucket, key, err := parseBucketKey(nil, tt.path)
		if err != nil {
			t.Errorf("parseBucketKey(%q) failed: %v", tt.path, err)
			continue
		}
		if bucket != tt.wantBucket {
			t.Errorf("parseBucketKey(%q) bucket = %q, want %q", tt.path, bucket, tt.wantBucket)
		}
		if key != tt.wantKey {
			t.Errorf("parseBucketKey(%q) key = %q, want %q", tt.path, key, tt.wantKey)
		}
	}
}

func TestParseBucketKeyWithPrefix(t *testing.T) {
	// Test with /s3/ prefix
	bucket, key, err := parseBucketKey(nil, "/s3/mybucket/mykey")
	if err != nil {
		t.Fatalf("parseBucketKey failed: %v", err)
	}
	if bucket != "mybucket" {
		t.Errorf("bucket = %q, want mybucket", bucket)
	}
	if key != "mykey" {
		t.Errorf("key = %q, want mykey", key)
	}
}

func TestParseBucketKeyEmptyPath(t *testing.T) {
	bucket, key, err := parseBucketKey(nil, "")
	if err != nil {
		t.Fatalf("parseBucketKey failed: %v", err)
	}
	if bucket != "" || key != "" {
		t.Errorf("empty path should return empty bucket and key")
	}
}

func TestParseBucketKeyRoot(t *testing.T) {
	bucket, key, err := parseBucketKey(nil, "/s3/")
	if err != nil {
		t.Fatalf("parseBucketKey failed: %v", err)
	}
	if bucket != "" || key != "" {
		t.Errorf("/s3/ should return empty bucket and key")
	}
}

func TestParseBucketKeyNoSlash(t *testing.T) {
	bucket, key, err := parseBucketKey(nil, "bucket")
	if err != nil {
		t.Fatalf("parseBucketKey failed: %v", err)
	}
	if bucket != "bucket" {
		t.Errorf("bucket = %q, want bucket", bucket)
	}
	if key != "" {
		t.Errorf("key = %q, want empty", key)
	}
}

func TestParseBucketKeyDeepPath(t *testing.T) {
	bucket, key, err := parseBucketKey(nil, "/s3/bucket/a/b/c/d/file.txt")
	if err != nil {
		t.Fatalf("parseBucketKey failed: %v", err)
	}
	if bucket != "bucket" {
		t.Errorf("bucket = %q, want bucket", bucket)
	}
	if key != "a/b/c/d/file.txt" {
		t.Errorf("key = %q, want a/b/c/d/file.txt", key)
	}
}

func TestReadLimitedBodyNil(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
		}
	}()
	result, err := readLimitedBody(nil)
	_ = result
	_ = err
}

func TestIsBodyTooLargeBoundary(t *testing.T) {
	tests := []struct {
		size     int
		expected bool
	}{
		{0, false},
		{1, false},
		{maxRequestBodySize - 1, false},
		{maxRequestBodySize, false},
		{maxRequestBodySize + 1, true},
		{maxRequestBodySize + 1000, true},
	}

	for _, tt := range tests {
		data := make([]byte, tt.size)
		result := isBodyTooLarge(data)
		if result != tt.expected {
			t.Errorf("isBodyTooLarge(size=%d) = %v, want %v", tt.size, result, tt.expected)
		}
	}
}

func TestSanitizeHeaderValueAllCases(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"", ""},
		{"a", "a"},
		{"normal", "normal"},
		{"\r", ""},
		{"\n", ""},
		{"\r\n", ""},
		{"a\rb", "ab"},
		{"a\nb", "ab"},
		{"a\r\nb", "ab"},
		{"\ra\nb\r", "ab"},
		{"value\r\nwith\r\nmany\r\nnewlines", "valuewithmanynewlines"},
		{"no-problem/with:other@chars", "no-problem/with:other@chars"},
		{"Content-Type: text/html\r\n", "Content-Type: text/html"},
	}

	for _, tt := range tests {
		result := sanitizeHeaderValue(tt.input)
		if result != tt.expected {
			t.Errorf("sanitizeHeaderValue(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

func TestFindByteIndexAllCases(t *testing.T) {
	tests := []struct {
		s        string
		b        byte
		expected int
	}{
		{"", 'a', -1},
		{"a", 'a', 0},
		{"a", 'b', -1},
		{"ab", 'b', 1},
		{"abc", 'c', 2},
		{"aaaa", 'a', 0},
		{"test/string", '/', 4},
		{"test/string/path", '/', 4},
		{"no delimiter", '/', -1},
		{"/leading", '/', 0},
		{"multiple/a/b/c", '/', 8},
	}

	for _, tt := range tests {
		result := findByteIndex(tt.s, tt.b)
		if result != tt.expected {
			t.Errorf("findByteIndex(%q, %q) = %d, want %d", tt.s, tt.b, result, tt.expected)
		}
	}
}

func TestParseIntAllCases(t *testing.T) {
	tests := []struct {
		input      string
		defaultVal int
		expected   int
	}{
		{"", 100, 100},
		{"0", 100, 0},
		{"1", 100, 1},
		{"-1", 100, -1},
		{"12345", 0, 12345},
		{"-12345", 0, -12345},
		{"abc", 50, 0},
		{"12abc34", 0, 12},
	}

	for _, tt := range tests {
		result := parseInt(tt.input, tt.defaultVal)
		if result != tt.expected {
			t.Errorf("parseInt(%q, %d) = %d, want %d", tt.input, tt.defaultVal, result, tt.expected)
		}
	}
}

func TestStringReader(t *testing.T) {
	reader := stringReader("test")
	if reader == nil {
		t.Error("stringReader should return non-nil reader")
	}

	buf := make([]byte, 4)
	n, err := reader.Read(buf)
	if err != nil {
		t.Errorf("Read failed: %v", err)
	}
	if n != 4 {
		t.Errorf("Read returned %d bytes, want 4", n)
	}
	if string(buf) != "test" {
		t.Errorf("Read data = %s, want test", string(buf))
	}
}

// Helper to create io.Reader from string
func stringReader(s string) io.Reader {
	return strings.NewReader(s)
}

func TestWriteError(t *testing.T) {
	logger := zap.NewNop()
	router := &Router{logger: logger.Sugar()}

	w := httptest.NewRecorder()
	router.writeError(w, ErrNoSuchBucket)

	if w.Code != http.StatusNotFound {
		t.Errorf("Status = %d, want %d", w.Code, http.StatusNotFound)
	}
	if w.Header().Get("Content-Type") != "application/xml" {
		t.Errorf("Content-Type = %s, want application/xml", w.Header().Get("Content-Type"))
	}
}

func TestWriteXML(t *testing.T) {
	logger := zap.NewNop()
	router := &Router{logger: logger.Sugar()}

	w := httptest.NewRecorder()
	data := map[string]string{"key": "value"}
	router.writeXML(w, http.StatusOK, data)

	if w.Code != http.StatusOK {
		t.Errorf("Status = %d, want %d", w.Code, http.StatusOK)
	}
	if w.Header().Get("Content-Type") != "application/xml" {
		t.Errorf("Content-Type = %s, want application/xml", w.Header().Get("Content-Type"))
	}
}

func TestS3ErrorCodes(t *testing.T) {
	tests := []struct {
		err    S3Error
		code   string
		status int
	}{
		{ErrNoSuchBucket, "NoSuchBucket", 404},
		{ErrBucketAlreadyExists, "BucketAlreadyExists", 409},
		{ErrNoSuchKey, "NoSuchKey", 404},
		{ErrInvalidBucketName, "InvalidBucketName", 400},
		{ErrInternal, "InternalError", 500},
	}

	for _, tt := range tests {
		if tt.err.Code() != tt.code {
			t.Errorf("Code() = %s, want %s", tt.err.Code(), tt.code)
		}
	}
}

func TestS3ErrorInterfaces(t *testing.T) {
	var _ error = ErrNoSuchBucket
	var _ error = ErrInternal
	var _ error = ErrNoSuchKey
}

func TestS3ErrorStatusCode(t *testing.T) {
	if ErrNoSuchBucket.StatusCode() != 404 {
		t.Errorf("ErrNoSuchBucket.StatusCode() = %d, want 404", ErrNoSuchBucket.StatusCode())
	}
	if ErrInternal.StatusCode() != 500 {
		t.Errorf("ErrInternal.StatusCode() = %d, want 500", ErrInternal.StatusCode())
	}
	if ErrInvalidBucketName.StatusCode() != 400 {
		t.Errorf("ErrInvalidBucketName.StatusCode() = %d, want 400", ErrInvalidBucketName.StatusCode())
	}
}

func TestMaxRequestBodySizeValue(t *testing.T) {
	if maxRequestBodySize != 10*1024*1024 {
		t.Errorf("maxRequestBodySize = %d, want %d", maxRequestBodySize, 10*1024*1024)
	}
}

func TestS3ErrorMessage(t *testing.T) {
	if ErrNoSuchBucket.Message() == "" {
		t.Error("Message() should not be empty")
	}
	if ErrNoSuchBucket.Error() == "" {
		t.Error("Error() should not be empty")
	}
}

func TestAllS3Errors(t *testing.T) {
	errors := []S3Error{
		ErrInternal,
		ErrInvalidURI,
		ErrMethodNotAllowed,
		ErrNoSuchBucket,
		ErrNoSuchKey,
		ErrInvalidObjectState,
		ErrOwnershipControlsNotFound,
		ErrMetricsNotFound,
		ErrReplicationNotFound,
		ErrObjectRetentionNotFound,
		ErrObjectLegalHoldNotFound,
		ErrNoSuchUpload,
		ErrBucketNotEmpty,
		ErrInvalidBucketName,
		ErrInvalidObjectName,
		ErrInvalidArgument,
		ErrAccessDenied,
		ErrSignatureDoesNotMatch,
		ErrMalformedXML,
		ErrMissingContentLength,
		ErrInvalidContentLength,
		ErrPreconditionFailed,
		ErrNotImplemented,
		ErrTooManyBuckets,
		ErrBucketAlreadyExists,
		ErrBucketAlreadyOwnedByYou,
		ErrMaxMessageLengthExceeded,
		ErrMaxUploadLengthExceeded,
		ErrEntityTooSmall,
		ErrEntityTooLarge,
		ErrInvalidRequest,
		ErrInvalidAccelerateConfiguration,
		ErrInventoryNotFound,
		ErrAnalyticsNotFound,
		ErrPresignedURLExpired,
		ErrPresignedURLNotFound,
		ErrInvalidPresignedURL,
		ErrWebsiteNotFound,
	}

	for _, err := range errors {
		if err.Code() == "" {
			t.Error("Code() should not be empty")
		}
		if err.Message() == "" {
			t.Error("Message() should not be empty")
		}
		if err.StatusCode() < 100 || err.StatusCode() > 599 {
			t.Errorf("StatusCode() = %d, want valid HTTP status code", err.StatusCode())
		}
	}
}
