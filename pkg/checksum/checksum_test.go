package checksum

import (
	"bytes"
	"hash"
	"testing"
)

func TestMD5(t *testing.T) {
	h := MD5()
	if h == nil {
		t.Fatal("MD5 hash should not be nil")
	}

	data := []byte("test data")
	n, err := h.Write(data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if n != len(data) {
		t.Errorf("Written bytes = %d, want %d", n, len(data))
	}

	sum := h.Sum(nil)
	if len(sum) != 16 {
		t.Errorf("MD5 sum length = %d, want 16", len(sum))
	}
}

func TestSHA1(t *testing.T) {
	h := SHA1()
	if h == nil {
		t.Fatal("SHA1 hash should not be nil")
	}

	data := []byte("test data")
	h.Write(data)

	sum := h.Sum(nil)
	if len(sum) != 20 {
		t.Errorf("SHA1 sum length = %d, want 20", len(sum))
	}
}

func TestSHA256(t *testing.T) {
	h := SHA256()
	if h == nil {
		t.Fatal("SHA256 hash should not be nil")
	}

	data := []byte("test data")
	h.Write(data)

	sum := h.Sum(nil)
	if len(sum) != 32 {
		t.Errorf("SHA256 sum length = %d, want 32", len(sum))
	}
}

func TestSHA512(t *testing.T) {
	h := SHA512()
	if h == nil {
		t.Fatal("SHA512 hash should not be nil")
	}

	data := []byte("test data")
	h.Write(data)

	sum := h.Sum(nil)
	if len(sum) != 64 {
		t.Errorf("SHA512 sum length = %d, want 64", len(sum))
	}
}

func TestCRC32(t *testing.T) {
	h := CRC32()
	if h == nil {
		t.Fatal("CRC32 hash should not be nil")
	}

	data := []byte("test data")
	h.Write(data)

	sum := h.Sum(nil)
	if len(sum) != 4 {
		t.Errorf("CRC32 sum length = %d, want 4", len(sum))
	}
}

func TestHashBytes(t *testing.T) {
	data := []byte("test data for hashing")

	tests := []struct {
		algorithm string
		wantLen   int
	}{
		{"md5", 32},      // hex encoded = 16 bytes * 2
		{"sha1", 40},     // 20 * 2
		{"sha256", 64},   // 32 * 2
		{"sha512", 128},  // 64 * 2
		{"crc32", 8},     // 4 * 2
		{"unknown", 64},  // defaults to sha256
	}

	for _, tt := range tests {
		result, err := HashBytes(data, tt.algorithm)
		if err != nil {
			t.Errorf("HashBytes(%s) error: %v", tt.algorithm, err)
			continue
		}

		if len(result) != tt.wantLen {
			t.Errorf("HashBytes(%s) length = %d, want %d", tt.algorithm, len(result), tt.wantLen)
		}
	}
}

func TestHashBytes_Consistency(t *testing.T) {
	data := []byte("same data")

	result1, _ := HashBytes(data, "sha256")
	result2, _ := HashBytes(data, "sha256")

	if result1 != result2 {
		t.Error("Same data should produce same hash")
	}
}

func TestHashBytes_DifferentData(t *testing.T) {
	data1 := []byte("data1")
	data2 := []byte("data2")

	result1, _ := HashBytes(data1, "sha256")
	result2, _ := HashBytes(data2, "sha256")

	if result1 == result2 {
		t.Error("Different data should produce different hashes")
	}
}

func TestHashReader(t *testing.T) {
	data := []byte("test data for reader")
	reader := bytes.NewReader(data)

	hash, size, err := HashReader(reader)
	if err != nil {
		t.Fatalf("HashReader failed: %v", err)
	}

	if size != int64(len(data)) {
		t.Errorf("Size = %d, want %d", size, len(data))
	}

	if len(hash) != 64 { // SHA256 hex encoded
		t.Errorf("Hash length = %d, want 64", len(hash))
	}
}

func TestHashReader_Empty(t *testing.T) {
	reader := bytes.NewReader([]byte{})

	hash, size, err := HashReader(reader)
	if err != nil {
		t.Fatalf("HashReader failed: %v", err)
	}

	if size != 0 {
		t.Errorf("Size = %d, want 0", size)
	}

	// Empty data should still produce a valid hash
	if len(hash) != 64 {
		t.Errorf("Hash length = %d, want 64", len(hash))
	}
}

func TestHashToBase64(t *testing.T) {
	data := []byte("test data")

	tests := []struct {
		algorithm string
	}{
		{"md5"},
		{"sha1"},
		{"sha256"},
		{"sha512"},
		{"crc32"},
		{"unknown"}, // defaults to sha256
	}

	for _, tt := range tests {
		result, err := HashToBase64(data, tt.algorithm)
		if err != nil {
			t.Errorf("HashToBase64(%s) error: %v", tt.algorithm, err)
			continue
		}

		if len(result) == 0 {
			t.Errorf("HashToBase64(%s) returned empty string", tt.algorithm)
		}

		// Verify it's valid base64
		// base64.StdEncoding.DecodeString(result) would validate
	}
}

func TestETag(t *testing.T) {
	hash := "abc123def456"

	etag := ETag(hash)

	expected := "\"abc123def456\""
	if etag != expected {
		t.Errorf("ETag = %s, want %s", etag, expected)
	}
}

func TestETag_Empty(t *testing.T) {
	etag := ETag("")
	expected := "\"\""
	if etag != expected {
		t.Errorf("ETag('') = %s, want %s", etag, expected)
	}
}

func TestVerifyETag(t *testing.T) {
	hash := "abc123"
	etag := ETag(hash)

	tests := []struct {
		etag      string
		hash      string
		expected  bool
	}{
		{etag, hash, true},                    // Exact match
		{"abc123", hash, true},                // Without quotes
		{"\"abc123\"", hash, true},            // With quotes
		{"W/\"abc123\"", hash, true},          // Weak ETag
		{"\"different\"", hash, false},        // Different hash
		{"", hash, false},                     // Empty etag
		{etag, "different", false},            // Different hash
	}

	for _, tt := range tests {
		result := VerifyETag(tt.etag, tt.hash)
		if result != tt.expected {
			t.Errorf("VerifyETag(%s, %s) = %v, want %v", tt.etag, tt.hash, result, tt.expected)
		}
	}
}

func TestVerifyETag_EdgeCases(t *testing.T) {
	// Test with malformed weak ETag
	if VerifyETag("W/", "hash") {
		t.Error("Malformed weak ETag should not match")
	}

	// Test with single quote
	if VerifyETag("\"hash", "hash") {
		t.Error("Malformed ETag should not match")
	}
}

func TestCRC32Checksum(t *testing.T) {
	tests := []struct {
		data     []byte
		expected uint32
	}{
		{[]byte("hello world"), 0xd4a1185},
		{[]byte{}, 0x0},
		{[]byte("test"), 0xd39f6367},
	}

	for _, tt := range tests {
		result := CRC32Checksum(tt.data)
		if result != tt.expected {
			t.Errorf("CRC32Checksum(%s) = %x, want %x", string(tt.data), result, tt.expected)
		}
	}
}

func TestCRC32HashString(t *testing.T) {
	data := []byte("test data")

	result := CRC32HashString(data)

	// Should be 8 hex characters
	if len(result) != 8 {
		t.Errorf("CRC32HashString length = %d, want 8", len(result))
	}

	// Should be consistent
	result2 := CRC32HashString(data)
	if result != result2 {
		t.Error("CRC32HashString should be consistent")
	}
}

func TestHashConsistency(t *testing.T) {
	data := []byte("consistency test data")

	// Test that multiple algorithms produce consistent results
	md5Hash1, _ := HashBytes(data, "md5")
	md5Hash2, _ := HashBytes(data, "md5")

	sha256Hash1, _ := HashBytes(data, "sha256")
	sha256Hash2, _ := HashBytes(data, "sha256")

	if md5Hash1 != md5Hash2 {
		t.Error("MD5 hashes should be consistent")
	}

	if sha256Hash1 != sha256Hash2 {
		t.Error("SHA256 hashes should be consistent")
	}
}

func TestHashAlgorithmNames(t *testing.T) {
	// Test various algorithm name formats
	tests := []struct {
		name      string
		algorithm string
	}{
		{"sha256", "sha256"},
		{"sha-256", "sha-256"},
		{"sha1", "sha1"},
		{"sha-1", "sha-1"},
		{"sha512", "sha512"},
		{"sha-512", "sha-512"},
	}

	for _, tt := range tests {
		_, err := HashBytes([]byte("test"), tt.algorithm)
		if err != nil {
			t.Errorf("Algorithm %s should be supported", tt.name)
		}
	}
}

func BenchmarkMD5(b *testing.B) {
	data := make([]byte, 1024)
	for i := 0; i < b.N; i++ {
		h := MD5()
		h.Write(data)
		h.Sum(nil)
	}
}

func BenchmarkSHA256(b *testing.B) {
	data := make([]byte, 1024)
	for i := 0; i < b.N; i++ {
		h := SHA256()
		h.Write(data)
		h.Sum(nil)
	}
}

func BenchmarkSHA512(b *testing.B) {
	data := make([]byte, 1024)
	for i := 0; i < b.N; i++ {
		h := SHA512()
		h.Write(data)
		h.Sum(nil)
	}
}

func BenchmarkCRC32(b *testing.B) {
	data := make([]byte, 1024)
	for i := 0; i < b.N; i++ {
		h := CRC32()
		h.Write(data)
		h.Sum(nil)
	}
}

func BenchmarkHashBytes(b *testing.B) {
	data := make([]byte, 1024)
	for i := 0; i < b.N; i++ {
		HashBytes(data, "sha256")
	}
}

func BenchmarkHashReader(b *testing.B) {
	data := make([]byte, 1024)
	for i := 0; i < b.N; i++ {
		HashReader(bytes.NewReader(data))
	}
}

// Ensure hash.Hash interface is implemented
var _ hash.Hash = MD5()
var _ hash.Hash = SHA1()
var _ hash.Hash = SHA256()
var _ hash.Hash = SHA512()
var _ hash.Hash = CRC32()
