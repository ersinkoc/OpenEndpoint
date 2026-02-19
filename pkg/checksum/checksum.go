package checksum

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"hash"
	"io"
)

// MD5 creates an MD5 hash
func MD5() hash.Hash {
	return md5.New()
}

// SHA1 creates a SHA1 hash
func SHA1() hash.Hash {
	return sha1.New()
}

// SHA256 creates a SHA256 hash
func SHA256() hash.Hash {
	return sha256.New()
}

// SHA512 creates a SHA512 hash
func SHA512() hash.Hash {
	return sha512.New()
}

// HashBytes calculates hash of data using the specified algorithm
func HashBytes(data []byte, algorithm string) (string, error) {
	var h hash.Hash
	switch algorithm {
	case "md5":
		h = md5.New()
	case "sha1", "sha-1":
		h = sha1.New()
	case "sha256", "sha-256":
		h = sha256.New()
	case "sha512", "sha-512":
		h = sha512.New()
	default:
		h = sha256.New()
	}
	h.Write(data)
	return hex.EncodeToString(h.Sum(nil)), nil
}

// HashReader calculates hash of a reader using SHA256
func HashReader(r io.Reader) (string, int64, error) {
	h := sha256.New()
	size, err := io.Copy(h, r)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), size, nil
}

// HashToBase64 calculates hash and returns base64 encoded
func HashToBase64(data []byte, algorithm string) (string, error) {
	var h hash.Hash
	switch algorithm {
	case "md5":
		h = md5.New()
	case "sha1", "sha-1":
		h = sha1.New()
	case "sha256", "sha-256":
		h = sha256.New()
	case "sha512", "sha-512":
		h = sha512.New()
	default:
		h = sha256.New()
	}
	h.Write(data)
	return base64.StdEncoding.EncodeToString(h.Sum(nil)), nil
}

// ETag generates an ETag from content hash
func ETag(hash string) string {
	return "\"" + hash + "\""
}

// VerifyETag compares an ETag with a computed hash
func VerifyETag(etag, computedHash string) bool {
	// Handle weak ETags (prefixed with "W/")
	if len(etag) > 2 && etag[:2] == "W/" {
		etag = etag[2:]
	}
	// Remove quotes if present
	if len(etag) >= 2 && etag[0] == '"' && etag[len(etag)-1] == '"' {
		etag = etag[1 : len(etag)-1]
	}
	return etag == computedHash
}

// CRC32 is not implemented - using xxhash or similar would require external package
// For now, use SHA256 as a fallback
func CRC32(data []byte) uint32 {
	// Simple implementation would require external package
	// Return hash of first 4 bytes as placeholder
	h := sha256.Sum256(data)
	return uint32(h[0]) | uint32(h[1])<<8 | uint32(h[2])<<16 | uint32(h[3])<<24
}
