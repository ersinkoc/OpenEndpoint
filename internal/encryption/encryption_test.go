package encryption

import (
	"bytes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"testing"
)

func TestNewKeyManager(t *testing.T) {
	km := NewKeyManager()
	if km == nil {
		t.Fatal("KeyManager should not be nil")
	}
	if km.keys == nil {
		t.Error("keys map should be initialized")
	}
}

func TestKeyManagerGenerateKey(t *testing.T) {
	km := NewKeyManager()

	key, err := km.GenerateKey("test-key")
	if err != nil {
		t.Fatalf("GenerateKey failed: %v", err)
	}

	if len(key) != 32 {
		t.Errorf("Key length = %d, want 32", len(key))
	}

	retrieved, ok := km.GetKey("test-key")
	if !ok {
		t.Fatal("Should retrieve generated key")
	}
	if !bytes.Equal(key, retrieved) {
		t.Error("Retrieved key does not match generated key")
	}
}

func TestKeyManagerGenerateKeyDifferent(t *testing.T) {
	km := NewKeyManager()

	key1, _ := km.GenerateKey("key1")
	key2, _ := km.GenerateKey("key2")

	if bytes.Equal(key1, key2) {
		t.Error("Different keys should have different values")
	}
}

func TestKeyManagerSetKey(t *testing.T) {
	km := NewKeyManager()

	key := make([]byte, 32)
	rand.Read(key)

	km.SetKey("key1", key)

	retrieved, ok := km.GetKey("key1")
	if !ok {
		t.Fatal("Should retrieve key")
	}
	if !bytes.Equal(key, retrieved) {
		t.Error("Retrieved key does not match original")
	}
}

func TestKeyManagerGetKeyNotFound(t *testing.T) {
	km := NewKeyManager()

	_, ok := km.GetKey("nonexistent")
	if ok {
		t.Error("GetKey should return false for nonexistent key")
	}
}

func TestKeyManagerDeleteKey(t *testing.T) {
	km := NewKeyManager()

	key := make([]byte, 32)
	rand.Read(key)

	km.SetKey("key1", key)
	km.DeleteKey("key1")

	_, ok := km.GetKey("key1")
	if ok {
		t.Error("Key should be deleted")
	}
}

func TestEncryptDecrypt(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)

	plaintext := []byte("test data for encryption")

	ciphertext, err := Encrypt(key, plaintext)
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}

	if len(ciphertext) <= len(plaintext) {
		t.Error("Ciphertext should be longer than plaintext (includes nonce)")
	}

	decrypted, err := Decrypt(key, ciphertext)
	if err != nil {
		t.Fatalf("Decrypt failed: %v", err)
	}

	if !bytes.Equal(plaintext, decrypted) {
		t.Error("Decrypted text does not match original")
	}
}

func TestEncryptDifferentEachTime(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)

	plaintext := []byte("test data")

	ciphertext1, _ := Encrypt(key, plaintext)
	ciphertext2, _ := Encrypt(key, plaintext)

	if bytes.Equal(ciphertext1, ciphertext2) {
		t.Error("Same plaintext should produce different ciphertext (random nonce)")
	}
}

func TestDecryptWrongKey(t *testing.T) {
	key1 := make([]byte, 32)
	key2 := make([]byte, 32)
	rand.Read(key1)
	rand.Read(key2)

	plaintext := []byte("test data")
	ciphertext, _ := Encrypt(key1, plaintext)

	_, err := Decrypt(key2, ciphertext)
	if err == nil {
		t.Error("Decrypt with wrong key should fail")
	}
}

func TestDecryptInvalidCiphertext(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)

	_, err := Decrypt(key, []byte("short"))
	if err == nil {
		t.Error("Decrypt with short ciphertext should fail")
	}
}

func TestEncryptDecryptString(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)

	plaintext := "test string for encryption"

	ciphertext, err := EncryptString(key, plaintext)
	if err != nil {
		t.Fatalf("EncryptString failed: %v", err)
	}

	if len(ciphertext) == 0 {
		t.Error("Ciphertext should not be empty")
	}

	decrypted, err := DecryptString(key, ciphertext)
	if err != nil {
		t.Fatalf("DecryptString failed: %v", err)
	}

	if decrypted != plaintext {
		t.Errorf("Decrypted = %q, want %q", decrypted, plaintext)
	}
}

func TestDecryptStringInvalidBase64(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)

	_, err := DecryptString(key, "invalid base64!!!")
	if err == nil {
		t.Error("DecryptString with invalid base64 should fail")
	}
}

func TestNewSSEClientSideProcessor(t *testing.T) {
	km := NewKeyManager()
	processor := NewSSEClientSideProcessor(km)

	if processor == nil {
		t.Fatal("Processor should not be nil")
	}

	if processor.keyManager == nil {
		t.Error("KeyManager should be set")
	}
}

func TestSSEClientSideProcessorEncrypt(t *testing.T) {
	km := NewKeyManager()
	processor := NewSSEClientSideProcessor(km)

	key := make([]byte, 32)
	rand.Read(key)

	data := []byte("test data")
	opts := SSEClientSide{
		Key:                 key,
		KeyID:               "test-key-id",
		EncryptionAlgorithm: "AES256",
	}

	encrypted, err := processor.Encrypt("bucket", "key", data, opts)
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}

	if len(encrypted) <= len(data) {
		t.Error("Encrypted data should be longer than original")
	}
}

func TestSSEClientSideProcessorEncryptNoKey(t *testing.T) {
	km := NewKeyManager()
	processor := NewSSEClientSideProcessor(km)

	data := []byte("test data")
	opts := SSEClientSide{}

	encrypted, err := processor.Encrypt("bucket", "key", data, opts)
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}

	if !bytes.Equal(data, encrypted) {
		t.Error("Without key, data should be returned unchanged")
	}
}

func TestSSEClientSideProcessorDecrypt(t *testing.T) {
	km := NewKeyManager()
	processor := NewSSEClientSideProcessor(km)

	key := make([]byte, 32)
	rand.Read(key)

	data := []byte("test data")
	opts := SSEClientSide{Key: key}

	encrypted, _ := processor.Encrypt("bucket", "key", data, opts)
	decrypted, err := processor.Decrypt("bucket", "key", encrypted, opts)

	if err != nil {
		t.Fatalf("Decrypt failed: %v", err)
	}

	if !bytes.Equal(data, decrypted) {
		t.Error("Decrypted data should match original")
	}
}

func TestSSEClientSideProcessorDecryptNoKey(t *testing.T) {
	km := NewKeyManager()
	processor := NewSSEClientSideProcessor(km)

	data := []byte("test data")
	opts := SSEClientSide{}

	decrypted, err := processor.Decrypt("bucket", "key", data, opts)
	if err != nil {
		t.Fatalf("Decrypt failed: %v", err)
	}

	if !bytes.Equal(data, decrypted) {
		t.Error("Without key, data should be returned unchanged")
	}
}

func TestSSEClientSideProcessorGetTags(t *testing.T) {
	km := NewKeyManager()
	processor := NewSSEClientSideProcessor(km)

	opts := SSEClientSide{
		KeyID:               "test-key-id",
		EncryptionAlgorithm: "AES256",
	}

	tags := processor.GetTags(opts)

	if len(tags) == 0 {
		t.Error("Should return tags")
	}

	if _, ok := tags["X-Amz-Server-Side-Encryption"]; !ok {
		t.Error("Should have encryption algorithm tag")
	}
}

func TestSSEClientSideProcessorGetTagsEmpty(t *testing.T) {
	km := NewKeyManager()
	processor := NewSSEClientSideProcessor(km)

	opts := SSEClientSide{}
	tags := processor.GetTags(opts)

	if len(tags) != 0 {
		t.Error("Empty options should return empty tags")
	}
}

func TestSSEClientSide(t *testing.T) {
	opts := SSEClientSide{
		Key:                 []byte{1, 2, 3, 4},
		KeyID:               "test-id",
		EncryptionAlgorithm: "AES256",
	}

	if len(opts.Key) != 4 {
		t.Errorf("Key length = %d, want 4", len(opts.Key))
	}
	if opts.KeyID != "test-id" {
		t.Errorf("KeyID = %v, want test-id", opts.KeyID)
	}
}

func TestEncryptWithInvalidKey(t *testing.T) {
	_, err := Encrypt([]byte("short"), []byte("test"))
	if err == nil {
		t.Error("Encrypt with invalid key should fail")
	}
}

func TestDecryptWithInvalidKey(t *testing.T) {
	ciphertext := make([]byte, 100)
	rand.Read(ciphertext)

	_, err := Decrypt([]byte("short"), ciphertext)
	if err == nil {
		t.Error("Decrypt with invalid key should fail")
	}
}

type errorReader struct {
	err error
}

func (r *errorReader) Read(p []byte) (n int, err error) {
	return 0, r.err
}

func TestGenerateKeyRandError(t *testing.T) {
	originalReader := randReader
	randReader = &errorReader{err: errors.New("read error")}
	defer func() { randReader = originalReader }()

	km := NewKeyManager()
	_, err := km.GenerateKey("test-key")
	if err == nil {
		t.Error("GenerateKey should return error when rand read fails")
	}
}

func TestEncryptIOReadFullError(t *testing.T) {
	originalReader := randReader
	randReader = &errorReader{err: errors.New("read error")}
	defer func() { randReader = originalReader }()

	key := make([]byte, 32)
	_, err := Encrypt(key, []byte("test"))
	if err == nil {
		t.Error("Encrypt should return error when io.ReadFull fails")
	}
}

type mockBlock struct {
	blockSize int
}

func (b *mockBlock) BlockSize() int          { return b.blockSize }
func (b *mockBlock) Encrypt(dst, src []byte) { copy(dst, src) }
func (b *mockBlock) Decrypt(dst, src []byte) { copy(dst, src) }

func TestEncryptNewGCMError(t *testing.T) {
	originalNewGCM := newGCM
	newGCM = func(block cipher.Block) (cipher.AEAD, error) {
		return nil, errors.New("gcm error")
	}
	defer func() { newGCM = originalNewGCM }()

	key := make([]byte, 32)
	_, err := Encrypt(key, []byte("test"))
	if err == nil {
		t.Error("Encrypt should return error when NewGCM fails")
	}
}

func TestDecryptNewGCMError(t *testing.T) {
	originalNewGCM := newGCM
	newGCM = func(block cipher.Block) (cipher.AEAD, error) {
		return nil, errors.New("gcm error")
	}
	defer func() { newGCM = originalNewGCM }()

	key := make([]byte, 32)
	_, err := Decrypt(key, make([]byte, 100))
	if err == nil {
		t.Error("Decrypt should return error when NewGCM fails")
	}
}

func TestEncryptStringError(t *testing.T) {
	_, err := EncryptString([]byte("short"), "test")
	if err == nil {
		t.Error("EncryptString with invalid key should fail")
	}
}

func TestDecryptStringDecryptError(t *testing.T) {
	key := make([]byte, 32)
	shortCiphertext := "dG9vIHNob3J0"
	_, err := DecryptString(key, shortCiphertext)
	if err == nil {
		t.Error("DecryptString with too short ciphertext should fail")
	}
}
