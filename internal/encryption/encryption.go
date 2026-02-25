package encryption

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io"
)

var newCipher = aes.NewCipher
var newGCM = cipher.NewGCM
var randReader = rand.Reader

// KeyManager manages encryption keys
type KeyManager struct {
	keys map[string][]byte
}

// NewKeyManager creates a new key manager
func NewKeyManager() *KeyManager {
	return &KeyManager{
		keys: make(map[string][]byte),
	}
}

// GenerateKey generates a new encryption key
func (km *KeyManager) GenerateKey(keyID string) ([]byte, error) {
	key := make([]byte, 32)
	if _, err := io.ReadFull(randReader, key); err != nil {
		return nil, err
	}
	km.keys[keyID] = key
	return key, nil
}

// SetKey sets an encryption key
func (km *KeyManager) SetKey(keyID string, key []byte) {
	km.keys[keyID] = key
}

// GetKey gets an encryption key
func (km *KeyManager) GetKey(keyID string) ([]byte, bool) {
	key, ok := km.keys[keyID]
	return key, ok
}

// DeleteKey deletes an encryption key
func (km *KeyManager) DeleteKey(keyID string) {
	delete(km.keys, keyID)
}

// Encrypt encrypts data using AES-256-GCM
func Encrypt(key []byte, plaintext []byte) ([]byte, error) {
	block, err := newCipher(key)
	if err != nil {
		return nil, err
	}

	gcm, err := newGCM(block)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(randReader, nonce); err != nil {
		return nil, err
	}

	ciphertext := gcm.Seal(nonce, nonce, plaintext, nil)
	return ciphertext, nil
}

// Decrypt decrypts data using AES-256-GCM
func Decrypt(key []byte, ciphertext []byte) ([]byte, error) {
	block, err := newCipher(key)
	if err != nil {
		return nil, err
	}

	gcm, err := newGCM(block)
	if err != nil {
		return nil, err
	}

	nonceSize := gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, errors.New("ciphertext too short")
	}

	nonce, ciphertext := ciphertext[:nonceSize], ciphertext[nonceSize:]
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, err
	}

	return plaintext, nil
}

// EncryptString encrypts a string and returns base64 encoded result
func EncryptString(key []byte, plaintext string) (string, error) {
	ciphertext, err := Encrypt(key, []byte(plaintext))
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(ciphertext), nil
}

// DecryptString decrypts a base64 encoded string
func DecryptString(key []byte, ciphertext string) (string, error) {
	data, err := base64.StdEncoding.DecodeString(ciphertext)
	if err != nil {
		return "", err
	}

	plaintext, err := Decrypt(key, data)
	if err != nil {
		return "", err
	}

	return string(plaintext), nil
}

// SSEClientSide represents client-side encryption options
type SSEClientSide struct {
	Key                 []byte
	KeyID               string
	EncryptionAlgorithm string
}

// SSEClientSideProcessor handles client-side encryption
type SSEClientSideProcessor struct {
	keyManager *KeyManager
}

// NewSSEClientSideProcessor creates a new SSE-C processor
func NewSSEClientSideProcessor(km *KeyManager) *SSEClientSideProcessor {
	return &SSEClientSideProcessor{
		keyManager: km,
	}
}

// Encrypt encrypts data with SSE-C
func (p *SSEClientSideProcessor) Encrypt(bucket, key string, data []byte, opts SSEClientSide) ([]byte, error) {
	if len(opts.Key) == 0 {
		return data, nil
	}
	return Encrypt(opts.Key, data)
}

// Decrypt decrypts data with SSE-C
func (p *SSEClientSideProcessor) Decrypt(bucket, key string, data []byte, opts SSEClientSide) ([]byte, error) {
	if len(opts.Key) == 0 {
		return data, nil
	}
	return Decrypt(opts.Key, data)
}

// GetTags returns encryption tags for object
func (p *SSEClientSideProcessor) GetTags(opts SSEClientSide) map[string]string {
	tags := make(map[string]string)
	if opts.KeyID != "" {
		tags["X-Amz-Server-Side-Encryption-Customer-Key-Md5"] = "" // Would compute MD5
	}
	if opts.EncryptionAlgorithm != "" {
		tags["X-Amz-Server-Side-Encryption"] = opts.EncryptionAlgorithm
	}
	return tags
}
