package encryption

import (
	"bytes"
	"crypto/rand"
	"io"
	"testing"
)

func TestNewEncryptor(t *testing.T) {
	key := make([]byte, 32) // AES-256 key
	rand.Read(key)

	enc, err := NewEncryptor(key)
	if err != nil {
		t.Fatalf("NewEncryptor failed: %v", err)
	}

	if enc == nil {
		t.Fatal("Encryptor should not be nil")
	}
}

func TestNewEncryptor_InvalidKey(t *testing.T) {
	tests := []struct {
		name string
		key  []byte
	}{
		{"nil key", nil},
		{"empty key", []byte{}},
		{"short key", []byte("short")},
		{"16 bytes (AES-128)", make([]byte, 16)},
		{"24 bytes (AES-192)", make([]byte, 24)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewEncryptor(tt.key)
			// AES-128 and AES-192 should work, only nil/empty/short should fail
			if tt.key == nil || len(tt.key) < 16 {
				if err == nil {
					t.Error("Should return error for invalid key")
				}
			}
		})
	}
}

func TestEncryptDecrypt(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)

	enc, err := NewEncryptor(key)
	if err != nil {
		t.Fatalf("NewEncryptor failed: %v", err)
	}

	plaintext := []byte("This is a secret message")

	ciphertext, err := enc.Encrypt(plaintext)
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}

	if len(ciphertext) == 0 {
		t.Error("Ciphertext should not be empty")
	}

	// Ciphertext should be different from plaintext
	if bytes.Equal(ciphertext, plaintext) {
		t.Error("Ciphertext should not equal plaintext")
	}

	// Decrypt
	decrypted, err := enc.Decrypt(ciphertext)
	if err != nil {
		t.Fatalf("Decrypt failed: %v", err)
	}

	if !bytes.Equal(decrypted, plaintext) {
		t.Errorf("Decrypted = %s, want %s", decrypted, plaintext)
	}
}

func TestEncryptDecrypt_Empty(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)

	enc, _ := NewEncryptor(key)

	plaintext := []byte("")

	ciphertext, err := enc.Encrypt(plaintext)
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}

	decrypted, err := enc.Decrypt(ciphertext)
	if err != nil {
		t.Fatalf("Decrypt failed: %v", err)
	}

	if !bytes.Equal(decrypted, plaintext) {
		t.Error("Decrypted should equal plaintext")
	}
}

func TestEncryptDecrypt_LargeData(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)

	enc, _ := NewEncryptor(key)

	// 1MB of data
	plaintext := make([]byte, 1024*1024)
	rand.Read(plaintext)

	ciphertext, err := enc.Encrypt(plaintext)
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}

	decrypted, err := enc.Decrypt(ciphertext)
	if err != nil {
		t.Fatalf("Decrypt failed: %v", err)
	}

	if !bytes.Equal(decrypted, plaintext) {
		t.Error("Decrypted should equal plaintext")
	}
}

func TestEncrypt_UniqueCiphertext(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)

	enc, _ := NewEncryptor(key)

	plaintext := []byte("same message")

	ciphertext1, _ := enc.Encrypt(plaintext)
	ciphertext2, _ := enc.Encrypt(plaintext)

	// Same plaintext should produce different ciphertext (due to random nonce)
	if bytes.Equal(ciphertext1, ciphertext2) {
		t.Error("Same plaintext should produce different ciphertext")
	}

	// Both should decrypt correctly
	decrypted1, _ := enc.Decrypt(ciphertext1)
	decrypted2, _ := enc.Decrypt(ciphertext2)

	if !bytes.Equal(decrypted1, plaintext) {
		t.Error("First decryption failed")
	}

	if !bytes.Equal(decrypted2, plaintext) {
		t.Error("Second decryption failed")
	}
}

func TestDecrypt_WrongKey(t *testing.T) {
	key1 := make([]byte, 32)
	key2 := make([]byte, 32)
	rand.Read(key1)
	rand.Read(key2)

	enc1, _ := NewEncryptor(key1)
	enc2, _ := NewEncryptor(key2)

	plaintext := []byte("secret message")

	ciphertext, _ := enc1.Encrypt(plaintext)

	// Try to decrypt with wrong key
	_, err := enc2.Decrypt(ciphertext)
	if err == nil {
		t.Error("Should fail to decrypt with wrong key")
	}
}

func TestDecrypt_Corrupted(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)

	enc, _ := NewEncryptor(key)

	plaintext := []byte("secret message")
	ciphertext, _ := enc.Encrypt(plaintext)

	// Corrupt the ciphertext
	ciphertext[0] ^= 0xFF

	_, err := enc.Decrypt(ciphertext)
	if err == nil {
		t.Error("Should fail to decrypt corrupted ciphertext")
	}
}

func TestEncryptReader(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)

	enc, _ := NewEncryptor(key)

	plaintext := []byte("This is a test message for reader encryption")
	reader := bytes.NewReader(plaintext)

	encryptedReader := enc.EncryptReader(reader)

	ciphertext, err := io.ReadAll(encryptedReader)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(ciphertext) == 0 {
		t.Error("Ciphertext should not be empty")
	}
}

func TestDecryptReader(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)

	enc, _ := NewEncryptor(key)

	plaintext := []byte("This is a test message")

	// Encrypt
	ciphertext, _ := enc.Encrypt(plaintext)

	// Decrypt via reader
	decryptReader := enc.DecryptReader(bytes.NewReader(ciphertext))
	decrypted, err := io.ReadAll(decryptReader)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if !bytes.Equal(decrypted, plaintext) {
		t.Error("Decrypted should equal plaintext")
	}
}

func TestEncryptWriter(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)

	enc, _ := NewEncryptor(key)

	plaintext := []byte("This is a test message for writer encryption")
	var buf bytes.Buffer

	encryptedWriter, err := enc.EncryptWriter(&buf)
	if err != nil {
		t.Fatalf("EncryptWriter failed: %v", err)
	}

	encryptedWriter.Write(plaintext)
	encryptedWriter.Close()

	ciphertext := buf.Bytes()
	if len(ciphertext) == 0 {
		t.Error("Ciphertext should not be empty")
	}

	// Verify by decrypting
	decrypted, err := enc.Decrypt(ciphertext)
	if err != nil {
		t.Fatalf("Decrypt failed: %v", err)
	}

	if !bytes.Equal(decrypted, plaintext) {
		t.Error("Decrypted should equal plaintext")
	}
}

func TestDecryptWriter(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)

	enc, _ := NewEncryptor(key)

	plaintext := []byte("Test message")
	ciphertext, _ := enc.Encrypt(plaintext)

	var buf bytes.Buffer

	decryptWriter, err := enc.DecryptWriter(&buf)
	if err != nil {
		t.Fatalf("DecryptWriter failed: %v", err)
	}

	decryptWriter.Write(ciphertext)
	decryptWriter.Close()

	decrypted := buf.Bytes()
	if !bytes.Equal(decrypted, plaintext) {
		t.Error("Decrypted should equal plaintext")
	}
}

func TestKeyRotation(t *testing.T) {
	oldKey := make([]byte, 32)
	newKey := make([]byte, 32)
	rand.Read(oldKey)
	rand.Read(newKey)

	oldEnc, _ := NewEncryptor(oldKey)
	newEnc, _ := NewEncryptor(newKey)

	plaintext := []byte("secret data")

	// Encrypt with old key
	ciphertext, _ := oldEnc.Encrypt(plaintext)

	// Re-encrypt with new key
	decrypted, _ := oldEnc.Decrypt(ciphertext)
	newCiphertext, _ := newEnc.Encrypt(decrypted)

	// Verify new key can decrypt
	finalDecrypted, _ := newEnc.Decrypt(newCiphertext)

	if !bytes.Equal(finalDecrypted, plaintext) {
		t.Error("Key rotation failed")
	}
}

func TestConcurrency(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)

	enc, _ := NewEncryptor(key)

	done := make(chan bool)

	for i := 0; i < 10; i++ {
		go func(id int) {
			plaintext := []byte("concurrent test message")
			for j := 0; j < 10; j++ {
				ciphertext, err := enc.Encrypt(plaintext)
				if err != nil {
					t.Errorf("Encrypt failed: %v", err)
				}

				decrypted, err := enc.Decrypt(ciphertext)
				if err != nil {
					t.Errorf("Decrypt failed: %v", err)
				}

				if !bytes.Equal(decrypted, plaintext) {
					t.Errorf("Decryption mismatch")
				}
			}
			done <- true
		}(i)
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}

func BenchmarkEncrypt(b *testing.B) {
	key := make([]byte, 32)
	rand.Read(key)
	enc, _ := NewEncryptor(key)

	data := make([]byte, 1024)
	rand.Read(data)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		enc.Encrypt(data)
	}
}

func BenchmarkDecrypt(b *testing.B) {
	key := make([]byte, 32)
	rand.Read(key)
	enc, _ := NewEncryptor(key)

	data := make([]byte, 1024)
	rand.Read(data)
	ciphertext, _ := enc.Encrypt(data)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		enc.Decrypt(ciphertext)
	}
}

func BenchmarkEncryptDecrypt(b *testing.B) {
	key := make([]byte, 32)
	rand.Read(key)
	enc, _ := NewEncryptor(key)

	data := make([]byte, 1024)
	rand.Read(data)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ciphertext, _ := enc.Encrypt(data)
		enc.Decrypt(ciphertext)
	}
}
