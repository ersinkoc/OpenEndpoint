package dedup

import (
	"bytes"
	"testing"

	"go.uber.org/zap"
)

func TestNewStore(t *testing.T) {
	logger := zap.NewNop().Sugar()
	store := NewStore(logger)

	if store == nil {
		t.Fatal("Store should not be nil")
	}

	if store.fingerprints == nil {
		t.Error("Fingerprints map should be initialized")
	}
}

func TestNewDeduplicator(t *testing.T) {
	logger := zap.NewNop().Sugar()
	dedup := NewDeduplicator(logger)

	if dedup == nil {
		t.Fatal("Deduplicator should not be nil")
	}

	if dedup.store == nil {
		t.Error("Store should be initialized")
	}
}

func TestStore_ProcessWrite(t *testing.T) {
	logger := zap.NewNop().Sugar()
	store := NewStore(logger)

	data := []byte("test data")
	bucket := "test-bucket"
	key := "test-key"

	result, err := store.ProcessWrite(bucket, key, data)
	if err != nil {
		t.Fatalf("ProcessWrite failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result should not be nil")
	}

	if len(result.Fingerprint) == 0 {
		t.Error("Fingerprint should not be empty")
	}
}

func TestStore_ProcessWrite_SameData(t *testing.T) {
	logger := zap.NewNop().Sugar()
	store := NewStore(logger)

	data := []byte("test data")

	// First write
	result1, _ := store.ProcessWrite("bucket1", "key1", data)

	// Second write with same data
	result2, _ := store.ProcessWrite("bucket2", "key2", data)

	// Fingerprints should be the same
	if !bytes.Equal(result1.Fingerprint, result2.Fingerprint) {
		t.Error("Same data should produce same fingerprint")
	}

	// Second should be deduplicated
	if !result2.Deduplicated {
		t.Error("Second write should be deduplicated")
	}
}

func TestStore_ProcessWrite_DifferentData(t *testing.T) {
	logger := zap.NewNop().Sugar()
	store := NewStore(logger)

	data1 := []byte("test data 1")
	data2 := []byte("test data 2")

	// First write
	result1, _ := store.ProcessWrite("bucket", "key1", data1)

	// Second write with different data
	result2, _ := store.ProcessWrite("bucket", "key2", data2)

	// Fingerprints should be different
	if bytes.Equal(result1.Fingerprint, result2.Fingerprint) {
		t.Error("Different data should produce different fingerprints")
	}

	// Neither should be deduplicated
	if result1.Deduplicated || result2.Deduplicated {
		t.Error("First occurrence of different data should not be deduplicated")
	}
}

func TestStore_Get(t *testing.T) {
	logger := zap.NewNop().Sugar()
	store := NewStore(logger)

	data := []byte("test data")
	bucket := "test-bucket"
	key := "test-key"

	// Write data
	result, _ := store.ProcessWrite(bucket, key, data)

	// Get by fingerprint
	info, ok := store.Get(result.Fingerprint)
	if !ok {
		t.Fatal("Should find stored fingerprint")
	}

	if info.Size != int64(len(data)) {
		t.Errorf("Size = %d, want %d", info.Size, len(data))
	}
}

func TestStore_Get_NotFound(t *testing.T) {
	logger := zap.NewNop().Sugar()
	store := NewStore(logger)

	_, ok := store.Get([]byte("non-existent-fingerprint"))
	if ok {
		t.Error("Should not find non-existent fingerprint")
	}
}

func TestStore_RemoveObject(t *testing.T) {
	logger := zap.NewNop().Sugar()
	store := NewStore(logger)

	data := []byte("test data")
	bucket := "test-bucket"
	key := "test-key"

	// Write data
	store.ProcessWrite(bucket, key, data)

	// Remove object
	err := store.RemoveObject(bucket, key)
	if err != nil {
		t.Fatalf("RemoveObject failed: %v", err)
	}

	// Verify removed - ref count should be 0
	stats := store.GetStats()
	if stats.TotalObjects != 0 {
		t.Errorf("TotalObjects = %d, want 0", stats.TotalObjects)
	}
}

func TestStore_RemoveObject_NotFound(t *testing.T) {
	logger := zap.NewNop().Sugar()
	store := NewStore(logger)

	err := store.RemoveObject("non-existent-bucket", "non-existent-key")
	if err == nil {
		t.Error("Should return error for non-existent object")
	}
}

func TestStore_RemoveObject_MultipleRefs(t *testing.T) {
	logger := zap.NewNop().Sugar()
	store := NewStore(logger)

	data := []byte("test data")

	// Write same data to multiple keys
	store.ProcessWrite("bucket", "key1", data)
	store.ProcessWrite("bucket", "key2", data)

	stats := store.GetStats()
	if stats.TotalObjects != 2 {
		t.Errorf("TotalObjects = %d, want 2", stats.TotalObjects)
	}

	// Remove one object
	store.RemoveObject("bucket", "key1")

	// Should still have one reference
	stats = store.GetStats()
	if stats.TotalObjects != 1 {
		t.Errorf("TotalObjects after remove = %d, want 1", stats.TotalObjects)
	}
}

func TestStore_GetStats(t *testing.T) {
	logger := zap.NewNop().Sugar()
	store := NewStore(logger)

	// Empty stats
	stats := store.GetStats()
	if stats.TotalObjects != 0 {
		t.Errorf("Empty store TotalObjects = %d, want 0", stats.TotalObjects)
	}

	// Add some data
	store.ProcessWrite("bucket", "key1", []byte("data1"))
	store.ProcessWrite("bucket", "key2", []byte("data2"))

	stats = store.GetStats()
	if stats.TotalObjects != 2 {
		t.Errorf("TotalObjects = %d, want 2", stats.TotalObjects)
	}

	if stats.UniqueChunks != 2 {
		t.Errorf("UniqueChunks = %d, want 2", stats.UniqueChunks)
	}
}

func TestStore_GetStats_Dedup(t *testing.T) {
	logger := zap.NewNop().Sugar()
	store := NewStore(logger)

	// Add same data twice
	store.ProcessWrite("bucket", "key1", []byte("same data"))
	store.ProcessWrite("bucket", "key2", []byte("same data"))

	stats := store.GetStats()

	// Should have 2 objects but only 1 unique chunk
	if stats.TotalObjects != 2 {
		t.Errorf("TotalObjects = %d, want 2", stats.TotalObjects)
	}

	if stats.UniqueChunks != 1 {
		t.Errorf("UniqueChunks = %d, want 1", stats.UniqueChunks)
	}

	if stats.DuplicateObjects != 1 {
		t.Errorf("DuplicateObjects = %d, want 1", stats.DuplicateObjects)
	}
}

func TestDeduplicationWriter_Write(t *testing.T) {
	logger := zap.NewNop()
	store := NewStore(logger.Sugar())

	var buf bytes.Buffer
	writer := NewDeduplicationWriter(nil, store, "bucket", "key", 100, logger)

	data := []byte("test data for deduplication writer")
	n, err := writer.Write(data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if n != len(data) {
		t.Errorf("Written bytes = %d, want %d", n, len(data))
	}

	buf.Write(data)
}

func TestDeduplicationWriter_Close(t *testing.T) {
	logger := zap.NewNop()
	store := NewStore(logger.Sugar())

	writer := NewDeduplicationWriter(nil, store, "bucket", "key", 10, logger)

	// Write some data
	writer.Write([]byte("test data"))

	// Close should not fail
	err := writer.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestDeduplicationWriter_Threshold(t *testing.T) {
	logger := zap.NewNop()
	store := NewStore(logger.Sugar())

	threshold := 10
	writer := NewDeduplicationWriter(nil, store, "bucket", "key", threshold, logger)

	// Write data larger than threshold
	data := make([]byte, threshold*2)
	for i := range data {
		data[i] = byte(i)
	}

	n, err := writer.Write(data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if n != len(data) {
		t.Errorf("Written bytes = %d, want %d", n, len(data))
	}

	// Check that data was processed
	stats := store.GetStats()
	if stats.TotalObjects == 0 {
		t.Error("Data should have been processed after exceeding threshold")
	}
}

func TestFingerprintConsistency(t *testing.T) {
	logger := zap.NewNop().Sugar()
	store := NewStore(logger)

	// Same data should always produce same fingerprint
	data := []byte("consistent data")

	result1, _ := store.ProcessWrite("bucket", "key1", data)
	result2, _ := store.ProcessWrite("bucket", "key2", data)
	result3, _ := store.ProcessWrite("bucket", "key3", data)

	if !bytes.Equal(result1.Fingerprint, result2.Fingerprint) {
		t.Error("Fingerprints should be consistent")
	}

	if !bytes.Equal(result2.Fingerprint, result3.Fingerprint) {
		t.Error("Fingerprints should be consistent")
	}
}

func TestConcurrentProcessWrite(t *testing.T) {
	logger := zap.NewNop().Sugar()
	store := NewStore(logger)

	done := make(chan bool)

	// Concurrent writes
	for i := 0; i < 10; i++ {
		go func(id int) {
			data := []byte("test data")
			store.ProcessWrite("bucket", string(rune('A'+id)), data)
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// All should be deduplicated to same chunk
	stats := store.GetStats()
	if stats.UniqueChunks != 1 {
		t.Errorf("UniqueChunks = %d, want 1", stats.UniqueChunks)
	}

	if stats.TotalObjects != 10 {
		t.Errorf("TotalObjects = %d, want 10", stats.TotalObjects)
	}
}

func TestRemoveObject_RaceCondition(t *testing.T) {
	logger := zap.NewNop().Sugar()
	store := NewStore(logger)

	// Add multiple references to same data
	data := []byte("test data")
	for i := 0; i < 10; i++ {
		store.ProcessWrite("bucket", string(rune('A'+i)), data)
	}

	// Concurrent removes
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(id int) {
			store.RemoveObject("bucket", string(rune('A'+id)))
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Should not panic and all should be removed
	stats := store.GetStats()
	if stats.TotalObjects != 0 {
		t.Errorf("TotalObjects after concurrent removes = %d, want 0", stats.TotalObjects)
	}
}
