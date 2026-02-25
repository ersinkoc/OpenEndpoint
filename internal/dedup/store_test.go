package dedup

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"go.uber.org/zap"
)

func TestNewStore(t *testing.T) {
	logger := zap.NewNop()
	store := NewStore(logger)

	if store == nil {
		t.Fatal("Store should not be nil")
	}

	if store.fingerprints == nil {
		t.Error("Fingerprints map should be initialized")
	}
}

func TestNewDeduplicator(t *testing.T) {
	logger := zap.NewNop()
	dedup := NewDeduplicator(logger)

	if dedup == nil {
		t.Fatal("Deduplicator should not be nil")
	}

	if dedup.store == nil {
		t.Error("Store should be initialized")
	}
}

func TestComputeFingerprint(t *testing.T) {
	data := []byte("test data")
	fp := ComputeFingerprint(data)
	if len(fp) == 0 {
		t.Error("Fingerprint should not be empty")
	}

	fp2 := ComputeFingerprint(data)
	if fp != fp2 {
		t.Error("Same data should produce same fingerprint")
	}

	fp3 := ComputeFingerprint([]byte("different data"))
	if fp == fp3 {
		t.Error("Different data should produce different fingerprint")
	}
}

func TestComputeFingerprintFromReader(t *testing.T) {
	data := []byte("test data for reader")
	reader := bytes.NewReader(data)

	fp, size, err := ComputeFingerprintFromReader(reader)
	if err != nil {
		t.Fatalf("ComputeFingerprintFromReader failed: %v", err)
	}
	if len(fp) == 0 {
		t.Error("Fingerprint should not be empty")
	}
	if size != int64(len(data)) {
		t.Errorf("size = %d, want %d", size, len(data))
	}

	reader.Seek(0, io.SeekStart)
	fp2, _, _ := ComputeFingerprintFromReader(reader)
	if fp != fp2 {
		t.Error("Same data should produce same fingerprint from reader")
	}
}

func TestAddObjectNew(t *testing.T) {
	logger := zap.NewNop()
	store := NewStore(logger)

	data := []byte("test data")
	fp, isNew, err := store.AddObject("bucket", "key", data)
	if err != nil {
		t.Fatalf("AddObject failed: %v", err)
	}
	if fp == "" {
		t.Error("Fingerprint should not be empty")
	}
	if isNew {
		t.Error("First object should not be marked as deduplicated")
	}
}

func TestAddObjectDuplicate(t *testing.T) {
	logger := zap.NewNop()
	store := NewStore(logger)

	data := []byte("test data")

	store.AddObject("bucket", "key1", data)
	fp2, isNew, err := store.AddObject("bucket", "key2", data)

	if err != nil {
		t.Fatalf("AddObject failed: %v", err)
	}
	if !isNew {
		t.Error("Duplicate should be marked as deduplicated")
	}
	if fp2 == "" {
		t.Error("Fingerprint should not be empty")
	}
}

func TestGetFingerprintInfo(t *testing.T) {
	logger := zap.NewNop()
	store := NewStore(logger)

	data := []byte("test data")
	fp, _, _ := store.AddObject("bucket", "key", data)

	info, ok := store.GetFingerprintInfo(fp)
	if !ok {
		t.Fatal("GetFingerprintInfo should return ok=true")
	}
	if info.Size != int64(len(data)) {
		t.Errorf("Size = %d, want %d", info.Size, len(data))
	}
	if info.RefCount != 1 {
		t.Errorf("RefCount = %d, want 1", info.RefCount)
	}
}

func TestGetFingerprintInfoNotFound(t *testing.T) {
	logger := zap.NewNop()
	store := NewStore(logger)

	_, ok := store.GetFingerprintInfo("nonexistent")
	if ok {
		t.Error("GetFingerprintInfo should return ok=false for nonexistent fingerprint")
	}
}

func TestRemoveObject(t *testing.T) {
	logger := zap.NewNop()
	store := NewStore(logger)

	data := []byte("test data")
	store.AddObject("bucket", "key", data)

	err := store.RemoveObject("bucket", "key")
	if err != nil {
		t.Fatalf("RemoveObject failed: %v", err)
	}

	stats := store.GetStats()
	if stats.TotalObjects != 0 {
		t.Errorf("TotalObjects after remove = %d, want 0", stats.TotalObjects)
	}
}

func TestRemoveObjectNotFound(t *testing.T) {
	logger := zap.NewNop()
	store := NewStore(logger)

	err := store.RemoveObject("bucket", "nonexistent")
	if err == nil {
		t.Error("RemoveObject should fail for nonexistent object")
	}
}

func TestGetStats(t *testing.T) {
	logger := zap.NewNop()
	store := NewStore(logger)

	stats := store.GetStats()
	if stats.TotalObjects != 0 {
		t.Errorf("Empty store TotalObjects = %d, want 0", stats.TotalObjects)
	}

	data := []byte("test data")
	store.AddObject("bucket", "key1", data)
	store.AddObject("bucket", "key2", data)

	stats = store.GetStats()
	if stats.TotalObjects != 2 {
		t.Errorf("TotalObjects = %d, want 2", stats.TotalObjects)
	}
	if stats.UniqueObjects != 1 {
		t.Errorf("UniqueObjects = %d, want 1", stats.UniqueObjects)
	}
	if stats.DuplicateObjects != 1 {
		t.Errorf("DuplicateObjects = %d, want 1", stats.DuplicateObjects)
	}
	if stats.SpaceSaved != int64(len(data)) {
		t.Errorf("SpaceSaved = %d, want %d", stats.SpaceSaved, len(data))
	}
}

func TestDeduplicatorProcessWrite(t *testing.T) {
	logger := zap.NewNop()
	dedup := NewDeduplicator(logger)

	data := []byte("test data")
	result, err := dedup.ProcessWrite(context.Background(), "bucket", "key", data)
	if err != nil {
		t.Fatalf("ProcessWrite failed: %v", err)
	}
	if result == nil {
		t.Fatal("Result should not be nil")
	}
	if result.Fingerprint == "" {
		t.Error("Fingerprint should not be empty")
	}
	if result.Size != int64(len(data)) {
		t.Errorf("Size = %d, want %d", result.Size, len(data))
	}
}

func TestDeduplicatorProcessWriteDeduplicated(t *testing.T) {
	logger := zap.NewNop()
	dedup := NewDeduplicator(logger)

	data := []byte("test data")

	result1, _ := dedup.ProcessWrite(context.Background(), "bucket1", "key1", data)
	result2, _ := dedup.ProcessWrite(context.Background(), "bucket2", "key2", data)

	if result1.Fingerprint != result2.Fingerprint {
		t.Error("Same data should produce same fingerprint")
	}
	if !result2.Deduplicated {
		t.Error("Second write should be deduplicated")
	}
}

func TestDeduplicatorProcessRead(t *testing.T) {
	logger := zap.NewNop()
	dedup := NewDeduplicator(logger)

	_, err := dedup.ProcessRead("bucket", "key")
	if err == nil {
		t.Error("ProcessRead should return error (not implemented)")
	}
}

func TestWriteResult(t *testing.T) {
	result := &WriteResult{
		Fingerprint:  "abc123",
		Deduplicated: true,
		Size:         100,
	}

	if result.Fingerprint != "abc123" {
		t.Errorf("Fingerprint = %v, want abc123", result.Fingerprint)
	}
	if !result.Deduplicated {
		t.Error("Deduplicated should be true")
	}
	if result.Size != 100 {
		t.Errorf("Size = %d, want 100", result.Size)
	}
}

func TestNewChunkedFingerprinter(t *testing.T) {
	logger := zap.NewNop()
	fp := NewChunkedFingerprinter(logger, 1024, 65536, 64)

	if fp == nil {
		t.Fatal("ChunkedFingerprinter should not be nil")
	}
	if fp.minChunk != 1024 {
		t.Errorf("minChunk = %d, want 1024", fp.minChunk)
	}
}

func TestFingerprintChunks(t *testing.T) {
	logger := zap.NewNop()
	fp := NewChunkedFingerprinter(logger, 1024, 65536, 64)

	data := make([]byte, 200*1024)
	chunks := fp.FingerprintChunks(data)

	if len(chunks) < 1 {
		t.Error("Should produce at least one chunk")
	}
}

func TestFingerprintChunksSmall(t *testing.T) {
	logger := zap.NewNop()
	fp := NewChunkedFingerprinter(logger, 1024, 65536, 64)

	data := []byte("small data")
	chunks := fp.FingerprintChunks(data)

	if len(chunks) != 1 {
		t.Errorf("Small data should produce 1 chunk, got %d", len(chunks))
	}
}

func TestCompareChunks(t *testing.T) {
	logger := zap.NewNop()
	fp := NewChunkedFingerprinter(logger, 1024, 65536, 64)

	data1 := []byte("test data chunk 1")
	data2 := []byte("test data chunk 2")

	chunks1 := fp.FingerprintChunks(data1)
	chunks2 := fp.FingerprintChunks(data2)

	common, uniqueB := fp.CompareChunks(chunks1, chunks2)

	if common < 0 || uniqueB < 0 {
		t.Errorf("CompareChunks returned negative values: common=%d, uniqueB=%d", common, uniqueB)
	}
}

func TestCompareChunksIdentical(t *testing.T) {
	logger := zap.NewNop()
	fp := NewChunkedFingerprinter(logger, 1024, 65536, 64)

	data := []byte("test data")
	chunks := fp.FingerprintChunks(data)

	common, uniqueB := fp.CompareChunks(chunks, chunks)

	if common != len(chunks) {
		t.Errorf("Identical chunks should have all common: common=%d, want %d", common, len(chunks))
	}
	if uniqueB != 0 {
		t.Errorf("Identical chunks should have 0 unique in B: uniqueB=%d", uniqueB)
	}
}

func TestNewRollingFingerprinter(t *testing.T) {
	logger := zap.NewNop()
	rf := NewRollingFingerprinter(logger, 64)

	if rf == nil {
		t.Fatal("RollingFingerprinter should not be nil")
	}
	if rf.window != 64 {
		t.Errorf("window = %d, want 64", rf.window)
	}
}

func TestFindChunkBoundaries(t *testing.T) {
	logger := zap.NewNop()
	rf := NewRollingFingerprinter(logger, 64)

	data := make([]byte, 200*1024)
	boundaries := rf.FindChunkBoundaries(data)

	if len(boundaries) < 1 {
		t.Error("Should find at least one boundary for large data")
	}
}

func TestFindChunkBoundariesSmall(t *testing.T) {
	logger := zap.NewNop()
	rf := NewRollingFingerprinter(logger, 64)

	data := []byte("small data")
	boundaries := rf.FindChunkBoundaries(data)

	if len(boundaries) != 0 {
		t.Errorf("Small data should have 0 boundaries, got %d", len(boundaries))
	}
}

func TestHashRange(t *testing.T) {
	logger := zap.NewNop()
	rf := NewRollingFingerprinter(logger, 64)

	data := []byte("test data for hashing")
	hash := rf.HashRange(data, 0, len(data))

	if hash == 0 {
		t.Error("Hash should not be 0")
	}
}

func TestHashRangeSame(t *testing.T) {
	logger := zap.NewNop()
	rf := NewRollingFingerprinter(logger, 64)

	data := []byte("test data for hashing")
	hash1 := rf.HashRange(data, 0, len(data))
	hash2 := rf.HashRange(data, 0, len(data))

	if hash1 != hash2 {
		t.Error("Same range should produce same hash")
	}
}

func TestNewDeduplicationWriter(t *testing.T) {
	logger := zap.NewNop()
	dedup := NewDeduplicator(logger)
	buf := &bytes.Buffer{}

	writer := NewDeduplicationWriter(context.Background(), buf, dedup, "bucket", "key", 1024, logger)

	if writer == nil {
		t.Fatal("DeduplicationWriter should not be nil")
	}
}

func TestNewDeduplicationWriterNilContext(t *testing.T) {
	logger := zap.NewNop()
	dedup := NewDeduplicator(logger)
	buf := &bytes.Buffer{}

	writer := NewDeduplicationWriter(nil, buf, dedup, "bucket", "key", 1024, logger)

	if writer == nil {
		t.Fatal("DeduplicationWriter should not be nil even with nil context")
	}
}

func TestDeduplicationWriterWrite(t *testing.T) {
	logger := zap.NewNop()
	dedup := NewDeduplicator(logger)
	buf := &bytes.Buffer{}
	writer := NewDeduplicationWriter(context.Background(), buf, dedup, "bucket", "key", 1024, logger)

	data := []byte("test data")
	n, err := writer.Write(data)

	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != len(data) {
		t.Errorf("Write returned %d, want %d", n, len(data))
	}
}

func TestDeduplicationWriterClose(t *testing.T) {
	logger := zap.NewNop()
	dedup := NewDeduplicator(logger)
	buf := &bytes.Buffer{}
	writer := NewDeduplicationWriter(context.Background(), buf, dedup, "bucket", "key", 1024, logger)

	data := []byte("test data")
	writer.Write(data)

	err := writer.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestDeduplicationWriterThreshold(t *testing.T) {
	logger := zap.NewNop()
	dedup := NewDeduplicator(logger)
	buf := &bytes.Buffer{}
	threshold := 10
	writer := NewDeduplicationWriter(context.Background(), buf, dedup, "bucket", "key", threshold, logger)

	data := make([]byte, threshold+1)
	n, err := writer.Write(data)

	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != len(data) {
		t.Errorf("Write returned %d, want %d", n, len(data))
	}

	stats := dedup.store.GetStats()
	if stats.TotalObjects != 1 {
		t.Errorf("TotalObjects = %d, want 1 (should dedup after threshold)", stats.TotalObjects)
	}
}

func TestFingerprintInfo(t *testing.T) {
	info := &FingerprintInfo{
		Fingerprint: "abc123",
		Size:        100,
		RefCount:    5,
		FirstSeen:   1234567890,
		Objects: []ObjectRef{
			{Bucket: "bucket1", Key: "key1"},
		},
	}

	if info.Fingerprint != "abc123" {
		t.Errorf("Fingerprint = %v, want abc123", info.Fingerprint)
	}
}

func TestObjectRef(t *testing.T) {
	ref := ObjectRef{Bucket: "bucket", Key: "key"}

	if ref.Bucket != "bucket" {
		t.Errorf("Bucket = %v, want bucket", ref.Bucket)
	}
	if ref.Key != "key" {
		t.Errorf("Key = %v, want key", ref.Key)
	}
}

func TestStats(t *testing.T) {
	stats := Stats{
		TotalObjects:     100,
		UniqueObjects:    50,
		DuplicateObjects: 50,
		SpaceSaved:       5000,
		DedupRatio:       50.0,
	}

	if stats.TotalObjects != 100 {
		t.Errorf("TotalObjects = %d, want 100", stats.TotalObjects)
	}
}

type errorReader struct {
	err error
}

func (r *errorReader) Read(p []byte) (n int, err error) {
	return 0, r.err
}

func TestComputeFingerprintFromReaderError(t *testing.T) {
	expectedErr := io.ErrUnexpectedEOF
	reader := &errorReader{err: expectedErr}

	_, _, err := ComputeFingerprintFromReader(reader)
	if err != expectedErr {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}
}

func TestDeduplicationWriterWriteDeduplicated(t *testing.T) {
	logger := zap.NewNop()
	dedup := NewDeduplicator(logger)
	buf := &bytes.Buffer{}
	threshold := 10
	writer := NewDeduplicationWriter(context.Background(), buf, dedup, "bucket", "key", threshold, logger)

	data := make([]byte, threshold+1)
	writer.Write(data)

	writer2 := NewDeduplicationWriter(context.Background(), buf, dedup, "bucket", "key2", threshold, logger)
	n, err := writer2.Write(data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != len(data) {
		t.Errorf("Write returned %d, want %d", n, len(data))
	}

	stats := dedup.store.GetStats()
	if stats.DuplicateObjects != 1 {
		t.Errorf("DuplicateObjects = %d, want 1", stats.DuplicateObjects)
	}
}

func TestAddObjectError(t *testing.T) {
	logger := zap.NewNop()
	store := NewStore(logger)
	expectedErr := fmt.Errorf("injected error")
	store.SetAddObjectError(expectedErr)

	_, _, err := store.AddObject("bucket", "key", []byte("data"))
	if err != expectedErr {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}
}

func TestProcessWriteError(t *testing.T) {
	logger := zap.NewNop()
	dedup := NewDeduplicator(logger)
	expectedErr := fmt.Errorf("injected error")
	dedup.store.SetAddObjectError(expectedErr)

	_, err := dedup.ProcessWrite(context.Background(), "bucket", "key", []byte("data"))
	if err != expectedErr {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}
}

func TestDeduplicationWriterWriteError(t *testing.T) {
	logger := zap.NewNop()
	dedup := NewDeduplicator(logger)
	buf := &bytes.Buffer{}
	threshold := 1
	writer := NewDeduplicationWriter(context.Background(), buf, dedup, "bucket", "key", threshold, logger)

	expectedErr := fmt.Errorf("injected error")
	dedup.store.SetAddObjectError(expectedErr)

	data := []byte("test data that exceeds threshold")
	_, err := writer.Write(data)
	if err != expectedErr {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}
}

func TestDeduplicationWriterCloseError(t *testing.T) {
	logger := zap.NewNop()
	dedup := NewDeduplicator(logger)
	buf := &bytes.Buffer{}
	threshold := 100
	writer := NewDeduplicationWriter(context.Background(), buf, dedup, "bucket", "key", threshold, logger)

	writer.Write([]byte("small data"))

	expectedErr := fmt.Errorf("injected error")
	dedup.store.SetAddObjectError(expectedErr)

	err := writer.Close()
	if err != expectedErr {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}
}
