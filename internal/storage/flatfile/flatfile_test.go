package flatfile

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/openendpoint/openendpoint/internal/storage"
)

func TestNew(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "flatfile-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	ff, err := New(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create FlatFile: %v", err)
	}

	if ff == nil {
		t.Fatal("FlatFile should not be nil")
	}

	if ff.rootDir != tmpDir {
		t.Errorf("rootDir = %s, want %s", ff.rootDir, tmpDir)
	}
}

func TestPutAndGet(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "flatfile-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	ff, err := New(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create FlatFile: %v", err)
	}

	ctx := context.Background()
	bucket := "test-bucket"
	key := "test-key"
	data := []byte("Hello, World!")

	// Test Put
	err = ff.Put(ctx, bucket, key, bytes.NewReader(data), int64(len(data)), storage.PutOptions{})
	if err != nil {
		t.Fatalf("Failed to put object: %v", err)
	}

	// Test Get
	reader, err := ff.Get(ctx, bucket, key, storage.GetOptions{})
	if err != nil {
		t.Fatalf("Failed to get object: %v", err)
	}
	defer reader.Close()

	gotData := make([]byte, len(data))
	n, err := reader.Read(gotData)
	if err != nil && err.Error() != "EOF" {
		t.Fatalf("Failed to read data: %v", err)
	}

	if n != len(data) {
		t.Errorf("Read %d bytes, want %d", n, len(data))
	}

	if !bytes.Equal(gotData, data) {
		t.Errorf("Got data = %s, want %s", gotData, data)
	}
}

func TestDelete(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "flatfile-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	ff, err := New(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create FlatFile: %v", err)
	}

	ctx := context.Background()
	bucket := "test-bucket"
	key := "test-key"
	data := []byte("test data")

	// Put object
	err = ff.Put(ctx, bucket, key, bytes.NewReader(data), int64(len(data)), storage.PutOptions{})
	if err != nil {
		t.Fatalf("Failed to put object: %v", err)
	}

	// Delete object
	err = ff.Delete(ctx, bucket, key)
	if err != nil {
		t.Fatalf("Failed to delete object: %v", err)
	}

	// Verify deleted
	_, err = ff.Get(ctx, bucket, key, storage.GetOptions{})
	if err == nil {
		t.Error("Expected error when getting deleted object, got nil")
	}
}

func TestHead(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "flatfile-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	ff, err := New(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create FlatFile: %v", err)
	}

	ctx := context.Background()
	bucket := "test-bucket"
	key := "test-key"
	data := []byte("test data")

	// Put object
	err = ff.Put(ctx, bucket, key, bytes.NewReader(data), int64(len(data)), storage.PutOptions{})
	if err != nil {
		t.Fatalf("Failed to put object: %v", err)
	}

	// Test Head
	info, err := ff.Head(ctx, bucket, key)
	if err != nil {
		t.Fatalf("Failed to head object: %v", err)
	}

	if info.Key != key {
		t.Errorf("Key = %s, want %s", info.Key, key)
	}

	if info.Size != int64(len(data)) {
		t.Errorf("Size = %d, want %d", info.Size, len(data))
	}

	if info.ETag == "" {
		t.Error("ETag should not be empty")
	}
}

func TestList(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "flatfile-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	ff, err := New(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create FlatFile: %v", err)
	}

	ctx := context.Background()
	bucket := "test-bucket"

	// Put multiple objects
	objects := map[string][]byte{
		"file1.txt": []byte("data1"),
		"file2.txt": []byte("data2"),
		"file3.txt": []byte("data3"),
	}

	for key, data := range objects {
		err = ff.Put(ctx, bucket, key, bytes.NewReader(data), int64(len(data)), storage.PutOptions{})
		if err != nil {
			t.Fatalf("Failed to put object %s: %v", key, err)
		}
	}

	// Test List
	result, err := ff.List(ctx, bucket, "", storage.ListOptions{})
	if err != nil {
		t.Fatalf("Failed to list objects: %v", err)
	}

	if len(result.Objects) != len(objects) {
		t.Errorf("Got %d objects, want %d", len(result.Objects), len(objects))
	}
}

func TestListBuckets(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "flatfile-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	ff, err := New(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create FlatFile: %v", err)
	}

	ctx := context.Background()

	// Create buckets by putting objects
	buckets := []string{"bucket1", "bucket2", "bucket3"}
	for _, bucket := range buckets {
		err = ff.Put(ctx, bucket, "test-key", bytes.NewReader([]byte("data")), 4, storage.PutOptions{})
		if err != nil {
			t.Fatalf("Failed to put object in bucket %s: %v", bucket, err)
		}
	}

	// Test ListBuckets
	result, err := ff.ListBuckets(ctx)
	if err != nil {
		t.Fatalf("Failed to list buckets: %v", err)
	}

	if len(result) != len(buckets) {
		t.Errorf("Got %d buckets, want %d", len(result), len(buckets))
	}
}

func TestPathTraversalProtection(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "flatfile-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	ff, err := New(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create FlatFile: %v", err)
	}

	ctx := context.Background()

	// Test path traversal attempts
	maliciousInputs := []struct {
		bucket string
		key    string
	}{
		{"../escape", "file.txt"},
		{"bucket", "../../../etc/passwd"},
		{"..\\escape", "file.txt"},
		{"bucket", "..\\..\\..\\windows\\system32"},
	}

	for _, input := range maliciousInputs {
		err = ff.Put(ctx, input.bucket, input.key, bytes.NewReader([]byte("data")), 4, storage.PutOptions{})
		if err != nil {
			// Error is acceptable for malicious input
			continue
		}

		// Verify file is created within the allowed directory
		objectPath := ff.objectPath(input.bucket, input.key)
		absRoot, _ := filepath.Abs(tmpDir)
		absObject, _ := filepath.Abs(objectPath)

		if !filepath.HasPrefix(absObject, absRoot) {
			t.Errorf("Path traversal detected: object created outside root dir: %s", absObject)
		}
	}
}

func TestCreateBucket(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "flatfile-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	ff, err := New(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create FlatFile: %v", err)
	}

	ctx := context.Background()
	bucket := "new-bucket"

	err = ff.CreateBucket(ctx, bucket)
	if err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}

	// Verify bucket directory exists
	bucketPath := ff.bucketPath(bucket)
	if _, err := os.Stat(bucketPath); os.IsNotExist(err) {
		t.Errorf("Bucket directory was not created: %s", bucketPath)
	}
}

func TestDeleteBucket(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "flatfile-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	ff, err := New(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create FlatFile: %v", err)
	}

	ctx := context.Background()
	bucket := "test-bucket"

	// Create bucket
	err = ff.CreateBucket(ctx, bucket)
	if err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}

	// Delete bucket
	err = ff.DeleteBucket(ctx, bucket)
	if err != nil {
		t.Fatalf("Failed to delete bucket: %v", err)
	}

	// Verify bucket directory is deleted
	bucketPath := ff.bucketPath(bucket)
	if _, err := os.Stat(bucketPath); !os.IsNotExist(err) {
		t.Errorf("Bucket directory was not deleted: %s", bucketPath)
	}
}

func TestCache(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "flatfile-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	ff, err := New(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create FlatFile: %v", err)
	}

	// Test cache creation
	cache := newCache(100)
	if cache == nil {
		t.Fatal("Cache should not be nil")
	}

	// Test cache set and get
	testData := []byte("test data")
	cache.set("test-key", testData)

	gotData, ok := cache.get("test-key")
	if !ok {
		t.Error("Cache key not found")
	}

	if !bytes.Equal(gotData, testData) {
		t.Errorf("Cache data = %s, want %s", gotData, testData)
	}

	// Test cache invalidate
	cache.invalidate("test-key")
	_, ok = cache.get("test-key")
	if ok {
		t.Error("Cache key should be invalidated")
	}
}

func TestLRUCacheEviction(t *testing.T) {
	cache := newCache(3)

	// Add 4 items to a cache with max size 3
	cache.set("key1", []byte("data1"))
	cache.set("key2", []byte("data2"))
	cache.set("key3", []byte("data3"))
	cache.set("key4", []byte("data4")) // Should trigger eviction

	// Cache should have at most 3 items
	if len(cache.data) > 3 {
		t.Errorf("Cache has %d items, should have at most 3", len(cache.data))
	}
}

func TestETagGeneration(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "flatfile-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	ff, err := New(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create FlatFile: %v", err)
	}

	ctx := context.Background()
	bucket := "test-bucket"
	key := "test-key"
	data := []byte("test data")

	// Put object
	err = ff.Put(ctx, bucket, key, bytes.NewReader(data), int64(len(data)), storage.PutOptions{})
	if err != nil {
		t.Fatalf("Failed to put object: %v", err)
	}

	// Get ETag
	info1, err := ff.Head(ctx, bucket, key)
	if err != nil {
		t.Fatalf("Failed to head object: %v", err)
	}

	// Get ETag again
	info2, err := ff.Head(ctx, bucket, key)
	if err != nil {
		t.Fatalf("Failed to head object: %v", err)
	}

	// ETags should be consistent
	if info1.ETag != info2.ETag {
		t.Errorf("ETags are inconsistent: %s != %s", info1.ETag, info2.ETag)
	}
}

func TestRangeRequests(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "flatfile-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	ff, err := New(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create FlatFile: %v", err)
	}

	ctx := context.Background()
	bucket := "test-bucket"
	key := "test-key"
	data := []byte("0123456789") // 10 bytes

	// Put object
	err = ff.Put(ctx, bucket, key, bytes.NewReader(data), int64(len(data)), storage.PutOptions{})
	if err != nil {
		t.Fatalf("Failed to put object: %v", err)
	}

	// Get range (bytes 2-5)
	reader, err := ff.Get(ctx, bucket, key, storage.GetOptions{
		Range: &storage.Range{
			Start: 2,
			End:   6,
		},
	})
	if err != nil {
		t.Fatalf("Failed to get object with range: %v", err)
	}
	defer reader.Close()

	gotData := make([]byte, 4)
	n, err := reader.Read(gotData)
	if err != nil && err.Error() != "EOF" {
		t.Fatalf("Failed to read data: %v", err)
	}

	expected := data[2:6]
	if n != len(expected) {
		t.Errorf("Read %d bytes, want %d", n, len(expected))
	}

	if !bytes.Equal(gotData[:n], expected) {
		t.Errorf("Got data = %s, want %s", gotData[:n], expected)
	}
}

func TestConcurrentAccess(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "flatfile-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	ff, err := New(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create FlatFile: %v", err)
	}

	ctx := context.Background()
	bucket := "test-bucket"

	// Concurrent writes
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(id int) {
			key := string(rune('A' + id))
			data := []byte("data from goroutine")
			err := ff.Put(ctx, bucket, key, bytes.NewReader(data), int64(len(data)), storage.PutOptions{})
			if err != nil {
				t.Errorf("Concurrent put failed: %v", err)
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestSanitizePathComponent(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"normal", "normal"},
		{"../escape", "escape"},
		{"..\\escape", "escape"},
		{"path/to/file", "pathtofile"},
		{".../test", ".test"},
	}

	for _, test := range tests {
		result := sanitizePathComponent(test.input)
		if result != test.expected {
			t.Errorf("sanitizePathComponent(%s) = %s, want %s", test.input, result, test.expected)
		}
	}
}

func TestEscapePath(t *testing.T) {
	tests := []struct {
		input    string
		contains string
	}{
		{"path/to/file", "__ESCAPE__"},
		{"path\\to\\file", "__BSLASH__"},
		{"../escape", ""},
	}

	for _, test := range tests {
		result := escapePath(test.input)
		if test.contains != "" && !bytes.Contains([]byte(result), []byte(test.contains)) {
			t.Errorf("escapePath(%s) = %s, should contain %s", test.input, result, test.contains)
		}
	}
}
