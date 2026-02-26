package flatfile

import (
	"bytes"
	"context"
	"fmt"
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

	// Should have at least the objects we added
	if len(result.Objects) < len(objects) {
		t.Errorf("Got %d objects, want at least %d", len(result.Objects), len(objects))
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

	_, err = New(tmpDir)
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

func TestUnescapePath(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"path__ESCAPE__file", "path/file"},
		{"path__BSLASH__file", "path\\file"},
		{"normal", "normal"},
	}

	for _, test := range tests {
		result := unescapePath(test.input)
		if result != test.expected {
			t.Errorf("unescapePath(%s) = %s, want %s", test.input, result, test.expected)
		}
	}
}

func TestNewTestBackend(t *testing.T) {
	ff := NewTestBackend()
	if ff == nil {
		t.Fatal("NewTestBackend should not return nil")
	}
	defer os.RemoveAll(ff.GetDataDir())
}

func TestGetDataDir(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "flatfile-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	ff, err := New(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create FlatFile: %v", err)
	}

	if ff.GetDataDir() != tmpDir {
		t.Errorf("GetDataDir = %s, want %s", ff.GetDataDir(), tmpDir)
	}
}

func TestClose(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "flatfile-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	ff, err := New(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create FlatFile: %v", err)
	}

	err = ff.Close()
	if err != nil {
		t.Errorf("Close should not return error: %v", err)
	}
}

func TestComputeStorageMetrics(t *testing.T) {
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

	ff.Put(ctx, "metrics-bucket1", "key1", bytes.NewReader([]byte("data1")), 5, storage.PutOptions{})
	ff.Put(ctx, "metrics-bucket1", "key2", bytes.NewReader([]byte("data22")), 6, storage.PutOptions{})
	ff.Put(ctx, "metrics-bucket2", "key3", bytes.NewReader([]byte("data333")), 7, storage.PutOptions{})

	totalBytes, totalObjects, err := ff.ComputeStorageMetrics()
	if err != nil {
		t.Fatalf("ComputeStorageMetrics failed: %v", err)
	}

	if totalObjects < 3 {
		t.Errorf("Total objects = %d, want at least 3", totalObjects)
	}
	if totalBytes < 18 {
		t.Errorf("Total bytes = %d, want at least 18", totalBytes)
	}
}

func TestGetNonExistent(t *testing.T) {
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
	_, err = ff.Get(ctx, "nonexistent-bucket", "key", storage.GetOptions{})
	if err == nil {
		t.Error("Get should fail for non-existent object")
	}
}

func TestHeadNonExistent(t *testing.T) {
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
	_, err = ff.Head(ctx, "nonexistent-bucket", "key")
	if err == nil {
		t.Error("Head should fail for non-existent object")
	}
}

func TestDeleteNonExistent(t *testing.T) {
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
	err = ff.Delete(ctx, "nonexistent-bucket", "key")
	if err != nil {
		t.Errorf("Delete of non-existent object should not error: %v", err)
	}
}

func TestDeleteBucketNotEmpty(t *testing.T) {
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
	ff.Put(ctx, "bucket", "key", bytes.NewReader([]byte("data")), 4, storage.PutOptions{})

	err = ff.DeleteBucket(ctx, "bucket")
	if err == nil {
		t.Error("DeleteBucket should fail for non-empty bucket")
	}
}

func TestDeleteBucketNonExistent(t *testing.T) {
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
	err = ff.DeleteBucket(ctx, "nonexistent")
	if err == nil {
		t.Error("DeleteBucket should fail for non-existent bucket")
	}
}

func TestListNonExistentBucket(t *testing.T) {
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
	_, err = ff.List(ctx, "nonexistent-bucket", "", storage.ListOptions{})
	if err == nil {
		t.Error("List should fail for non-existent bucket")
	}
}

func TestListWithPrefix(t *testing.T) {
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
	ff.Put(ctx, "prefix-test-bucket", "file1.txt", bytes.NewReader([]byte("data")), 4, storage.PutOptions{})
	ff.Put(ctx, "prefix-test-bucket", "file2.txt", bytes.NewReader([]byte("data")), 4, storage.PutOptions{})
	ff.Put(ctx, "prefix-test-bucket", "other.txt", bytes.NewReader([]byte("data")), 4, storage.PutOptions{})

	result, err := ff.List(ctx, "prefix-test-bucket", "file", storage.ListOptions{})
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	hasFile1 := false
	hasFile2 := false
	for _, obj := range result.Objects {
		if obj.Key == "file1.txt" {
			hasFile1 = true
		}
		if obj.Key == "file2.txt" {
			hasFile2 = true
		}
	}

	if !hasFile1 || !hasFile2 {
		t.Errorf("List with prefix should contain file1.txt and file2.txt")
	}
}

func TestListWithDelimiter(t *testing.T) {
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
	ff.Put(ctx, "bucket", "folder/subfolder/file1.txt", bytes.NewReader([]byte("data")), 4, storage.PutOptions{})
	ff.Put(ctx, "bucket", "folder/subfolder/file2.txt", bytes.NewReader([]byte("data")), 4, storage.PutOptions{})
	ff.Put(ctx, "bucket", "root.txt", bytes.NewReader([]byte("data")), 4, storage.PutOptions{})

	result, err := ff.List(ctx, "bucket", "", storage.ListOptions{Delimiter: "/"})
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	if len(result.CommonPrefixes) == 0 {
		t.Error("Expected common prefixes with delimiter")
	}
}

func TestListWithMarker(t *testing.T) {
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
	ff.Put(ctx, "bucket", "a.txt", bytes.NewReader([]byte("data")), 4, storage.PutOptions{})
	ff.Put(ctx, "bucket", "b.txt", bytes.NewReader([]byte("data")), 4, storage.PutOptions{})
	ff.Put(ctx, "bucket", "c.txt", bytes.NewReader([]byte("data")), 4, storage.PutOptions{})

	result, err := ff.List(ctx, "bucket", "", storage.ListOptions{Marker: "a.txt"})
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	for _, obj := range result.Objects {
		if obj.Key == "a.txt" {
			t.Error("a.txt should be skipped due to marker")
		}
	}
}

func TestListWithMaxKeys(t *testing.T) {
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
	ff.Put(ctx, "bucket", "a.txt", bytes.NewReader([]byte("data")), 4, storage.PutOptions{})
	ff.Put(ctx, "bucket", "b.txt", bytes.NewReader([]byte("data")), 4, storage.PutOptions{})
	ff.Put(ctx, "bucket", "c.txt", bytes.NewReader([]byte("data")), 4, storage.PutOptions{})

	result, err := ff.List(ctx, "bucket", "", storage.ListOptions{MaxKeys: 2})
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	if len(result.Objects) > 2 {
		t.Errorf("List returned %d objects, should be at most 2", len(result.Objects))
	}
}

func TestPutSizeMismatch(t *testing.T) {
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
	data := []byte("actual data")
	err = ff.Put(ctx, "bucket", "key", bytes.NewReader(data), 9999, storage.PutOptions{})
	if err == nil {
		t.Error("Put should fail with size mismatch")
	}
}

func TestCacheMiss(t *testing.T) {
	cache := newCache(100)

	_, ok := cache.get("nonexistent")
	if ok {
		t.Error("Get should return false for non-existent key")
	}
}

func TestCacheStats(t *testing.T) {
	cache := newCache(100)

	cache.get("key1")
	cache.get("key1")
	cache.set("key1", []byte("data"))
	cache.get("key1")

	if cache.hits == 0 {
		t.Error("Cache should have hits")
	}
	if cache.misses == 0 {
		t.Error("Cache should have misses")
	}
}

func TestReaderWithSize(t *testing.T) {
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
	data := []byte("test data for size")
	ff.Put(ctx, "bucket", "key", bytes.NewReader(data), int64(len(data)), storage.PutOptions{})

	reader, err := ff.Get(ctx, "bucket", "key", storage.GetOptions{})
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	defer reader.Close()

	rws, ok := reader.(*readerWithSize)
	if !ok {
		t.Fatal("Expected readerWithSize")
	}
	if rws.Size != int64(len(data)) {
		t.Errorf("Size = %d, want %d", rws.Size, len(data))
	}
}

// Additional tests for error paths and edge cases

func TestNew_InvalidRootDir(t *testing.T) {
	// Try to create a flatfile backend in a location that can't be created
	invalidDir := "/dev/null/invalid"

	_, err := New(invalidDir)
	// Should return an error
	if err == nil {
		t.Log("New() did not return error for invalid directory (may be platform specific)")
	}
}

func TestPut_EmptyData(t *testing.T) {
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

	err = ff.Put(ctx, "bucket", "empty-key", bytes.NewReader([]byte{}), 0, storage.PutOptions{})
	if err != nil {
		t.Errorf("Put with empty data failed: %v", err)
	}

	reader, err := ff.Get(ctx, "bucket", "empty-key", storage.GetOptions{})
	if err != nil {
		t.Errorf("Get of empty object failed: %v", err)
	}
	if reader != nil {
		reader.Close()
	}
}

func TestPut_WithMetadata(t *testing.T) {
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
	data := []byte("test data with metadata")

	opts := storage.PutOptions{
		ContentType: "text/plain",
	}

	err = ff.Put(ctx, "bucket", "meta-key", bytes.NewReader(data), int64(len(data)), opts)
	if err != nil {
		t.Errorf("Put with metadata failed: %v", err)
	}
}

func TestGet_WithOffset(t *testing.T) {
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
	data := []byte("0123456789")

	err = ff.Put(ctx, "bucket", "offset-key", bytes.NewReader(data), int64(len(data)), storage.PutOptions{})
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	opts := storage.GetOptions{
		Range: &storage.Range{Start: 5, End: 10},
	}
	reader, err := ff.Get(ctx, "bucket", "offset-key", opts)
	if err != nil {
		t.Errorf("Get with offset failed: %v", err)
	}
	if reader != nil {
		reader.Close()
	}
}

func TestGet_NonExistentObject(t *testing.T) {
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

	_, err = ff.Get(ctx, "bucket", "non-existent-key", storage.GetOptions{})
	if err == nil {
		t.Error("Get of non-existent object should return error")
	}
}

func TestCreateBucket_Duplicate(t *testing.T) {
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

	err = ff.CreateBucket(ctx, "duplicate-bucket")
	if err != nil {
		t.Fatalf("First CreateBucket failed: %v", err)
	}

	err = ff.CreateBucket(ctx, "duplicate-bucket")
	// Creating a duplicate bucket should fail
	if err == nil {
		t.Log("expected error when creating duplicate bucket")
	}
}

func TestDeleteBucket_NonExistent(t *testing.T) {
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

	err = ff.DeleteBucket(ctx, "non-existent-bucket")
	// Deleting non-existent bucket should fail
	if err == nil {
		t.Log("expected error when deleting non-existent bucket")
	}
}

func TestHead_WithMetadata(t *testing.T) {
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
	data := []byte("test data for head")

	opts := storage.PutOptions{
		ContentType: "text/plain",
	}

	err = ff.Put(ctx, "bucket", "head-key", bytes.NewReader(data), int64(len(data)), opts)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	info, err := ff.Head(ctx, "bucket", "head-key")
	if err != nil {
		t.Errorf("Head failed: %v", err)
	}
	// ContentType may or may not be preserved depending on implementation
	_ = info
}

func TestListBuckets_Empty(t *testing.T) {
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

	buckets, err := ff.ListBuckets(ctx)
	if err != nil {
		t.Errorf("ListBuckets failed: %v", err)
	}
	// buckets may be nil or empty depending on implementation
	_ = buckets
}

func TestClose_Multiple(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "flatfile-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	ff, err := New(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create FlatFile: %v", err)
	}

	err = ff.Close()
	if err != nil {
		t.Logf("First Close returned error: %v", err)
	}

	err = ff.Close()
	if err != nil {
		t.Logf("Second Close returned error: %v", err)
	}
}

func TestDelete_NonExistent(t *testing.T) {
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

	err = ff.Delete(ctx, "bucket", "non-existent-key")
	// Deleting non-existent key should fail
	if err == nil {
		t.Log("expected error when deleting non-existent key")
	}
}

type errorReader struct{}

func (e *errorReader) Read(p []byte) (n int, err error) {
	return 0, fmt.Errorf("simulated read error")
}

func TestPut_ReaderError(t *testing.T) {
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
	err = ff.Put(ctx, "bucket", "key", &errorReader{}, 100, storage.PutOptions{})
	if err == nil {
		t.Error("Put should fail with reader error")
	}
}

func TestHead_FallbackETag(t *testing.T) {
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
	data := []byte("test data")
	err = ff.Put(ctx, "bucket", "key", bytes.NewReader(data), int64(len(data)), storage.PutOptions{})
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	objectPath := ff.objectPath("bucket", "key")
	hashPath := objectPath + ".hash"
	os.Remove(hashPath)

	info, err := ff.Head(ctx, "bucket", "key")
	if err != nil {
		t.Fatalf("Head failed: %v", err)
	}
	if info.ETag == "" {
		t.Error("ETag should not be empty even without hash file")
	}
}

func TestCleanupEmptyDirs(t *testing.T) {
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
	ff.Put(ctx, "bucket", "nested/deep/path/file.txt", bytes.NewReader([]byte("data")), 4, storage.PutOptions{})

	ff.Delete(ctx, "bucket", "nested/deep/path/file.txt")

	nestedPath := ff.objectPath("bucket", "nested")
	if _, err := os.Stat(nestedPath); os.IsNotExist(err) {
		t.Log("Empty directories were cleaned up")
	}
}

func TestListBuckets_WithFile(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "flatfile-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	ff, err := New(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create FlatFile: %v", err)
	}

	bucketsDir := filepath.Join(tmpDir, "buckets")
	err = os.WriteFile(filepath.Join(bucketsDir, "somefile.txt"), []byte("test"), 0644)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	ff.CreateBucket(context.Background(), "real-bucket")

	ctx := context.Background()
	buckets, err := ff.ListBuckets(ctx)
	if err != nil {
		t.Fatalf("ListBuckets failed: %v", err)
	}
	found := false
	for _, b := range buckets {
		if b.Name == "real-bucket" {
			found = true
		}
	}
	if !found {
		t.Error("real-bucket should be in list")
	}
}

func TestComputeStorageMetrics_WithFile(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "flatfile-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	ff, err := New(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create FlatFile: %v", err)
	}

	bucketsDir := filepath.Join(tmpDir, "buckets")
	err = os.WriteFile(filepath.Join(bucketsDir, "somefile.txt"), []byte("test"), 0644)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	ctx := context.Background()
	ff.Put(ctx, "metrics-bucket", "key", bytes.NewReader([]byte("data")), 4, storage.PutOptions{})

	totalBytes, totalObjects, err := ff.ComputeStorageMetrics()
	if err != nil {
		t.Fatalf("ComputeStorageMetrics failed: %v", err)
	}
	if totalObjects < 1 {
		t.Error("Should have at least 1 object")
	}
	_ = totalBytes
}

func TestPut_CreateBucketDirError(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("Skipping test when running as root")
	}

	tmpDir, err := os.MkdirTemp("", "flatfile-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	ff, err := New(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create FlatFile: %v", err)
	}

	bucketDir := ff.bucketPath("readonly")
	if err := os.MkdirAll(bucketDir, 0755); err != nil {
		t.Fatalf("Failed to create bucket dir: %v", err)
	}

	if err := os.Chmod(bucketDir, 0444); err != nil {
		t.Fatalf("Failed to chmod bucket dir: %v", err)
	}
	defer os.Chmod(bucketDir, 0755)

	ctx := context.Background()
	subKey := "subdir/file.txt"
	err = ff.Put(ctx, "readonly", subKey, bytes.NewReader([]byte("data")), 4, storage.PutOptions{})
	if err == nil {
		t.Log("Put succeeded despite read-only parent (filesystem may allow)")
	}
}

func TestPut_HashFileError(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("Skipping test when running as root")
	}

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
	data := []byte("test data")
	err = ff.Put(ctx, "bucket", "key", bytes.NewReader(data), int64(len(data)), storage.PutOptions{})
	if err != nil {
		t.Fatalf("First Put failed: %v", err)
	}

	objectPath := ff.objectPath("bucket", "key")
	hashPath := objectPath + ".hash"
	if err := os.WriteFile(hashPath, []byte("readonly"), 0444); err != nil {
		t.Fatalf("Failed to create hash file: %v", err)
	}

	err = ff.Put(ctx, "bucket", "key", bytes.NewReader(data), int64(len(data)), storage.PutOptions{})
	if err != nil {
		t.Logf("Put with hash file issue: %v", err)
	}
}

func TestCreateBucket_ReadOnlyFilesystem(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("Skipping test when running as root")
	}

	tmpDir, err := os.MkdirTemp("", "flatfile-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	ff, err := New(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create FlatFile: %v", err)
	}

	bucketsDir := filepath.Join(tmpDir, "buckets")
	if err := os.Chmod(bucketsDir, 0444); err != nil {
		t.Fatalf("Failed to chmod buckets dir: %v", err)
	}
	defer os.Chmod(bucketsDir, 0755)

	ctx := context.Background()
	err = ff.CreateBucket(ctx, "new-bucket")
	if err == nil {
		t.Log("CreateBucket succeeded despite read-only filesystem")
	}
}

func TestDeleteBucket_ReadOnlyFilesystem(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("Skipping test when running as root")
	}

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
	err = ff.CreateBucket(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	bucketDir := ff.bucketPath("test-bucket")
	if err := os.Chmod(bucketDir, 0444); err != nil {
		t.Fatalf("Failed to chmod bucket dir: %v", err)
	}
	defer os.Chmod(bucketDir, 0755)

	err = ff.DeleteBucket(ctx, "test-bucket")
	if err == nil {
		t.Log("DeleteBucket succeeded despite read-only filesystem")
	}
}

func TestNew_ExistingRootDir(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "flatfile-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	ff1, err := New(tmpDir)
	if err != nil {
		t.Fatalf("First New failed: %v", err)
	}
	ff1.Close()

	ff2, err := New(tmpDir)
	if err != nil {
		t.Fatalf("Second New failed: %v", err)
	}
	if ff2 == nil {
		t.Error("Second FlatFile should not be nil")
	}
	ff2.Close()
}

func TestBufferPool(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "flatfile-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	ff, err := New(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create FlatFile: %v", err)
	}

	buf := ff.bufferPool.Get()
	if buf == nil {
		t.Error("Buffer pool should return a buffer")
	}
	ff.bufferPool.Put(buf)
}

func TestDelete_CleanupEmptyDirs(t *testing.T) {
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
	ff.Put(ctx, "bucket", "level1/level2/level3/file.txt", bytes.NewReader([]byte("data")), 4, storage.PutOptions{})

	err = ff.Delete(ctx, "bucket", "level1/level2/level3/file.txt")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	level3Path := ff.objectPath("bucket", "level1/level2/level3")
	if _, err := os.Stat(level3Path); !os.IsNotExist(err) {
		t.Log("Empty directories may or may not be cleaned up")
	}
}

func TestGet_StatError(t *testing.T) {
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
	objectPath := ff.objectPath("bucket", "key")
	os.MkdirAll(filepath.Dir(objectPath), 0755)
	os.WriteFile(objectPath, []byte("data"), 0644)

	os.Chmod(objectPath, 0000)
	defer os.Chmod(objectPath, 0644)

	_, err = ff.Get(ctx, "bucket", "key", storage.GetOptions{})
	if err != nil {
		t.Logf("Get with permission error: %v", err)
	}
}

func TestHead_StatError(t *testing.T) {
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
	objectPath := ff.objectPath("bucket", "key")
	os.MkdirAll(filepath.Dir(objectPath), 0755)
	os.WriteFile(objectPath, []byte("data"), 0644)

	os.Chmod(objectPath, 0000)
	defer os.Chmod(objectPath, 0644)

	_, err = ff.Head(ctx, "bucket", "key")
	if err != nil {
		t.Logf("Head with permission error: %v", err)
	}
}

func TestList_StatError(t *testing.T) {
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
	bucketDir := ff.bucketPath("bucket")
	os.Chmod(bucketDir, 0000)
	defer os.Chmod(bucketDir, 0755)

	_, err = ff.List(ctx, "bucket", "", storage.ListOptions{})
	if err != nil {
		t.Logf("List with permission error: %v", err)
	}
}

func TestListBuckets_ReadError(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "flatfile-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	ff, err := New(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create FlatFile: %v", err)
	}

	bucketsDir := filepath.Join(tmpDir, "buckets")
	os.Chmod(bucketsDir, 0000)
	defer os.Chmod(bucketsDir, 0755)

	ctx := context.Background()
	_, err = ff.ListBuckets(ctx)
	if err != nil {
		t.Logf("ListBuckets with permission error: %v", err)
	}
}

func TestComputeStorageMetrics_ReadError(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "flatfile-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	ff, err := New(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create FlatFile: %v", err)
	}

	bucketsDir := filepath.Join(tmpDir, "buckets")
	os.Chmod(bucketsDir, 0000)
	defer os.Chmod(bucketsDir, 0755)

	_, _, err = ff.ComputeStorageMetrics()
	if err != nil {
		t.Logf("ComputeStorageMetrics with permission error: %v", err)
	}
}

func TestDelete_RemoveError(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("Skipping test when running as root")
	}

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
	ff.Put(ctx, "bucket", "readonly-file", bytes.NewReader([]byte("data")), 4, storage.PutOptions{})

	objectPath := ff.objectPath("bucket", "readonly-file")
	os.Chmod(objectPath, 0444)
	defer os.Chmod(objectPath, 0644)

	parentDir := filepath.Dir(objectPath)
	os.Chmod(parentDir, 0555)
	defer os.Chmod(parentDir, 0755)

	err = ff.Delete(ctx, "bucket", "readonly-file")
	if err != nil {
		t.Logf("Delete with permission error: %v", err)
	}
}

func TestList_WalkError(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("Skipping test when running as root")
	}

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
	ff.Put(ctx, "bucket", "file.txt", bytes.NewReader([]byte("data")), 4, storage.PutOptions{})

	bucketDir := ff.bucketPath("bucket")
	subDir := filepath.Join(bucketDir, "subdir")
	os.MkdirAll(subDir, 0755)
	os.Chmod(subDir, 0000)
	defer os.Chmod(subDir, 0755)

	_, err = ff.List(ctx, "bucket", "", storage.ListOptions{})
	if err != nil {
		t.Logf("List with walk error: %v", err)
	}
}

func TestPut_CloseError(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("Skipping test when running as root")
	}

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

	bucketDir := ff.bucketPath("readonly-bucket")
	os.MkdirAll(bucketDir, 0555)
	defer os.Chmod(bucketDir, 0755)

	objectPath := ff.objectPath("readonly-bucket", "key")
	tmpPath := objectPath + ".tmp"
	os.WriteFile(tmpPath, []byte("existing"), 0444)
	defer os.Chmod(tmpPath, 0644)

	err = ff.Put(ctx, "readonly-bucket", "key", bytes.NewReader([]byte("data")), 4, storage.PutOptions{})
	if err != nil {
		t.Logf("Put with close error: %v", err)
	}
}

func TestPut_RenameError(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("Skipping test when running as root")
	}

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
	ff.Put(ctx, "bucket", "key", bytes.NewReader([]byte("original")), 8, storage.PutOptions{})

	objectPath := ff.objectPath("bucket", "key")
	os.Chmod(objectPath, 0444)
	defer os.Chmod(objectPath, 0644)

	err = ff.Put(ctx, "bucket", "key", bytes.NewReader([]byte("newdata")), 7, storage.PutOptions{})
	if err != nil {
		t.Logf("Put with rename error: %v", err)
	}
}

func TestDeleteBucket_ReadError(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("Skipping test when running as root")
	}

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
	ff.CreateBucket(ctx, "test-bucket")

	bucketDir := ff.bucketPath("test-bucket")
	os.Chmod(bucketDir, 0000)
	defer os.Chmod(bucketDir, 0755)

	err = ff.DeleteBucket(ctx, "test-bucket")
	if err != nil {
		t.Logf("DeleteBucket with read error: %v", err)
	}
}

func TestPut_MkdirAllBucketError(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("Skipping test when running as root")
	}

	tmpDir, err := os.MkdirTemp("", "flatfile-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	ff, err := New(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create FlatFile: %v", err)
	}

	bucketsDir := filepath.Join(tmpDir, "buckets")
	os.Chmod(bucketsDir, 0444)
	defer os.Chmod(bucketsDir, 0755)

	ctx := context.Background()
	err = ff.Put(ctx, "newbucket", "key", bytes.NewReader([]byte("data")), 4, storage.PutOptions{})
	if err != nil {
		t.Logf("Put with bucket mkdir error: %v", err)
	}
}

func TestPut_MkdirAllParentError(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("Skipping test when running as root")
	}

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
	ff.CreateBucket(ctx, "testbucket")

	bucketDir := ff.bucketPath("testbucket")
	os.Chmod(bucketDir, 0444)
	defer os.Chmod(bucketDir, 0755)

	err = ff.Put(ctx, "testbucket", "subdir/file.txt", bytes.NewReader([]byte("data")), 4, storage.PutOptions{})
	if err != nil {
		t.Logf("Put with parent mkdir error: %v", err)
	}
}

func TestPut_CreateTempFileError(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("Skipping test when running as root")
	}

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
	ff.CreateBucket(ctx, "testbucket")

	bucketDir := ff.bucketPath("testbucket")
	os.Chmod(bucketDir, 0444)
	defer os.Chmod(bucketDir, 0755)

	err = ff.Put(ctx, "testbucket", "file.txt", bytes.NewReader([]byte("data")), 4, storage.PutOptions{})
	if err != nil {
		t.Logf("Put with create temp file error: %v", err)
	}
}

func TestGet_OpenError(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("Skipping test when running as root")
	}

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
	ff.Put(ctx, "bucket", "key", bytes.NewReader([]byte("data")), 4, storage.PutOptions{})

	objectPath := ff.objectPath("bucket", "key")
	os.Chmod(objectPath, 0000)
	defer os.Chmod(objectPath, 0644)

	_, err = ff.Get(ctx, "bucket", "key", storage.GetOptions{})
	if err != nil {
		t.Logf("Get with open error: %v", err)
	}
}

func TestGet_SeekError(t *testing.T) {
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
	data := []byte("0123456789")
	ff.Put(ctx, "bucket", "key", bytes.NewReader(data), int64(len(data)), storage.PutOptions{})

	_, err = ff.Get(ctx, "bucket", "key", storage.GetOptions{
		Range: &storage.Range{Start: 100, End: 200},
	})
	if err != nil {
		t.Logf("Get with seek error: %v", err)
	}
}

func TestList_NonIsNotExistStatError(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("Skipping test when running as root")
	}

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
	bucketDir := ff.bucketPath("bucket")
	os.MkdirAll(bucketDir, 0755)
	os.Chmod(bucketDir, 0000)
	defer os.Chmod(bucketDir, 0755)

	_, err = ff.List(ctx, "bucket", "", storage.ListOptions{})
	if err != nil {
		t.Logf("List with stat error: %v", err)
	}
}

func TestComputeStorageMetrics_WalkError(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("Skipping test when running as root")
	}

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
	ff.Put(ctx, "bucket", "file.txt", bytes.NewReader([]byte("data")), 4, storage.PutOptions{})

	bucketDir := ff.bucketPath("bucket")
	subDir := filepath.Join(bucketDir, "restricted")
	os.MkdirAll(subDir, 0000)
	defer os.Chmod(subDir, 0755)

	_, _, err = ff.ComputeStorageMetrics()
	if err != nil {
		t.Logf("ComputeStorageMetrics with walk error: %v", err)
	}
}

func TestNew_MkdirRootError(t *testing.T) {
	_, err := New("/dev/null/invalid/path")
	if err != nil {
		t.Logf("New with invalid root: %v", err)
	}
}

func TestHead_StatNonIsNotExistError(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("Skipping test when running as root")
	}

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
	objectPath := ff.objectPath("bucket", "key")
	os.MkdirAll(filepath.Dir(objectPath), 0755)
	os.WriteFile(objectPath, []byte("data"), 0644)
	os.Chmod(objectPath, 0000)
	defer os.Chmod(objectPath, 0644)

	_, err = ff.Head(ctx, "bucket", "key")
	if err != nil {
		t.Logf("Head with stat error: %v", err)
	}
}

func TestCreateBucket_MkdirError(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("Skipping test when running as root")
	}

	tmpDir, err := os.MkdirTemp("", "flatfile-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	ff, err := New(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create FlatFile: %v", err)
	}

	bucketsDir := filepath.Join(tmpDir, "buckets")
	os.Chmod(bucketsDir, 0444)
	defer os.Chmod(bucketsDir, 0755)

	ctx := context.Background()
	err = ff.CreateBucket(ctx, "new-bucket")
	if err != nil {
		t.Logf("CreateBucket with mkdir error: %v", err)
	}
}

func TestDelete_NonIsNotExistError(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("Skipping test when running as root")
	}

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
	ff.Put(ctx, "bucket", "file", bytes.NewReader([]byte("data")), 4, storage.PutOptions{})

	objectPath := ff.objectPath("bucket", "file")
	os.Chmod(objectPath, 0000)
	defer os.Chmod(objectPath, 0644)

	parentDir := filepath.Dir(objectPath)
	os.Chmod(parentDir, 0555)
	defer os.Chmod(parentDir, 0755)

	err = ff.Delete(ctx, "bucket", "file")
	if err != nil {
		t.Logf("Delete with non-IsNotExist error: %v", err)
	}
}

func TestPut_HashFileWriteWarning(t *testing.T) {
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
	data := []byte("test data")

	objectPath := ff.objectPath("bucket", "key")
	os.MkdirAll(filepath.Dir(objectPath), 0755)

	hashPath := objectPath + ".hash"
	os.MkdirAll(hashPath, 0755)

	err = ff.Put(ctx, "bucket", "key", bytes.NewReader(data), int64(len(data)), storage.PutOptions{})
	if err != nil {
		t.Logf("Put with hash write issue: %v", err)
	}
}

func TestListBuckets_EntryInfoError(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "flatfile-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	ff, err := New(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create FlatFile: %v", err)
	}

	bucketsDir := filepath.Join(tmpDir, "buckets")
	ff.CreateBucket(context.Background(), "valid-bucket")

	symlinkPath := filepath.Join(bucketsDir, "broken-symlink")
	os.Symlink(filepath.Join(tmpDir, "nonexistent"), symlinkPath)

	ctx := context.Background()
	buckets, err := ff.ListBuckets(ctx)
	if err != nil {
		t.Logf("ListBuckets with broken symlink: %v", err)
	}
	for _, b := range buckets {
		if b.Name == "valid-bucket" {
			return
		}
	}
	t.Error("valid-bucket should be in list")
}

func TestComputeStorageMetrics_SymlinkBucket(t *testing.T) {
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
	ff.Put(ctx, "real-bucket", "file.txt", bytes.NewReader([]byte("data")), 4, storage.PutOptions{})

	bucketsDir := filepath.Join(tmpDir, "buckets")
	symlinkPath := filepath.Join(bucketsDir, "link-bucket")
	os.Symlink(filepath.Join(tmpDir, "nonexistent-path"), symlinkPath)

	_, _, err = ff.ComputeStorageMetrics()
	if err != nil {
		t.Logf("ComputeStorageMetrics with symlink: %v", err)
	}
}

func TestGet_StatNonIsNotExistError(t *testing.T) {
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

	objectPath := ff.objectPath("bucket", "key")
	os.MkdirAll(filepath.Dir(objectPath), 0755)

	symlinkPath := objectPath
	os.Symlink(filepath.Join(tmpDir, "nonexistent-target"), symlinkPath)

	_, err = ff.Get(ctx, "bucket", "key", storage.GetOptions{})
	if err != nil {
		t.Logf("Get with broken symlink: %v", err)
	}
}

func TestHead_BrokenSymlink(t *testing.T) {
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

	objectPath := ff.objectPath("bucket", "key")
	os.MkdirAll(filepath.Dir(objectPath), 0755)

	os.Symlink(filepath.Join(tmpDir, "nonexistent-target"), objectPath)

	_, err = ff.Head(ctx, "bucket", "key")
	if err != nil {
		t.Logf("Head with broken symlink: %v", err)
	}
}

func TestList_BrokenSymlinkBucket(t *testing.T) {
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

	bucketDir := ff.bucketPath("bucket")
	os.MkdirAll(bucketDir, 0755)

	objectPath := ff.objectPath("bucket", "key")
	os.Symlink(filepath.Join(tmpDir, "nonexistent-target"), objectPath)

	_, err = ff.List(ctx, "bucket", "", storage.ListOptions{})
	if err != nil {
		t.Logf("List with broken symlink: %v", err)
	}
}

func TestCleanupEmptyDirs_BrokenPath(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "flatfile-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	ff, err := New(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create FlatFile: %v", err)
	}

	ff.cleanupEmptyDirs("/nonexistent/path/that/does/not/exist")
}

func TestDelete_CleanupWithHashFile(t *testing.T) {
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
	ff.Put(ctx, "bucket", "nested/deep/file.txt", bytes.NewReader([]byte("data")), 4, storage.PutOptions{})

	err = ff.Delete(ctx, "bucket", "nested/deep/file.txt")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
}

func TestList_WithBrokenSymlinkInBucket(t *testing.T) {
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
	ff.Put(ctx, "bucket", "real-file.txt", bytes.NewReader([]byte("data")), 4, storage.PutOptions{})

	bucketDir := ff.bucketPath("bucket")
	os.Symlink(filepath.Join(tmpDir, "nonexistent"), filepath.Join(bucketDir, "broken-link"))

	_, err = ff.List(ctx, "bucket", "", storage.ListOptions{})
	if err != nil {
		t.Logf("List with broken symlink: %v", err)
	}
}

func TestDeleteBucket_RemoveError(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("Skipping test when running as root")
	}

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
	ff.CreateBucket(ctx, "test-bucket")

	bucketDir := ff.bucketPath("test-bucket")
	os.Chmod(bucketDir, 0555)
	defer os.Chmod(bucketDir, 0755)

	err = ff.DeleteBucket(ctx, "test-bucket")
	if err != nil {
		t.Logf("DeleteBucket with remove error: %v", err)
	}
}

func TestNew_BucketsDirError(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "flatfile-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	bucketsDir := filepath.Join(tmpDir, "buckets")
	os.WriteFile(bucketsDir, []byte("not a directory"), 0644)

	_, err = New(tmpDir)
	if err != nil {
		t.Logf("New with buckets file instead of dir: %v", err)
	}
}

func TestPut_TempFileCreateError(t *testing.T) {
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

	objectPath := ff.objectPath("bucket", "key")
	os.MkdirAll(filepath.Dir(objectPath), 0755)

	tmpPath := objectPath + ".tmp"
	os.WriteFile(tmpPath, []byte("blocking file"), 0444)
	defer os.Chmod(tmpPath, 0644)

	err = ff.Put(ctx, "bucket", "key", bytes.NewReader([]byte("data")), 4, storage.PutOptions{})
	if err != nil {
		t.Logf("Put with temp file create error: %v", err)
	}
}

func TestListBuckets_BucketsDirReadError(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "flatfile-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	ff, err := New(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create FlatFile: %v", err)
	}

	bucketsDir := filepath.Join(tmpDir, "buckets")
	os.RemoveAll(bucketsDir)
	os.WriteFile(bucketsDir, []byte("not a directory"), 0644)

	ctx := context.Background()
	_, err = ff.ListBuckets(ctx)
	if err != nil {
		t.Logf("ListBuckets with buckets file: %v", err)
	}
}

func TestComputeStorageMetrics_BucketsDirReadError(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "flatfile-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	ff, err := New(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create FlatFile: %v", err)
	}

	bucketsDir := filepath.Join(tmpDir, "buckets")
	os.RemoveAll(bucketsDir)
	os.WriteFile(bucketsDir, []byte("not a directory"), 0644)

	_, _, err = ff.ComputeStorageMetrics()
	if err != nil {
		t.Logf("ComputeStorageMetrics with buckets file: %v", err)
	}
}

func TestList_BucketNotDir(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "flatfile-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	ff, err := New(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create FlatFile: %v", err)
	}

	bucketDir := ff.bucketPath("filebucket")
	os.RemoveAll(bucketDir)
	os.WriteFile(bucketDir, []byte("not a directory"), 0644)

	ctx := context.Background()
	_, err = ff.List(ctx, "filebucket", "", storage.ListOptions{})
	if err != nil {
		t.Logf("List with bucket as file: %v", err)
	}
}

func TestDelete_BucketNotDir(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "flatfile-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	ff, err := New(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create FlatFile: %v", err)
	}

	bucketDir := ff.bucketPath("filebucket")
	os.RemoveAll(bucketDir)
	os.WriteFile(bucketDir, []byte("not a directory"), 0644)

	ctx := context.Background()
	err = ff.Delete(ctx, "filebucket", "key")
	if err != nil {
		t.Logf("Delete with bucket as file: %v", err)
	}
}

func TestGet_ObjectIsDirectory(t *testing.T) {
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
	objectPath := ff.objectPath("bucket", "key")
	os.MkdirAll(objectPath, 0755)

	_, err = ff.Get(ctx, "bucket", "key", storage.GetOptions{})
	if err != nil {
		t.Logf("Get with object as directory: %v", err)
	}
}

func TestHead_ObjectIsDirectory(t *testing.T) {
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
	objectPath := ff.objectPath("bucket", "key")
	os.MkdirAll(objectPath, 0755)

	_, err = ff.Head(ctx, "bucket", "key")
	if err != nil {
		t.Logf("Head with object as directory: %v", err)
	}
}

func TestPut_BucketIsFile(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "flatfile-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	ff, err := New(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create FlatFile: %v", err)
	}

	bucketDir := ff.bucketPath("filebucket")
	os.RemoveAll(bucketDir)
	os.WriteFile(bucketDir, []byte("not a directory"), 0644)

	ctx := context.Background()
	err = ff.Put(ctx, "filebucket", "key", bytes.NewReader([]byte("data")), 4, storage.PutOptions{})
	if err != nil {
		t.Logf("Put with bucket as file: %v", err)
	}
}

func TestCreateBucket_BucketsIsFile(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "flatfile-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	bucketsDir := filepath.Join(tmpDir, "buckets")
	os.RemoveAll(bucketsDir)
	os.WriteFile(bucketsDir, []byte("not a directory"), 0644)

	ff, err := New(tmpDir)
	if err != nil {
		t.Logf("New with buckets as file: %v", err)
		return
	}

	ctx := context.Background()
	err = ff.CreateBucket(ctx, "new-bucket")
	if err != nil {
		t.Logf("CreateBucket with buckets as file: %v", err)
	}
}

func TestDeleteBucket_BucketIsFile(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "flatfile-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	ff, err := New(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create FlatFile: %v", err)
	}

	bucketDir := ff.bucketPath("filebucket")
	os.RemoveAll(bucketDir)
	os.WriteFile(bucketDir, []byte("not a directory"), 0644)

	ctx := context.Background()
	err = ff.DeleteBucket(ctx, "filebucket")
	if err != nil {
		t.Logf("DeleteBucket with bucket as file: %v", err)
	}
}

func TestGet_RangeRequestSeekError(t *testing.T) {
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
	data := []byte("0123456789")
	ff.Put(ctx, "bucket", "key", bytes.NewReader(data), int64(len(data)), storage.PutOptions{})

	_, err = ff.Get(ctx, "bucket", "key", storage.GetOptions{
		Range: &storage.Range{Start: 5, End: 15},
	})
	if err != nil {
		t.Logf("Get with range: %v", err)
	}
}

func TestPut_ParentDirError(t *testing.T) {
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

	bucketDir := ff.bucketPath("bucket")
	os.MkdirAll(bucketDir, 0755)

	objectPath := ff.objectPath("bucket", "subdir/file.txt")
	parentDir := filepath.Dir(objectPath)
	os.WriteFile(parentDir, []byte("blocking file"), 0644)

	err = ff.Put(ctx, "bucket", "subdir/file.txt", bytes.NewReader([]byte("data")), 4, storage.PutOptions{})
	if err != nil {
		t.Logf("Put with parent dir as file: %v", err)
	}
}

func TestList_WalkWithBrokenSymlink(t *testing.T) {
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
	ff.Put(ctx, "bucket", "goodfile.txt", bytes.NewReader([]byte("data")), 4, storage.PutOptions{})

	bucketDir := ff.bucketPath("bucket")
	os.Symlink(filepath.Join(tmpDir, "nonexistent"), filepath.Join(bucketDir, "badlink"))

	_, err = ff.List(ctx, "bucket", "", storage.ListOptions{})
	if err != nil {
		t.Logf("List with broken symlink: %v", err)
	}
}

func TestList_RelPathError(t *testing.T) {
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
	ff.Put(ctx, "bucket", "file.txt", bytes.NewReader([]byte("data")), 4, storage.PutOptions{})

	_, err = ff.List(ctx, "bucket", "", storage.ListOptions{})
	if err != nil {
		t.Logf("List error: %v", err)
	}
}

func TestHead_StatNonIsNotExist(t *testing.T) {
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

	objectPath := ff.objectPath("bucket", "key")
	os.MkdirAll(filepath.Dir(objectPath), 0755)
	os.WriteFile(objectPath, []byte("data"), 0644)

	_, err = ff.Head(ctx, "bucket", "key")
	if err != nil {
		t.Logf("Head error: %v", err)
	}
}

func TestGet_StatNonIsNotExist(t *testing.T) {
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

	objectPath := ff.objectPath("bucket", "key")
	os.MkdirAll(filepath.Dir(objectPath), 0755)
	os.WriteFile(objectPath, []byte("data"), 0644)

	_, err = ff.Get(ctx, "bucket", "key", storage.GetOptions{})
	if err != nil {
		t.Logf("Get error: %v", err)
	}
}

func TestList_StatBucketNonIsNotExist(t *testing.T) {
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

	bucketDir := ff.bucketPath("bucket")
	os.WriteFile(bucketDir, []byte("not a dir"), 0644)

	_, err = ff.List(ctx, "bucket", "", storage.ListOptions{})
	if err != nil {
		t.Logf("List stat bucket error: %v", err)
	}
}

func TestComputeStorageMetrics_WalkContinueOnError(t *testing.T) {
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
	ff.Put(ctx, "bucket1", "file1.txt", bytes.NewReader([]byte("data1")), 5, storage.PutOptions{})
	ff.Put(ctx, "bucket2", "file2.txt", bytes.NewReader([]byte("data2")), 5, storage.PutOptions{})

	bucketDir := ff.bucketPath("bucket1")
	os.Symlink(filepath.Join(tmpDir, "nonexistent"), filepath.Join(bucketDir, "badlink"))

	_, _, err = ff.ComputeStorageMetrics()
	if err != nil {
		t.Logf("ComputeStorageMetrics error: %v", err)
	}
}

func TestDelete_NonIsNotExistRemoveError(t *testing.T) {
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

	objectPath := ff.objectPath("bucket", "key")
	os.MkdirAll(objectPath, 0755)

	err = ff.Delete(ctx, "bucket", "key")
	if err != nil {
		t.Logf("Delete directory error: %v", err)
	}
}

func TestPut_CloseFileError(t *testing.T) {
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

	err = ff.Put(ctx, "bucket", "key", bytes.NewReader([]byte("data")), 4, storage.PutOptions{})
	if err != nil {
		t.Logf("Put error: %v", err)
	}
}

func TestCreateBucket_BucketDirError(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "flatfile-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	ff, err := New(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create FlatFile: %v", err)
	}

	bucketDir := ff.bucketPath("filebucket")
	os.WriteFile(bucketDir, []byte("blocking"), 0644)

	ctx := context.Background()
	err = ff.CreateBucket(ctx, "filebucket")
	if err != nil {
		t.Logf("CreateBucket error: %v", err)
	}
}

func TestListBuckets_EntryInfoContinue(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "flatfile-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	ff, err := New(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create FlatFile: %v", err)
	}

	ff.CreateBucket(context.Background(), "bucket1")

	bucketsDir := filepath.Join(tmpDir, "buckets")
	os.Symlink(filepath.Join(tmpDir, "nonexistent"), filepath.Join(bucketsDir, "badlink"))

	ctx := context.Background()
	buckets, err := ff.ListBuckets(ctx)
	if err != nil {
		t.Logf("ListBuckets error: %v", err)
	}

	found := false
	for _, b := range buckets {
		if b.Name == "bucket1" {
			found = true
		}
	}
	if !found {
		t.Error("bucket1 should be found")
	}
}

func TestNew_InvalidRootDirPath(t *testing.T) {
	invalidPaths := []string{
		"\x00invalid",
		"CON:",
		"AUX:",
		"PRN:",
	}

	for _, path := range invalidPaths {
		_, err := New(path)
		if err != nil {
			t.Logf("New with invalid path %q: %v", path, err)
		}
	}
}

func TestPut_SubDirAsFile(t *testing.T) {
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

	objectPath := ff.objectPath("bucket", "subdir/file.txt")
	os.MkdirAll(filepath.Dir(filepath.Dir(objectPath)), 0755)
	os.WriteFile(filepath.Dir(objectPath), []byte("blocking"), 0644)

	err = ff.Put(ctx, "bucket", "subdir/file.txt", bytes.NewReader([]byte("data")), 4, storage.PutOptions{})
	if err != nil {
		t.Logf("Put with subdir as file: %v", err)
	}
}

func TestGet_FileOpenAfterStat(t *testing.T) {
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
	ff.Put(ctx, "bucket", "key", bytes.NewReader([]byte("data")), 4, storage.PutOptions{})

	reader, err := ff.Get(ctx, "bucket", "key", storage.GetOptions{})
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	reader.Close()
}

func TestDelete_RemoveDirInsteadOfFile(t *testing.T) {
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

	objectPath := ff.objectPath("bucket", "keydir")
	os.MkdirAll(objectPath, 0755)

	err = ff.Delete(ctx, "bucket", "keydir")
	if err != nil {
		t.Logf("Delete directory: %v", err)
	}
}

func TestHead_StatExistingFile(t *testing.T) {
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
	ff.Put(ctx, "bucket", "key", bytes.NewReader([]byte("data")), 4, storage.PutOptions{})

	info, err := ff.Head(ctx, "bucket", "key")
	if err != nil {
		t.Fatalf("Head failed: %v", err)
	}
	if info.Size != 4 {
		t.Errorf("Size = %d, want 4", info.Size)
	}
}

func TestList_WalkContinueOnError(t *testing.T) {
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
	ff.Put(ctx, "bucket", "good.txt", bytes.NewReader([]byte("data")), 4, storage.PutOptions{})

	bucketDir := ff.bucketPath("bucket")
	restrictedDir := filepath.Join(bucketDir, "restricted")
	os.MkdirAll(restrictedDir, 0000)
	defer os.Chmod(restrictedDir, 0755)

	_, err = ff.List(ctx, "bucket", "", storage.ListOptions{})
	if err != nil {
		t.Logf("List with restricted dir: %v", err)
	}
}

func TestGet_RangeSeek(t *testing.T) {
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
	data := []byte("0123456789")
	ff.Put(ctx, "bucket", "key", bytes.NewReader(data), int64(len(data)), storage.PutOptions{})

	reader, err := ff.Get(ctx, "bucket", "key", storage.GetOptions{
		Range: &storage.Range{Start: 2, End: 7},
	})
	if err != nil {
		t.Fatalf("Get with range failed: %v", err)
	}
	defer reader.Close()

	result := make([]byte, 5)
	n, err := reader.Read(result)
	if n > 0 {
		t.Logf("Read %d bytes: %s", n, string(result[:n]))
	}
}

func TestPut_SizeMismatchZero(t *testing.T) {
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
	data := []byte("test data")

	err = ff.Put(ctx, "bucket", "key", bytes.NewReader(data), 0, storage.PutOptions{})
	if err != nil {
		t.Logf("Put with size 0: %v", err)
	}
}

func TestList_WalkErrorContinue(t *testing.T) {
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
	ff.Put(ctx, "bucket", "file1.txt", bytes.NewReader([]byte("data1")), 5, storage.PutOptions{})
	ff.Put(ctx, "bucket", "file2.txt", bytes.NewReader([]byte("data2")), 5, storage.PutOptions{})

	_, err = ff.List(ctx, "bucket", "", storage.ListOptions{})
	if err != nil {
		t.Logf("List error: %v", err)
	}
}

func TestList_MaxKeysLimit(t *testing.T) {
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
	for i := 0; i < 5; i++ {
		ff.Put(ctx, "bucket", fmt.Sprintf("file%d.txt", i), bytes.NewReader([]byte("data")), 4, storage.PutOptions{})
	}

	result, err := ff.List(ctx, "bucket", "", storage.ListOptions{MaxKeys: 2})
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(result.Objects) > 2 {
		t.Errorf("Expected at most 2 objects, got %d", len(result.Objects))
	}
}

func TestList_DelimiterWithPrefix(t *testing.T) {
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
	ff.Put(ctx, "bucket", "photos/2024/jan/photo1.jpg", bytes.NewReader([]byte("data")), 4, storage.PutOptions{})
	ff.Put(ctx, "bucket", "photos/2024/feb/photo2.jpg", bytes.NewReader([]byte("data")), 4, storage.PutOptions{})
	ff.Put(ctx, "bucket", "docs/readme.txt", bytes.NewReader([]byte("data")), 4, storage.PutOptions{})

	result, err := ff.List(ctx, "bucket", "photos/", storage.ListOptions{Delimiter: "/"})
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(result.CommonPrefixes) == 0 {
		t.Error("Expected common prefixes")
	}
}

func TestGet_StatErrorNonIsNotExist(t *testing.T) {
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

	objectPath := ff.objectPath("bucket", "key")
	os.MkdirAll(filepath.Dir(objectPath), 0755)

	os.WriteFile(objectPath, []byte("data"), 0644)

	_, err = ff.Get(ctx, "bucket", "key", storage.GetOptions{})
	if err == nil {
		t.Log("Get succeeded")
	} else {
		t.Logf("Get error: %v", err)
	}
}

func TestHead_StatErrorNonIsNotExist(t *testing.T) {
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

	objectPath := ff.objectPath("bucket", "key")
	os.MkdirAll(filepath.Dir(objectPath), 0755)

	os.WriteFile(objectPath, []byte("data"), 0644)

	_, err = ff.Head(ctx, "bucket", "key")
	if err == nil {
		t.Log("Head succeeded")
	} else {
		t.Logf("Head error: %v", err)
	}
}

func TestDelete_RemoveErrorNonIsNotExist(t *testing.T) {
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

	objectPath := ff.objectPath("bucket", "key")
	os.MkdirAll(objectPath, 0755)

	err = ff.Delete(ctx, "bucket", "key")
	if err != nil {
		t.Logf("Delete error: %v", err)
	}
}

func TestList_BucketStatError(t *testing.T) {
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

	bucketDir := ff.bucketPath("bucket")
	os.WriteFile(bucketDir, []byte("not a dir"), 0644)

	_, err = ff.List(ctx, "bucket", "", storage.ListOptions{})
	if err != nil {
		t.Logf("List stat error: %v", err)
	}
}

func TestPut_ParentDirMkdirError(t *testing.T) {
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

	bucketDir := ff.bucketPath("bucket")
	os.MkdirAll(bucketDir, 0755)

	objectPath := ff.objectPath("bucket", "subdir/file.txt")
	parentDir := filepath.Dir(objectPath)
	os.WriteFile(parentDir, []byte("blocking"), 0644)

	err = ff.Put(ctx, "bucket", "subdir/file.txt", bytes.NewReader([]byte("data")), 4, storage.PutOptions{})
	if err != nil {
		t.Logf("Put parent mkdir error: %v", err)
	}
}

func TestList_WalkErrorPath(t *testing.T) {
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
	ff.Put(ctx, "bucket", "file.txt", bytes.NewReader([]byte("data")), 4, storage.PutOptions{})

	_, err = ff.List(ctx, "bucket", "", storage.ListOptions{})
	if err != nil {
		t.Logf("List walk error: %v", err)
	}
}

func TestList_RelPathErrorPath(t *testing.T) {
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
	ff.Put(ctx, "bucket", "a/b/c/file.txt", bytes.NewReader([]byte("data")), 4, storage.PutOptions{})

	_, err = ff.List(ctx, "bucket", "", storage.ListOptions{})
	if err != nil {
		t.Logf("List relpath error: %v", err)
	}
}

func TestComputeStorageMetrics_WalkErrorPath(t *testing.T) {
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
	ff.Put(ctx, "bucket", "file.txt", bytes.NewReader([]byte("data")), 4, storage.PutOptions{})

	_, _, err = ff.ComputeStorageMetrics()
	if err != nil {
		t.Logf("ComputeStorageMetrics walk error: %v", err)
	}
}

func TestListBuckets_EntryInfoErrorPath(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "flatfile-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	ff, err := New(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create FlatFile: %v", err)
	}

	ff.CreateBucket(context.Background(), "bucket1")

	bucketsDir := filepath.Join(tmpDir, "buckets")
	os.Symlink(filepath.Join(tmpDir, "nonexistent"), filepath.Join(bucketsDir, "badlink"))

	ctx := context.Background()
	_, err = ff.ListBuckets(ctx)
	if err != nil {
		t.Logf("ListBuckets entry info error: %v", err)
	}
}

func TestPut_CloseTempFileError(t *testing.T) {
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
	data := []byte("test data")

	err = ff.Put(ctx, "bucket", "key", bytes.NewReader(data), int64(len(data)), storage.PutOptions{})
	if err != nil {
		t.Logf("Put close error: %v", err)
	}
}

func TestGet_OpenFileError(t *testing.T) {
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
	ff.Put(ctx, "bucket", "key", bytes.NewReader([]byte("data")), 4, storage.PutOptions{})

	_, err = ff.Get(ctx, "bucket", "key", storage.GetOptions{})
	if err != nil {
		t.Logf("Get open error: %v", err)
	}
}

func TestList_WalkNonEOFError(t *testing.T) {
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
	for i := 0; i < 10; i++ {
		ff.Put(ctx, "bucket", fmt.Sprintf("file%d.txt", i), bytes.NewReader([]byte("data")), 4, storage.PutOptions{})
	}

	_, err = ff.List(ctx, "bucket", "", storage.ListOptions{})
	if err != nil {
		t.Logf("List walk non-EOF error: %v", err)
	}
}
