package storage

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"
)

// MockStorageBackend implements StorageBackend interface for testing
type MockStorageBackend struct {
	PutFunc                  func(ctx context.Context, bucket, key string, data io.Reader, size int64, opts PutOptions) error
	GetFunc                  func(ctx context.Context, bucket, key string, opts GetOptions) (io.ReadCloser, error)
	DeleteFunc               func(ctx context.Context, bucket, key string) error
	HeadFunc                 func(ctx context.Context, bucket, key string) (*ObjectInfo, error)
	ListFunc                 func(ctx context.Context, bucket, prefix string, opts ListOptions) (*ListResult, error)
	CreateBucketFunc         func(ctx context.Context, bucket string) error
	DeleteBucketFunc         func(ctx context.Context, bucket string) error
	ListBucketsFunc          func(ctx context.Context) ([]BucketInfo, error)
	ComputeStorageMetricsFunc func() (int64, int64, error)
	CloseFunc                func() error
}

func (m *MockStorageBackend) Put(ctx context.Context, bucket, key string, data io.Reader, size int64, opts PutOptions) error {
	if m.PutFunc != nil {
		return m.PutFunc(ctx, bucket, key, data, size, opts)
	}
	return nil
}

func (m *MockStorageBackend) Get(ctx context.Context, bucket, key string, opts GetOptions) (io.ReadCloser, error) {
	if m.GetFunc != nil {
		return m.GetFunc(ctx, bucket, key, opts)
	}
	return nil, nil
}

func (m *MockStorageBackend) Delete(ctx context.Context, bucket, key string) error {
	if m.DeleteFunc != nil {
		return m.DeleteFunc(ctx, bucket, key)
	}
	return nil
}

func (m *MockStorageBackend) Head(ctx context.Context, bucket, key string) (*ObjectInfo, error) {
	if m.HeadFunc != nil {
		return m.HeadFunc(ctx, bucket, key)
	}
	return nil, nil
}

func (m *MockStorageBackend) List(ctx context.Context, bucket, prefix string, opts ListOptions) (*ListResult, error) {
	if m.ListFunc != nil {
		return m.ListFunc(ctx, bucket, prefix, opts)
	}
	return nil, nil
}

func (m *MockStorageBackend) CreateBucket(ctx context.Context, bucket string) error {
	if m.CreateBucketFunc != nil {
		return m.CreateBucketFunc(ctx, bucket)
	}
	return nil
}

func (m *MockStorageBackend) DeleteBucket(ctx context.Context, bucket string) error {
	if m.DeleteBucketFunc != nil {
		return m.DeleteBucketFunc(ctx, bucket)
	}
	return nil
}

func (m *MockStorageBackend) ListBuckets(ctx context.Context) ([]BucketInfo, error) {
	if m.ListBucketsFunc != nil {
		return m.ListBucketsFunc(ctx)
	}
	return nil, nil
}

func (m *MockStorageBackend) ComputeStorageMetrics() (int64, int64, error) {
	if m.ComputeStorageMetricsFunc != nil {
		return m.ComputeStorageMetricsFunc()
	}
	return 0, 0, nil
}

func (m *MockStorageBackend) Close() error {
	if m.CloseFunc != nil {
		return m.CloseFunc()
	}
	return nil
}

func TestPutResult(t *testing.T) {
	result := PutResult{
		ETag:         "abc123",
		VersionID:    "v1",
		StorageClass: "STANDARD",
	}

	if result.ETag != "abc123" {
		t.Errorf("expected ETag abc123, got %s", result.ETag)
	}
	if result.VersionID != "v1" {
		t.Errorf("expected VersionID v1, got %s", result.VersionID)
	}
	if result.StorageClass != "STANDARD" {
		t.Errorf("expected StorageClass STANDARD, got %s", result.StorageClass)
	}
}

func TestGetResult(t *testing.T) {
	body := io.NopCloser(strings.NewReader("test data"))
	result := GetResult{
		Body:         body,
		ObjectInfo:   &ObjectInfo{Key: "test.txt"},
		Range:        &Range{Start: 0, End: 10},
		AcceptRanges: "bytes",
	}

	if result.ObjectInfo.Key != "test.txt" {
		t.Errorf("expected Key test.txt, got %s", result.ObjectInfo.Key)
	}
	if result.Range.Start != 0 {
		t.Errorf("expected Range.Start 0, got %d", result.Range.Start)
	}
	if result.Range.End != 10 {
		t.Errorf("expected Range.End 10, got %d", result.Range.End)
	}
	if result.AcceptRanges != "bytes" {
		t.Errorf("expected AcceptRanges bytes, got %s", result.AcceptRanges)
	}
}

func TestDeleteOptions(t *testing.T) {
	opts := DeleteOptions{
		VersionID: "v1",
	}

	if opts.VersionID != "v1" {
		t.Errorf("expected VersionID v1, got %s", opts.VersionID)
	}
}

func TestPutOptions(t *testing.T) {
	opts := PutOptions{
		ContentType:     "text/plain",
		ContentEncoding: "gzip",
		CacheControl:    "max-age=3600",
		Metadata:        map[string]string{"key": "value"},
		StorageClass:    "STANDARD",
	}

	if opts.ContentType != "text/plain" {
		t.Errorf("expected ContentType text/plain, got %s", opts.ContentType)
	}
	if opts.ContentEncoding != "gzip" {
		t.Errorf("expected ContentEncoding gzip, got %s", opts.ContentEncoding)
	}
	if opts.CacheControl != "max-age=3600" {
		t.Errorf("expected CacheControl max-age=3600, got %s", opts.CacheControl)
	}
	if opts.Metadata["key"] != "value" {
		t.Errorf("expected Metadata key=value, got %s", opts.Metadata["key"])
	}
	if opts.StorageClass != "STANDARD" {
		t.Errorf("expected StorageClass STANDARD, got %s", opts.StorageClass)
	}
}

func TestGetOptions(t *testing.T) {
	opts := GetOptions{
		Range:             &Range{Start: 0, End: 100},
		IfMatch:           "abc123",
		IfNoneMatch:       "def456",
		IfModifiedSince:   "Wed, 01 Jan 2020 00:00:00 GMT",
		IfUnmodifiedSince: "Thu, 02 Jan 2020 00:00:00 GMT",
	}

	if opts.Range.Start != 0 {
		t.Errorf("expected Range.Start 0, got %d", opts.Range.Start)
	}
	if opts.IfMatch != "abc123" {
		t.Errorf("expected IfMatch abc123, got %s", opts.IfMatch)
	}
	if opts.IfNoneMatch != "def456" {
		t.Errorf("expected IfNoneMatch def456, got %s", opts.IfNoneMatch)
	}
	if opts.IfModifiedSince != "Wed, 01 Jan 2020 00:00:00 GMT" {
		t.Errorf("expected IfModifiedSince Wed, 01 Jan 2020 00:00:00 GMT, got %s", opts.IfModifiedSince)
	}
	if opts.IfUnmodifiedSince != "Thu, 02 Jan 2020 00:00:00 GMT" {
		t.Errorf("expected IfUnmodifiedSince Thu, 02 Jan 2020 00:00:00 GMT, got %s", opts.IfUnmodifiedSince)
	}
}

func TestRange(t *testing.T) {
	r := Range{
		Start: 0,
		End:   100,
	}

	if r.Start != 0 {
		t.Errorf("expected Start 0, got %d", r.Start)
	}
	if r.End != 100 {
		t.Errorf("expected End 100, got %d", r.End)
	}
}

func TestListOptions(t *testing.T) {
	opts := ListOptions{
		Prefix:    "test/",
		Delimiter: "/",
		MaxKeys:   100,
		Marker:    "start-key",
	}

	if opts.Prefix != "test/" {
		t.Errorf("expected Prefix test/, got %s", opts.Prefix)
	}
	if opts.Delimiter != "/" {
		t.Errorf("expected Delimiter /, got %s", opts.Delimiter)
	}
	if opts.MaxKeys != 100 {
		t.Errorf("expected MaxKeys 100, got %d", opts.MaxKeys)
	}
	if opts.Marker != "start-key" {
		t.Errorf("expected Marker start-key, got %s", opts.Marker)
	}
}

func TestObjectInfo(t *testing.T) {
	obj := ObjectInfo{
		Key:            "test.txt",
		Size:           1024,
		ETag:           "abc123",
		LastModified:   time.Now().Unix(),
		ContentType:    "text/plain",
		Metadata:       map[string]string{"key": "value"},
		StorageClass:   "STANDARD",
		VersionID:      "v1",
		IsDeleteMarker: false,
	}

	if obj.Key != "test.txt" {
		t.Errorf("expected Key test.txt, got %s", obj.Key)
	}
	if obj.Size != 1024 {
		t.Errorf("expected Size 1024, got %d", obj.Size)
	}
	if obj.ETag != "abc123" {
		t.Errorf("expected ETag abc123, got %s", obj.ETag)
	}
	if obj.ContentType != "text/plain" {
		t.Errorf("expected ContentType text/plain, got %s", obj.ContentType)
	}
	if obj.StorageClass != "STANDARD" {
		t.Errorf("expected StorageClass STANDARD, got %s", obj.StorageClass)
	}
	if obj.VersionID != "v1" {
		t.Errorf("expected VersionID v1, got %s", obj.VersionID)
	}
	if obj.IsDeleteMarker != false {
		t.Errorf("expected IsDeleteMarker false, got %v", obj.IsDeleteMarker)
	}
}

func TestListResult(t *testing.T) {
	result := ListResult{
		Objects: []ObjectInfo{
			{Key: "obj1.txt"},
			{Key: "obj2.txt"},
		},
		CommonPrefixes: []string{"prefix1/", "prefix2/"},
	}

	if len(result.Objects) != 2 {
		t.Errorf("expected 2 objects, got %d", len(result.Objects))
	}
	if result.Objects[0].Key != "obj1.txt" {
		t.Errorf("expected first object key obj1.txt, got %s", result.Objects[0].Key)
	}
	if len(result.CommonPrefixes) != 2 {
		t.Errorf("expected 2 common prefixes, got %d", len(result.CommonPrefixes))
	}
	if result.CommonPrefixes[0] != "prefix1/" {
		t.Errorf("expected first prefix prefix1/, got %s", result.CommonPrefixes[0])
	}
}

func TestBucketInfo(t *testing.T) {
	bucket := BucketInfo{
		Name:         "test-bucket",
		CreationDate: time.Now().Unix(),
	}

	if bucket.Name != "test-bucket" {
		t.Errorf("expected Name test-bucket, got %s", bucket.Name)
	}
	if bucket.CreationDate == 0 {
		t.Error("expected non-zero CreationDate")
	}
}

func TestBackendAlias(t *testing.T) {
	// Test that Backend is an alias for StorageBackend
	var _ StorageBackend = (*MockStorageBackend)(nil)
	var _ Backend = (*MockStorageBackend)(nil)
}

func TestMockStorageBackend(t *testing.T) {
	ctx := context.Background()

	t.Run("Put", func(t *testing.T) {
		called := false
		mock := &MockStorageBackend{
			PutFunc: func(ctx context.Context, bucket, key string, data io.Reader, size int64, opts PutOptions) error {
				called = true
				if bucket != "test-bucket" {
					t.Errorf("expected bucket test-bucket, got %s", bucket)
				}
				if key != "test-key" {
					t.Errorf("expected key test-key, got %s", key)
				}
				return nil
			},
		}

		err := mock.Put(ctx, "test-bucket", "test-key", nil, 0, PutOptions{})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if !called {
			t.Error("PutFunc was not called")
		}
	})

	t.Run("Get", func(t *testing.T) {
		called := false
		mock := &MockStorageBackend{
			GetFunc: func(ctx context.Context, bucket, key string, opts GetOptions) (io.ReadCloser, error) {
				called = true
				return io.NopCloser(strings.NewReader("test")), nil
			},
		}

		reader, err := mock.Get(ctx, "test-bucket", "test-key", GetOptions{})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if reader != nil {
			reader.Close()
		}
		if !called {
			t.Error("GetFunc was not called")
		}
	})

	t.Run("Delete", func(t *testing.T) {
		called := false
		mock := &MockStorageBackend{
			DeleteFunc: func(ctx context.Context, bucket, key string) error {
				called = true
				return nil
			},
		}

		err := mock.Delete(ctx, "test-bucket", "test-key")
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if !called {
			t.Error("DeleteFunc was not called")
		}
	})

	t.Run("Head", func(t *testing.T) {
		called := false
		mock := &MockStorageBackend{
			HeadFunc: func(ctx context.Context, bucket, key string) (*ObjectInfo, error) {
				called = true
				return &ObjectInfo{Key: key}, nil
			},
		}

		info, err := mock.Head(ctx, "test-bucket", "test-key")
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if info == nil {
			t.Error("expected non-nil info")
		}
		if !called {
			t.Error("HeadFunc was not called")
		}
	})

	t.Run("List", func(t *testing.T) {
		called := false
		mock := &MockStorageBackend{
			ListFunc: func(ctx context.Context, bucket, prefix string, opts ListOptions) (*ListResult, error) {
				called = true
				return &ListResult{Objects: []ObjectInfo{{Key: "test"}}}, nil
			},
		}

		result, err := mock.List(ctx, "test-bucket", "", ListOptions{})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if result == nil {
			t.Error("expected non-nil result")
		}
		if !called {
			t.Error("ListFunc was not called")
		}
	})

	t.Run("CreateBucket", func(t *testing.T) {
		called := false
		mock := &MockStorageBackend{
			CreateBucketFunc: func(ctx context.Context, bucket string) error {
				called = true
				return nil
			},
		}

		err := mock.CreateBucket(ctx, "test-bucket")
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if !called {
			t.Error("CreateBucketFunc was not called")
		}
	})

	t.Run("DeleteBucket", func(t *testing.T) {
		called := false
		mock := &MockStorageBackend{
			DeleteBucketFunc: func(ctx context.Context, bucket string) error {
				called = true
				return nil
			},
		}

		err := mock.DeleteBucket(ctx, "test-bucket")
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if !called {
			t.Error("DeleteBucketFunc was not called")
		}
	})

	t.Run("ListBuckets", func(t *testing.T) {
		called := false
		mock := &MockStorageBackend{
			ListBucketsFunc: func(ctx context.Context) ([]BucketInfo, error) {
				called = true
				return []BucketInfo{{Name: "test-bucket"}}, nil
			},
		}

		buckets, err := mock.ListBuckets(ctx)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if len(buckets) != 1 {
			t.Errorf("expected 1 bucket, got %d", len(buckets))
		}
		if !called {
			t.Error("ListBucketsFunc was not called")
		}
	})

	t.Run("ComputeStorageMetrics", func(t *testing.T) {
		called := false
		mock := &MockStorageBackend{
			ComputeStorageMetricsFunc: func() (int64, int64, error) {
				called = true
				return 1024, 10, nil
			},
		}

		bytes, objects, err := mock.ComputeStorageMetrics()
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if bytes != 1024 {
			t.Errorf("expected 1024 bytes, got %d", bytes)
		}
		if objects != 10 {
			t.Errorf("expected 10 objects, got %d", objects)
		}
		if !called {
			t.Error("ComputeStorageMetricsFunc was not called")
		}
	})

	t.Run("Close", func(t *testing.T) {
		called := false
		mock := &MockStorageBackend{
			CloseFunc: func() error {
				called = true
				return nil
			},
		}

		err := mock.Close()
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if !called {
			t.Error("CloseFunc was not called")
		}
	})
}

func TestMockStorageBackendDefaults(t *testing.T) {
	ctx := context.Background()
	mock := &MockStorageBackend{}

	// All methods should return nil/error-free defaults when functions are not set
	if err := mock.Put(ctx, "b", "k", nil, 0, PutOptions{}); err != nil {
		t.Errorf("Put default failed: %v", err)
	}
	if _, err := mock.Get(ctx, "b", "k", GetOptions{}); err != nil {
		t.Errorf("Get default failed: %v", err)
	}
	if err := mock.Delete(ctx, "b", "k"); err != nil {
		t.Errorf("Delete default failed: %v", err)
	}
	if _, err := mock.Head(ctx, "b", "k"); err != nil {
		t.Errorf("Head default failed: %v", err)
	}
	if _, err := mock.List(ctx, "b", "", ListOptions{}); err != nil {
		t.Errorf("List default failed: %v", err)
	}
	if err := mock.CreateBucket(ctx, "b"); err != nil {
		t.Errorf("CreateBucket default failed: %v", err)
	}
	if err := mock.DeleteBucket(ctx, "b"); err != nil {
		t.Errorf("DeleteBucket default failed: %v", err)
	}
	if _, err := mock.ListBuckets(ctx); err != nil {
		t.Errorf("ListBuckets default failed: %v", err)
	}
	if _, _, err := mock.ComputeStorageMetrics(); err != nil {
		t.Errorf("ComputeStorageMetrics default failed: %v", err)
	}
	if err := mock.Close(); err != nil {
		t.Errorf("Close default failed: %v", err)
	}
}
