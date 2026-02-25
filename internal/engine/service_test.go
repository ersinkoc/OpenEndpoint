package engine

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/openendpoint/openendpoint/internal/metadata"
	"github.com/openendpoint/openendpoint/internal/storage"
	"go.uber.org/zap"
)

// MockStorageBackend implements storage.StorageBackend for testing
type MockStorageBackend struct {
	mu      sync.RWMutex
	objects map[string][]byte
	buckets map[string]bool
}

func NewMockStorageBackend() *MockStorageBackend {
	return &MockStorageBackend{
		objects: make(map[string][]byte),
		buckets: make(map[string]bool),
	}
}

func (m *MockStorageBackend) objectKey(bucket, key string) string {
	return bucket + "/" + key
}

func (m *MockStorageBackend) Put(ctx context.Context, bucket, key string, data io.Reader, size int64, opts storage.PutOptions) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	b, err := io.ReadAll(data)
	if err != nil {
		return err
	}
	m.objects[m.objectKey(bucket, key)] = b
	return nil
}

func (m *MockStorageBackend) Get(ctx context.Context, bucket, key string, opts storage.GetOptions) (io.ReadCloser, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	data, ok := m.objects[m.objectKey(bucket, key)]
	if !ok {
		return nil, io.EOF
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *MockStorageBackend) Delete(ctx context.Context, bucket, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, m.objectKey(bucket, key))
	return nil
}

func (m *MockStorageBackend) Head(ctx context.Context, bucket, key string) (*storage.ObjectInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	data, ok := m.objects[m.objectKey(bucket, key)]
	if !ok {
		return nil, io.EOF
	}
	return &storage.ObjectInfo{
		Key:  key,
		Size: int64(len(data)),
	}, nil
}

func (m *MockStorageBackend) List(ctx context.Context, bucket, prefix string, opts storage.ListOptions) (*storage.ListResult, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var objects []storage.ObjectInfo
	prefixKey := bucket + "/" + prefix
	for k, v := range m.objects {
		if len(k) >= len(prefixKey) && k[:len(prefixKey)] == prefixKey {
			objects = append(objects, storage.ObjectInfo{
				Key:  k[len(bucket)+1:],
				Size: int64(len(v)),
			})
		}
	}
	return &storage.ListResult{Objects: objects}, nil
}

func (m *MockStorageBackend) CreateBucket(ctx context.Context, bucket string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.buckets[bucket] = true
	return nil
}

func (m *MockStorageBackend) DeleteBucket(ctx context.Context, bucket string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.buckets, bucket)
	return nil
}

func (m *MockStorageBackend) ListBuckets(ctx context.Context) ([]storage.BucketInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var buckets []storage.BucketInfo
	for b := range m.buckets {
		buckets = append(buckets, storage.BucketInfo{Name: b})
	}
	return buckets, nil
}

func (m *MockStorageBackend) ComputeStorageMetrics() (int64, int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var totalSize int64
	for _, v := range m.objects {
		totalSize += int64(len(v))
	}
	return totalSize, int64(len(m.objects)), nil
}

func (m *MockStorageBackend) Close() error {
	return nil
}

// MockMetadataStore implements metadata.Store for testing
type MockMetadataStore struct {
	mu          sync.RWMutex
	buckets     map[string]*metadata.BucketMetadata
	objects     map[string]*metadata.ObjectMetadata
	versioning  map[string]*metadata.BucketVersioning
	cors        map[string]*metadata.CORSConfiguration
	policies    map[string]*string
	encryption  map[string]*metadata.BucketEncryption
	tags        map[string]map[string]string
	replication map[string]*metadata.ReplicationConfig
	lifecycle   map[string][]metadata.LifecycleRule
	uploads     map[string][]metadata.MultipartUploadMetadata
	parts       map[string][]metadata.PartMetadata
}

func NewMockMetadataStore() *MockMetadataStore {
	return &MockMetadataStore{
		buckets:     make(map[string]*metadata.BucketMetadata),
		objects:     make(map[string]*metadata.ObjectMetadata),
		versioning:  make(map[string]*metadata.BucketVersioning),
		cors:        make(map[string]*metadata.CORSConfiguration),
		policies:    make(map[string]*string),
		encryption:  make(map[string]*metadata.BucketEncryption),
		tags:        make(map[string]map[string]string),
		replication: make(map[string]*metadata.ReplicationConfig),
		lifecycle:   make(map[string][]metadata.LifecycleRule),
		uploads:     make(map[string][]metadata.MultipartUploadMetadata),
		parts:       make(map[string][]metadata.PartMetadata),
	}
}

func (m *MockMetadataStore) objectKey(bucket, key string) string {
	return bucket + "/" + key
}

func (m *MockMetadataStore) CreateBucket(ctx context.Context, bucket string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.buckets[bucket] = &metadata.BucketMetadata{Name: bucket}
	return nil
}

func (m *MockMetadataStore) DeleteBucket(ctx context.Context, bucket string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.buckets, bucket)
	return nil
}

func (m *MockMetadataStore) GetBucket(ctx context.Context, bucket string) (*metadata.BucketMetadata, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if b, ok := m.buckets[bucket]; ok {
		return b, nil
	}
	return nil, io.EOF
}

func (m *MockMetadataStore) ListBuckets(ctx context.Context) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var buckets []string
	for b := range m.buckets {
		buckets = append(buckets, b)
	}
	return buckets, nil
}

func (m *MockMetadataStore) PutObject(ctx context.Context, bucket, key string, meta *metadata.ObjectMetadata) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objects[m.objectKey(bucket, key)] = meta
	return nil
}

func (m *MockMetadataStore) GetObject(ctx context.Context, bucket, key string, versionID string) (*metadata.ObjectMetadata, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if o, ok := m.objects[m.objectKey(bucket, key)]; ok {
		return o, nil
	}
	return nil, io.EOF
}

func (m *MockMetadataStore) DeleteObject(ctx context.Context, bucket, key string, versionID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, m.objectKey(bucket, key))
	return nil
}

func (m *MockMetadataStore) ListObjects(ctx context.Context, bucket, prefix string, opts metadata.ListOptions) ([]metadata.ObjectMetadata, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var objects []metadata.ObjectMetadata
	prefixKey := bucket + "/" + prefix
	for k, v := range m.objects {
		if len(k) >= len(prefixKey) && k[:len(prefixKey)] == prefixKey {
			objects = append(objects, *v)
		}
	}
	return objects, nil
}

func (m *MockMetadataStore) Close() error {
	return nil
}

// Stub methods to satisfy interface
func (m *MockMetadataStore) CreateMultipartUpload(ctx context.Context, bucket, key, uploadID string, meta *metadata.ObjectMetadata) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.uploads[bucket] = append(m.uploads[bucket], metadata.MultipartUploadMetadata{
		UploadID: uploadID,
		Key:      key,
		Bucket:   bucket,
	})
	return nil
}
func (m *MockMetadataStore) PutPart(ctx context.Context, bucket, key, uploadID string, partNumber int, partMeta *metadata.PartMetadata) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.parts[bucket+":"+uploadID] = append(m.parts[bucket+":"+uploadID], *partMeta)
	return nil
}
func (m *MockMetadataStore) CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []metadata.PartInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.uploads, bucket)
	delete(m.parts, bucket+":"+uploadID)
	return nil
}
func (m *MockMetadataStore) AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.uploads, bucket)
	delete(m.parts, bucket+":"+uploadID)
	return nil
}
func (m *MockMetadataStore) ListParts(ctx context.Context, bucket, key, uploadID string) ([]metadata.PartMetadata, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.parts[bucket+":"+uploadID], nil
}
func (m *MockMetadataStore) ListMultipartUploads(ctx context.Context, bucket, prefix string) ([]metadata.MultipartUploadMetadata, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.uploads[bucket], nil
}
func (m *MockMetadataStore) PutLifecycleRule(ctx context.Context, bucket string, rule *metadata.LifecycleRule) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lifecycle[bucket] = append(m.lifecycle[bucket], *rule)
	return nil
}
func (m *MockMetadataStore) GetLifecycleRules(ctx context.Context, bucket string) ([]metadata.LifecycleRule, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.lifecycle[bucket], nil
}
func (m *MockMetadataStore) DeleteLifecycleRule(ctx context.Context, bucket, ruleID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var newRules []metadata.LifecycleRule
	for _, r := range m.lifecycle[bucket] {
		if r.ID != ruleID {
			newRules = append(newRules, r)
		}
	}
	m.lifecycle[bucket] = newRules
	return nil
}
func (m *MockMetadataStore) PutBucketVersioning(ctx context.Context, bucket string, versioning *metadata.BucketVersioning) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.versioning[bucket] = versioning
	return nil
}
func (m *MockMetadataStore) GetBucketVersioning(ctx context.Context, bucket string) (*metadata.BucketVersioning, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if v, ok := m.versioning[bucket]; ok {
		return v, nil
	}
	return &metadata.BucketVersioning{Status: "Disabled"}, nil
}
func (m *MockMetadataStore) PutBucketCors(ctx context.Context, bucket string, cors *metadata.CORSConfiguration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cors[bucket] = cors
	return nil
}
func (m *MockMetadataStore) GetBucketCors(ctx context.Context, bucket string) (*metadata.CORSConfiguration, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.cors[bucket], nil
}
func (m *MockMetadataStore) DeleteBucketCors(ctx context.Context, bucket string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.cors, bucket)
	return nil
}
func (m *MockMetadataStore) PutBucketPolicy(ctx context.Context, bucket string, policy *string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.policies[bucket] = policy
	return nil
}
func (m *MockMetadataStore) GetBucketPolicy(ctx context.Context, bucket string) (*string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.policies[bucket], nil
}
func (m *MockMetadataStore) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.policies, bucket)
	return nil
}
func (m *MockMetadataStore) PutBucketEncryption(ctx context.Context, bucket string, encryption *metadata.BucketEncryption) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.encryption[bucket] = encryption
	return nil
}
func (m *MockMetadataStore) GetBucketEncryption(ctx context.Context, bucket string) (*metadata.BucketEncryption, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.encryption[bucket], nil
}
func (m *MockMetadataStore) DeleteBucketEncryption(ctx context.Context, bucket string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.encryption, bucket)
	return nil
}
func (m *MockMetadataStore) PutReplicationConfig(ctx context.Context, bucket string, config *metadata.ReplicationConfig) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.replication[bucket] = config
	return nil
}
func (m *MockMetadataStore) GetReplicationConfig(ctx context.Context, bucket string) (*metadata.ReplicationConfig, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.replication[bucket], nil
}
func (m *MockMetadataStore) DeleteReplicationConfig(ctx context.Context, bucket string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.replication, bucket)
	return nil
}
func (m *MockMetadataStore) PutBucketTags(ctx context.Context, bucket string, tags map[string]string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tags[bucket] = tags
	return nil
}
func (m *MockMetadataStore) GetBucketTags(ctx context.Context, bucket string) (map[string]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.tags[bucket], nil
}
func (m *MockMetadataStore) DeleteBucketTags(ctx context.Context, bucket string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.tags, bucket)
	return nil
}
func (m *MockMetadataStore) PutObjectLock(ctx context.Context, bucket string, config *metadata.ObjectLockConfig) error {
	return nil
}
func (m *MockMetadataStore) GetObjectLock(ctx context.Context, bucket string) (*metadata.ObjectLockConfig, error) {
	return nil, nil
}
func (m *MockMetadataStore) DeleteObjectLock(ctx context.Context, bucket string) error {
	return nil
}
func (m *MockMetadataStore) PutObjectRetention(ctx context.Context, bucket, key string, retention *metadata.ObjectRetention) error {
	return nil
}
func (m *MockMetadataStore) GetObjectRetention(ctx context.Context, bucket, key string) (*metadata.ObjectRetention, error) {
	return nil, nil
}
func (m *MockMetadataStore) PutObjectLegalHold(ctx context.Context, bucket, key string, legalHold *metadata.ObjectLegalHold) error {
	return nil
}
func (m *MockMetadataStore) GetObjectLegalHold(ctx context.Context, bucket, key string) (*metadata.ObjectLegalHold, error) {
	return nil, nil
}
func (m *MockMetadataStore) PutPublicAccessBlock(ctx context.Context, bucket string, config *metadata.PublicAccessBlockConfiguration) error {
	return nil
}
func (m *MockMetadataStore) GetPublicAccessBlock(ctx context.Context, bucket string) (*metadata.PublicAccessBlockConfiguration, error) {
	return nil, nil
}
func (m *MockMetadataStore) DeletePublicAccessBlock(ctx context.Context, bucket string) error {
	return nil
}
func (m *MockMetadataStore) PutBucketAccelerate(ctx context.Context, bucket string, config *metadata.BucketAccelerateConfiguration) error {
	return nil
}
func (m *MockMetadataStore) GetBucketAccelerate(ctx context.Context, bucket string) (*metadata.BucketAccelerateConfiguration, error) {
	return nil, nil
}
func (m *MockMetadataStore) DeleteBucketAccelerate(ctx context.Context, bucket string) error {
	return nil
}
func (m *MockMetadataStore) PutBucketInventory(ctx context.Context, bucket, id string, config *metadata.InventoryConfiguration) error {
	return nil
}
func (m *MockMetadataStore) GetBucketInventory(ctx context.Context, bucket, id string) (*metadata.InventoryConfiguration, error) {
	return nil, nil
}
func (m *MockMetadataStore) ListBucketInventory(ctx context.Context, bucket string) ([]metadata.InventoryConfiguration, error) {
	return nil, nil
}
func (m *MockMetadataStore) DeleteBucketInventory(ctx context.Context, bucket, id string) error {
	return nil
}
func (m *MockMetadataStore) PutBucketAnalytics(ctx context.Context, bucket, id string, config *metadata.AnalyticsConfiguration) error {
	return nil
}
func (m *MockMetadataStore) GetBucketAnalytics(ctx context.Context, bucket, id string) (*metadata.AnalyticsConfiguration, error) {
	return nil, nil
}
func (m *MockMetadataStore) ListBucketAnalytics(ctx context.Context, bucket string) ([]metadata.AnalyticsConfiguration, error) {
	return nil, nil
}
func (m *MockMetadataStore) DeleteBucketAnalytics(ctx context.Context, bucket, id string) error {
	return nil
}
func (m *MockMetadataStore) PutPresignedURL(ctx context.Context, url string, req *metadata.PresignedURLRequest) error {
	return nil
}
func (m *MockMetadataStore) GetPresignedURL(ctx context.Context, url string) (*metadata.PresignedURLRequest, error) {
	return nil, nil
}
func (m *MockMetadataStore) DeletePresignedURL(ctx context.Context, url string) error {
	return nil
}
func (m *MockMetadataStore) PutBucketWebsite(ctx context.Context, bucket string, config *metadata.WebsiteConfiguration) error {
	return nil
}
func (m *MockMetadataStore) GetBucketWebsite(ctx context.Context, bucket string) (*metadata.WebsiteConfiguration, error) {
	return nil, nil
}
func (m *MockMetadataStore) DeleteBucketWebsite(ctx context.Context, bucket string) error {
	return nil
}
func (m *MockMetadataStore) PutBucketNotification(ctx context.Context, bucket string, config *metadata.NotificationConfiguration) error {
	return nil
}
func (m *MockMetadataStore) GetBucketNotification(ctx context.Context, bucket string) (*metadata.NotificationConfiguration, error) {
	return nil, nil
}
func (m *MockMetadataStore) DeleteBucketNotification(ctx context.Context, bucket string) error {
	return nil
}
func (m *MockMetadataStore) PutBucketLogging(ctx context.Context, bucket string, config *metadata.LoggingConfiguration) error {
	return nil
}
func (m *MockMetadataStore) GetBucketLogging(ctx context.Context, bucket string) (*metadata.LoggingConfiguration, error) {
	return nil, nil
}
func (m *MockMetadataStore) DeleteBucketLogging(ctx context.Context, bucket string) error {
	return nil
}
func (m *MockMetadataStore) PutBucketLocation(ctx context.Context, bucket string, location string) error {
	return nil
}
func (m *MockMetadataStore) GetBucketLocation(ctx context.Context, bucket string) (string, error) {
	return "", nil
}
func (m *MockMetadataStore) PutBucketOwnershipControls(ctx context.Context, bucket string, config *metadata.OwnershipControls) error {
	return nil
}
func (m *MockMetadataStore) GetBucketOwnershipControls(ctx context.Context, bucket string) (*metadata.OwnershipControls, error) {
	return nil, nil
}
func (m *MockMetadataStore) DeleteBucketOwnershipControls(ctx context.Context, bucket string) error {
	return nil
}
func (m *MockMetadataStore) PutBucketMetrics(ctx context.Context, bucket string, id string, config *metadata.MetricsConfiguration) error {
	return nil
}
func (m *MockMetadataStore) GetBucketMetrics(ctx context.Context, bucket string, id string) (*metadata.MetricsConfiguration, error) {
	return nil, nil
}
func (m *MockMetadataStore) DeleteBucketMetrics(ctx context.Context, bucket string, id string) error {
	return nil
}
func (m *MockMetadataStore) ListBucketMetrics(ctx context.Context, bucket string) ([]metadata.MetricsConfiguration, error) {
	return nil, nil
}

func TestNew(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	svc := New(storage, meta, logger)
	if svc == nil {
		t.Fatal("New() returned nil")
	}
}

func TestObjectService_Close(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	svc := New(storage, meta, logger)
	err := svc.Close()
	if err != nil {
		t.Errorf("Close() error = %v", err)
	}
}

func TestObjectService_ComputeStorageMetrics(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	svc := New(storage, meta, logger)

	size, count, err := svc.ComputeStorageMetrics()
	if err != nil {
		t.Errorf("ComputeStorageMetrics() error = %v", err)
	}
	_ = size
	_ = count
}

func TestObjectService_PutObject(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	// Create bucket first
	meta.CreateBucket(context.Background(), "test-bucket")

	svc := New(storage, meta, logger)

	ctx := context.Background()
	data := bytes.NewReader([]byte("test data"))
	opts := PutObjectOptions{ContentType: "text/plain"}

	result, err := svc.PutObject(ctx, "test-bucket", "test-key", data, opts)
	if err != nil {
		t.Fatalf("PutObject() error = %v", err)
	}
	if result == nil {
		t.Fatal("PutObject() returned nil result")
	}
	if result.ETag == "" {
		t.Error("PutObject() ETag should not be empty")
	}
}

func TestObjectService_PutObject_BucketNotFound(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	svc := New(storage, meta, logger)

	ctx := context.Background()
	data := bytes.NewReader([]byte("test data"))
	opts := PutObjectOptions{ContentType: "text/plain"}

	_, err := svc.PutObject(ctx, "nonexistent", "test-key", data, opts)
	if err == nil {
		t.Error("PutObject() should fail for nonexistent bucket")
	}
}

func TestObjectService_CreateBucket(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	svc := New(storage, meta, logger)

	ctx := context.Background()
	err := svc.CreateBucket(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("CreateBucket() error = %v", err)
	}
}

func TestObjectService_DeleteBucket(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	svc := New(storage, meta, logger)

	ctx := context.Background()
	svc.CreateBucket(ctx, "test-bucket")

	err := svc.DeleteBucket(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("DeleteBucket() error = %v", err)
	}
}

func TestObjectService_ListBuckets(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	svc := New(storage, meta, logger)

	ctx := context.Background()
	svc.CreateBucket(ctx, "bucket1")
	svc.CreateBucket(ctx, "bucket2")

	buckets, err := svc.ListBuckets(ctx)
	if err != nil {
		t.Fatalf("ListBuckets() error = %v", err)
	}
	if len(buckets) != 2 {
		t.Errorf("ListBuckets() returned %d buckets, want 2", len(buckets))
	}
}

func TestObjectService_GetObject(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	ctx := context.Background()
	meta.CreateBucket(ctx, "test-bucket")

	svc := New(storage, meta, logger)

	data := bytes.NewReader([]byte("test data"))
	opts := PutObjectOptions{ContentType: "text/plain"}
	_, err := svc.PutObject(ctx, "test-bucket", "test-key", data, opts)
	if err != nil {
		t.Fatalf("PutObject() error = %v", err)
	}

	result, err := svc.GetObject(ctx, "test-bucket", "test-key", GetObjectOptions{})
	if err != nil {
		t.Fatalf("GetObject() error = %v", err)
	}
	defer result.Body.Close()

	body, err := io.ReadAll(result.Body)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if string(body) != "test data" {
		t.Errorf("GetObject() body = %q, want %q", string(body), "test data")
	}
}

func TestObjectService_GetObject_BucketNotFound(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	svc := New(storage, meta, logger)

	_, err := svc.GetObject(context.Background(), "nonexistent", "key", GetObjectOptions{})
	if err == nil {
		t.Error("GetObject() should fail for nonexistent bucket")
	}
}

func TestObjectService_DeleteObject(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	ctx := context.Background()
	meta.CreateBucket(ctx, "test-bucket")

	svc := New(storage, meta, logger)

	data := bytes.NewReader([]byte("test data"))
	opts := PutObjectOptions{ContentType: "text/plain"}
	_, err := svc.PutObject(ctx, "test-bucket", "test-key", data, opts)
	if err != nil {
		t.Fatalf("PutObject() error = %v", err)
	}

	err = svc.DeleteObject(ctx, "test-bucket", "test-key", DeleteObjectOptions{})
	if err != nil {
		t.Fatalf("DeleteObject() error = %v", err)
	}
}

func TestObjectService_DeleteObject_BucketNotFound(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	svc := New(storage, meta, logger)

	err := svc.DeleteObject(context.Background(), "nonexistent", "key", DeleteObjectOptions{})
	if err == nil {
		t.Error("DeleteObject() should fail for nonexistent bucket")
	}
}

func TestObjectService_HeadObject(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	ctx := context.Background()
	meta.CreateBucket(ctx, "test-bucket")

	svc := New(storage, meta, logger)

	data := bytes.NewReader([]byte("test data"))
	opts := PutObjectOptions{ContentType: "text/plain"}
	_, err := svc.PutObject(ctx, "test-bucket", "test-key", data, opts)
	if err != nil {
		t.Fatalf("PutObject() error = %v", err)
	}

	info, err := svc.HeadObject(ctx, "test-bucket", "test-key")
	if err != nil {
		t.Fatalf("HeadObject() error = %v", err)
	}
	if info.Key != "test-key" {
		t.Errorf("HeadObject() Key = %q, want %q", info.Key, "test-key")
	}
	if info.Size != 9 {
		t.Errorf("HeadObject() Size = %d, want 9", info.Size)
	}
}

func TestObjectService_HeadObject_BucketNotFound(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	svc := New(storage, meta, logger)

	_, err := svc.HeadObject(context.Background(), "nonexistent", "key")
	if err == nil {
		t.Error("HeadObject() should fail for nonexistent bucket")
	}
}

func TestObjectService_ListObjects(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	ctx := context.Background()
	meta.CreateBucket(ctx, "test-bucket")

	svc := New(storage, meta, logger)

	for i := 0; i < 3; i++ {
		data := bytes.NewReader([]byte("test data"))
		opts := PutObjectOptions{ContentType: "text/plain"}
		_, err := svc.PutObject(ctx, "test-bucket", fmt.Sprintf("key%d", i), data, opts)
		if err != nil {
			t.Fatalf("PutObject() error = %v", err)
		}
	}

	result, err := svc.ListObjects(ctx, "test-bucket", ListObjectsOptions{})
	if err != nil {
		t.Fatalf("ListObjects() error = %v", err)
	}
	if len(result.Objects) != 3 {
		t.Errorf("ListObjects() returned %d objects, want 3", len(result.Objects))
	}
}

func TestObjectService_ListObjects_BucketNotFound(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	svc := New(storage, meta, logger)

	_, err := svc.ListObjects(context.Background(), "nonexistent", ListObjectsOptions{})
	if err == nil {
		t.Error("ListObjects() should fail for nonexistent bucket")
	}
}

func TestObjectService_HeadBucket(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	ctx := context.Background()

	svc := New(storage, meta, logger)

	svc.CreateBucket(ctx, "test-bucket")

	err := svc.HeadBucket(ctx, "test-bucket")
	if err != nil {
		t.Errorf("HeadBucket() error = %v", err)
	}
}

func TestObjectService_BucketExists(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	ctx := context.Background()

	svc := New(storage, meta, logger)

	svc.CreateBucket(ctx, "test-bucket")

	exists, err := svc.BucketExists(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("BucketExists() error = %v", err)
	}
	if !exists {
		t.Error("BucketExists() should return true for existing bucket")
	}

	exists, err = svc.BucketExists(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("BucketExists() error = %v", err)
	}
	if exists {
		t.Error("BucketExists() should return false for nonexistent bucket")
	}
}

func TestObjectService_GetBucket(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	ctx := context.Background()

	svc := New(storage, meta, logger)

	svc.CreateBucket(ctx, "test-bucket")

	bucket, err := svc.GetBucket(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("GetBucket() error = %v", err)
	}
	if bucket == nil {
		t.Fatal("GetBucket() returned nil")
	}
	if bucket.Name != "test-bucket" {
		t.Errorf("GetBucket() Name = %q, want %q", bucket.Name, "test-bucket")
	}
}

func TestObjectService_LifecycleRules(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	ctx := context.Background()

	svc := New(storage, meta, logger)

	rule := &metadata.LifecycleRule{
		ID:     "test-rule",
		Prefix: "logs/",
		Status: "Enabled",
	}

	err := svc.PutLifecycleRule(ctx, "test-bucket", rule)
	if err != nil {
		t.Fatalf("PutLifecycleRule() error = %v", err)
	}

	rules, err := svc.GetLifecycleRules(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("GetLifecycleRules() error = %v", err)
	}
	if len(rules) != 1 {
		t.Errorf("GetLifecycleRules() returned %d rules, want 1", len(rules))
	}

	err = svc.DeleteLifecycleRule(ctx, "test-bucket", "test-rule")
	if err != nil {
		t.Fatalf("DeleteLifecycleRule() error = %v", err)
	}
}

func TestObjectService_BucketVersioning(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	ctx := context.Background()

	svc := New(storage, meta, logger)

	versioning := &metadata.BucketVersioning{Status: "Enabled"}

	err := svc.PutBucketVersioning(ctx, "test-bucket", versioning)
	if err != nil {
		t.Fatalf("PutBucketVersioning() error = %v", err)
	}

	result, err := svc.GetBucketVersioning(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("GetBucketVersioning() error = %v", err)
	}
	if result.Status != "Enabled" {
		t.Errorf("GetBucketVersioning() Status = %q, want %q", result.Status, "Enabled")
	}
}

func TestObjectService_BucketCors(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	ctx := context.Background()

	svc := New(storage, meta, logger)

	cors := &metadata.CORSConfiguration{
		CORSRules: []metadata.CORSRule{
			{AllowedMethods: []string{"GET"}, AllowedOrigins: []string{"*"}},
		},
	}

	err := svc.PutBucketCors(ctx, "test-bucket", cors)
	if err != nil {
		t.Fatalf("PutBucketCors() error = %v", err)
	}

	result, err := svc.GetBucketCors(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("GetBucketCors() error = %v", err)
	}
	if len(result.CORSRules) != 1 {
		t.Errorf("GetBucketCors() returned %d rules, want 1", len(result.CORSRules))
	}

	err = svc.DeleteBucketCors(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("DeleteBucketCors() error = %v", err)
	}
}

func TestObjectService_BucketCors_Nil(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	svc := New(storage, meta, logger)

	err := svc.PutBucketCors(context.Background(), "test-bucket", nil)
	if err == nil {
		t.Error("PutBucketCors() should fail for nil config")
	}
}

func TestObjectService_BucketPolicy(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	ctx := context.Background()

	svc := New(storage, meta, logger)

	policy := `{"Version":"2012-10-17"}`
	policyPtr := &policy

	err := svc.PutBucketPolicy(ctx, "test-bucket", policyPtr)
	if err != nil {
		t.Fatalf("PutBucketPolicy() error = %v", err)
	}

	result, err := svc.GetBucketPolicy(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("GetBucketPolicy() error = %v", err)
	}
	if result == nil || *result != policy {
		t.Error("GetBucketPolicy() policy mismatch")
	}

	err = svc.DeleteBucketPolicy(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("DeleteBucketPolicy() error = %v", err)
	}
}

func TestObjectService_BucketPolicy_Nil(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	svc := New(storage, meta, logger)

	err := svc.PutBucketPolicy(context.Background(), "test-bucket", nil)
	if err == nil {
		t.Error("PutBucketPolicy() should fail for nil policy")
	}
}

func TestObjectService_BucketEncryption(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	ctx := context.Background()

	svc := New(storage, meta, logger)

	encryption := &metadata.BucketEncryption{
		Rule: metadata.EncryptionRule{
			Apply: metadata.ApplyEncryptionConfiguration{
				SSEAlgorithm: "AES256",
			},
		},
	}

	err := svc.PutBucketEncryption(ctx, "test-bucket", encryption)
	if err != nil {
		t.Fatalf("PutBucketEncryption() error = %v", err)
	}

	result, err := svc.GetBucketEncryption(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("GetBucketEncryption() error = %v", err)
	}
	if result.Rule.Apply.SSEAlgorithm != "AES256" {
		t.Errorf("GetBucketEncryption() SSEAlgorithm = %q, want %q", result.Rule.Apply.SSEAlgorithm, "AES256")
	}

	err = svc.DeleteBucketEncryption(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("DeleteBucketEncryption() error = %v", err)
	}
}

func TestObjectService_BucketTags(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	ctx := context.Background()

	svc := New(storage, meta, logger)

	tags := map[string]string{"env": "test", "team": "dev"}

	err := svc.PutBucketTags(ctx, "test-bucket", tags)
	if err != nil {
		t.Fatalf("PutBucketTags() error = %v", err)
	}

	result, err := svc.GetBucketTags(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("GetBucketTags() error = %v", err)
	}
	if result["env"] != "test" {
		t.Errorf("GetBucketTags() env = %q, want %q", result["env"], "test")
	}

	err = svc.DeleteBucketTags(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("DeleteBucketTags() error = %v", err)
	}
}

func TestObjectService_ReplicationConfig(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	ctx := context.Background()

	svc := New(storage, meta, logger)

	config := &metadata.ReplicationConfig{
		Role: "arn:aws:iam::123:role/replication",
	}

	err := svc.PutReplicationConfig(ctx, "test-bucket", config)
	if err != nil {
		t.Fatalf("PutReplicationConfig() error = %v", err)
	}

	result, err := svc.GetReplicationConfig(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("GetReplicationConfig() error = %v", err)
	}
	if result.Role != config.Role {
		t.Error("GetReplicationConfig() role mismatch")
	}

	err = svc.DeleteReplicationConfig(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("DeleteReplicationConfig() error = %v", err)
	}
}

func TestObjectService_BucketLifecycle(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	ctx := context.Background()

	svc := New(storage, meta, logger)

	rules := []metadata.LifecycleRule{
		{ID: "rule1", Prefix: "logs/", Status: "Enabled"},
	}

	err := svc.PutBucketLifecycle(ctx, "test-bucket", rules)
	if err != nil {
		t.Fatalf("PutBucketLifecycle() error = %v", err)
	}

	result, err := svc.GetBucketLifecycle(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("GetBucketLifecycle() error = %v", err)
	}
	if len(result) != 1 {
		t.Errorf("GetBucketLifecycle() returned %d rules, want 1", len(result))
	}

	err = svc.PutBucketLifecycle(ctx, "test-bucket", nil)
	if err != nil {
		t.Fatalf("PutBucketLifecycle() with nil error = %v", err)
	}
}

func TestObjectService_CreateMultipartUpload(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	ctx := context.Background()

	svc := New(storage, meta, logger)

	result, err := svc.CreateMultipartUpload(ctx, "test-bucket", "test-key", PutObjectOptions{ContentType: "text/plain"})
	if err != nil {
		t.Fatalf("CreateMultipartUpload() error = %v", err)
	}
	if result == nil {
		t.Fatal("CreateMultipartUpload() returned nil")
	}
	if result.UploadID == "" {
		t.Error("CreateMultipartUpload() UploadID should not be empty")
	}
	if result.Key != "test-key" {
		t.Errorf("CreateMultipartUpload() Key = %q, want %q", result.Key, "test-key")
	}
}

func TestObjectService_InitiateMultipartUpload(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	ctx := context.Background()

	svc := New(storage, meta, logger)

	result, err := svc.InitiateMultipartUpload(ctx, "test-bucket", "test-key", nil)
	if err != nil {
		t.Fatalf("InitiateMultipartUpload() error = %v", err)
	}
	if result == nil {
		t.Fatal("InitiateMultipartUpload() returned nil")
	}
}

func TestObjectService_AbortMultipartUpload(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	ctx := context.Background()

	svc := New(storage, meta, logger)

	_, err := svc.CreateMultipartUpload(ctx, "test-bucket", "test-key", PutObjectOptions{})
	if err != nil {
		t.Fatalf("CreateMultipartUpload() error = %v", err)
	}

	err = svc.AbortMultipartUpload(ctx, "test-bucket", "test-key", "upload-123")
	if err != nil {
		t.Fatalf("AbortMultipartUpload() error = %v", err)
	}
}

func TestObjectService_ListMultipartUpload(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	ctx := context.Background()

	svc := New(storage, meta, logger)

	_, err := svc.CreateMultipartUpload(ctx, "test-bucket", "test-key", PutObjectOptions{})
	if err != nil {
		t.Fatalf("CreateMultipartUpload() error = %v", err)
	}

	result, err := svc.ListMultipartUpload(ctx, "test-bucket", "")
	if err != nil {
		t.Fatalf("ListMultipartUpload() error = %v", err)
	}
	if result == nil {
		t.Fatal("ListMultipartUpload() returned nil")
	}
}

func TestObjectService_ListParts(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	ctx := context.Background()

	svc := New(storage, meta, logger)

	result, err := svc.ListParts(ctx, "test-bucket", "test-key", "upload-123")
	if err != nil {
		t.Fatalf("ListParts() error = %v", err)
	}
	_ = result
}

func TestObjectService_ObjectLock(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	ctx := context.Background()

	svc := New(storage, meta, logger)

	config := &metadata.ObjectLockConfig{Enabled: true}
	err := svc.PutObjectLock(ctx, "test-bucket", config)
	if err != nil {
		t.Fatalf("PutObjectLock() error = %v", err)
	}

	result, err := svc.GetObjectLock(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("GetObjectLock() error = %v", err)
	}
	_ = result

	err = svc.DeleteObjectLock(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("DeleteObjectLock() error = %v", err)
	}
}

func TestObjectService_ObjectRetention(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	ctx := context.Background()

	svc := New(storage, meta, logger)

	retention := &metadata.ObjectRetention{Mode: "GOVERNANCE"}
	err := svc.PutObjectRetention(ctx, "test-bucket", "test-key", retention)
	if err != nil {
		t.Fatalf("PutObjectRetention() error = %v", err)
	}

	result, err := svc.GetObjectRetention(ctx, "test-bucket", "test-key")
	if err != nil {
		t.Fatalf("GetObjectRetention() error = %v", err)
	}
	_ = result
}

func TestObjectService_ObjectLegalHold(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	ctx := context.Background()

	svc := New(storage, meta, logger)

	legalHold := &metadata.ObjectLegalHold{Status: "ON"}
	err := svc.PutObjectLegalHold(ctx, "test-bucket", "test-key", legalHold)
	if err != nil {
		t.Fatalf("PutObjectLegalHold() error = %v", err)
	}

	result, err := svc.GetObjectLegalHold(ctx, "test-bucket", "test-key")
	if err != nil {
		t.Fatalf("GetObjectLegalHold() error = %v", err)
	}
	_ = result
}

func TestObjectService_PublicAccessBlock(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	ctx := context.Background()

	svc := New(storage, meta, logger)

	config := &metadata.PublicAccessBlockConfiguration{
		BlockPublicAcls:       true,
		BlockPublicPolicy:     true,
		IgnorePublicAcls:      true,
		RestrictPublicBuckets: true,
	}
	err := svc.PutPublicAccessBlock(ctx, "test-bucket", config)
	if err != nil {
		t.Fatalf("PutPublicAccessBlock() error = %v", err)
	}

	result, err := svc.GetPublicAccessBlock(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("GetPublicAccessBlock() error = %v", err)
	}
	_ = result

	err = svc.DeletePublicAccessBlock(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("DeletePublicAccessBlock() error = %v", err)
	}
}

func TestObjectService_BucketAccelerate(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	ctx := context.Background()

	svc := New(storage, meta, logger)

	config := &metadata.BucketAccelerateConfiguration{Status: "Enabled"}
	err := svc.PutBucketAccelerate(ctx, "test-bucket", config)
	if err != nil {
		t.Fatalf("PutBucketAccelerate() error = %v", err)
	}

	result, err := svc.GetBucketAccelerate(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("GetBucketAccelerate() error = %v", err)
	}
	_ = result

	err = svc.DeleteBucketAccelerate(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("DeleteBucketAccelerate() error = %v", err)
	}
}

func TestObjectService_BucketInventory(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	ctx := context.Background()

	svc := New(storage, meta, logger)

	config := &metadata.InventoryConfiguration{ID: "inventory-1"}
	err := svc.PutBucketInventory(ctx, "test-bucket", "inventory-1", config)
	if err != nil {
		t.Fatalf("PutBucketInventory() error = %v", err)
	}

	result, err := svc.GetBucketInventory(ctx, "test-bucket", "inventory-1")
	if err != nil {
		t.Fatalf("GetBucketInventory() error = %v", err)
	}
	_ = result

	list, err := svc.ListBucketInventory(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("ListBucketInventory() error = %v", err)
	}
	_ = list

	err = svc.DeleteBucketInventory(ctx, "test-bucket", "inventory-1")
	if err != nil {
		t.Fatalf("DeleteBucketInventory() error = %v", err)
	}
}

func TestObjectService_BucketAnalytics(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	ctx := context.Background()

	svc := New(storage, meta, logger)

	config := &metadata.AnalyticsConfiguration{ID: "analytics-1"}
	err := svc.PutBucketAnalytics(ctx, "test-bucket", "analytics-1", config)
	if err != nil {
		t.Fatalf("PutBucketAnalytics() error = %v", err)
	}

	result, err := svc.GetBucketAnalytics(ctx, "test-bucket", "analytics-1")
	if err != nil {
		t.Fatalf("GetBucketAnalytics() error = %v", err)
	}
	_ = result

	list, err := svc.ListBucketAnalytics(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("ListBucketAnalytics() error = %v", err)
	}
	_ = list

	err = svc.DeleteBucketAnalytics(ctx, "test-bucket", "analytics-1")
	if err != nil {
		t.Fatalf("DeleteBucketAnalytics() error = %v", err)
	}
}

func TestObjectService_BucketWebsite(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	ctx := context.Background()

	svc := New(storage, meta, logger)

	config := &metadata.WebsiteConfiguration{IndexDocument: &metadata.IndexDocument{Suffix: "index.html"}}
	err := svc.PutBucketWebsite(ctx, "test-bucket", config)
	if err != nil {
		t.Fatalf("PutBucketWebsite() error = %v", err)
	}

	result, err := svc.GetBucketWebsite(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("GetBucketWebsite() error = %v", err)
	}
	_ = result

	err = svc.DeleteBucketWebsite(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("DeleteBucketWebsite() error = %v", err)
	}
}

func TestObjectService_BucketNotification(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	ctx := context.Background()

	svc := New(storage, meta, logger)

	config := &metadata.NotificationConfiguration{}
	err := svc.PutBucketNotification(ctx, "test-bucket", config)
	if err != nil {
		t.Fatalf("PutBucketNotification() error = %v", err)
	}

	result, err := svc.GetBucketNotification(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("GetBucketNotification() error = %v", err)
	}
	_ = result

	err = svc.DeleteBucketNotification(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("DeleteBucketNotification() error = %v", err)
	}
}

func TestObjectService_BucketLogging(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	ctx := context.Background()

	svc := New(storage, meta, logger)

	config := &metadata.LoggingConfiguration{TargetBucket: "log-bucket"}
	err := svc.PutBucketLogging(ctx, "test-bucket", config)
	if err != nil {
		t.Fatalf("PutBucketLogging() error = %v", err)
	}

	result, err := svc.GetBucketLogging(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("GetBucketLogging() error = %v", err)
	}
	_ = result

	err = svc.DeleteBucketLogging(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("DeleteBucketLogging() error = %v", err)
	}
}

func TestObjectService_BucketLocation(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	ctx := context.Background()

	svc := New(storage, meta, logger)

	err := svc.PutBucketLocation(ctx, "test-bucket", "us-east-1")
	if err != nil {
		t.Fatalf("PutBucketLocation() error = %v", err)
	}

	result, err := svc.GetBucketLocation(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("GetBucketLocation() error = %v", err)
	}
	_ = result
}

func TestObjectService_BucketOwnershipControls(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	ctx := context.Background()

	svc := New(storage, meta, logger)

	config := &metadata.OwnershipControls{}
	err := svc.PutBucketOwnershipControls(ctx, "test-bucket", config)
	if err != nil {
		t.Fatalf("PutBucketOwnershipControls() error = %v", err)
	}

	result, err := svc.GetBucketOwnershipControls(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("GetBucketOwnershipControls() error = %v", err)
	}
	_ = result

	err = svc.DeleteBucketOwnershipControls(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("DeleteBucketOwnershipControls() error = %v", err)
	}
}

func TestObjectService_BucketMetrics(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	ctx := context.Background()

	svc := New(storage, meta, logger)

	config := &metadata.MetricsConfiguration{ID: "metrics-1"}
	err := svc.PutBucketMetrics(ctx, "test-bucket", "metrics-1", config)
	if err != nil {
		t.Fatalf("PutBucketMetrics() error = %v", err)
	}

	result, err := svc.GetBucketMetrics(ctx, "test-bucket", "metrics-1")
	if err != nil {
		t.Fatalf("GetBucketMetrics() error = %v", err)
	}
	_ = result

	list, err := svc.ListBucketMetrics(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("ListBucketMetrics() error = %v", err)
	}
	_ = list

	err = svc.DeleteBucketMetrics(ctx, "test-bucket", "metrics-1")
	if err != nil {
		t.Fatalf("DeleteBucketMetrics() error = %v", err)
	}
}

func TestObjectService_PresignedURL(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	ctx := context.Background()

	svc := New(storage, meta, logger)

	url, err := svc.GeneratePresignedURL(ctx, "test-bucket", "test-key", "GET", 3600)
	if err != nil {
		t.Fatalf("GeneratePresignedURL() error = %v", err)
	}
	_ = url

	result, err := svc.ValidatePresignedURL(ctx, "http://example.com/signed-url")
	if err != nil {
		t.Fatalf("ValidatePresignedURL() error = %v", err)
	}
	_ = result

	err = svc.DeletePresignedURL(ctx, "http://example.com/signed-url")
	if err != nil {
		t.Fatalf("DeletePresignedURL() error = %v", err)
	}
}

func TestObjectService_CopyObject(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	ctx := context.Background()
	meta.CreateBucket(ctx, "src-bucket")
	meta.CreateBucket(ctx, "dst-bucket")

	svc := New(storage, meta, logger)

	data := bytes.NewReader([]byte("test data"))
	opts := PutObjectOptions{ContentType: "text/plain"}
	_, err := svc.PutObject(ctx, "src-bucket", "src-key", data, opts)
	if err != nil {
		t.Fatalf("PutObject() error = %v", err)
	}

	result, err := svc.CopyObject(ctx, "src-bucket", "src-key", "dst-bucket", "dst-key")
	if err != nil {
		t.Fatalf("CopyObject() error = %v", err)
	}
	if result == nil {
		t.Fatal("CopyObject() returned nil")
	}
}

func TestObjectService_CopyObject_SourceBucketNotFound(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	svc := New(storage, meta, logger)

	_, err := svc.CopyObject(context.Background(), "nonexistent", "src-key", "dst-bucket", "dst-key")
	if err == nil {
		t.Error("CopyObject() should fail for nonexistent source bucket")
	}
}

func TestObjectService_CopyObject_DestBucketNotFound(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	ctx := context.Background()
	meta.CreateBucket(ctx, "src-bucket")

	svc := New(storage, meta, logger)

	_, err := svc.CopyObject(ctx, "src-bucket", "src-key", "nonexistent", "dst-key")
	if err == nil {
		t.Error("CopyObject() should fail for nonexistent destination bucket")
	}
}

func TestObjectService_GetObjectAttributes(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	ctx := context.Background()
	meta.CreateBucket(ctx, "test-bucket")

	svc := New(storage, meta, logger)

	data := bytes.NewReader([]byte("test data"))
	opts := PutObjectOptions{ContentType: "text/plain"}
	_, err := svc.PutObject(ctx, "test-bucket", "test-key", data, opts)
	if err != nil {
		t.Fatalf("PutObject() error = %v", err)
	}

	result, err := svc.GetObjectAttributes(ctx, "test-bucket", "test-key", "")
	if err != nil {
		t.Fatalf("GetObjectAttributes() error = %v", err)
	}
	if result == nil {
		t.Fatal("GetObjectAttributes() returned nil")
	}
	if result.Size != 9 {
		t.Errorf("GetObjectAttributes() Size = %d, want 9", result.Size)
	}
}

func TestObjectService_GetObjectAttributes_BucketNotFound(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	svc := New(storage, meta, logger)

	_, err := svc.GetObjectAttributes(context.Background(), "nonexistent", "key", "")
	if err == nil {
		t.Error("GetObjectAttributes() should fail for nonexistent bucket")
	}
}

func TestObjectService_CompleteMultipartUpload(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	ctx := context.Background()

	svc := New(storage, meta, logger)

	uploadResult, err := svc.CreateMultipartUpload(ctx, "test-bucket", "test-key", PutObjectOptions{})
	if err != nil {
		t.Fatalf("CreateMultipartUpload() error = %v", err)
	}

	parts := []PartInfo{{PartNumber: 1, ETag: "etag1"}}
	result, err := svc.CompleteMultipartUpload(ctx, "test-bucket", "test-key", uploadResult.UploadID, parts)
	if err != nil {
		t.Fatalf("CompleteMultipartUpload() error = %v", err)
	}
	_ = result
}

func TestObjectService_SelectObjectContent(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	ctx := context.Background()
	meta.CreateBucket(ctx, "test-bucket")

	svc := New(storage, meta, logger)

	data := bytes.NewReader([]byte("name,age\njohn,30\njane,25"))
	opts := PutObjectOptions{ContentType: "text/csv"}
	_, err := svc.PutObject(ctx, "test-bucket", "test.csv", data, opts)
	if err != nil {
		t.Fatalf("PutObject() error = %v", err)
	}

	result, err := svc.SelectObjectContent(ctx, "test-bucket", "test.csv", "SELECT * FROM s3object")
	if err != nil {
		t.Fatalf("SelectObjectContent() error = %v", err)
	}
	if result == nil {
		t.Fatal("SelectObjectContent() returned nil")
	}
}

func TestObjectService_SelectObjectContent_BucketNotFound(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	svc := New(storage, meta, logger)

	_, err := svc.SelectObjectContent(context.Background(), "nonexistent", "key", "SELECT *")
	if err == nil {
		t.Error("SelectObjectContent() should fail for nonexistent bucket")
	}
}

func TestObjectService_SelectObjectContent_ObjectNotFound(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	ctx := context.Background()
	meta.CreateBucket(ctx, "test-bucket")

	svc := New(storage, meta, logger)

	_, err := svc.SelectObjectContent(ctx, "test-bucket", "nonexistent.csv", "SELECT *")
	if err == nil {
		t.Error("SelectObjectContent() should fail for nonexistent object")
	}
}

func TestObjectService_UploadPart(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	ctx := context.Background()

	svc := New(storage, meta, logger)

	uploadResult, err := svc.CreateMultipartUpload(ctx, "test-bucket", "test-key", PutObjectOptions{})
	if err != nil {
		t.Fatalf("CreateMultipartUpload() error = %v", err)
	}

	data := bytes.NewReader([]byte("part data"))
	result, err := svc.UploadPart(ctx, "test-bucket", "test-key", uploadResult.UploadID, 1, data)
	if err != nil {
		t.Fatalf("UploadPart() error = %v", err)
	}
	if result == nil {
		t.Fatal("UploadPart() returned nil")
	}
	if result.ETag == "" {
		t.Error("UploadPart() ETag should not be empty")
	}
}

func TestObjectService_PutPart(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	ctx := context.Background()

	svc := New(storage, meta, logger)

	uploadResult, err := svc.CreateMultipartUpload(ctx, "test-bucket", "test-key", PutObjectOptions{})
	if err != nil {
		t.Fatalf("CreateMultipartUpload() error = %v", err)
	}

	err = svc.PutPart(ctx, "test-bucket", "test-key", uploadResult.UploadID, 1, []byte("part data"))
	if err != nil {
		t.Fatalf("PutPart() error = %v", err)
	}
}

func TestObjectService_CompleteMultipartUploadWithParts(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	ctx := context.Background()

	svc := New(storage, meta, logger)

	uploadResult, err := svc.CreateMultipartUpload(ctx, "test-bucket", "test-key", PutObjectOptions{})
	if err != nil {
		t.Fatalf("CreateMultipartUpload() error = %v", err)
	}

	data1 := bytes.NewReader([]byte("part one "))
	_, err = svc.UploadPart(ctx, "test-bucket", "test-key", uploadResult.UploadID, 1, data1)
	if err != nil {
		t.Fatalf("UploadPart(1) error = %v", err)
	}

	data2 := bytes.NewReader([]byte("part two"))
	_, err = svc.UploadPart(ctx, "test-bucket", "test-key", uploadResult.UploadID, 2, data2)
	if err != nil {
		t.Fatalf("UploadPart(2) error = %v", err)
	}

	parts := []PartInfo{
		{PartNumber: 1, ETag: "etag1"},
		{PartNumber: 2, ETag: "etag2"},
	}
	result, err := svc.CompleteMultipartUpload(ctx, "test-bucket", "test-key", uploadResult.UploadID, parts)
	if err != nil {
		t.Fatalf("CompleteMultipartUpload() error = %v", err)
	}
	if result == nil {
		t.Fatal("CompleteMultipartUpload() returned nil")
	}
	if result.Size != 17 {
		t.Errorf("CompleteMultipartUpload() Size = %d, want 17", result.Size)
	}
}

func TestObjectService_CompleteMultipartUpload_NoParts(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	ctx := context.Background()

	svc := New(storage, meta, logger)

	uploadResult, err := svc.CreateMultipartUpload(ctx, "test-bucket", "test-key", PutObjectOptions{})
	if err != nil {
		t.Fatalf("CreateMultipartUpload() error = %v", err)
	}

	parts := []PartInfo{}
	_, err = svc.CompleteMultipartUpload(ctx, "test-bucket", "test-key", uploadResult.UploadID, parts)
	if err != nil {
		t.Logf("CompleteMultipartUpload() with no parts: %v", err)
	}
}

func TestObjectService_DeleteBucketWithObjects(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	ctx := context.Background()

	svc := New(storage, meta, logger)

	svc.CreateBucket(ctx, "test-bucket")

	data := bytes.NewReader([]byte("test data"))
	opts := PutObjectOptions{ContentType: "text/plain"}
	_, err := svc.PutObject(ctx, "test-bucket", "test-key", data, opts)
	if err != nil {
		t.Fatalf("PutObject() error = %v", err)
	}

	err = svc.DeleteBucket(ctx, "test-bucket")
	if err != nil {
		t.Logf("DeleteBucket() with objects: %v", err)
	}
}

func TestObjectService_CreateBucketAlreadyExists(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	ctx := context.Background()

	svc := New(storage, meta, logger)

	err := svc.CreateBucket(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("CreateBucket() first call error = %v", err)
	}

	err = svc.CreateBucket(ctx, "test-bucket")
	if err != nil {
		t.Logf("CreateBucket() second call: %v", err)
	}
}

func TestObjectService_GeneratePresignedURLErrors(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	svc := New(storage, meta, logger)

	_, err := svc.GeneratePresignedURL(context.Background(), "", "key", "GET", 3600)
	if err == nil {
		t.Error("GeneratePresignedURL() should fail with empty bucket")
	}

	_, err = svc.GeneratePresignedURL(context.Background(), "bucket", "", "GET", 3600)
	if err == nil {
		t.Error("GeneratePresignedURL() should fail with empty key")
	}
}

func TestObjectService_GetObjectAttributesWithVersion(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	ctx := context.Background()
	meta.CreateBucket(ctx, "test-bucket")

	svc := New(storage, meta, logger)

	data := bytes.NewReader([]byte("test data"))
	opts := PutObjectOptions{ContentType: "text/plain"}
	_, err := svc.PutObject(ctx, "test-bucket", "test-key", data, opts)
	if err != nil {
		t.Fatalf("PutObject() error = %v", err)
	}

	result, err := svc.GetObjectAttributes(ctx, "test-bucket", "test-key", "version-123")
	if err != nil {
		t.Fatalf("GetObjectAttributes() error = %v", err)
	}
	_ = result
}

func TestObjectService_GetObjectWithVersion(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	ctx := context.Background()
	meta.CreateBucket(ctx, "test-bucket")

	svc := New(storage, meta, logger)

	data := bytes.NewReader([]byte("test data"))
	opts := PutObjectOptions{ContentType: "text/plain"}
	_, err := svc.PutObject(ctx, "test-bucket", "test-key", data, opts)
	if err != nil {
		t.Fatalf("PutObject() error = %v", err)
	}

	result, err := svc.GetObject(ctx, "test-bucket", "test-key", GetObjectOptions{VersionID: "version-123"})
	if err != nil {
		t.Fatalf("GetObject() error = %v", err)
	}
	defer result.Body.Close()
}

func TestObjectService_DeleteObjectWithVersion(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	ctx := context.Background()
	meta.CreateBucket(ctx, "test-bucket")

	svc := New(storage, meta, logger)

	data := bytes.NewReader([]byte("test data"))
	opts := PutObjectOptions{ContentType: "text/plain"}
	_, err := svc.PutObject(ctx, "test-bucket", "test-key", data, opts)
	if err != nil {
		t.Fatalf("PutObject() error = %v", err)
	}

	err = svc.DeleteObject(ctx, "test-bucket", "test-key", DeleteObjectOptions{VersionID: "version-123"})
	if err != nil {
		t.Fatalf("DeleteObject() error = %v", err)
	}
}

func TestObjectService_ListObjectsWithDelimiter(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	ctx := context.Background()
	meta.CreateBucket(ctx, "test-bucket")

	svc := New(storage, meta, logger)

	for _, key := range []string{"a/1.txt", "a/2.txt", "b/1.txt", "c.txt"} {
		data := bytes.NewReader([]byte("test"))
		opts := PutObjectOptions{ContentType: "text/plain"}
		_, err := svc.PutObject(ctx, "test-bucket", key, data, opts)
		if err != nil {
			t.Fatalf("PutObject() error = %v", err)
		}
	}

	result, err := svc.ListObjects(ctx, "test-bucket", ListObjectsOptions{Delimiter: "/"})
	if err != nil {
		t.Fatalf("ListObjects() error = %v", err)
	}
	_ = result
}

func TestObjectService_ListObjectsWithMaxKeys(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	ctx := context.Background()
	meta.CreateBucket(ctx, "test-bucket")

	svc := New(storage, meta, logger)

	for i := 0; i < 10; i++ {
		data := bytes.NewReader([]byte("test"))
		opts := PutObjectOptions{ContentType: "text/plain"}
		_, err := svc.PutObject(ctx, "test-bucket", fmt.Sprintf("key%d", i), data, opts)
		if err != nil {
			t.Fatalf("PutObject() error = %v", err)
		}
	}

	result, err := svc.ListObjects(ctx, "test-bucket", ListObjectsOptions{MaxKeys: 5})
	if err != nil {
		t.Fatalf("ListObjects() error = %v", err)
	}
	_ = result
}

func TestParseInt(t *testing.T) {
	tests := []struct {
		input    string
		default_ int
		expected int
	}{
		{"123", 0, 123},
		{"", 100, 100},
		{"abc", 50, 50},
		{"-5", 0, -5},
		{"0", 10, 0},
	}

	for _, tt := range tests {
		result := parseInt(tt.input, tt.default_)
		if result != tt.expected {
			t.Errorf("parseInt(%q, %d) = %d, want %d", tt.input, tt.default_, result, tt.expected)
		}
	}
}

func TestValidateBucketName(t *testing.T) {
	validNames := []string{
		"my-bucket",
		"bucket123",
		"my.bucket",
		"mybucket",
		"192.168.1.1",
		"my-bucket-",
	}
	for _, name := range validNames {
		if err := validateBucketName(name); err != nil {
			t.Errorf("validateBucketName(%q) = %v, want nil", name, err)
		}
	}

	invalidNames := []string{
		"",
		"ab",
		"BUCKET",
		"ab",
	}
	for _, name := range invalidNames {
		if err := validateBucketName(name); err == nil {
			t.Errorf("validateBucketName(%q) = nil, want error", name)
		}
	}
}

func TestValidateBucketName_IPAddr(t *testing.T) {
	err := validateBucketName("1.2.3.4.ipaddr")
	if err == nil {
		t.Error("validateBucketName() should fail for .ipaddr suffix")
	}
}

func TestObjectService_ComputeStorageMetrics_NilStorage(t *testing.T) {
	logger := zap.NewNop().Sugar()
	svc := New(nil, NewMockMetadataStore(), logger)

	size, count, err := svc.ComputeStorageMetrics()
	if err != nil {
		t.Errorf("ComputeStorageMetrics() error = %v", err)
	}
	if size != 0 || count != 0 {
		t.Errorf("ComputeStorageMetrics() = %d, %d, want 0, 0", size, count)
	}
}

type errorReader struct{}

func (e *errorReader) Read(p []byte) (n int, err error) {
	return 0, fmt.Errorf("read error")
}

func TestObjectService_PutObject_ReadError(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	meta.CreateBucket(context.Background(), "test-bucket")
	svc := New(storage, meta, logger)

	_, err := svc.PutObject(context.Background(), "test-bucket", "key", &errorReader{}, PutObjectOptions{})
	if err == nil {
		t.Error("PutObject() should fail with read error")
	}
}

func TestObjectService_PutObject_ExceedsMaxSize(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	meta.CreateBucket(context.Background(), "test-bucket")
	svc := New(storage, meta, logger)

	largeData := make([]byte, MaxUploadSize+2)
	_, err := svc.PutObject(context.Background(), "test-bucket", "key", bytes.NewReader(largeData), PutObjectOptions{})
	if err == nil {
		t.Error("PutObject() should fail for data exceeding max size")
	}
}

type errorStorage struct {
	*MockStorageBackend
	putErr     error
	getErr     error
	deleteErr  error
	headErr    error
	listErr    error
	createErr  error
	deleteBErr error
	listBErr   error
}

func (e *errorStorage) Put(ctx context.Context, bucket, key string, data io.Reader, size int64, opts storage.PutOptions) error {
	if e.putErr != nil {
		return e.putErr
	}
	return e.MockStorageBackend.Put(ctx, bucket, key, data, size, opts)
}

func (e *errorStorage) Get(ctx context.Context, bucket, key string, opts storage.GetOptions) (io.ReadCloser, error) {
	if e.getErr != nil {
		return nil, e.getErr
	}
	return e.MockStorageBackend.Get(ctx, bucket, key, opts)
}

func (e *errorStorage) Delete(ctx context.Context, bucket, key string) error {
	if e.deleteErr != nil {
		return e.deleteErr
	}
	return e.MockStorageBackend.Delete(ctx, bucket, key)
}

func (e *errorStorage) Head(ctx context.Context, bucket, key string) (*storage.ObjectInfo, error) {
	if e.headErr != nil {
		return nil, e.headErr
	}
	return e.MockStorageBackend.Head(ctx, bucket, key)
}

func (e *errorStorage) List(ctx context.Context, bucket, prefix string, opts storage.ListOptions) (*storage.ListResult, error) {
	if e.listErr != nil {
		return nil, e.listErr
	}
	return e.MockStorageBackend.List(ctx, bucket, prefix, opts)
}

func (e *errorStorage) CreateBucket(ctx context.Context, bucket string) error {
	if e.createErr != nil {
		return e.createErr
	}
	return e.MockStorageBackend.CreateBucket(ctx, bucket)
}

func (e *errorStorage) DeleteBucket(ctx context.Context, bucket string) error {
	if e.deleteBErr != nil {
		return e.deleteBErr
	}
	return e.MockStorageBackend.DeleteBucket(ctx, bucket)
}

func (e *errorStorage) ListBuckets(ctx context.Context) ([]storage.BucketInfo, error) {
	if e.listBErr != nil {
		return nil, e.listBErr
	}
	return e.MockStorageBackend.ListBuckets(ctx)
}

func TestObjectService_PutObject_StorageError(t *testing.T) {
	meta := NewMockMetadataStore()
	meta.CreateBucket(context.Background(), "test-bucket")
	storage := &errorStorage{MockStorageBackend: NewMockStorageBackend(), putErr: fmt.Errorf("storage error")}
	svc := New(storage, meta, zap.NewNop().Sugar())

	_, err := svc.PutObject(context.Background(), "test-bucket", "key", bytes.NewReader([]byte("data")), PutObjectOptions{})
	if err == nil {
		t.Error("PutObject() should fail with storage error")
	}
}

func TestObjectService_CopyObject_ObjectNotFound(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	meta.CreateBucket(context.Background(), "src-bucket")
	meta.CreateBucket(context.Background(), "dst-bucket")
	svc := New(storage, meta, zap.NewNop().Sugar())

	_, err := svc.CopyObject(context.Background(), "src-bucket", "nonexistent", "dst-bucket", "dst-key")
	if err == nil {
		t.Error("CopyObject() should fail for nonexistent object")
	}
}

func TestObjectService_CopyObject_StorageGetError(t *testing.T) {
	meta := NewMockMetadataStore()
	meta.CreateBucket(context.Background(), "src-bucket")
	meta.CreateBucket(context.Background(), "dst-bucket")
	storage := &errorStorage{MockStorageBackend: NewMockStorageBackend(), getErr: fmt.Errorf("get error")}
	svc := New(storage, meta, zap.NewNop().Sugar())

	meta.PutObject(context.Background(), "src-bucket", "src-key", &metadata.ObjectMetadata{Key: "src-key"})
	_, err := svc.CopyObject(context.Background(), "src-bucket", "src-key", "dst-bucket", "dst-key")
	if err == nil {
		t.Error("CopyObject() should fail with storage get error")
	}
}

func TestObjectService_CopyObject_StoragePutError(t *testing.T) {
	mockStorage := NewMockStorageBackend()
	mockStorage.CreateBucket(context.Background(), "src-bucket")
	mockStorage.CreateBucket(context.Background(), "dst-bucket")
	mockStorage.Put(context.Background(), "src-bucket", "src-key", bytes.NewReader([]byte("data")), 4, storage.PutOptions{})

	meta := NewMockMetadataStore()
	meta.CreateBucket(context.Background(), "src-bucket")
	meta.CreateBucket(context.Background(), "dst-bucket")
	meta.PutObject(context.Background(), "src-bucket", "src-key", &metadata.ObjectMetadata{Key: "src-key"})

	storage := &errorStorage{MockStorageBackend: mockStorage, putErr: fmt.Errorf("put error")}
	svc := New(storage, meta, zap.NewNop().Sugar())

	_, err := svc.CopyObject(context.Background(), "src-bucket", "src-key", "dst-bucket", "dst-key")
	if err == nil {
		t.Error("CopyObject() should fail with storage put error")
	}
}

func TestObjectService_GetObject_ObjectNotFound(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	meta.CreateBucket(context.Background(), "test-bucket")
	svc := New(storage, meta, zap.NewNop().Sugar())

	_, err := svc.GetObject(context.Background(), "test-bucket", "nonexistent", GetObjectOptions{})
	if err == nil {
		t.Error("GetObject() should fail for nonexistent object")
	}
}

func TestObjectService_GetObject_StorageError(t *testing.T) {
	meta := NewMockMetadataStore()
	meta.CreateBucket(context.Background(), "test-bucket")
	meta.PutObject(context.Background(), "test-bucket", "key", &metadata.ObjectMetadata{Key: "key"})
	storage := &errorStorage{MockStorageBackend: NewMockStorageBackend(), getErr: fmt.Errorf("get error")}
	svc := New(storage, meta, zap.NewNop().Sugar())

	_, err := svc.GetObject(context.Background(), "test-bucket", "key", GetObjectOptions{})
	if err == nil {
		t.Error("GetObject() should fail with storage error")
	}
}

func TestObjectService_DeleteObject_StorageError(t *testing.T) {
	meta := NewMockMetadataStore()
	meta.CreateBucket(context.Background(), "test-bucket")
	storage := &errorStorage{MockStorageBackend: NewMockStorageBackend(), deleteErr: fmt.Errorf("delete error")}
	svc := New(storage, meta, zap.NewNop().Sugar())

	err := svc.DeleteObject(context.Background(), "test-bucket", "key", DeleteObjectOptions{})
	if err == nil {
		t.Error("DeleteObject() should fail with storage error")
	}
}

func TestObjectService_HeadObject_ObjectNotFound(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	meta.CreateBucket(context.Background(), "test-bucket")
	svc := New(storage, meta, zap.NewNop().Sugar())

	_, err := svc.HeadObject(context.Background(), "test-bucket", "nonexistent")
	if err == nil {
		t.Error("HeadObject() should fail for nonexistent object")
	}
}

func TestObjectService_HeadObject_StorageError(t *testing.T) {
	meta := NewMockMetadataStore()
	meta.CreateBucket(context.Background(), "test-bucket")
	meta.PutObject(context.Background(), "test-bucket", "key", &metadata.ObjectMetadata{Key: "key"})
	storage := &errorStorage{MockStorageBackend: NewMockStorageBackend(), headErr: fmt.Errorf("head error")}
	svc := New(storage, meta, zap.NewNop().Sugar())

	_, err := svc.HeadObject(context.Background(), "test-bucket", "key")
	if err == nil {
		t.Error("HeadObject() should fail with storage error")
	}
}

func TestObjectService_GetObjectAttributes_ObjectNotFound(t *testing.T) {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	meta.CreateBucket(context.Background(), "test-bucket")
	svc := New(storage, meta, zap.NewNop().Sugar())

	_, err := svc.GetObjectAttributes(context.Background(), "test-bucket", "nonexistent", "")
	if err == nil {
		t.Error("GetObjectAttributes() should fail for nonexistent object")
	}
}

func TestObjectService_GetObjectAttributes_StorageError(t *testing.T) {
	meta := NewMockMetadataStore()
	meta.CreateBucket(context.Background(), "test-bucket")
	meta.PutObject(context.Background(), "test-bucket", "key", &metadata.ObjectMetadata{Key: "key"})
	storage := &errorStorage{MockStorageBackend: NewMockStorageBackend(), headErr: fmt.Errorf("head error")}
	svc := New(storage, meta, zap.NewNop().Sugar())

	_, err := svc.GetObjectAttributes(context.Background(), "test-bucket", "key", "")
	if err == nil {
		t.Error("GetObjectAttributes() should fail with storage error")
	}
}

func TestObjectService_GetObjectAttributes_WithParts(t *testing.T) {
	mockStorage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	meta.CreateBucket(context.Background(), "test-bucket")
	svc := New(mockStorage, meta, zap.NewNop().Sugar())

	data := bytes.NewReader([]byte("test data"))
	_, err := svc.PutObject(context.Background(), "test-bucket", "key", data, PutObjectOptions{})
	if err != nil {
		t.Fatalf("PutObject() error = %v", err)
	}

	objMeta := &metadata.ObjectMetadata{
		Key:   "key-with-parts",
		Parts: []metadata.PartInfo{{PartNumber: 1, ETag: "etag1"}},
	}
	meta.PutObject(context.Background(), "test-bucket", "key-with-parts", objMeta)
	mockStorage.Put(context.Background(), "test-bucket", "key-with-parts", bytes.NewReader([]byte("data")), 4, storage.PutOptions{})

	result, err := svc.GetObjectAttributes(context.Background(), "test-bucket", "key-with-parts", "")
	if err != nil {
		t.Fatalf("GetObjectAttributes() error = %v", err)
	}
	_ = result
}

type errorMetadataStore struct {
	*MockMetadataStore
	getObjErr      error
	putObjErr      error
	delObjErr      error
	listPartsErr   error
	listUploadsErr error
	createMpuErr   error
	getBktErr      error
	createBktErr   error
	delBktErr      error
	lifecycleErr   error
}

func (e *errorMetadataStore) GetObject(ctx context.Context, bucket, key, versionID string) (*metadata.ObjectMetadata, error) {
	if e.getObjErr != nil {
		return nil, e.getObjErr
	}
	return e.MockMetadataStore.GetObject(ctx, bucket, key, versionID)
}

func (e *errorMetadataStore) PutObject(ctx context.Context, bucket, key string, meta *metadata.ObjectMetadata) error {
	if e.putObjErr != nil {
		return e.putObjErr
	}
	return e.MockMetadataStore.PutObject(ctx, bucket, key, meta)
}

func (e *errorMetadataStore) DeleteObject(ctx context.Context, bucket, key, versionID string) error {
	if e.delObjErr != nil {
		return e.delObjErr
	}
	return e.MockMetadataStore.DeleteObject(ctx, bucket, key, versionID)
}

func (e *errorMetadataStore) ListParts(ctx context.Context, bucket, key, uploadID string) ([]metadata.PartMetadata, error) {
	if e.listPartsErr != nil {
		return nil, e.listPartsErr
	}
	return e.MockMetadataStore.ListParts(ctx, bucket, key, uploadID)
}

func (e *errorMetadataStore) ListMultipartUploads(ctx context.Context, bucket, prefix string) ([]metadata.MultipartUploadMetadata, error) {
	if e.listUploadsErr != nil {
		return nil, e.listUploadsErr
	}
	return e.MockMetadataStore.ListMultipartUploads(ctx, bucket, prefix)
}

func (e *errorMetadataStore) CreateMultipartUpload(ctx context.Context, bucket, key, uploadID string, meta *metadata.ObjectMetadata) error {
	if e.createMpuErr != nil {
		return e.createMpuErr
	}
	return e.MockMetadataStore.CreateMultipartUpload(ctx, bucket, key, uploadID, meta)
}

func (e *errorMetadataStore) GetBucket(ctx context.Context, bucket string) (*metadata.BucketMetadata, error) {
	if e.getBktErr != nil {
		return nil, e.getBktErr
	}
	return e.MockMetadataStore.GetBucket(ctx, bucket)
}

func (e *errorMetadataStore) CreateBucket(ctx context.Context, bucket string) error {
	if e.createBktErr != nil {
		return e.createBktErr
	}
	return e.MockMetadataStore.CreateBucket(ctx, bucket)
}

func (e *errorMetadataStore) DeleteBucket(ctx context.Context, bucket string) error {
	if e.delBktErr != nil {
		return e.delBktErr
	}
	return e.MockMetadataStore.DeleteBucket(ctx, bucket)
}

func (e *errorMetadataStore) GetLifecycleRules(ctx context.Context, bucket string) ([]metadata.LifecycleRule, error) {
	if e.lifecycleErr != nil {
		return nil, e.lifecycleErr
	}
	return e.MockMetadataStore.GetLifecycleRules(ctx, bucket)
}

func TestObjectService_SelectObjectContent_StorageError(t *testing.T) {
	meta := NewMockMetadataStore()
	meta.CreateBucket(context.Background(), "test-bucket")
	meta.PutObject(context.Background(), "test-bucket", "key", &metadata.ObjectMetadata{Key: "key"})
	storage := &errorStorage{MockStorageBackend: NewMockStorageBackend(), getErr: fmt.Errorf("get error")}
	svc := New(storage, meta, zap.NewNop().Sugar())

	_, err := svc.SelectObjectContent(context.Background(), "test-bucket", "key", "SELECT *")
	if err == nil {
		t.Error("SelectObjectContent() should fail with storage error")
	}
}

func TestObjectService_ListObjects_StorageError(t *testing.T) {
	meta := NewMockMetadataStore()
	meta.CreateBucket(context.Background(), "test-bucket")
	storage := &errorStorage{MockStorageBackend: NewMockStorageBackend(), listErr: fmt.Errorf("list error")}
	svc := New(storage, meta, zap.NewNop().Sugar())

	_, err := svc.ListObjects(context.Background(), "test-bucket", ListObjectsOptions{})
	if err == nil {
		t.Error("ListObjects() should fail with storage error")
	}
}

func TestObjectService_CreateBucket_InvalidName(t *testing.T) {
	svc := New(NewMockStorageBackend(), NewMockMetadataStore(), zap.NewNop().Sugar())

	err := svc.CreateBucket(context.Background(), "AB")
	if err == nil {
		t.Error("CreateBucket() should fail with invalid bucket name")
	}
}

func TestObjectService_CreateBucket_StorageError(t *testing.T) {
	storage := &errorStorage{MockStorageBackend: NewMockStorageBackend(), createErr: fmt.Errorf("create error")}
	svc := New(storage, NewMockMetadataStore(), zap.NewNop().Sugar())

	err := svc.CreateBucket(context.Background(), "valid-bucket")
	if err == nil {
		t.Error("CreateBucket() should fail with storage error")
	}
}

func TestObjectService_CreateBucket_MetadataError(t *testing.T) {
	mockStorage := NewMockStorageBackend()
	mockStorage.CreateBucket(context.Background(), "valid-bucket")
	meta := &errorMetadataStore{MockMetadataStore: NewMockMetadataStore(), createBktErr: fmt.Errorf("create error")}
	svc := New(mockStorage, meta, zap.NewNop().Sugar())

	err := svc.CreateBucket(context.Background(), "valid-bucket")
	if err == nil {
		t.Error("CreateBucket() should fail with metadata error")
	}
}

func TestObjectService_DeleteBucket_ListError(t *testing.T) {
	storage := &errorStorage{MockStorageBackend: NewMockStorageBackend(), listErr: fmt.Errorf("list error")}
	svc := New(storage, NewMockMetadataStore(), zap.NewNop().Sugar())

	err := svc.DeleteBucket(context.Background(), "test-bucket")
	if err == nil {
		t.Error("DeleteBucket() should fail with list error")
	}
}

func TestObjectService_DeleteBucket_StorageError(t *testing.T) {
	mockStorage := NewMockStorageBackend()
	mockStorage.CreateBucket(context.Background(), "test-bucket")
	storage := &errorStorage{MockStorageBackend: mockStorage, deleteBErr: fmt.Errorf("delete error")}
	svc := New(storage, NewMockMetadataStore(), zap.NewNop().Sugar())

	err := svc.DeleteBucket(context.Background(), "test-bucket")
	if err == nil {
		t.Error("DeleteBucket() should fail with storage error")
	}
}

func TestObjectService_ListBuckets_StorageError(t *testing.T) {
	storage := &errorStorage{MockStorageBackend: NewMockStorageBackend(), listBErr: fmt.Errorf("list error")}
	svc := New(storage, NewMockMetadataStore(), zap.NewNop().Sugar())

	_, err := svc.ListBuckets(context.Background())
	if err == nil {
		t.Error("ListBuckets() should fail with storage error")
	}
}

func TestObjectService_CreateMultipartUpload_MetadataError(t *testing.T) {
	meta := &errorMetadataStore{MockMetadataStore: NewMockMetadataStore(), createMpuErr: fmt.Errorf("create error")}
	svc := New(NewMockStorageBackend(), meta, zap.NewNop().Sugar())

	_, err := svc.CreateMultipartUpload(context.Background(), "bucket", "key", PutObjectOptions{})
	if err == nil {
		t.Error("CreateMultipartUpload() should fail with metadata error")
	}
}

func TestObjectService_UploadPart_ReadError(t *testing.T) {
	meta := NewMockMetadataStore()
	svc := New(NewMockStorageBackend(), meta, zap.NewNop().Sugar())

	_, err := svc.UploadPart(context.Background(), "bucket", "key", "upload-id", 1, &errorReader{})
	if err == nil {
		t.Error("UploadPart() should fail with read error")
	}
}

type seekerReader struct {
	*bytes.Reader
	seekErr bool
}

func (s *seekerReader) Seek(offset int64, whence int) (int64, error) {
	if s.seekErr {
		return 0, fmt.Errorf("seek error")
	}
	return s.Reader.Seek(offset, whence)
}

func TestObjectService_UploadPart_SeekError(t *testing.T) {
	svc := New(NewMockStorageBackend(), NewMockMetadataStore(), zap.NewNop().Sugar())

	reader := &seekerReader{Reader: bytes.NewReader([]byte("data")), seekErr: true}
	_, err := svc.UploadPart(context.Background(), "bucket", "key", "upload-id", 1, reader)
	if err != nil {
		t.Errorf("UploadPart() should not fail with seek error, got: %v", err)
	}
}

func TestObjectService_CompleteMultipartUpload_ListPartsError(t *testing.T) {
	meta := &errorMetadataStore{MockMetadataStore: NewMockMetadataStore(), listPartsErr: fmt.Errorf("list parts error")}
	svc := New(NewMockStorageBackend(), meta, zap.NewNop().Sugar())

	_, err := svc.CompleteMultipartUpload(context.Background(), "bucket", "key", "upload-id", []PartInfo{})
	if err == nil {
		t.Error("CompleteMultipartUpload() should fail with list parts error")
	}
}

func TestObjectService_CompleteMultipartUpload_ReadPartError(t *testing.T) {
	meta := NewMockMetadataStore()
	meta.PutPart(context.Background(), "bucket", "key", "upload-id", 1, &metadata.PartMetadata{PartNumber: 1})
	storage := &errorStorage{MockStorageBackend: NewMockStorageBackend(), getErr: fmt.Errorf("get error")}
	svc := New(storage, meta, zap.NewNop().Sugar())

	_, err := svc.CompleteMultipartUpload(context.Background(), "bucket", "key", "upload-id", []PartInfo{{PartNumber: 1}})
	if err == nil {
		t.Error("CompleteMultipartUpload() should fail with read part error")
	}
}

func TestObjectService_CompleteMultipartUpload_WriteError(t *testing.T) {
	mockMeta := NewMockMetadataStore()
	mockMeta.PutPart(context.Background(), "bucket", "key", "upload-id", 1, &metadata.PartMetadata{PartNumber: 1})

	mockStorage := NewMockStorageBackend()
	mockStorage.Put(context.Background(), "bucket", "bucket/key/upload-id/1", bytes.NewReader([]byte("part")), 4, storage.PutOptions{})

	storage := &errorStorage{MockStorageBackend: mockStorage, putErr: fmt.Errorf("put error")}
	svc := New(storage, mockMeta, zap.NewNop().Sugar())

	_, err := svc.CompleteMultipartUpload(context.Background(), "bucket", "key", "upload-id", []PartInfo{{PartNumber: 1}})
	if err == nil {
		t.Error("CompleteMultipartUpload() should fail with write error")
	}
}

func TestObjectService_CompleteMultipartUpload_MetadataError(t *testing.T) {
	mockMeta := NewMockMetadataStore()
	mockMeta.PutPart(context.Background(), "bucket", "key", "upload-id", 1, &metadata.PartMetadata{PartNumber: 1})

	mockStorage := NewMockStorageBackend()
	mockStorage.Put(context.Background(), "bucket", "bucket/key/upload-id/1", bytes.NewReader([]byte("part")), 4, storage.PutOptions{})

	meta := &errorMetadataStore{MockMetadataStore: mockMeta, putObjErr: fmt.Errorf("put error")}
	svc := New(mockStorage, meta, zap.NewNop().Sugar())

	_, err := svc.CompleteMultipartUpload(context.Background(), "bucket", "key", "upload-id", []PartInfo{{PartNumber: 1}})
	if err == nil {
		t.Error("CompleteMultipartUpload() should fail with metadata put error")
	}
}

func TestObjectService_CompleteMultipartUpload_SortParts(t *testing.T) {
	mockMeta := NewMockMetadataStore()
	mockMeta.PutPart(context.Background(), "bucket", "key", "upload-id", 2, &metadata.PartMetadata{PartNumber: 2})
	mockMeta.PutPart(context.Background(), "bucket", "key", "upload-id", 1, &metadata.PartMetadata{PartNumber: 1})

	mockStorage := NewMockStorageBackend()
	mockStorage.Put(context.Background(), "bucket", "bucket/key/upload-id/1", bytes.NewReader([]byte("part1")), 5, storage.PutOptions{})
	mockStorage.Put(context.Background(), "bucket", "bucket/key/upload-id/2", bytes.NewReader([]byte("part2")), 5, storage.PutOptions{})

	svc := New(mockStorage, mockMeta, zap.NewNop().Sugar())

	result, err := svc.CompleteMultipartUpload(context.Background(), "bucket", "key", "upload-id", []PartInfo{{PartNumber: 1}, {PartNumber: 2}})
	if err != nil {
		t.Fatalf("CompleteMultipartUpload() error = %v", err)
	}
	if result.Size != 10 {
		t.Errorf("CompleteMultipartUpload() Size = %d, want 10", result.Size)
	}
}

func TestObjectService_ListMultipartUpload_MetadataError(t *testing.T) {
	meta := &errorMetadataStore{MockMetadataStore: NewMockMetadataStore(), listUploadsErr: fmt.Errorf("list error")}
	svc := New(NewMockStorageBackend(), meta, zap.NewNop().Sugar())

	_, err := svc.ListMultipartUpload(context.Background(), "bucket", "")
	if err == nil {
		t.Error("ListMultipartUpload() should fail with metadata error")
	}
}

func TestObjectService_ListParts_MetadataError(t *testing.T) {
	meta := &errorMetadataStore{MockMetadataStore: NewMockMetadataStore(), listPartsErr: fmt.Errorf("list error")}
	svc := New(NewMockStorageBackend(), meta, zap.NewNop().Sugar())

	_, err := svc.ListParts(context.Background(), "bucket", "key", "upload-id")
	if err == nil {
		t.Error("ListParts() should fail with metadata error")
	}
}

func TestObjectService_PutBucketLifecycle_EmptyRulesError(t *testing.T) {
	meta := &errorMetadataStore{MockMetadataStore: NewMockMetadataStore(), lifecycleErr: fmt.Errorf("lifecycle error")}
	svc := New(NewMockStorageBackend(), meta, zap.NewNop().Sugar())

	err := svc.PutBucketLifecycle(context.Background(), "bucket", nil)
	if err == nil {
		t.Error("PutBucketLifecycle() should fail with lifecycle error")
	}
}

func TestObjectService_BucketEncryption_Nil(t *testing.T) {
	svc := New(NewMockStorageBackend(), NewMockMetadataStore(), zap.NewNop().Sugar())

	err := svc.PutBucketEncryption(context.Background(), "bucket", nil)
	if err == nil {
		t.Error("PutBucketEncryption() should fail for nil config")
	}
}

func TestObjectService_ReplicationConfig_Nil(t *testing.T) {
	svc := New(NewMockStorageBackend(), NewMockMetadataStore(), zap.NewNop().Sugar())

	err := svc.PutReplicationConfig(context.Background(), "bucket", nil)
	if err == nil {
		t.Error("PutReplicationConfig() should fail for nil config")
	}
}

func TestObjectService_ObjectLock_Nil(t *testing.T) {
	svc := New(NewMockStorageBackend(), NewMockMetadataStore(), zap.NewNop().Sugar())

	err := svc.PutObjectLock(context.Background(), "bucket", nil)
	if err == nil {
		t.Error("PutObjectLock() should fail for nil config")
	}
}

func TestObjectService_PublicAccessBlock_Nil(t *testing.T) {
	svc := New(NewMockStorageBackend(), NewMockMetadataStore(), zap.NewNop().Sugar())

	err := svc.PutPublicAccessBlock(context.Background(), "bucket", nil)
	if err == nil {
		t.Error("PutPublicAccessBlock() should fail for nil config")
	}
}

func TestObjectService_BucketAccelerate_Nil(t *testing.T) {
	svc := New(NewMockStorageBackend(), NewMockMetadataStore(), zap.NewNop().Sugar())

	err := svc.PutBucketAccelerate(context.Background(), "bucket", nil)
	if err == nil {
		t.Error("PutBucketAccelerate() should fail for nil config")
	}
}

func TestObjectService_BucketInventory_Nil(t *testing.T) {
	svc := New(NewMockStorageBackend(), NewMockMetadataStore(), zap.NewNop().Sugar())

	err := svc.PutBucketInventory(context.Background(), "bucket", "id", nil)
	if err == nil {
		t.Error("PutBucketInventory() should fail for nil config")
	}
}

func TestObjectService_BucketAnalytics_Nil(t *testing.T) {
	svc := New(NewMockStorageBackend(), NewMockMetadataStore(), zap.NewNop().Sugar())

	err := svc.PutBucketAnalytics(context.Background(), "bucket", "id", nil)
	if err == nil {
		t.Error("PutBucketAnalytics() should fail for nil config")
	}
}

func TestObjectService_BucketWebsite_Nil(t *testing.T) {
	svc := New(NewMockStorageBackend(), NewMockMetadataStore(), zap.NewNop().Sugar())

	err := svc.PutBucketWebsite(context.Background(), "bucket", nil)
	if err == nil {
		t.Error("PutBucketWebsite() should fail for nil config")
	}
}

func TestObjectService_BucketNotification_Nil(t *testing.T) {
	svc := New(NewMockStorageBackend(), NewMockMetadataStore(), zap.NewNop().Sugar())

	err := svc.PutBucketNotification(context.Background(), "bucket", nil)
	if err == nil {
		t.Error("PutBucketNotification() should fail for nil config")
	}
}

func TestObjectService_BucketLogging_Nil(t *testing.T) {
	svc := New(NewMockStorageBackend(), NewMockMetadataStore(), zap.NewNop().Sugar())

	err := svc.PutBucketLogging(context.Background(), "bucket", nil)
	if err == nil {
		t.Error("PutBucketLogging() should fail for nil config")
	}
}

func TestObjectService_GeneratePresignedURL_EmptyMethod(t *testing.T) {
	svc := New(NewMockStorageBackend(), NewMockMetadataStore(), zap.NewNop().Sugar())

	_, err := svc.GeneratePresignedURL(context.Background(), "bucket", "key", "", 3600)
	if err == nil {
		t.Error("GeneratePresignedURL() should fail with empty method")
	}
}

func TestObjectService_GeneratePresignedURL_ObjectNotFound(t *testing.T) {
	meta := NewMockMetadataStore()
	meta.CreateBucket(context.Background(), "bucket")
	svc := New(NewMockStorageBackend(), meta, zap.NewNop().Sugar())

	_, err := svc.GeneratePresignedURL(context.Background(), "bucket", "nonexistent", "PUT", 3600)
	if err == nil {
		t.Error("GeneratePresignedURL() should fail for nonexistent object with PUT method")
	}
}

func TestObjectService_GeneratePresignedURL_DELETE_ObjectNotFound(t *testing.T) {
	meta := NewMockMetadataStore()
	meta.CreateBucket(context.Background(), "bucket")
	svc := New(NewMockStorageBackend(), meta, zap.NewNop().Sugar())

	_, err := svc.GeneratePresignedURL(context.Background(), "bucket", "nonexistent", "DELETE", 3600)
	if err == nil {
		t.Error("GeneratePresignedURL() should fail for nonexistent object with DELETE method")
	}
}

type errorPutPresignedURLMetadata struct {
	*MockMetadataStore
	putPresignedURLErr error
}

func (e *errorPutPresignedURLMetadata) PutPresignedURL(ctx context.Context, url string, req *metadata.PresignedURLRequest) error {
	if e.putPresignedURLErr != nil {
		return e.putPresignedURLErr
	}
	return e.MockMetadataStore.PutPresignedURL(ctx, url, req)
}

func TestObjectService_GeneratePresignedURL_PutError(t *testing.T) {
	meta := &errorPutPresignedURLMetadata{MockMetadataStore: NewMockMetadataStore(), putPresignedURLErr: fmt.Errorf("put error")}
	svc := New(NewMockStorageBackend(), meta, zap.NewNop().Sugar())

	_, err := svc.GeneratePresignedURL(context.Background(), "bucket", "key", "GET", 3600)
	if err == nil {
		t.Error("GeneratePresignedURL() should fail with put error")
	}
}

type errorDeleteBucketMetadata struct {
	*MockMetadataStore
	delBucketErr error
}

func (e *errorDeleteBucketMetadata) DeleteBucket(ctx context.Context, bucket string) error {
	if e.delBucketErr != nil {
		return e.delBucketErr
	}
	return e.MockMetadataStore.DeleteBucket(ctx, bucket)
}

func TestObjectService_DeleteBucket_MetadataError(t *testing.T) {
	mockStorage := NewMockStorageBackend()
	mockStorage.CreateBucket(context.Background(), "test-bucket")
	meta := &errorDeleteBucketMetadata{MockMetadataStore: NewMockMetadataStore(), delBucketErr: fmt.Errorf("delete error")}
	meta.CreateBucket(context.Background(), "test-bucket")
	svc := New(mockStorage, meta, zap.NewNop().Sugar())

	err := svc.DeleteBucket(context.Background(), "test-bucket")
	if err != nil {
		t.Errorf("DeleteBucket() should not fail with metadata delete error: %v", err)
	}
}

type errorDeleteObjectMetadata struct {
	*MockMetadataStore
	delObjErr error
}

func (e *errorDeleteObjectMetadata) DeleteObject(ctx context.Context, bucket, key, versionID string) error {
	if e.delObjErr != nil {
		return e.delObjErr
	}
	return e.MockMetadataStore.DeleteObject(ctx, bucket, key, versionID)
}

func TestObjectService_DeleteObject_MetadataError(t *testing.T) {
	mockStorage := NewMockStorageBackend()
	mockStorage.CreateBucket(context.Background(), "test-bucket")
	mockStorage.Put(context.Background(), "test-bucket", "key", bytes.NewReader([]byte("data")), 4, storage.PutOptions{})

	meta := NewMockMetadataStore()
	meta.CreateBucket(context.Background(), "test-bucket")
	meta.PutObject(context.Background(), "test-bucket", "key", &metadata.ObjectMetadata{Key: "key"})

	errMeta := &errorDeleteObjectMetadata{MockMetadataStore: meta, delObjErr: fmt.Errorf("delete error")}
	svc := New(mockStorage, errMeta, zap.NewNop().Sugar())

	err := svc.DeleteObject(context.Background(), "test-bucket", "key", DeleteObjectOptions{})
	if err != nil {
		t.Errorf("DeleteObject() should not fail with metadata delete error: %v", err)
	}
}

type errorPutObjectMetadata struct {
	*MockMetadataStore
	putObjErr error
}

func (e *errorPutObjectMetadata) PutObject(ctx context.Context, bucket, key string, meta *metadata.ObjectMetadata) error {
	if e.putObjErr != nil {
		return e.putObjErr
	}
	return e.MockMetadataStore.PutObject(ctx, bucket, key, meta)
}

func TestObjectService_PutObject_MetadataError(t *testing.T) {
	mockStorage := NewMockStorageBackend()
	mockStorage.CreateBucket(context.Background(), "test-bucket")

	meta := NewMockMetadataStore()
	meta.CreateBucket(context.Background(), "test-bucket")

	errMeta := &errorPutObjectMetadata{MockMetadataStore: meta, putObjErr: fmt.Errorf("put error")}
	svc := New(mockStorage, errMeta, zap.NewNop().Sugar())

	_, err := svc.PutObject(context.Background(), "test-bucket", "key", bytes.NewReader([]byte("data")), PutObjectOptions{})
	if err != nil {
		t.Errorf("PutObject() should not fail with metadata put error: %v", err)
	}
}

func TestObjectService_CopyObject_MetadataError(t *testing.T) {
	mockStorage := NewMockStorageBackend()
	mockStorage.CreateBucket(context.Background(), "src-bucket")
	mockStorage.CreateBucket(context.Background(), "dst-bucket")
	mockStorage.Put(context.Background(), "src-bucket", "src-key", bytes.NewReader([]byte("data")), 4, storage.PutOptions{})

	meta := NewMockMetadataStore()
	meta.CreateBucket(context.Background(), "src-bucket")
	meta.CreateBucket(context.Background(), "dst-bucket")
	meta.PutObject(context.Background(), "src-bucket", "src-key", &metadata.ObjectMetadata{Key: "src-key"})

	errMeta := &errorPutObjectMetadata{MockMetadataStore: meta, putObjErr: fmt.Errorf("put error")}
	svc := New(mockStorage, errMeta, zap.NewNop().Sugar())

	_, err := svc.CopyObject(context.Background(), "src-bucket", "src-key", "dst-bucket", "dst-key")
	if err != nil {
		t.Errorf("CopyObject() should not fail with metadata put error: %v", err)
	}
}

type errorListPartsMetadata struct {
	*MockMetadataStore
	listPartsErr error
}

func (e *errorListPartsMetadata) ListParts(ctx context.Context, bucket, key, uploadID string) ([]metadata.PartMetadata, error) {
	if e.listPartsErr != nil {
		return nil, e.listPartsErr
	}
	return e.MockMetadataStore.ListParts(ctx, bucket, key, uploadID)
}

func TestObjectService_GetObjectAttributes_ListPartsError(t *testing.T) {
	mockStorage := NewMockStorageBackend()
	mockStorage.CreateBucket(context.Background(), "test-bucket")
	mockStorage.Put(context.Background(), "test-bucket", "key-with-parts", bytes.NewReader([]byte("data")), 4, storage.PutOptions{})

	meta := NewMockMetadataStore()
	meta.CreateBucket(context.Background(), "test-bucket")
	objMeta := &metadata.ObjectMetadata{
		Key:   "key-with-parts",
		Parts: []metadata.PartInfo{{PartNumber: 1, ETag: "etag1"}},
	}
	meta.PutObject(context.Background(), "test-bucket", "key-with-parts", objMeta)

	errMeta := &errorListPartsMetadata{MockMetadataStore: meta, listPartsErr: fmt.Errorf("list parts error")}
	svc := New(mockStorage, errMeta, zap.NewNop().Sugar())

	_, err := svc.GetObjectAttributes(context.Background(), "test-bucket", "key-with-parts", "")
	if err != nil {
		t.Errorf("GetObjectAttributes() should not fail with list parts error: %v", err)
	}
}

type errorReadCloser struct {
	err error
}

func (e *errorReadCloser) Read(p []byte) (n int, err error) {
	return 0, e.err
}

func (e *errorReadCloser) Close() error {
	return nil
}

type errorGetStorage struct {
	*MockStorageBackend
	getErr error
}

func (e *errorGetStorage) Get(ctx context.Context, bucket, key string, opts storage.GetOptions) (io.ReadCloser, error) {
	if e.getErr != nil {
		return nil, e.getErr
	}
	return e.MockStorageBackend.Get(ctx, bucket, key, opts)
}

type errorReaderStorage struct {
	*MockStorageBackend
}

func (e *errorReaderStorage) Get(ctx context.Context, bucket, key string, opts storage.GetOptions) (io.ReadCloser, error) {
	return &errorReadCloser{err: fmt.Errorf("read error")}, nil
}

func TestObjectService_SelectObjectContent_ReadError(t *testing.T) {
	mockStorage := &errorReaderStorage{MockStorageBackend: NewMockStorageBackend()}
	mockStorage.CreateBucket(context.Background(), "test-bucket")
	mockStorage.Put(context.Background(), "test-bucket", "key", bytes.NewReader([]byte("data")), 4, storage.PutOptions{})

	meta := NewMockMetadataStore()
	meta.CreateBucket(context.Background(), "test-bucket")
	meta.PutObject(context.Background(), "test-bucket", "key", &metadata.ObjectMetadata{Key: "key"})

	svc := New(mockStorage, meta, zap.NewNop().Sugar())

	_, err := svc.SelectObjectContent(context.Background(), "test-bucket", "key", "SELECT *")
	if err == nil {
		t.Error("SelectObjectContent() should fail with read error")
	}
}

type errorPartMetadataStore struct {
	*MockMetadataStore
	putPartErr error
}

func (e *errorPartMetadataStore) PutPart(ctx context.Context, bucket, key, uploadID string, partNumber int, partMeta *metadata.PartMetadata) error {
	if e.putPartErr != nil {
		return e.putPartErr
	}
	return e.MockMetadataStore.PutPart(ctx, bucket, key, uploadID, partNumber, partMeta)
}

func TestObjectService_UploadPart_MetadataError(t *testing.T) {
	mockStorage := NewMockStorageBackend()
	meta := &errorPartMetadataStore{MockMetadataStore: NewMockMetadataStore(), putPartErr: fmt.Errorf("put part error")}
	svc := New(mockStorage, meta, zap.NewNop().Sugar())

	_, err := svc.UploadPart(context.Background(), "bucket", "key", "upload-id", 1, bytes.NewReader([]byte("data")))
	if err != nil {
		t.Errorf("UploadPart() should not fail with metadata put error: %v", err)
	}
}

type errorDeleteStorage struct {
	*MockStorageBackend
}

func (e *errorDeleteStorage) Delete(ctx context.Context, bucket, key string) error {
	return fmt.Errorf("delete error")
}

func TestObjectService_CompleteMultipartUpload_DeletePartError(t *testing.T) {
	mockStorage := &errorDeleteStorage{MockStorageBackend: NewMockStorageBackend()}
	mockStorage.CreateBucket(context.Background(), "bucket")
	mockStorage.Put(context.Background(), "bucket", "bucket/key/upload-id/1", bytes.NewReader([]byte("part")), 4, storage.PutOptions{})

	meta := NewMockMetadataStore()
	meta.PutPart(context.Background(), "bucket", "key", "upload-id", 1, &metadata.PartMetadata{PartNumber: 1})

	svc := New(mockStorage, meta, zap.NewNop().Sugar())

	_, err := svc.CompleteMultipartUpload(context.Background(), "bucket", "key", "upload-id", []PartInfo{{PartNumber: 1}})
	if err != nil {
		t.Errorf("CompleteMultipartUpload() should not fail with delete part error: %v", err)
	}
}

func TestObjectService_AbortMultipartUpload_WithParts(t *testing.T) {
	mockStorage := NewMockStorageBackend()
	mockStorage.CreateBucket(context.Background(), "bucket")
	mockStorage.Put(context.Background(), "bucket", "bucket/key/upload-id/1", bytes.NewReader([]byte("part")), 4, storage.PutOptions{})

	meta := NewMockMetadataStore()
	meta.PutPart(context.Background(), "bucket", "key", "upload-id", 1, &metadata.PartMetadata{PartNumber: 1})

	svc := New(mockStorage, meta, zap.NewNop().Sugar())

	err := svc.AbortMultipartUpload(context.Background(), "bucket", "key", "upload-id")
	if err != nil {
		t.Errorf("AbortMultipartUpload() error = %v", err)
	}
}

type errorDeleteLifecycleMetadata struct {
	*MockMetadataStore
	delLifecycleErr error
}

func (e *errorDeleteLifecycleMetadata) DeleteLifecycleRule(ctx context.Context, bucket, ruleID string) error {
	if e.delLifecycleErr != nil {
		return e.delLifecycleErr
	}
	return e.MockMetadataStore.DeleteLifecycleRule(ctx, bucket, ruleID)
}

func TestObjectService_PutBucketLifecycle_DeleteError(t *testing.T) {
	meta := NewMockMetadataStore()
	meta.PutLifecycleRule(context.Background(), "bucket", &metadata.LifecycleRule{ID: "rule1"})

	errMeta := &errorDeleteLifecycleMetadata{MockMetadataStore: meta, delLifecycleErr: fmt.Errorf("delete error")}
	svc := New(NewMockStorageBackend(), errMeta, zap.NewNop().Sugar())

	err := svc.PutBucketLifecycle(context.Background(), "bucket", nil)
	if err == nil {
		t.Error("PutBucketLifecycle() should fail with delete error")
	}
}

type errorPutLifecycleMetadata struct {
	*MockMetadataStore
	putLifecycleErr error
}

func (e *errorPutLifecycleMetadata) PutLifecycleRule(ctx context.Context, bucket string, rule *metadata.LifecycleRule) error {
	if e.putLifecycleErr != nil {
		return e.putLifecycleErr
	}
	return e.MockMetadataStore.PutLifecycleRule(ctx, bucket, rule)
}

func TestObjectService_PutBucketLifecycle_PutError(t *testing.T) {
	errMeta := &errorPutLifecycleMetadata{MockMetadataStore: NewMockMetadataStore(), putLifecycleErr: fmt.Errorf("put error")}
	svc := New(NewMockStorageBackend(), errMeta, zap.NewNop().Sugar())

	rules := []metadata.LifecycleRule{{ID: "rule1", Prefix: "logs/", Status: "Enabled"}}
	err := svc.PutBucketLifecycle(context.Background(), "bucket", rules)
	if err == nil {
		t.Error("PutBucketLifecycle() should fail with put error")
	}
}

type errorCompleteMultipartMetadata struct {
	*MockMetadataStore
	completeMpuErr error
}

func (e *errorCompleteMultipartMetadata) CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []metadata.PartInfo) error {
	if e.completeMpuErr != nil {
		return e.completeMpuErr
	}
	return e.MockMetadataStore.CompleteMultipartUpload(ctx, bucket, key, uploadID, parts)
}

func TestObjectService_CompleteMultipartUpload_CompleteError(t *testing.T) {
	mockStorage := NewMockStorageBackend()
	mockStorage.CreateBucket(context.Background(), "bucket")
	mockStorage.Put(context.Background(), "bucket", "bucket/key/upload-id/1", bytes.NewReader([]byte("part")), 4, storage.PutOptions{})

	meta := NewMockMetadataStore()
	meta.PutPart(context.Background(), "bucket", "key", "upload-id", 1, &metadata.PartMetadata{PartNumber: 1})

	errMeta := &errorCompleteMultipartMetadata{MockMetadataStore: meta, completeMpuErr: fmt.Errorf("complete error")}
	svc := New(mockStorage, errMeta, zap.NewNop().Sugar())

	_, err := svc.CompleteMultipartUpload(context.Background(), "bucket", "key", "upload-id", []PartInfo{{PartNumber: 1}})
	if err != nil {
		t.Errorf("CompleteMultipartUpload() should not fail with complete error: %v", err)
	}
}

func TestObjectService_ListParts_WithParts(t *testing.T) {
	meta := NewMockMetadataStore()
	meta.PutPart(context.Background(), "bucket", "key", "upload-id", 1, &metadata.PartMetadata{PartNumber: 1, ETag: "etag1"})
	meta.PutPart(context.Background(), "bucket", "key", "upload-id", 2, &metadata.PartMetadata{PartNumber: 2, ETag: "etag2"})

	svc := New(NewMockStorageBackend(), meta, zap.NewNop().Sugar())

	parts, err := svc.ListParts(context.Background(), "bucket", "key", "upload-id")
	if err != nil {
		t.Fatalf("ListParts() error = %v", err)
	}
	if len(parts) != 2 {
		t.Errorf("ListParts() returned %d parts, want 2", len(parts))
	}
}

type copyErrorReader struct{}

func (c *copyErrorReader) Read(p []byte) (n int, err error) {
	return 0, fmt.Errorf("copy read error")
}

func TestObjectService_UploadPart_CopyError(t *testing.T) {
	svc := New(NewMockStorageBackend(), NewMockMetadataStore(), zap.NewNop().Sugar())

	_, err := svc.UploadPart(context.Background(), "bucket", "key", "upload-id", 1, &copyErrorReader{})
	if err == nil {
		t.Error("UploadPart() should fail with copy error")
	}
}

type errorReadAllStorage struct {
	*MockStorageBackend
}

func (e *errorReadAllStorage) Get(ctx context.Context, bucket, key string, opts storage.GetOptions) (io.ReadCloser, error) {
	return io.NopCloser(&errorReader{}), nil
}

func TestObjectService_CompleteMultipartUpload_ReadAllError(t *testing.T) {
	mockStorage := &errorReadAllStorage{MockStorageBackend: NewMockStorageBackend()}
	mockStorage.CreateBucket(context.Background(), "bucket")
	mockStorage.Put(context.Background(), "bucket", "bucket/key/upload-id/1", bytes.NewReader([]byte("part")), 4, storage.PutOptions{})

	meta := NewMockMetadataStore()
	meta.PutPart(context.Background(), "bucket", "key", "upload-id", 1, &metadata.PartMetadata{PartNumber: 1})

	svc := New(mockStorage, meta, zap.NewNop().Sugar())

	_, err := svc.CompleteMultipartUpload(context.Background(), "bucket", "key", "upload-id", []PartInfo{{PartNumber: 1}})
	if err == nil {
		t.Error("CompleteMultipartUpload() should fail with read all error")
	}
}

func TestObjectService_UploadPart_StoragePutError(t *testing.T) {
	storage := &errorStorage{MockStorageBackend: NewMockStorageBackend(), putErr: fmt.Errorf("put error")}
	svc := New(storage, NewMockMetadataStore(), zap.NewNop().Sugar())

	_, err := svc.UploadPart(context.Background(), "bucket", "key", "upload-id", 1, bytes.NewReader([]byte("data")))
	if err == nil {
		t.Error("UploadPart() should fail with storage put error")
	}
}
