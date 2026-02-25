package mgmt

import (
	"bytes"
	"context"
	"io"
	"os"
	"sync"

	"github.com/openendpoint/openendpoint/internal/metadata"
	"github.com/openendpoint/openendpoint/internal/storage"
)

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
	b, _ := io.ReadAll(data)
	m.objects[m.objectKey(bucket, key)] = b
	return nil
}

func (m *MockStorageBackend) Get(ctx context.Context, bucket, key string, opts storage.GetOptions) (io.ReadCloser, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if data, ok := m.objects[m.objectKey(bucket, key)]; ok {
		return io.NopCloser(bytes.NewReader(data)), nil
	}
	return nil, os.ErrNotExist
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
	if data, ok := m.objects[m.objectKey(bucket, key)]; ok {
		return &storage.ObjectInfo{Key: key, Size: int64(len(data))}, nil
	}
	return nil, os.ErrNotExist
}

func (m *MockStorageBackend) List(ctx context.Context, bucket, prefix string, opts storage.ListOptions) (*storage.ListResult, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var objects []storage.ObjectInfo
	prefixKey := bucket + "/" + prefix
	for k, v := range m.objects {
		if len(k) >= len(prefixKey) && k[:len(prefixKey)] == prefixKey {
			objects = append(objects, storage.ObjectInfo{Key: k[len(bucket)+1:], Size: int64(len(v))})
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

func (m *MockStorageBackend) Close() error { return nil }

type MockMetadataStore struct {
	mu      sync.RWMutex
	buckets map[string]*metadata.BucketMetadata
	objects map[string]*metadata.ObjectMetadata
}

func NewMockMetadataStore() *MockMetadataStore {
	return &MockMetadataStore{
		buckets: make(map[string]*metadata.BucketMetadata),
		objects: make(map[string]*metadata.ObjectMetadata),
	}
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
	return nil, os.ErrNotExist
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
	m.objects[bucket+"/"+key] = meta
	return nil
}

func (m *MockMetadataStore) GetObject(ctx context.Context, bucket, key string, versionID string) (*metadata.ObjectMetadata, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if o, ok := m.objects[bucket+"/"+key]; ok {
		return o, nil
	}
	return nil, os.ErrNotExist
}

func (m *MockMetadataStore) DeleteObject(ctx context.Context, bucket, key string, versionID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, bucket+"/"+key)
	return nil
}

func (m *MockMetadataStore) ListObjects(ctx context.Context, bucket, prefix string, opts metadata.ListOptions) ([]metadata.ObjectMetadata, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var objects []metadata.ObjectMetadata
	for k, v := range m.objects {
		if len(k) > len(bucket)+1 && k[:len(bucket)+1] == bucket+"/" {
			objects = append(objects, *v)
		}
	}
	return objects, nil
}

func (m *MockMetadataStore) CreateMultipartUpload(ctx context.Context, bucket, key, uploadID string, meta *metadata.ObjectMetadata) error {
	return nil
}
func (m *MockMetadataStore) PutPart(ctx context.Context, bucket, key, uploadID string, partNumber int, meta *metadata.PartMetadata) error {
	return nil
}
func (m *MockMetadataStore) CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []metadata.PartInfo) error {
	return nil
}
func (m *MockMetadataStore) AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	return nil
}
func (m *MockMetadataStore) ListParts(ctx context.Context, bucket, key, uploadID string) ([]metadata.PartMetadata, error) {
	return nil, nil
}
func (m *MockMetadataStore) ListMultipartUploads(ctx context.Context, bucket, prefix string) ([]metadata.MultipartUploadMetadata, error) {
	return nil, nil
}
func (m *MockMetadataStore) PutLifecycleRule(ctx context.Context, bucket string, rule *metadata.LifecycleRule) error {
	return nil
}
func (m *MockMetadataStore) GetLifecycleRules(ctx context.Context, bucket string) ([]metadata.LifecycleRule, error) {
	return nil, nil
}
func (m *MockMetadataStore) DeleteLifecycleRule(ctx context.Context, bucket, ruleID string) error {
	return nil
}
func (m *MockMetadataStore) PutReplicationConfig(ctx context.Context, bucket string, config *metadata.ReplicationConfig) error {
	return nil
}
func (m *MockMetadataStore) GetReplicationConfig(ctx context.Context, bucket string) (*metadata.ReplicationConfig, error) {
	return nil, nil
}
func (m *MockMetadataStore) DeleteReplicationConfig(ctx context.Context, bucket string) error {
	return nil
}
func (m *MockMetadataStore) PutBucketVersioning(ctx context.Context, bucket string, versioning *metadata.BucketVersioning) error {
	return nil
}
func (m *MockMetadataStore) GetBucketVersioning(ctx context.Context, bucket string) (*metadata.BucketVersioning, error) {
	return nil, nil
}
func (m *MockMetadataStore) PutBucketCors(ctx context.Context, bucket string, cors *metadata.CORSConfiguration) error {
	return nil
}
func (m *MockMetadataStore) GetBucketCors(ctx context.Context, bucket string) (*metadata.CORSConfiguration, error) {
	return nil, nil
}
func (m *MockMetadataStore) DeleteBucketCors(ctx context.Context, bucket string) error {
	return nil
}
func (m *MockMetadataStore) PutBucketPolicy(ctx context.Context, bucket string, policy *string) error {
	return nil
}
func (m *MockMetadataStore) GetBucketPolicy(ctx context.Context, bucket string) (*string, error) {
	return nil, nil
}
func (m *MockMetadataStore) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	return nil
}
func (m *MockMetadataStore) PutBucketEncryption(ctx context.Context, bucket string, encryption *metadata.BucketEncryption) error {
	return nil
}
func (m *MockMetadataStore) GetBucketEncryption(ctx context.Context, bucket string) (*metadata.BucketEncryption, error) {
	return nil, nil
}
func (m *MockMetadataStore) DeleteBucketEncryption(ctx context.Context, bucket string) error {
	return nil
}
func (m *MockMetadataStore) PutBucketTags(ctx context.Context, bucket string, tags map[string]string) error {
	return nil
}
func (m *MockMetadataStore) GetBucketTags(ctx context.Context, bucket string) (map[string]string, error) {
	return nil, nil
}
func (m *MockMetadataStore) DeleteBucketTags(ctx context.Context, bucket string) error {
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
	return "us-east-1", nil
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
func (m *MockMetadataStore) PutBucketMetrics(ctx context.Context, bucket, id string, config *metadata.MetricsConfiguration) error {
	return nil
}
func (m *MockMetadataStore) GetBucketMetrics(ctx context.Context, bucket, id string) (*metadata.MetricsConfiguration, error) {
	return nil, nil
}
func (m *MockMetadataStore) ListBucketMetrics(ctx context.Context, bucket string) ([]metadata.MetricsConfiguration, error) {
	return nil, nil
}
func (m *MockMetadataStore) DeleteBucketMetrics(ctx context.Context, bucket, id string) error {
	return nil
}
func (m *MockMetadataStore) Close() error { return nil }
