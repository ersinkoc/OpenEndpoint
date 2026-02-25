package lifecycle

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/openendpoint/openendpoint/internal/engine"
	"github.com/openendpoint/openendpoint/internal/metadata"
	"github.com/openendpoint/openendpoint/internal/storage"
	"go.uber.org/zap"
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
	b, err := io.ReadAll(data)
	if err != nil {
		return err
	}
	m.objects[m.objectKey(bucket, key)] = b
	m.buckets[bucket] = true
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
		Key:          key,
		Size:         int64(len(data)),
		LastModified: time.Now().Add(-48 * time.Hour).Unix(),
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
				Key:          k[len(bucket)+1:],
				Size:         int64(len(v)),
				LastModified: time.Now().Add(-48 * time.Hour).Unix(),
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

func createTestEngine(t *testing.T) *engine.ObjectService {
	storage := NewMockStorageBackend()
	meta := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()
	return engine.New(storage, meta, logger)
}

func TestNewProcessor(t *testing.T) {
	eng := createTestEngine(t)
	processor := NewProcessor(eng, time.Minute)
	if processor == nil {
		t.Fatal("NewProcessor returned nil")
	}
	if processor.engine == nil {
		t.Error("Processor engine should not be nil")
	}
	if processor.interval != time.Minute {
		t.Errorf("Processor interval = %v, want %v", processor.interval, time.Minute)
	}
}

func TestProcessor_StartStop(t *testing.T) {
	eng := createTestEngine(t)
	processor := NewProcessor(eng, time.Second)

	processor.Start()
	time.Sleep(50 * time.Millisecond)
	processor.Stop()
}

func TestProcessor_AddRule(t *testing.T) {
	eng := createTestEngine(t)
	processor := NewProcessor(eng, time.Minute)

	ctx := context.Background()
	rule := &metadata.LifecycleRule{
		ID:     "test-rule",
		Prefix: "logs/",
		Status: "Enabled",
	}

	err := processor.AddRule(ctx, "test-bucket", rule)
	if err != nil {
		t.Fatalf("AddRule failed: %v", err)
	}
}

func TestProcessor_GetRules(t *testing.T) {
	eng := createTestEngine(t)
	processor := NewProcessor(eng, time.Minute)

	ctx := context.Background()
	rule := &metadata.LifecycleRule{
		ID:     "test-rule",
		Prefix: "logs/",
		Status: "Enabled",
	}

	processor.AddRule(ctx, "test-bucket", rule)

	rules, err := processor.GetRules(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("GetRules failed: %v", err)
	}
	if len(rules) != 1 {
		t.Errorf("GetRules returned %d rules, want 1", len(rules))
	}
}

func TestProcessor_RemoveRule(t *testing.T) {
	eng := createTestEngine(t)
	processor := NewProcessor(eng, time.Minute)

	ctx := context.Background()
	rule := &metadata.LifecycleRule{
		ID:     "test-rule",
		Prefix: "logs/",
		Status: "Enabled",
	}

	processor.AddRule(ctx, "test-bucket", rule)

	err := processor.RemoveRule(ctx, "test-bucket", "test-rule")
	if err != nil {
		t.Fatalf("RemoveRule failed: %v", err)
	}
}

func TestProcessor_ProcessBucketWithRules(t *testing.T) {
	eng := createTestEngine(t)
	ctx := context.Background()

	eng.CreateBucket(ctx, "test-bucket")

	rule := &metadata.LifecycleRule{
		ID:     "test-rule",
		Prefix: "logs/",
		Status: "Enabled",
		Expiration: &metadata.Expiration{
			Days: 30,
		},
	}
	eng.PutLifecycleRule(ctx, "test-bucket", rule)

	processor := NewProcessor(eng, time.Minute)
	processor.Start()
	time.Sleep(100 * time.Millisecond)
	processor.Stop()
}

func TestProcessor_ProcessEmptyBucket(t *testing.T) {
	eng := createTestEngine(t)
	ctx := context.Background()

	eng.CreateBucket(ctx, "empty-bucket")

	processor := NewProcessor(eng, time.Minute)
	processor.Start()
	time.Sleep(100 * time.Millisecond)
	processor.Stop()
}

func TestProcessor_ProcessBucketWithTransitions(t *testing.T) {
	eng := createTestEngine(t)
	ctx := context.Background()

	eng.CreateBucket(ctx, "test-bucket")

	rule := &metadata.LifecycleRule{
		ID:     "transition-rule",
		Prefix: "archive/",
		Status: "Enabled",
		Transitions: []metadata.Transition{
			{Days: 30, StorageClass: "GLACIER"},
		},
	}
	eng.PutLifecycleRule(ctx, "test-bucket", rule)

	processor := NewProcessor(eng, time.Minute)
	processor.Start()
	time.Sleep(100 * time.Millisecond)
	processor.Stop()
}

func TestProcessor_ProcessBucketWithNoncurrentExpiration(t *testing.T) {
	eng := createTestEngine(t)
	ctx := context.Background()

	eng.CreateBucket(ctx, "test-bucket")

	rule := &metadata.LifecycleRule{
		ID:     "noncurrent-rule",
		Prefix: "versions/",
		Status: "Enabled",
		NoncurrentVersionExpiration: &metadata.NoncurrentVersionExpiration{
			NoncurrentDays: 90,
		},
	}
	eng.PutLifecycleRule(ctx, "test-bucket", rule)

	processor := NewProcessor(eng, time.Minute)
	processor.Start()
	time.Sleep(100 * time.Millisecond)
	processor.Stop()
}

func TestProcessor_ProcessDisabledRule(t *testing.T) {
	eng := createTestEngine(t)
	ctx := context.Background()

	eng.CreateBucket(ctx, "test-bucket")

	rule := &metadata.LifecycleRule{
		ID:     "disabled-rule",
		Prefix: "",
		Status: "Disabled",
		Expiration: &metadata.Expiration{
			Days: 30,
		},
	}
	eng.PutLifecycleRule(ctx, "test-bucket", rule)

	processor := NewProcessor(eng, time.Minute)
	processor.Start()
	time.Sleep(100 * time.Millisecond)
	processor.Stop()
}

func TestProcessor_ProcessMultipleRules(t *testing.T) {
	eng := createTestEngine(t)
	ctx := context.Background()

	eng.CreateBucket(ctx, "test-bucket")

	rule1 := &metadata.LifecycleRule{
		ID:     "rule-1",
		Prefix: "logs/",
		Status: "Enabled",
		Expiration: &metadata.Expiration{
			Days: 30,
		},
	}
	rule2 := &metadata.LifecycleRule{
		ID:     "rule-2",
		Prefix: "temp/",
		Status: "Enabled",
		Expiration: &metadata.Expiration{
			Days: 7,
		},
	}
	eng.PutLifecycleRule(ctx, "test-bucket", rule1)
	eng.PutLifecycleRule(ctx, "test-bucket", rule2)

	processor := NewProcessor(eng, time.Minute)
	processor.Start()
	time.Sleep(100 * time.Millisecond)
	processor.Stop()
}

func TestProcessor_ProcessMultipleBuckets(t *testing.T) {
	eng := createTestEngine(t)
	ctx := context.Background()

	eng.CreateBucket(ctx, "bucket-1")
	eng.CreateBucket(ctx, "bucket-2")

	rule := &metadata.LifecycleRule{
		ID:     "rule",
		Prefix: "",
		Status: "Enabled",
		Expiration: &metadata.Expiration{
			Days: 30,
		},
	}
	eng.PutLifecycleRule(ctx, "bucket-1", rule)
	eng.PutLifecycleRule(ctx, "bucket-2", rule)

	processor := NewProcessor(eng, time.Minute)
	processor.Start()
	time.Sleep(100 * time.Millisecond)
	processor.Stop()
}

func TestProcessor_ProcessExpirationWithObjects(t *testing.T) {
	eng := createTestEngine(t)
	ctx := context.Background()

	eng.CreateBucket(ctx, "test-bucket")

	rule := &metadata.LifecycleRule{
		ID:     "expiration-rule",
		Prefix: "",
		Status: "Enabled",
		Expiration: &metadata.Expiration{
			Days: 1,
		},
	}
	eng.PutLifecycleRule(ctx, "test-bucket", rule)

	eng.PutObject(ctx, "test-bucket", "old-file.txt", strings.NewReader("old content"), engine.PutObjectOptions{})

	processor := NewProcessor(eng, time.Minute)
	processor.Start()
	time.Sleep(100 * time.Millisecond)
	processor.Stop()
}

func TestProcessor_ProcessTransitionsWithObjects(t *testing.T) {
	eng := createTestEngine(t)
	ctx := context.Background()

	eng.CreateBucket(ctx, "test-bucket")

	rule := &metadata.LifecycleRule{
		ID:     "transition-rule",
		Prefix: "",
		Status: "Enabled",
		Transitions: []metadata.Transition{
			{Days: 1, StorageClass: "GLACIER"},
		},
	}
	eng.PutLifecycleRule(ctx, "test-bucket", rule)

	eng.PutObject(ctx, "test-bucket", "file1.txt", strings.NewReader("content"), engine.PutObjectOptions{})
	eng.PutObject(ctx, "test-bucket", "file2.txt", strings.NewReader("more content"), engine.PutObjectOptions{})

	processor := NewProcessor(eng, time.Minute)
	processor.Start()
	time.Sleep(100 * time.Millisecond)
	processor.Stop()
}

func TestProcessor_RemoveRuleNotFound(t *testing.T) {
	eng := createTestEngine(t)
	processor := NewProcessor(eng, time.Minute)

	ctx := context.Background()

	err := processor.RemoveRule(ctx, "nonexistent-bucket", "nonexistent-rule")
	if err != nil {
		t.Logf("RemoveRule error (expected): %v", err)
	}
}

func TestProcessor_RemoveRuleLastRule(t *testing.T) {
	eng := createTestEngine(t)
	processor := NewProcessor(eng, time.Minute)

	ctx := context.Background()
	rule := &metadata.LifecycleRule{
		ID:     "only-rule",
		Prefix: "logs/",
		Status: "Enabled",
	}

	processor.AddRule(ctx, "test-bucket", rule)

	err := processor.RemoveRule(ctx, "test-bucket", "only-rule")
	if err != nil {
		t.Fatalf("RemoveRule failed: %v", err)
	}
}

func TestProcessor_ProcessExpirationZeroDays(t *testing.T) {
	eng := createTestEngine(t)
	ctx := context.Background()

	eng.CreateBucket(ctx, "test-bucket")

	rule := &metadata.LifecycleRule{
		ID:     "zero-days-rule",
		Prefix: "",
		Status: "Enabled",
		Expiration: &metadata.Expiration{
			Days: 0,
		},
	}
	eng.PutLifecycleRule(ctx, "test-bucket", rule)

	processor := NewProcessor(eng, time.Minute)
	processor.Start()
	time.Sleep(100 * time.Millisecond)
	processor.Stop()
}

func TestProcessor_ProcessTransitionsZeroDays(t *testing.T) {
	eng := createTestEngine(t)
	ctx := context.Background()

	eng.CreateBucket(ctx, "test-bucket")

	rule := &metadata.LifecycleRule{
		ID:     "zero-days-transition",
		Prefix: "",
		Status: "Enabled",
		Transitions: []metadata.Transition{
			{Days: 0, StorageClass: "GLACIER"},
		},
	}
	eng.PutLifecycleRule(ctx, "test-bucket", rule)

	processor := NewProcessor(eng, time.Minute)
	processor.Start()
	time.Sleep(100 * time.Millisecond)
	processor.Stop()
}

func TestProcessor_ProcessNoncurrentVersionExpirationZeroDays(t *testing.T) {
	eng := createTestEngine(t)
	ctx := context.Background()

	eng.CreateBucket(ctx, "test-bucket")

	rule := &metadata.LifecycleRule{
		ID:     "zero-days-noncurrent",
		Prefix: "",
		Status: "Enabled",
		NoncurrentVersionExpiration: &metadata.NoncurrentVersionExpiration{
			NoncurrentDays: 0,
		},
	}
	eng.PutLifecycleRule(ctx, "test-bucket", rule)

	processor := NewProcessor(eng, time.Minute)
	processor.Start()
	time.Sleep(100 * time.Millisecond)
	processor.Stop()
}

func TestProcessor_ProcessBucketNoRules(t *testing.T) {
	eng := createTestEngine(t)
	ctx := context.Background()

	eng.CreateBucket(ctx, "empty-bucket")

	processor := NewProcessor(eng, time.Minute)
	processor.Start()
	time.Sleep(100 * time.Millisecond)
	processor.Stop()
}

func TestProcessor_ProcessWithNilExpiration(t *testing.T) {
	eng := createTestEngine(t)
	ctx := context.Background()

	eng.CreateBucket(ctx, "test-bucket")

	rule := &metadata.LifecycleRule{
		ID:          "nil-expiration-rule",
		Prefix:      "",
		Status:      "Enabled",
		Expiration:  nil,
		Transitions: nil,
	}
	eng.PutLifecycleRule(ctx, "test-bucket", rule)

	processor := NewProcessor(eng, time.Minute)
	processor.Start()
	time.Sleep(100 * time.Millisecond)
	processor.Stop()
}

func TestProcessor_RemoveRuleMultipleRules(t *testing.T) {
	eng := createTestEngine(t)
	processor := NewProcessor(eng, time.Minute)

	ctx := context.Background()
	rule1 := &metadata.LifecycleRule{
		ID:     "rule-1",
		Prefix: "logs/",
		Status: "Enabled",
	}
	rule2 := &metadata.LifecycleRule{
		ID:     "rule-2",
		Prefix: "temp/",
		Status: "Enabled",
	}

	processor.AddRule(ctx, "test-bucket", rule1)
	processor.AddRule(ctx, "test-bucket", rule2)

	err := processor.RemoveRule(ctx, "test-bucket", "rule-1")
	if err != nil {
		t.Fatalf("RemoveRule failed: %v", err)
	}
}

func TestProcessor_ProcessTransitionsSameStorageClass(t *testing.T) {
	eng := createTestEngine(t)
	ctx := context.Background()

	eng.CreateBucket(ctx, "test-bucket")

	rule := &metadata.LifecycleRule{
		ID:     "transition-rule",
		Prefix: "",
		Status: "Enabled",
		Transitions: []metadata.Transition{
			{Days: 1, StorageClass: "STANDARD"},
		},
	}
	eng.PutLifecycleRule(ctx, "test-bucket", rule)

	eng.PutObject(ctx, "test-bucket", "file1.txt", strings.NewReader("content"), engine.PutObjectOptions{})

	processor := NewProcessor(eng, time.Minute)
	processor.Start()
	time.Sleep(100 * time.Millisecond)
	processor.Stop()
}

func TestProcessor_ProcessExpirationWithPrefix(t *testing.T) {
	eng := createTestEngine(t)
	ctx := context.Background()

	eng.CreateBucket(ctx, "test-bucket")

	rule := &metadata.LifecycleRule{
		ID:     "expiration-rule",
		Prefix: "old/",
		Status: "Enabled",
		Expiration: &metadata.Expiration{
			Days: 1,
		},
	}
	eng.PutLifecycleRule(ctx, "test-bucket", rule)

	eng.PutObject(ctx, "test-bucket", "old/file.txt", strings.NewReader("old content"), engine.PutObjectOptions{})
	eng.PutObject(ctx, "test-bucket", "new/file.txt", strings.NewReader("new content"), engine.PutObjectOptions{})

	processor := NewProcessor(eng, time.Minute)
	processor.Start()
	time.Sleep(100 * time.Millisecond)
	processor.Stop()
}

func TestProcessor_ProcessTransitionsMultipleTransitions(t *testing.T) {
	eng := createTestEngine(t)
	ctx := context.Background()

	eng.CreateBucket(ctx, "test-bucket")

	rule := &metadata.LifecycleRule{
		ID:     "transition-rule",
		Prefix: "",
		Status: "Enabled",
		Transitions: []metadata.Transition{
			{Days: 30, StorageClass: "STANDARD_IA"},
			{Days: 90, StorageClass: "GLACIER"},
		},
	}
	eng.PutLifecycleRule(ctx, "test-bucket", rule)

	eng.PutObject(ctx, "test-bucket", "file1.txt", strings.NewReader("content"), engine.PutObjectOptions{})

	processor := NewProcessor(eng, time.Minute)
	processor.Start()
	time.Sleep(100 * time.Millisecond)
	processor.Stop()
}

func TestProcessor_ProcessEmptyTransitions(t *testing.T) {
	eng := createTestEngine(t)
	ctx := context.Background()

	eng.CreateBucket(ctx, "test-bucket")

	rule := &metadata.LifecycleRule{
		ID:          "empty-transitions-rule",
		Prefix:      "",
		Status:      "Enabled",
		Transitions: []metadata.Transition{},
	}
	eng.PutLifecycleRule(ctx, "test-bucket", rule)

	processor := NewProcessor(eng, time.Minute)
	processor.Start()
	time.Sleep(100 * time.Millisecond)
	processor.Stop()
}

type ErrorMockStorageBackend struct {
	*MockStorageBackend
	listBucketsErr  error
	listObjectsErr  error
	deleteObjectErr error
	headObjectErr   error
	copyObjectErr   error
}

func (m *ErrorMockStorageBackend) ListBuckets(ctx context.Context) ([]storage.BucketInfo, error) {
	if m.listBucketsErr != nil {
		return nil, m.listBucketsErr
	}
	return m.MockStorageBackend.ListBuckets(ctx)
}

func (m *ErrorMockStorageBackend) List(ctx context.Context, bucket, prefix string, opts storage.ListOptions) (*storage.ListResult, error) {
	if m.listObjectsErr != nil {
		return nil, m.listObjectsErr
	}
	return m.MockStorageBackend.List(ctx, bucket, prefix, opts)
}

func (m *ErrorMockStorageBackend) Delete(ctx context.Context, bucket, key string) error {
	if m.deleteObjectErr != nil {
		return m.deleteObjectErr
	}
	return m.MockStorageBackend.Delete(ctx, bucket, key)
}

func (m *ErrorMockStorageBackend) Head(ctx context.Context, bucket, key string) (*storage.ObjectInfo, error) {
	if m.headObjectErr != nil {
		return nil, m.headObjectErr
	}
	return m.MockStorageBackend.Head(ctx, bucket, key)
}

func (m *ErrorMockStorageBackend) CopyObject(ctx context.Context, srcBucket, srcKey, dstBucket, dstKey string) (*engine.CopyObjectResult, error) {
	if m.copyObjectErr != nil {
		return nil, m.copyObjectErr
	}
	return &engine.CopyObjectResult{}, nil
}

type ErrorMockMetadataStore struct {
	*MockMetadataStore
	getLifecycleRulesErr error
}

func (m *ErrorMockMetadataStore) GetLifecycleRules(ctx context.Context, bucket string) ([]metadata.LifecycleRule, error) {
	if m.getLifecycleRulesErr != nil {
		return nil, m.getLifecycleRulesErr
	}
	return m.MockMetadataStore.GetLifecycleRules(ctx, bucket)
}

func createErrorTestEngine(t *testing.T, storageErr *ErrorMockStorageBackend, metaErr *ErrorMockMetadataStore) *engine.ObjectService {
	storageBackend := NewMockStorageBackend()
	metaStore := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()

	if storageErr != nil {
		storageErr.MockStorageBackend = storageBackend
	}
	if metaErr != nil {
		metaErr.MockMetadataStore = metaStore
	}

	if storageErr != nil && metaErr != nil {
		return engine.New(storageErr, metaErr, logger)
	} else if storageErr != nil {
		return engine.New(storageErr, metaStore, logger)
	} else if metaErr != nil {
		return engine.New(storageBackend, metaErr, logger)
	}
	return engine.New(storageBackend, metaStore, logger)
}

func TestProcessor_ProcessBucketsError(t *testing.T) {
	storageErr := &ErrorMockStorageBackend{
		listBucketsErr: io.EOF,
	}
	eng := createErrorTestEngine(t, storageErr, nil)

	processor := NewProcessor(eng, time.Minute)
	processor.Start()
	time.Sleep(100 * time.Millisecond)
	processor.Stop()
}

func TestProcessor_ProcessBucketGetRulesError(t *testing.T) {
	storageBackend := NewMockStorageBackend()
	baseMetaStore := NewMockMetadataStore()
	metaErr := &ErrorMockMetadataStore{
		MockMetadataStore:    baseMetaStore,
		getLifecycleRulesErr: io.EOF,
	}
	logger := zap.NewNop().Sugar()
	eng := engine.New(storageBackend, metaErr, logger)

	ctx := context.Background()
	eng.CreateBucket(ctx, "test-bucket")

	processor := NewProcessor(eng, time.Minute)
	processor.Start()
	time.Sleep(100 * time.Millisecond)
	processor.Stop()
}

func TestProcessor_ProcessExpirationListObjectsError(t *testing.T) {
	storageErr := &ErrorMockStorageBackend{
		listObjectsErr: io.EOF,
	}
	eng := createErrorTestEngine(t, storageErr, nil)

	ctx := context.Background()
	eng.CreateBucket(ctx, "test-bucket")

	rule := &metadata.LifecycleRule{
		ID:     "expiration-rule",
		Prefix: "",
		Status: "Enabled",
		Expiration: &metadata.Expiration{
			Days: 30,
		},
	}
	eng.PutLifecycleRule(ctx, "test-bucket", rule)

	processor := NewProcessor(eng, time.Minute)
	processor.Start()
	time.Sleep(100 * time.Millisecond)
	processor.Stop()
}

func TestProcessor_ProcessExpirationDeleteError(t *testing.T) {
	storageErr := &ErrorMockStorageBackend{
		deleteObjectErr: io.EOF,
	}
	eng := createErrorTestEngine(t, storageErr, nil)

	ctx := context.Background()
	eng.CreateBucket(ctx, "test-bucket")

	rule := &metadata.LifecycleRule{
		ID:     "expiration-rule",
		Prefix: "",
		Status: "Enabled",
		Expiration: &metadata.Expiration{
			Days: 1,
		},
	}
	eng.PutLifecycleRule(ctx, "test-bucket", rule)

	eng.PutObject(ctx, "test-bucket", "old-file.txt", strings.NewReader("old content"), engine.PutObjectOptions{})

	processor := NewProcessor(eng, time.Minute)
	processor.Start()
	time.Sleep(100 * time.Millisecond)
	processor.Stop()
}

func TestProcessor_ProcessTransitionsListObjectsError(t *testing.T) {
	storageErr := &ErrorMockStorageBackend{
		listObjectsErr: io.EOF,
	}
	eng := createErrorTestEngine(t, storageErr, nil)

	ctx := context.Background()
	eng.CreateBucket(ctx, "test-bucket")

	rule := &metadata.LifecycleRule{
		ID:     "transition-rule",
		Prefix: "",
		Status: "Enabled",
		Transitions: []metadata.Transition{
			{Days: 30, StorageClass: "GLACIER"},
		},
	}
	eng.PutLifecycleRule(ctx, "test-bucket", rule)

	processor := NewProcessor(eng, time.Minute)
	processor.Start()
	time.Sleep(100 * time.Millisecond)
	processor.Stop()
}

func TestProcessor_ProcessTransitionsHeadObjectError(t *testing.T) {
	storageErr := &ErrorMockStorageBackend{
		headObjectErr: io.EOF,
	}
	eng := createErrorTestEngine(t, storageErr, nil)

	ctx := context.Background()
	eng.CreateBucket(ctx, "test-bucket")

	rule := &metadata.LifecycleRule{
		ID:     "transition-rule",
		Prefix: "",
		Status: "Enabled",
		Transitions: []metadata.Transition{
			{Days: 1, StorageClass: "GLACIER"},
		},
	}
	eng.PutLifecycleRule(ctx, "test-bucket", rule)

	eng.PutObject(ctx, "test-bucket", "file1.txt", strings.NewReader("content"), engine.PutObjectOptions{})

	processor := NewProcessor(eng, time.Minute)
	processor.Start()
	time.Sleep(100 * time.Millisecond)
	processor.Stop()
}

func TestProcessor_ProcessTransitionsCopyError(t *testing.T) {
	storageErr := &ErrorMockStorageBackend{
		copyObjectErr: io.EOF,
	}
	eng := createErrorTestEngine(t, storageErr, nil)

	ctx := context.Background()
	eng.CreateBucket(ctx, "test-bucket")

	rule := &metadata.LifecycleRule{
		ID:     "transition-rule",
		Prefix: "",
		Status: "Enabled",
		Transitions: []metadata.Transition{
			{Days: 1, StorageClass: "GLACIER"},
		},
	}
	eng.PutLifecycleRule(ctx, "test-bucket", rule)

	eng.PutObject(ctx, "test-bucket", "file1.txt", strings.NewReader("content"), engine.PutObjectOptions{})

	processor := NewProcessor(eng, time.Minute)
	processor.Start()
	time.Sleep(100 * time.Millisecond)
	processor.Stop()
}

func TestProcessor_TickerFires(t *testing.T) {
	eng := createTestEngine(t)
	ctx := context.Background()

	eng.CreateBucket(ctx, "test-bucket")

	rule := &metadata.LifecycleRule{
		ID:     "ticker-rule",
		Prefix: "",
		Status: "Enabled",
		Expiration: &metadata.Expiration{
			Days: 30,
		},
	}
	eng.PutLifecycleRule(ctx, "test-bucket", rule)

	processor := NewProcessor(eng, 50*time.Millisecond)
	processor.Start()
	time.Sleep(150 * time.Millisecond)
	processor.Stop()
}

func TestProcessor_StopDuringProcessing(t *testing.T) {
	eng := createTestEngine(t)
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		bucketName := fmt.Sprintf("bucket-%d", i)
		eng.CreateBucket(ctx, bucketName)

		rule := &metadata.LifecycleRule{
			ID:     "rule",
			Prefix: "",
			Status: "Enabled",
			Expiration: &metadata.Expiration{
				Days: 30,
			},
		}
		eng.PutLifecycleRule(ctx, bucketName, rule)
	}

	processor := NewProcessor(eng, time.Minute)

	done := make(chan struct{})
	go func() {
		time.Sleep(10 * time.Millisecond)
		processor.Stop()
		close(done)
	}()

	processor.Start()
	<-done
}

func TestProcessor_StopDuringRuleProcessing(t *testing.T) {
	eng := createTestEngine(t)
	ctx := context.Background()

	eng.CreateBucket(ctx, "test-bucket")

	for i := 0; i < 10; i++ {
		rule := &metadata.LifecycleRule{
			ID:     fmt.Sprintf("rule-%d", i),
			Prefix: "",
			Status: "Enabled",
			Expiration: &metadata.Expiration{
				Days: 30,
			},
		}
		eng.PutLifecycleRule(ctx, "test-bucket", rule)
	}

	processor := NewProcessor(eng, time.Minute)

	done := make(chan struct{})
	go func() {
		time.Sleep(10 * time.Millisecond)
		processor.Stop()
		close(done)
	}()

	processor.Start()
	<-done
}

type StorageClassMockStorageBackend struct {
	*MockStorageBackend
	storageClass string
}

func (m *StorageClassMockStorageBackend) Head(ctx context.Context, bucket, key string) (*storage.ObjectInfo, error) {
	info, err := m.MockStorageBackend.Head(ctx, bucket, key)
	if err != nil {
		return nil, err
	}
	info.StorageClass = m.storageClass
	return info, nil
}

type GetErrorMockStorageBackend struct {
	*MockStorageBackend
	getErr error
}

func (m *GetErrorMockStorageBackend) Get(ctx context.Context, bucket, key string, opts storage.GetOptions) (io.ReadCloser, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	return m.MockStorageBackend.Get(ctx, bucket, key, opts)
}

type SlowMockStorageBackend struct {
	*MockStorageBackend
	delay time.Duration
}

func (m *SlowMockStorageBackend) List(ctx context.Context, bucket, prefix string, opts storage.ListOptions) (*storage.ListResult, error) {
	time.Sleep(m.delay)
	return m.MockStorageBackend.List(ctx, bucket, prefix, opts)
}

func TestProcessor_StopDuringBucketLoop(t *testing.T) {
	storageBackend := NewMockStorageBackend()
	slowBackend := &SlowMockStorageBackend{
		MockStorageBackend: storageBackend,
		delay:              50 * time.Millisecond,
	}
	metaStore := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()
	eng := engine.New(slowBackend, metaStore, logger)

	ctx := context.Background()
	for i := 0; i < 20; i++ {
		bucketName := fmt.Sprintf("bucket-%d", i)
		eng.CreateBucket(ctx, bucketName)

		rule := &metadata.LifecycleRule{
			ID:     "rule",
			Prefix: "",
			Status: "Enabled",
			Expiration: &metadata.Expiration{
				Days: 30,
			},
		}
		eng.PutLifecycleRule(ctx, bucketName, rule)
	}

	processor := NewProcessor(eng, time.Minute)
	processor.Start()
	time.Sleep(100 * time.Millisecond)
	processor.Stop()
}

type SlowRuleMockMetadataStore struct {
	*MockMetadataStore
	delay time.Duration
}

func (m *SlowRuleMockMetadataStore) GetLifecycleRules(ctx context.Context, bucket string) ([]metadata.LifecycleRule, error) {
	time.Sleep(m.delay)
	return m.MockMetadataStore.GetLifecycleRules(ctx, bucket)
}

func TestProcessor_StopDuringRuleLoop(t *testing.T) {
	storageBackend := NewMockStorageBackend()
	baseMetaStore := NewMockMetadataStore()
	slowMetaStore := &SlowRuleMockMetadataStore{
		MockMetadataStore: baseMetaStore,
		delay:             50 * time.Millisecond,
	}
	logger := zap.NewNop().Sugar()
	eng := engine.New(storageBackend, slowMetaStore, logger)

	ctx := context.Background()
	eng.CreateBucket(ctx, "test-bucket")

	for i := 0; i < 20; i++ {
		rule := &metadata.LifecycleRule{
			ID:     fmt.Sprintf("rule-%d", i),
			Prefix: "",
			Status: "Enabled",
			Expiration: &metadata.Expiration{
				Days: 30,
			},
		}
		eng.PutLifecycleRule(ctx, "test-bucket", rule)
	}

	processor := NewProcessor(eng, time.Minute)
	processor.Start()
	time.Sleep(100 * time.Millisecond)
	processor.Stop()
}

type BlockingMockStorageBackend struct {
	*MockStorageBackend
	blockCh chan struct{}
}

func (m *BlockingMockStorageBackend) List(ctx context.Context, bucket, prefix string, opts storage.ListOptions) (*storage.ListResult, error) {
	<-m.blockCh
	return m.MockStorageBackend.List(ctx, bucket, prefix, opts)
}

func TestProcessor_StopWhileBlocked(t *testing.T) {
	storageBackend := NewMockStorageBackend()
	blockingBackend := &BlockingMockStorageBackend{
		MockStorageBackend: storageBackend,
		blockCh:            make(chan struct{}),
	}
	metaStore := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()
	eng := engine.New(blockingBackend, metaStore, logger)

	ctx := context.Background()
	eng.CreateBucket(ctx, "test-bucket")

	rule := &metadata.LifecycleRule{
		ID:     "rule",
		Prefix: "",
		Status: "Enabled",
		Expiration: &metadata.Expiration{
			Days: 30,
		},
	}
	eng.PutLifecycleRule(ctx, "test-bucket", rule)

	processor := NewProcessor(eng, time.Minute)
	processor.Start()

	time.Sleep(50 * time.Millisecond)
	close(blockingBackend.blockCh)
	time.Sleep(50 * time.Millisecond)
	processor.Stop()
}

func TestProcessor_ProcessTransitionsWithEmptyObjectList(t *testing.T) {
	eng := createTestEngine(t)
	ctx := context.Background()

	eng.CreateBucket(ctx, "test-bucket")

	rule := &metadata.LifecycleRule{
		ID:     "transition-rule",
		Prefix: "",
		Status: "Enabled",
		Transitions: []metadata.Transition{
			{Days: 1, StorageClass: "GLACIER"},
		},
	}
	eng.PutLifecycleRule(ctx, "test-bucket", rule)

	processor := NewProcessor(eng, time.Minute)
	processor.Start()
	time.Sleep(100 * time.Millisecond)
	processor.Stop()
}

func TestProcessor_ProcessTransitionsWithObjectNotOldEnough(t *testing.T) {
	eng := createTestEngine(t)
	ctx := context.Background()

	eng.CreateBucket(ctx, "test-bucket")

	rule := &metadata.LifecycleRule{
		ID:     "transition-rule",
		Prefix: "",
		Status: "Enabled",
		Transitions: []metadata.Transition{
			{Days: 365, StorageClass: "GLACIER"},
		},
	}
	eng.PutLifecycleRule(ctx, "test-bucket", rule)

	eng.PutObject(ctx, "test-bucket", "file1.txt", strings.NewReader("content"), engine.PutObjectOptions{})

	processor := NewProcessor(eng, time.Minute)
	processor.Start()
	time.Sleep(100 * time.Millisecond)
	processor.Stop()
}

func TestProcessor_ProcessTransitionsSameStorageClassSkip(t *testing.T) {
	storageBackend := NewMockStorageBackend()
	storageClassBackend := &StorageClassMockStorageBackend{
		MockStorageBackend: storageBackend,
		storageClass:       "GLACIER",
	}
	metaStore := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()
	eng := engine.New(storageClassBackend, metaStore, logger)

	ctx := context.Background()
	eng.CreateBucket(ctx, "test-bucket")

	rule := &metadata.LifecycleRule{
		ID:     "transition-rule",
		Prefix: "",
		Status: "Enabled",
		Transitions: []metadata.Transition{
			{Days: 1, StorageClass: "GLACIER"},
		},
	}
	eng.PutLifecycleRule(ctx, "test-bucket", rule)

	eng.PutObject(ctx, "test-bucket", "file1.txt", strings.NewReader("content"), engine.PutObjectOptions{})

	processor := NewProcessor(eng, time.Minute)
	processor.Start()
	time.Sleep(100 * time.Millisecond)
	processor.Stop()
}

type ErrorObjectService struct {
	*engine.ObjectService
	copyObjectErr error
}

func (e *ErrorObjectService) CopyObject(ctx context.Context, srcBucket, srcKey, dstBucket, dstKey string) (*engine.CopyObjectResult, error) {
	if e.copyObjectErr != nil {
		return nil, e.copyObjectErr
	}
	return e.ObjectService.CopyObject(ctx, srcBucket, srcKey, dstBucket, dstKey)
}

func TestProcessor_RemoveRuleGetRulesError(t *testing.T) {
	storageBackend := NewMockStorageBackend()
	baseMetaStore := NewMockMetadataStore()
	metaErr := &ErrorMockMetadataStore{
		MockMetadataStore:    baseMetaStore,
		getLifecycleRulesErr: io.EOF,
	}
	logger := zap.NewNop().Sugar()
	eng := engine.New(storageBackend, metaErr, logger)

	processor := NewProcessor(eng, time.Minute)
	ctx := context.Background()

	err := processor.RemoveRule(ctx, "test-bucket", "nonexistent-rule")
	if err == nil {
		t.Error("Expected error from RemoveRule with GetLifecycleRules error")
	}
}

func TestProcessor_ProcessBucketWithDisabledAndEnabledRules(t *testing.T) {
	eng := createTestEngine(t)
	ctx := context.Background()

	eng.CreateBucket(ctx, "test-bucket")

	disabledRule := &metadata.LifecycleRule{
		ID:     "disabled-rule",
		Prefix: "",
		Status: "Disabled",
		Expiration: &metadata.Expiration{
			Days: 30,
		},
	}
	enabledRule := &metadata.LifecycleRule{
		ID:     "enabled-rule",
		Prefix: "",
		Status: "Enabled",
		Expiration: &metadata.Expiration{
			Days: 30,
		},
	}
	eng.PutLifecycleRule(ctx, "test-bucket", disabledRule)
	eng.PutLifecycleRule(ctx, "test-bucket", enabledRule)

	processor := NewProcessor(eng, time.Minute)
	processor.Start()
	time.Sleep(100 * time.Millisecond)
	processor.Stop()
}

func TestProcessor_ProcessTransitionsCopyObjectError(t *testing.T) {
	storageBackend := NewMockStorageBackend()
	getErrBackend := &GetErrorMockStorageBackend{
		MockStorageBackend: storageBackend,
		getErr:             io.EOF,
	}
	metaStore := NewMockMetadataStore()
	logger := zap.NewNop().Sugar()
	eng := engine.New(getErrBackend, metaStore, logger)

	ctx := context.Background()
	eng.CreateBucket(ctx, "test-bucket")

	rule := &metadata.LifecycleRule{
		ID:     "transition-rule",
		Prefix: "",
		Status: "Enabled",
		Transitions: []metadata.Transition{
			{Days: 1, StorageClass: "GLACIER"},
		},
	}
	eng.PutLifecycleRule(ctx, "test-bucket", rule)

	eng.PutObject(ctx, "test-bucket", "file1.txt", strings.NewReader("content"), engine.PutObjectOptions{})

	processor := NewProcessor(eng, time.Minute)
	processor.Start()
	time.Sleep(100 * time.Millisecond)
	processor.Stop()
}
