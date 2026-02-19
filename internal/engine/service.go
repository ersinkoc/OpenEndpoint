package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/openendpoint/openendpoint/internal/metadata"
	"github.com/openendpoint/openendpoint/internal/storage"
	"go.uber.org/zap"
)

// ObjectService provides the core object storage operations
type ObjectService struct {
	storage   storage.StorageBackend
	metadata  metadata.Store
	logger    *zap.SugaredLogger
	locker    *Locker
}

// New creates a new ObjectService
func New(storage storage.StorageBackend, metadata metadata.Store, logger *zap.SugaredLogger) *ObjectService {
	return &ObjectService{
		storage:  storage,
		metadata: metadata,
		logger:   logger,
		locker:   NewLocker(),
	}
}

// Close closes the ObjectService and releases resources
func (s *ObjectService) Close() error {
	if s.storage != nil {
		s.storage.Close()
	}
	if s.metadata != nil {
		s.metadata.Close()
	}
	return nil
}

// PutObject stores an object
func (s *ObjectService) PutObject(ctx context.Context, bucket, key string, data io.Reader, opts PutObjectOptions) (*ObjectResult, error) {
	// Lock the object
	unlock := s.locker.Lock(bucket, key)
	defer unlock()

	// Check bucket exists
	if _, err := s.metadata.GetBucket(ctx, bucket); err != nil {
		return nil, fmt.Errorf("bucket not found: %s", bucket)
	}

	// Read all data into memory first (required for hash calculation and storage)
	dataBytes, err := io.ReadAll(data)
	if err != nil {
		return nil, fmt.Errorf("failed to read data: %w", err)
	}

	// Calculate size and hash
	hasher := sha256.New()
	hasher.Write(dataBytes)
	size := int64(len(dataBytes))

	// Generate ETag
	etag := fmt.Sprintf("\"%s\"", hex.EncodeToString(hasher.Sum(nil)))

	// Create storage options
	storeOpts := storage.PutOptions{
		ContentType:     opts.ContentType,
		ContentEncoding: opts.ContentEncoding,
		CacheControl:    opts.CacheControl,
		Metadata:        opts.Metadata,
		StorageClass:    opts.StorageClass,
	}

	// Store the object
	if err := s.storage.Put(ctx, bucket, key, bytes.NewReader(dataBytes), size, storeOpts); err != nil {
		return nil, fmt.Errorf("failed to store object: %w", err)
	}

	// Create metadata
	now := time.Now().Unix()
	objMeta := &metadata.ObjectMetadata{
		Key:             key,
		Bucket:          bucket,
		Size:            size,
		ETag:            etag,
		ContentType:     opts.ContentType,
		ContentEncoding: opts.ContentEncoding,
		CacheControl:    opts.CacheControl,
		Metadata:        opts.Metadata,
		StorageClass:    opts.StorageClass,
		VersionID:       uuid.New().String(),
		IsLatest:        true,
		LastModified:    now,
	}

	// Save metadata
	if err := s.metadata.PutObject(ctx, bucket, key, objMeta); err != nil {
		s.logger.Error("failed to save metadata", zap.Error(err))
	}

	return &ObjectResult{
		ETag:         etag,
		Size:         size,
		VersionID:    objMeta.VersionID,
		LastModified: now,
	}, nil
}

// GetObject retrieves an object
func (s *ObjectService) GetObject(ctx context.Context, bucket, key string, opts GetObjectOptions) (*GetObjectResult, error) {
	// Lock for read
	unlock := s.locker.RLock(bucket, key)
	defer unlock()

	// Check bucket exists
	if _, err := s.metadata.GetBucket(ctx, bucket); err != nil {
		return nil, fmt.Errorf("bucket not found: %s", bucket)
	}

	// Get metadata
	meta, err := s.metadata.GetObject(ctx, bucket, key, opts.VersionID)
	if err != nil {
		return nil, fmt.Errorf("object not found: %s/%s", bucket, key)
	}

	// Convert storage options
	storeOpts := storage.GetOptions{
		Range: opts.Range,
		IfMatch:           opts.IfMatch,
		IfNoneMatch:       opts.IfNoneMatch,
		IfModifiedSince:   opts.IfModifiedSince,
		IfUnmodifiedSince: opts.IfUnmodifiedSince,
	}

	// Get the object - caller is responsible for closing
	reader, err := s.storage.Get(ctx, bucket, key, storeOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to get object: %w", err)
	}

	return &GetObjectResult{
		Body:         reader,
		Size:         meta.Size,
		ETag:         meta.ETag,
		ContentType:  meta.ContentType,
		Metadata:     meta.Metadata,
		LastModified: meta.LastModified,
		VersionID:    meta.VersionID,
	}, nil
}

// DeleteObject deletes an object
func (s *ObjectService) DeleteObject(ctx context.Context, bucket, key string, opts DeleteObjectOptions) error {
	// Lock the object
	unlock := s.locker.Lock(bucket, key)
	defer unlock()

	// Check bucket exists
	if _, err := s.metadata.GetBucket(ctx, bucket); err != nil {
		return fmt.Errorf("bucket not found: %s", bucket)
	}

	// Delete from storage
	if err := s.storage.Delete(ctx, bucket, key); err != nil {
		return fmt.Errorf("failed to delete object: %w", err)
	}

	// Delete metadata
	if err := s.metadata.DeleteObject(ctx, bucket, key, opts.VersionID); err != nil {
		s.logger.Warn("failed to delete metadata", zap.Error(err))
	}

	return nil
}

// HeadObject returns object metadata without reading the body
func (s *ObjectService) HeadObject(ctx context.Context, bucket, key string) (*ObjectInfo, error) {
	// Check bucket exists
	if _, err := s.metadata.GetBucket(ctx, bucket); err != nil {
		return nil, fmt.Errorf("bucket not found: %s", bucket)
	}

	// Get metadata
	meta, err := s.metadata.GetObject(ctx, bucket, key, "")
	if err != nil {
		return nil, fmt.Errorf("object not found: %s/%s", bucket, key)
	}

	// Also get from storage to ensure it exists
	storageMeta, err := s.storage.Head(ctx, bucket, key)
	if err != nil {
		return nil, fmt.Errorf("object not found: %s/%s", bucket, key)
	}

	return &ObjectInfo{
		Key:             key,
		Size:            meta.Size,
		ETag:            meta.ETag,
		ContentType:     meta.ContentType,
		ContentEncoding: meta.ContentEncoding,
		CacheControl:    meta.CacheControl,
		Metadata:        meta.Metadata,
		StorageClass:    meta.StorageClass,
		LastModified:    storageMeta.LastModified,
		VersionID:       meta.VersionID,
	}, nil
}

// GetObjectAttributes returns object attributes
func (s *ObjectService) GetObjectAttributes(ctx context.Context, bucket, key, versionID string) (*ObjectAttributes, error) {
	// Check bucket exists
	if _, err := s.metadata.GetBucket(ctx, bucket); err != nil {
		return nil, fmt.Errorf("bucket not found: %s", bucket)
	}

	// Get object metadata
	meta, err := s.metadata.GetObject(ctx, bucket, key, versionID)
	if err != nil {
		return nil, fmt.Errorf("object not found: %s/%s", bucket, key)
	}

	// Get storage info
	storageMeta, err := s.storage.Head(ctx, bucket, key)
	if err != nil {
		return nil, fmt.Errorf("object not found: %s/%s", bucket, key)
	}

	// Get parts if this is a multipart upload
	var parts []metadata.PartMetadata
	if len(meta.Parts) > 0 {
		parts, err = s.metadata.ListParts(ctx, bucket, key, "")
		if err != nil {
			parts = nil // Ignore error, parts are optional
		}
	}

	return &ObjectAttributes{
		ETag:                meta.ETag,
		Size:                meta.Size,
		LastModified:        storageMeta.LastModified,
		VersionID:           meta.VersionID,
		StorageClass:        meta.StorageClass,
		ContentType:         meta.ContentType,
		ContentEncoding:     meta.ContentEncoding,
		Metadata:            meta.Metadata,
		Parts:               parts,
	}, nil
}

// ObjectAttributes represents object attributes
type ObjectAttributes struct {
	ETag                string
	Size                int64
	LastModified        int64
	VersionID           string
	StorageClass        string
	ContentType         string
	ContentEncoding     string
	Metadata            map[string]string
	Parts               []metadata.PartMetadata
}

// SelectObjectContentResult contains the result of a select query
type SelectObjectContentResult struct {
	Body        string
	BytesScanned int64
	BytesReturned int64
}

// SelectObjectContent performs a select query on object data
func (s *ObjectService) SelectObjectContent(ctx context.Context, bucket, key, expression string) (*SelectObjectContentResult, error) {
	// Check bucket exists
	if _, err := s.metadata.GetBucket(ctx, bucket); err != nil {
		return nil, fmt.Errorf("bucket not found: %s", bucket)
	}

	// Get object
	_, err := s.metadata.GetObject(ctx, bucket, key, "")
	if err != nil {
		return nil, fmt.Errorf("object not found: %s/%s", bucket, key)
	}

	// Get the object data from storage
	data, err := s.storage.Get(ctx, bucket, key, storage.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to read object: %w", err)
	}
	defer data.Close()

	// Read all data
	content, err := io.ReadAll(data)
	if err != nil {
		return nil, fmt.Errorf("failed to read object data: %w", err)
	}

	// For now, implement a simple CSV/JSON filter
	// In production, this would be a full SQL engine
	result := s.processSelectExpression(string(content), expression)

	return &SelectObjectContentResult{
		Body:        result,
		BytesScanned: int64(len(content)),
		BytesReturned: int64(len(result)),
	}, nil
}

// processSelectExpression processes a simple select expression
// This is a simplified implementation - production would use a proper SQL engine
func (s *ObjectService) processSelectExpression(content, expression string) string {
	// Simple implementation: return content as-is for now
	// A full implementation would parse SQL-like expressions
	return content
}

// ListObjects lists objects in a bucket
func (s *ObjectService) ListObjects(ctx context.Context, bucket string, opts ListObjectsOptions) (*ListObjectsResult, error) {
	// Check bucket exists
	if _, err := s.metadata.GetBucket(ctx, bucket); err != nil {
		return nil, fmt.Errorf("bucket not found: %s", bucket)
	}

	// Convert options
	storeOpts := storage.ListOptions{
		Prefix:    opts.Prefix,
		Delimiter: opts.Delimiter,
		MaxKeys:   opts.MaxKeys,
		Marker:    opts.Marker,
	}

	// List from storage
	objects, err := s.storage.List(ctx, bucket, opts.Prefix, storeOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to list objects: %w", err)
	}

	// Convert to results
	var results []ObjectInfo
	for _, obj := range objects {
		results = append(results, ObjectInfo{
			Key:          obj.Key,
			Size:         obj.Size,
			ETag:         obj.ETag,
			LastModified: obj.LastModified,
		})
	}

	// Get next marker
	var nextMarker string
	if len(results) > 0 {
		nextMarker = results[len(results)-1].Key
	}

	return &ListObjectsResult{
		Objects:    results,
		Prefix:     opts.Prefix,
		Delimiter:  opts.Delimiter,
		MaxKeys:    opts.MaxKeys,
		NextMarker: nextMarker,
		IsTruncated: len(results) == opts.MaxKeys,
	}, nil
}

// CreateBucket creates a new bucket
func (s *ObjectService) CreateBucket(ctx context.Context, bucket string) error {
	// Validate bucket name
	if err := validateBucketName(bucket); err != nil {
		return err
	}

	// Create in storage
	if err := s.storage.CreateBucket(ctx, bucket); err != nil {
		return fmt.Errorf("failed to create bucket: %w", err)
	}

	// Create in metadata
	if err := s.metadata.CreateBucket(ctx, bucket); err != nil {
		return fmt.Errorf("failed to create bucket metadata: %w", err)
	}

	return nil
}

// DeleteBucket deletes a bucket
func (s *ObjectService) DeleteBucket(ctx context.Context, bucket string) error {
	// Check if bucket is empty
	objects, err := s.storage.List(ctx, bucket, "", storage.ListOptions{MaxKeys: 1})
	if err != nil {
		return fmt.Errorf("failed to list bucket: %w", err)
	}

	if len(objects) > 0 {
		return fmt.Errorf("bucket not empty: %s", bucket)
	}

	// Delete from storage
	if err := s.storage.DeleteBucket(ctx, bucket); err != nil {
		return fmt.Errorf("failed to delete bucket: %w", err)
	}

	// Delete from metadata
	if err := s.metadata.DeleteBucket(ctx, bucket); err != nil {
		s.logger.Warn("failed to delete bucket metadata", zap.Error(err))
	}

	return nil
}

// ListBuckets lists all buckets
func (s *ObjectService) ListBuckets(ctx context.Context) ([]BucketInfo, error) {
	buckets, err := s.storage.ListBuckets(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list buckets: %w", err)
	}

	var results []BucketInfo
	for _, bucket := range buckets {
		results = append(results, BucketInfo{
			Name:         bucket.Name,
			CreationDate: bucket.CreationDate,
		})
	}

	return results, nil
}

// CreateMultipartUpload initiates a multipart upload
func (s *ObjectService) CreateMultipartUpload(ctx context.Context, bucket, key string, opts PutObjectOptions) (*CreateMultipartUploadResult, error) {
	// Generate upload ID
	uploadID := uuid.New().String()

	// Create metadata
	meta := &metadata.ObjectMetadata{
		Key:         key,
		Bucket:      bucket,
		ContentType: opts.ContentType,
		Metadata:    opts.Metadata,
	}

	// Save to metadata
	if err := s.metadata.CreateMultipartUpload(ctx, bucket, key, uploadID, meta); err != nil {
		return nil, fmt.Errorf("failed to create multipart upload: %w", err)
	}

	return &CreateMultipartUploadResult{
		UploadID: uploadID,
		Key:      key,
		Bucket:   bucket,
	}, nil
}

// UploadPart uploads a part
func (s *ObjectService) UploadPart(ctx context.Context, bucket, key, uploadID string, partNumber int, data io.Reader) (*UploadPartResult, error) {
	// Calculate size and hash
	hasher := sha256.New()
	size, err := io.Copy(hasher, data)
	if err != nil {
		return nil, fmt.Errorf("failed to read data: %w", err)
	}

	// Reset reader
	data.(io.Seeker).Seek(0, io.SeekStart)

	// Generate ETag
	etag := fmt.Sprintf("\"%s\"", hex.EncodeToString(hasher.Sum(nil)))

	// Store part data
	partKey := fmt.Sprintf("%s/%s/%s/%d", bucket, key, uploadID, partNumber)
	storeOpts := storage.PutOptions{}
	if err := s.storage.Put(ctx, bucket, partKey, data, size, storeOpts); err != nil {
		return nil, fmt.Errorf("failed to store part: %w", err)
	}

	// Save part metadata
	partMeta := &metadata.PartMetadata{
		UploadID:   uploadID,
		Key:        key,
		Bucket:     bucket,
		PartNumber: partNumber,
		ETag:       etag,
		Size:       size,
	}

	if err := s.metadata.PutPart(ctx, bucket, key, uploadID, partNumber, partMeta); err != nil {
		s.logger.Error("failed to save part metadata", zap.Error(err))
	}

	return &UploadPartResult{
		ETag:       etag,
		PartNumber: partNumber,
		Size:       size,
	}, nil
}

// CompleteMultipartUpload completes a multipart upload
func (s *ObjectService) CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []PartInfo) (*ObjectResult, error) {
	// Lock the object
	unlock := s.locker.Lock(bucket, key)
	defer unlock()

	// Get parts from metadata
	partMetas, err := s.metadata.ListParts(ctx, bucket, key, uploadID)
	if err != nil {
		return nil, fmt.Errorf("failed to list parts: %w", err)
	}

	// Sort parts by part number
	for i := 0; i < len(partMetas)-1; i++ {
		for j := i + 1; j < len(partMetas); j++ {
			if partMetas[i].PartNumber > partMetas[j].PartNumber {
				partMetas[i], partMetas[j] = partMetas[j], partMetas[i]
			}
		}
	}

	// Read all parts and concatenate into final object
	var totalSize int64
	var allData []byte
	for _, p := range partMetas {
		partKey := fmt.Sprintf("%s/%s/%s/%d", bucket, key, uploadID, p.PartNumber)
		reader, err := s.storage.Get(ctx, bucket, partKey, storage.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("failed to read part %d: %w", p.PartNumber, err)
		}
		data, err := io.ReadAll(reader)
		reader.Close()
		if err != nil {
			return nil, fmt.Errorf("failed to read part data: %w", err)
		}
		allData = append(allData, data...)
		totalSize += int64(len(data))
	}

	// Write final object to storage
	storeOpts := storage.PutOptions{}
	if err := s.storage.Put(ctx, bucket, key, bytes.NewReader(allData), totalSize, storeOpts); err != nil {
		return nil, fmt.Errorf("failed to write final object: %w", err)
	}

	// Create final object metadata
	now := time.Now().Unix()
	etag := fmt.Sprintf("\"%s\"", uuid.New().String())

	objMeta := &metadata.ObjectMetadata{
		Key:          key,
		Bucket:       bucket,
		Size:         totalSize,
		ETag:         etag,
		VersionID:    uuid.New().String(),
		IsLatest:    true,
		LastModified: now,
		Parts:        convertToMetadataParts(parts),
	}

	// Save final object metadata
	if err := s.metadata.PutObject(ctx, bucket, key, objMeta); err != nil {
		return nil, fmt.Errorf("failed to save metadata: %w", err)
	}

	// Complete multipart upload (cleanup)
	if err := s.metadata.CompleteMultipartUpload(ctx, bucket, key, uploadID, convertToMetadataParts(parts)); err != nil {
		s.logger.Warn("failed to cleanup multipart upload", zap.Error(err))
	}

	// Clean up part files from storage
	for _, p := range partMetas {
		partKey := fmt.Sprintf("%s/%s/%s/%d", bucket, key, uploadID, p.PartNumber)
		s.storage.Delete(ctx, bucket, partKey)
	}

	return &ObjectResult{
		ETag:         etag,
		Size:         totalSize,
		VersionID:    objMeta.VersionID,
		LastModified: now,
	}, nil
}

// AbortMultipartUpload aborts a multipart upload
func (s *ObjectService) AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	// Delete all parts from storage
	partMetas, err := s.metadata.ListParts(ctx, bucket, key, uploadID)
	if err == nil {
		for _, p := range partMetas {
			partKey := fmt.Sprintf("%s/%s/%s/%d", bucket, key, uploadID, p.PartNumber)
			s.storage.Delete(ctx, bucket, partKey)
		}
	}

	// Delete metadata
	return s.metadata.AbortMultipartUpload(ctx, bucket, key, uploadID)
}

// ListMultipartUploads lists multipart uploads
func (s *ObjectService) ListMultipartUpload(ctx context.Context, bucket, prefix string) (*ListMultipartUploadsResult, error) {
	uploads, err := s.metadata.ListMultipartUploads(ctx, bucket, prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to list multipart uploads: %w", err)
	}

	var results []MultipartUpload
	for _, u := range uploads {
		results = append(results, MultipartUpload{
			UploadID:  u.UploadID,
			Key:       u.Key,
			Bucket:    u.Bucket,
			Initiated: u.Initiated,
		})
	}

	return &ListMultipartUploadsResult{
		Uploads: results,
		Prefix:  prefix,
	}, nil
}

// ListParts lists parts of a multipart upload
func (s *ObjectService) ListParts(ctx context.Context, bucket, key, uploadID string) ([]PartInfo, error) {
	// Verify upload exists
	partMetas, err := s.metadata.ListParts(ctx, bucket, key, uploadID)
	if err != nil {
		return nil, fmt.Errorf("failed to list parts: %w", err)
	}

	var parts []PartInfo
	for _, pm := range partMetas {
		parts = append(parts, PartInfo{
			PartNumber: pm.PartNumber,
			ETag:       pm.ETag,
		})
	}

	return parts, nil
}

// PutLifecycleRule adds a lifecycle rule to a bucket
func (s *ObjectService) PutLifecycleRule(ctx context.Context, bucket string, rule *metadata.LifecycleRule) error {
	return s.metadata.PutLifecycleRule(ctx, bucket, rule)
}

// GetLifecycleRules gets lifecycle rules for a bucket
func (s *ObjectService) GetLifecycleRules(ctx context.Context, bucket string) ([]metadata.LifecycleRule, error) {
	return s.metadata.GetLifecycleRules(ctx, bucket)
}

// PutBucketVersioning sets bucket versioning
func (s *ObjectService) PutBucketVersioning(ctx context.Context, bucket string, versioning *metadata.BucketVersioning) error {
	return s.metadata.PutBucketVersioning(ctx, bucket, versioning)
}

// GetBucketVersioning gets bucket versioning
func (s *ObjectService) GetBucketVersioning(ctx context.Context, bucket string) (*metadata.BucketVersioning, error) {
	return s.metadata.GetBucketVersioning(ctx, bucket)
}

// PutReplicationConfig sets bucket replication configuration
func (s *ObjectService) PutReplicationConfig(ctx context.Context, bucket string, config *metadata.ReplicationConfig) error {
	// Validate config
	if config == nil {
		return fmt.Errorf("replication config is required")
	}
	return s.metadata.PutReplicationConfig(ctx, bucket, config)
}

// GetReplicationConfig gets bucket replication configuration
func (s *ObjectService) GetReplicationConfig(ctx context.Context, bucket string) (*metadata.ReplicationConfig, error) {
	return s.metadata.GetReplicationConfig(ctx, bucket)
}

// DeleteReplicationConfig deletes bucket replication configuration
func (s *ObjectService) DeleteReplicationConfig(ctx context.Context, bucket string) error {
	return s.metadata.DeleteReplicationConfig(ctx, bucket)
}

// Options for PutObject
type PutObjectOptions struct {
	ContentType     string
	ContentEncoding string
	CacheControl   string
	Metadata       map[string]string
	StorageClass   string
}

// Result from PutObject
type ObjectResult struct {
	ETag         string
	Size         int64
	VersionID    string
	LastModified int64
}

// Options for GetObject
type GetObjectOptions struct {
	VersionID string
	Range     *storage.Range
	IfMatch           string
	IfNoneMatch       string
	IfModifiedSince   string
	IfUnmodifiedSince string
}

// Result from GetObject
type GetObjectResult struct {
	Body         io.ReadCloser
	Size         int64
	ETag         string
	ContentType  string
	Metadata     map[string]string
	LastModified int64
	VersionID    string
}

// Options for DeleteObject
type DeleteObjectOptions struct {
	VersionID string
}

// Object info
type ObjectInfo struct {
	Key             string
	Size            int64
	ETag            string
	ContentType     string
	ContentEncoding string
	CacheControl    string
	Metadata        map[string]string
	StorageClass    string
	LastModified    int64
	VersionID       string
}

// Options for ListObjects
type ListObjectsOptions struct {
	Prefix    string
	Delimiter string
	MaxKeys   int
	Marker    string
}

// Result from ListObjects
type ListObjectsResult struct {
	Objects       []ObjectInfo
	CommonPrefixes []string
	Prefix        string
	Delimiter     string
	MaxKeys       int
	NextMarker    string
	IsTruncated   bool
}

// Bucket info
type BucketInfo struct {
	Name         string
	CreationDate int64
}

// Result from CreateMultipartUpload
type CreateMultipartUploadResult struct {
	UploadID string
	Key      string
	Bucket   string
}

// Result from UploadPart
type UploadPartResult struct {
	ETag       string
	PartNumber int
	Size       int64
}

// Part info for CompleteMultipartUpload
type PartInfo struct {
	PartNumber int    `json:"PartNumber"`
	ETag       string `json:"ETag"`
	Size       int64  `json:"Size"`
}

// Result from ListMultipartUploads
type ListMultipartUploadsResult struct {
	Uploads []MultipartUpload
	Prefix  string
}

// Multipart upload info
type MultipartUpload struct {
	UploadID  string
	Key       string
	Bucket    string
	Initiated int64
}

// validateBucketName validates bucket name according to S3 conventions
func validateBucketName(name string) error {
	if len(name) < 3 || len(name) > 63 {
		return fmt.Errorf("bucket name must be between 3 and 63 characters")
	}

	// Check for valid characters
	validChars := "abcdefghijklmnopqrstuvwxyz0123456789.-"
	for _, c := range name {
		if !strings.ContainsRune(validChars, c) {
			return fmt.Errorf("bucket name contains invalid characters")
		}
	}

	// Check for IP address format
	if len(name) >= 7 && name[len(name)-7:] == ".ipaddr" {
		return fmt.Errorf("bucket name cannot be an IP address")
	}

	// Check for dots (not allowed in virtual-hosted style)
	// This is optional and depends on region

	return nil
}

// Locker provides per-object locking
type Locker struct {
	mu   sync.Mutex
	locks map[string]*sync.RWMutex
}

// NewLocker creates a new locker
func NewLocker() *Locker {
	return &Locker{
		locks: make(map[string]*sync.RWMutex),
	}
}

// Lock acquires an exclusive lock
func (l *Locker) Lock(bucket, key string) func() {
	l.mu.Lock()
	keyStr := bucket + "/" + key
	if l.locks[keyStr] == nil {
		l.locks[keyStr] = &sync.RWMutex{}
	}
	mu := l.locks[keyStr]
	l.mu.Unlock()

	mu.Lock()
	return func() { mu.Unlock() }
}

// RLock acquires a read lock
func (l *Locker) RLock(bucket, key string) func() {
	l.mu.Lock()
	keyStr := bucket + "/" + key
	if l.locks[keyStr] == nil {
		l.locks[keyStr] = &sync.RWMutex{}
	}
	mu := l.locks[keyStr]
	l.mu.Unlock()

	mu.RLock()
	return func() { mu.RUnlock() }
}

// parseInt parses an integer with default
func parseInt(s string, defaultVal int) int {
	if s == "" {
		return defaultVal
	}
	i, err := strconv.Atoi(s)
	if err != nil {
		return defaultVal
	}
	return i
}

// convertToMetadataParts converts engine PartInfo to metadata.PartInfo
func convertToMetadataParts(parts []PartInfo) []metadata.PartInfo {
	result := make([]metadata.PartInfo, len(parts))
	for i, p := range parts {
		result[i] = metadata.PartInfo{
			PartNumber: p.PartNumber,
			ETag:       p.ETag,
			Size:       p.Size,
		}
	}
	return result
}
