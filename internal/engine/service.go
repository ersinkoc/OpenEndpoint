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
	"github.com/openendpoint/openendpoint/internal/telemetry"
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

	// Update telemetry metrics
	start := time.Now()
	telemetry.IncStorageBytes(size)
	telemetry.IncBucketObjects(bucket)
	telemetry.IncOperation("PutObject")
	telemetry.OperationsTotal.WithLabelValues("PutObject", "success").Inc()
	telemetry.OperationDuration.WithLabelValues("PutObject", "success").Observe(time.Since(start).Seconds())
	telemetry.UpdateDashboardMetrics(size, 0)
	telemetry.UpdateLatency("PutObject", time.Since(start).Seconds())

	return &ObjectResult{
		ETag:         etag,
		Size:         size,
		VersionID:    objMeta.VersionID,
		LastModified: now,
	}, nil
}

// CopyObjectResult contains the result of a copy operation
type CopyObjectResult struct {
	ETag         string
	LastModified int64
	VersionID    string
}

// CopyObject copies an object to another location
func (s *ObjectService) CopyObject(ctx context.Context, srcBucket, srcKey, dstBucket, dstKey string) (*CopyObjectResult, error) {
	// Lock for write
	unlock := s.locker.Lock(dstBucket, dstKey)
	defer unlock()

	// Check source bucket exists
	if _, err := s.metadata.GetBucket(ctx, srcBucket); err != nil {
		return nil, fmt.Errorf("source bucket not found: %s", srcBucket)
	}

	// Check destination bucket exists
	if _, err := s.metadata.GetBucket(ctx, dstBucket); err != nil {
		return nil, fmt.Errorf("destination bucket not found: %s", dstBucket)
	}

	// Get source object metadata
	srcMeta, err := s.metadata.GetObject(ctx, srcBucket, srcKey, "")
	if err != nil {
		return nil, fmt.Errorf("source object not found: %s/%s", srcBucket, srcKey)
	}

	// Get source object data
	data, err := s.storage.Get(ctx, srcBucket, srcKey, storage.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to read source object: %w", err)
	}
	defer data.Close()

	// Copy to destination
	dstMeta := &metadata.ObjectMetadata{
		Key:             dstKey,
		Bucket:          dstBucket,
		Size:            srcMeta.Size,
		ETag:            srcMeta.ETag,
		ContentType:     srcMeta.ContentType,
		ContentEncoding: srcMeta.ContentEncoding,
		CacheControl:    srcMeta.CacheControl,
		Metadata:        srcMeta.Metadata,
		StorageClass:    srcMeta.StorageClass,
		VersionID:       uuid.New().String(),
		IsLatest:        true,
		LastModified:    time.Now().Unix(),
	}

	// Write data to destination
	putOpts := storage.PutOptions{
		ContentType:     srcMeta.ContentType,
		ContentEncoding: srcMeta.ContentEncoding,
		CacheControl:    srcMeta.CacheControl,
		Metadata:        srcMeta.Metadata,
		StorageClass:    srcMeta.StorageClass,
	}
	if err := s.storage.Put(ctx, dstBucket, dstKey, data, srcMeta.Size, putOpts); err != nil {
		return nil, fmt.Errorf("failed to write destination object: %w", err)
	}

	// Save metadata
	if err := s.metadata.PutObject(ctx, dstBucket, dstKey, dstMeta); err != nil {
		s.logger.Error("failed to save copy metadata", zap.Error(err))
	}

	return &CopyObjectResult{
		ETag:         dstMeta.ETag,
		LastModified: dstMeta.LastModified,
		VersionID:    dstMeta.VersionID,
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

	// Update telemetry metrics
	start := time.Now()
	telemetry.IncOperation("GetObject")
	telemetry.OperationsTotal.WithLabelValues("GetObject", "success").Inc()
	telemetry.OperationDuration.WithLabelValues("GetObject", "success").Observe(time.Since(start).Seconds())
	telemetry.UpdateLatency("GetObject", time.Since(start).Seconds())
	// Note: actual bytes downloaded would be tracked when the reader is read

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

	// Update telemetry metrics
	telemetry.DecBucketObjects(bucket)
	telemetry.IncOperation("DeleteObject")
	telemetry.OperationsTotal.WithLabelValues("DeleteObject", "success").Inc()
	telemetry.OperationDuration.WithLabelValues("DeleteObject", "success").Observe(0) // Quick operation

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

	// Update telemetry metrics
	telemetry.OperationsTotal.WithLabelValues("HeadObject", "success").Inc()

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
	result, err := s.storage.List(ctx, bucket, opts.Prefix, storeOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to list objects: %w", err)
	}

	// Update telemetry metrics
	start := time.Now()
	telemetry.IncOperation("ListObjects")
	telemetry.OperationsTotal.WithLabelValues("ListObjects", "success").Inc()
	telemetry.OperationDuration.WithLabelValues("ListObjects", "success").Observe(time.Since(start).Seconds())

	// Convert to results
	var objectInfos []ObjectInfo
	for _, obj := range result.Objects {
		objectInfos = append(objectInfos, ObjectInfo{
			Key:          obj.Key,
			Size:         obj.Size,
			ETag:         obj.ETag,
			LastModified: obj.LastModified,
		})
	}

	// Get next marker
	var nextMarker string
	if len(objectInfos) > 0 {
		nextMarker = objectInfos[len(objectInfos)-1].Key
	}

	return &ListObjectsResult{
		Objects:        objectInfos,
		CommonPrefixes: result.CommonPrefixes,
		Prefix:         opts.Prefix,
		Delimiter:      opts.Delimiter,
		MaxKeys:        opts.MaxKeys,
		NextMarker:     nextMarker,
		IsTruncated:    len(objectInfos) == opts.MaxKeys,
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

	// Update telemetry metrics
	telemetry.SetStorageBuckets(0) // This will be updated by ListBuckets

	return nil
}

// DeleteBucket deletes a bucket
func (s *ObjectService) DeleteBucket(ctx context.Context, bucket string) error {
	// Check if bucket is empty
	result, err := s.storage.List(ctx, bucket, "", storage.ListOptions{MaxKeys: 1})
	if err != nil {
		return fmt.Errorf("failed to list bucket: %w", err)
	}

	if len(result.Objects) > 0 {
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

	// Update telemetry metrics
	telemetry.DeleteBucketMetrics(bucket)

	return nil
}

// ListBuckets lists all buckets
func (s *ObjectService) ListBuckets(ctx context.Context) ([]BucketInfo, error) {
	buckets, err := s.storage.ListBuckets(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list buckets: %w", err)
	}

	// Update telemetry metrics
	telemetry.SetStorageBuckets(int64(len(buckets)))

	var results []BucketInfo
	for _, bucket := range buckets {
		results = append(results, BucketInfo{
			Name:         bucket.Name,
			CreationDate: bucket.CreationDate,
		})
	}

	return results, nil
}

// GetBucket retrieves bucket metadata
func (s *ObjectService) GetBucket(ctx context.Context, bucket string) (*metadata.BucketMetadata, error) {
	return s.metadata.GetBucket(ctx, bucket)
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

// DeleteLifecycleRule deletes a lifecycle rule from a bucket
func (s *ObjectService) DeleteLifecycleRule(ctx context.Context, bucket, ruleID string) error {
	return s.metadata.DeleteLifecycleRule(ctx, bucket, ruleID)
}

// PutBucketVersioning sets bucket versioning
func (s *ObjectService) PutBucketVersioning(ctx context.Context, bucket string, versioning *metadata.BucketVersioning) error {
	return s.metadata.PutBucketVersioning(ctx, bucket, versioning)
}

// GetBucketVersioning gets bucket versioning
func (s *ObjectService) GetBucketVersioning(ctx context.Context, bucket string) (*metadata.BucketVersioning, error) {
	return s.metadata.GetBucketVersioning(ctx, bucket)
}

// PutBucketLifecycle sets lifecycle configuration for a bucket
func (s *ObjectService) PutBucketLifecycle(ctx context.Context, bucket string, rules []metadata.LifecycleRule) error {
	// If nil or empty rules, delete all lifecycle configuration
	if len(rules) == 0 {
		// Get existing rules and delete them one by one
		existingRules, err := s.metadata.GetLifecycleRules(ctx, bucket)
		if err != nil {
			return err
		}
		for _, rule := range existingRules {
			if err := s.metadata.DeleteLifecycleRule(ctx, bucket, rule.ID); err != nil {
				return err
			}
		}
		return nil
	}

	// Delete existing rules and add new ones
	for _, rule := range rules {
		if err := s.metadata.PutLifecycleRule(ctx, bucket, &rule); err != nil {
			return err
		}
	}
	return nil
}

// GetBucketLifecycle gets lifecycle configuration for a bucket
func (s *ObjectService) GetBucketLifecycle(ctx context.Context, bucket string) ([]metadata.LifecycleRule, error) {
	return s.metadata.GetLifecycleRules(ctx, bucket)
}

// PutBucketCors sets CORS configuration for a bucket
func (s *ObjectService) PutBucketCors(ctx context.Context, bucket string, cors *metadata.CORSConfiguration) error {
	if cors == nil {
		return fmt.Errorf("CORS configuration is required")
	}
	return s.metadata.PutBucketCors(ctx, bucket, cors)
}

// GetBucketCors gets CORS configuration for a bucket
func (s *ObjectService) GetBucketCors(ctx context.Context, bucket string) (*metadata.CORSConfiguration, error) {
	return s.metadata.GetBucketCors(ctx, bucket)
}

// DeleteBucketCors deletes CORS configuration
func (s *ObjectService) DeleteBucketCors(ctx context.Context, bucket string) error {
	return s.metadata.DeleteBucketCors(ctx, bucket)
}

// PutBucketPolicy sets bucket policy
func (s *ObjectService) PutBucketPolicy(ctx context.Context, bucket string, policy *string) error {
	if policy == nil {
		return fmt.Errorf("policy is required")
	}
	return s.metadata.PutBucketPolicy(ctx, bucket, policy)
}

// GetBucketPolicy gets bucket policy
func (s *ObjectService) GetBucketPolicy(ctx context.Context, bucket string) (*string, error) {
	return s.metadata.GetBucketPolicy(ctx, bucket)
}

// DeleteBucketPolicy deletes bucket policy
func (s *ObjectService) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	return s.metadata.DeleteBucketPolicy(ctx, bucket)
}

// PutBucketEncryption sets bucket encryption
func (s *ObjectService) PutBucketEncryption(ctx context.Context, bucket string, encryption *metadata.BucketEncryption) error {
	if encryption == nil {
		return fmt.Errorf("encryption is required")
	}
	return s.metadata.PutBucketEncryption(ctx, bucket, encryption)
}

// GetBucketEncryption gets bucket encryption
func (s *ObjectService) GetBucketEncryption(ctx context.Context, bucket string) (*metadata.BucketEncryption, error) {
	return s.metadata.GetBucketEncryption(ctx, bucket)
}

// DeleteBucketEncryption deletes bucket encryption
func (s *ObjectService) DeleteBucketEncryption(ctx context.Context, bucket string) error {
	return s.metadata.DeleteBucketEncryption(ctx, bucket)
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

// PutBucketTags sets bucket tags
func (s *ObjectService) PutBucketTags(ctx context.Context, bucket string, tags map[string]string) error {
	return s.metadata.PutBucketTags(ctx, bucket, tags)
}

// GetBucketTags gets bucket tags
func (s *ObjectService) GetBucketTags(ctx context.Context, bucket string) (map[string]string, error) {
	return s.metadata.GetBucketTags(ctx, bucket)
}

// DeleteBucketTags deletes bucket tags
func (s *ObjectService) DeleteBucketTags(ctx context.Context, bucket string) error {
	return s.metadata.DeleteBucketTags(ctx, bucket)
}

// PutObjectLock sets object lock configuration for a bucket
func (s *ObjectService) PutObjectLock(ctx context.Context, bucket string, config *metadata.ObjectLockConfig) error {
	if config == nil {
		return fmt.Errorf("object lock configuration is required")
	}
	return s.metadata.PutObjectLock(ctx, bucket, config)
}

// GetObjectLock gets object lock configuration for a bucket
func (s *ObjectService) GetObjectLock(ctx context.Context, bucket string) (*metadata.ObjectLockConfig, error) {
	return s.metadata.GetObjectLock(ctx, bucket)
}

// DeleteObjectLock deletes object lock configuration
func (s *ObjectService) DeleteObjectLock(ctx context.Context, bucket string) error {
	return s.metadata.DeleteObjectLock(ctx, bucket)
}

// PutObjectRetention sets object retention
func (s *ObjectService) PutObjectRetention(ctx context.Context, bucket, key string, retention *metadata.ObjectRetention) error {
	return s.metadata.PutObjectRetention(ctx, bucket, key, retention)
}

// GetObjectRetention gets object retention
func (s *ObjectService) GetObjectRetention(ctx context.Context, bucket, key string) (*metadata.ObjectRetention, error) {
	return s.metadata.GetObjectRetention(ctx, bucket, key)
}

// PutObjectLegalHold sets object legal hold
func (s *ObjectService) PutObjectLegalHold(ctx context.Context, bucket, key string, legalHold *metadata.ObjectLegalHold) error {
	return s.metadata.PutObjectLegalHold(ctx, bucket, key, legalHold)
}

// GetObjectLegalHold gets object legal hold
func (s *ObjectService) GetObjectLegalHold(ctx context.Context, bucket, key string) (*metadata.ObjectLegalHold, error) {
	return s.metadata.GetObjectLegalHold(ctx, bucket, key)
}

// PutPublicAccessBlock sets public access block configuration for a bucket
func (s *ObjectService) PutPublicAccessBlock(ctx context.Context, bucket string, config *metadata.PublicAccessBlockConfiguration) error {
	if config == nil {
		return fmt.Errorf("public access block configuration is required")
	}
	return s.metadata.PutPublicAccessBlock(ctx, bucket, config)
}

// GetPublicAccessBlock gets public access block configuration for a bucket
func (s *ObjectService) GetPublicAccessBlock(ctx context.Context, bucket string) (*metadata.PublicAccessBlockConfiguration, error) {
	return s.metadata.GetPublicAccessBlock(ctx, bucket)
}

// DeletePublicAccessBlock deletes public access block configuration
func (s *ObjectService) DeletePublicAccessBlock(ctx context.Context, bucket string) error {
	return s.metadata.DeletePublicAccessBlock(ctx, bucket)
}

// PutBucketAccelerate sets bucket accelerate configuration
func (s *ObjectService) PutBucketAccelerate(ctx context.Context, bucket string, config *metadata.BucketAccelerateConfiguration) error {
	if config == nil {
		return fmt.Errorf("accelerate configuration is required")
	}
	return s.metadata.PutBucketAccelerate(ctx, bucket, config)
}

// GetBucketAccelerate gets bucket accelerate configuration
func (s *ObjectService) GetBucketAccelerate(ctx context.Context, bucket string) (*metadata.BucketAccelerateConfiguration, error) {
	return s.metadata.GetBucketAccelerate(ctx, bucket)
}

// DeleteBucketAccelerate deletes bucket accelerate configuration
func (s *ObjectService) DeleteBucketAccelerate(ctx context.Context, bucket string) error {
	return s.metadata.DeleteBucketAccelerate(ctx, bucket)
}

// PutBucketInventory sets bucket inventory configuration
func (s *ObjectService) PutBucketInventory(ctx context.Context, bucket, id string, config *metadata.InventoryConfiguration) error {
	if config == nil {
		return fmt.Errorf("inventory configuration is required")
	}
	return s.metadata.PutBucketInventory(ctx, bucket, id, config)
}

// GetBucketInventory gets bucket inventory configuration
func (s *ObjectService) GetBucketInventory(ctx context.Context, bucket, id string) (*metadata.InventoryConfiguration, error) {
	return s.metadata.GetBucketInventory(ctx, bucket, id)
}

// ListBucketInventory lists all inventory configurations for a bucket
func (s *ObjectService) ListBucketInventory(ctx context.Context, bucket string) ([]metadata.InventoryConfiguration, error) {
	return s.metadata.ListBucketInventory(ctx, bucket)
}

// DeleteBucketInventory deletes bucket inventory configuration
func (s *ObjectService) DeleteBucketInventory(ctx context.Context, bucket, id string) error {
	return s.metadata.DeleteBucketInventory(ctx, bucket, id)
}

// PutBucketAnalytics sets bucket analytics configuration
func (s *ObjectService) PutBucketAnalytics(ctx context.Context, bucket, id string, config *metadata.AnalyticsConfiguration) error {
	if config == nil {
		return fmt.Errorf("analytics configuration is required")
	}
	return s.metadata.PutBucketAnalytics(ctx, bucket, id, config)
}

// GetBucketAnalytics gets bucket analytics configuration
func (s *ObjectService) GetBucketAnalytics(ctx context.Context, bucket, id string) (*metadata.AnalyticsConfiguration, error) {
	return s.metadata.GetBucketAnalytics(ctx, bucket, id)
}

// ListBucketAnalytics lists all analytics configurations for a bucket
func (s *ObjectService) ListBucketAnalytics(ctx context.Context, bucket string) ([]metadata.AnalyticsConfiguration, error) {
	return s.metadata.ListBucketAnalytics(ctx, bucket)
}

// DeleteBucketAnalytics deletes bucket analytics configuration
func (s *ObjectService) DeleteBucketAnalytics(ctx context.Context, bucket, id string) error {
	return s.metadata.DeleteBucketAnalytics(ctx, bucket, id)
}

// GeneratePresignedURL generates a presigned URL for an object
func (s *ObjectService) GeneratePresignedURL(ctx context.Context, bucket, key, method string, expires int64) (string, error) {
	if bucket == "" {
		return "", fmt.Errorf("bucket is required")
	}
	if key == "" {
		return "", fmt.Errorf("key is required")
	}
	if method == "" {
		return "", fmt.Errorf("method is required")
	}

	// Check if the object exists (for PUT/DELETE)
	if method == "PUT" || method == "DELETE" {
		_, err := s.metadata.GetObject(ctx, bucket, key, "")
		if err != nil {
			return "", fmt.Errorf("object not found: %w", err)
		}
	}

	// Generate presigned URL using AWS Signature V4
	// For now, we'll create a simple presigned URL that includes the expiration
	// In production, this would use proper AWS Signature V4 signing
	scheme := "http"
	host := "localhost:8080"

	// Create the presigned URL request
	req := &metadata.PresignedURLRequest{
		Bucket:  bucket,
		Key:     key,
		Method:  method,
		Expires: expires,
		Scheme:  scheme,
		Host:    host,
	}

	// Store the presigned URL metadata
	urlStr := fmt.Sprintf("%s://%s/s3/%s/%s?presigned=%d", scheme, host, bucket, key, time.Now().Unix()+expires)
	err := s.metadata.PutPresignedURL(ctx, urlStr, req)
	if err != nil {
		return "", fmt.Errorf("failed to store presigned URL: %w", err)
	}

	return urlStr, nil
}

// ValidatePresignedURL validates a presigned URL
func (s *ObjectService) ValidatePresignedURL(ctx context.Context, url string) (*metadata.PresignedURLRequest, error) {
	return s.metadata.GetPresignedURL(ctx, url)
}

// DeletePresignedURL deletes a presigned URL
func (s *ObjectService) DeletePresignedURL(ctx context.Context, url string) error {
	return s.metadata.DeletePresignedURL(ctx, url)
}

// PutBucketWebsite sets bucket website configuration
func (s *ObjectService) PutBucketWebsite(ctx context.Context, bucket string, config *metadata.WebsiteConfiguration) error {
	if config == nil {
		return fmt.Errorf("website configuration is required")
	}
	return s.metadata.PutBucketWebsite(ctx, bucket, config)
}

// GetBucketWebsite gets bucket website configuration
func (s *ObjectService) GetBucketWebsite(ctx context.Context, bucket string) (*metadata.WebsiteConfiguration, error) {
	return s.metadata.GetBucketWebsite(ctx, bucket)
}

// DeleteBucketWebsite deletes bucket website configuration
func (s *ObjectService) DeleteBucketWebsite(ctx context.Context, bucket string) error {
	return s.metadata.DeleteBucketWebsite(ctx, bucket)
}

// PutBucketNotification sets bucket notification configuration
func (s *ObjectService) PutBucketNotification(ctx context.Context, bucket string, config *metadata.NotificationConfiguration) error {
	if config == nil {
		return fmt.Errorf("notification configuration is required")
	}
	return s.metadata.PutBucketNotification(ctx, bucket, config)
}

// GetBucketNotification gets bucket notification configuration
func (s *ObjectService) GetBucketNotification(ctx context.Context, bucket string) (*metadata.NotificationConfiguration, error) {
	return s.metadata.GetBucketNotification(ctx, bucket)
}

// DeleteBucketNotification deletes bucket notification configuration
func (s *ObjectService) DeleteBucketNotification(ctx context.Context, bucket string) error {
	return s.metadata.DeleteBucketNotification(ctx, bucket)
}

// PutBucketLogging sets bucket logging configuration
func (s *ObjectService) PutBucketLogging(ctx context.Context, bucket string, config *metadata.LoggingConfiguration) error {
	if config == nil {
		return fmt.Errorf("logging configuration is required")
	}
	return s.metadata.PutBucketLogging(ctx, bucket, config)
}

// GetBucketLogging gets bucket logging configuration
func (s *ObjectService) GetBucketLogging(ctx context.Context, bucket string) (*metadata.LoggingConfiguration, error) {
	return s.metadata.GetBucketLogging(ctx, bucket)
}

// DeleteBucketLogging deletes bucket logging configuration
func (s *ObjectService) DeleteBucketLogging(ctx context.Context, bucket string) error {
	return s.metadata.DeleteBucketLogging(ctx, bucket)
}

// PutBucketLocation stores bucket location
func (s *ObjectService) PutBucketLocation(ctx context.Context, bucket string, location string) error {
	return s.metadata.PutBucketLocation(ctx, bucket, location)
}

// GetBucketLocation retrieves bucket location
func (s *ObjectService) GetBucketLocation(ctx context.Context, bucket string) (string, error) {
	return s.metadata.GetBucketLocation(ctx, bucket)
}

// PutBucketOwnershipControls stores bucket ownership controls
func (s *ObjectService) PutBucketOwnershipControls(ctx context.Context, bucket string, config *metadata.OwnershipControls) error {
	return s.metadata.PutBucketOwnershipControls(ctx, bucket, config)
}

// GetBucketOwnershipControls retrieves bucket ownership controls
func (s *ObjectService) GetBucketOwnershipControls(ctx context.Context, bucket string) (*metadata.OwnershipControls, error) {
	return s.metadata.GetBucketOwnershipControls(ctx, bucket)
}

// DeleteBucketOwnershipControls deletes bucket ownership controls
func (s *ObjectService) DeleteBucketOwnershipControls(ctx context.Context, bucket string) error {
	return s.metadata.DeleteBucketOwnershipControls(ctx, bucket)
}

// PutBucketMetrics stores bucket metrics configuration
func (s *ObjectService) PutBucketMetrics(ctx context.Context, bucket string, id string, config *metadata.MetricsConfiguration) error {
	return s.metadata.PutBucketMetrics(ctx, bucket, id, config)
}

// GetBucketMetrics retrieves bucket metrics configuration
func (s *ObjectService) GetBucketMetrics(ctx context.Context, bucket string, id string) (*metadata.MetricsConfiguration, error) {
	return s.metadata.GetBucketMetrics(ctx, bucket, id)
}

// DeleteBucketMetrics deletes bucket metrics configuration
func (s *ObjectService) DeleteBucketMetrics(ctx context.Context, bucket string, id string) error {
	return s.metadata.DeleteBucketMetrics(ctx, bucket, id)
}

// ListBucketMetrics lists all metrics configurations for a bucket
func (s *ObjectService) ListBucketMetrics(ctx context.Context, bucket string) ([]metadata.MetricsConfiguration, error) {
	return s.metadata.ListBucketMetrics(ctx, bucket)
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
	StorageClass string
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
	IsLatest        bool
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
