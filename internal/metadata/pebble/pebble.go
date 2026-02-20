package pebble

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/google/uuid"
	"github.com/openendpoint/openendpoint/internal/metadata"
)

type PebbleStore struct {
	db     *pebble.DB
	rootDir string
	mu     sync.RWMutex
}

// New creates a new Pebble metadata store
func New(rootDir string) (*PebbleStore, error) {
	dbPath := filepath.Join(rootDir, "metadata")

	opts := &pebble.Options{
		// Performance settings
		Cache:           pebble.NewCache(256 << 20), // 256MB cache
		MaxOpenFiles:   1000,
		BytesPerSync:   512 << 10,
		WALBytesPerSync: 512 << 10,

		// Memory settings
		MemTableSize: 8 << 20,
	}

	db, err := pebble.Open(dbPath, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open pebble database: %w", err)
	}

	return &PebbleStore{
		db:      db,
		rootDir: rootDir,
	}, nil
}

// bucketKey generates a bucket key
func bucketKey(bucket string) []byte {
	return []byte("bucket:" + bucket)
}

// objectKey generates an object key
func objectKey(bucket, key string) []byte {
	return []byte("object:" + bucket + "/" + key)
}

// multipartKey generates a multipart upload key
func multipartKey(bucket, key, uploadID string) []byte {
	return []byte("multipart:" + bucket + "/" + key + "/" + uploadID)
}

// lifecycleKey generates a lifecycle rule key
func lifecycleKey(bucket string) []byte {
	return []byte("lifecycle:" + bucket)
}

// versioningKey generates a versioning key
func versioningKey(bucket string) []byte {
	return []byte("versioning:" + bucket)
}

// replicationKey generates a replication config key
func replicationKey(bucket string) []byte {
	return []byte("replication:" + bucket)
}

// corsKey generates a CORS key
func corsKey(bucket string) []byte {
	return []byte("cors:" + bucket)
}

// policyKey generates a policy key
func policyKey(bucket string) []byte {
	return []byte("policy:" + bucket)
}

// encryptionKey generates an encryption key
func encryptionKey(bucket string) []byte {
	return []byte("encryption:" + bucket)
}

// tagsKey generates a tags key
func tagsKey(bucket string) []byte {
	return []byte("tags:" + bucket)
}

// objectLockKey generates an object lock key
func objectLockKey(bucket string) []byte {
	return []byte("objectlock:" + bucket)
}

// publicAccessBlockKey generates a public access block key
func publicAccessBlockKey(bucket string) []byte {
	return []byte("publicaccessblock:" + bucket)
}

// accelerateKey generates an accelerate key
func accelerateKey(bucket string) []byte {
	return []byte("accelerate:" + bucket)
}

// inventoryKey generates an inventory key
func inventoryKey(bucket, id string) []byte {
	return []byte("inventory:" + bucket + "/" + id)
}

// analyticsKey generates an analytics key
func analyticsKey(bucket, id string) []byte {
	return []byte("analytics:" + bucket + "/" + id)
}

// CreateBucket creates a new bucket
func (p *PebbleStore) CreateBucket(ctx context.Context, bucket string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	meta := &metadata.BucketMetadata{
		Name:          bucket,
		CreationDate:  nowUnix(),
		Owner:         "root",
		Region:        "us-east-1",
	}

	data, err := encodeMeta(meta)
	if err != nil {
		return err
	}

	return p.db.Set(bucketKey(bucket), data, pebble.Sync)
}

// DeleteBucket deletes a bucket
func (p *PebbleStore) DeleteBucket(ctx context.Context, bucket string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.db.Delete(bucketKey(bucket), pebble.Sync)
}

// GetBucket gets bucket metadata
func (p *PebbleStore) GetBucket(ctx context.Context, bucket string) (*metadata.BucketMetadata, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	data, closer, err := p.db.Get(bucketKey(bucket))
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, fmt.Errorf("bucket not found: %s", bucket)
		}
		return nil, err
	}
	defer closer.Close()

	var meta metadata.BucketMetadata
	if err := decodeMeta(data, &meta); err != nil {
		return nil, err
	}

	return &meta, nil
}

// ListBuckets lists all buckets
func (p *PebbleStore) ListBuckets(ctx context.Context) ([]string, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	iter, err := p.db.NewIter(nil)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var buckets []string
	for iter.First(); iter.Valid(); iter.Next() {
		key := string(iter.Key())
		if len(key) > 7 && key[:7] == "bucket:" {
			buckets = append(buckets, key[7:])
		}
	}

	return buckets, nil
}

// PutObject stores object metadata
func (p *PebbleStore) PutObject(ctx context.Context, bucket, key string, meta *metadata.ObjectMetadata) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	data, err := encodeMeta(meta)
	if err != nil {
		return err
	}

	// Include version ID in key if present
	keyStr := bucket + "/" + key
	if meta.VersionID != "" {
		keyStr += "?v=" + meta.VersionID
	}

	return p.db.Set(objectKey(bucket, key), data, pebble.Sync)
}

// GetObject gets object metadata
func (p *PebbleStore) GetObject(ctx context.Context, bucket, key string, versionID string) (*metadata.ObjectMetadata, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	data, closer, err := p.db.Get(objectKey(bucket, key))
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, fmt.Errorf("object not found: %s/%s", bucket, key)
		}
		return nil, err
	}
	defer closer.Close()

	var meta metadata.ObjectMetadata
	if err := decodeMeta(data, &meta); err != nil {
		return nil, err
	}

	// If version ID specified, verify it matches
	if versionID != "" && meta.VersionID != versionID {
		return nil, fmt.Errorf("version not found: %s", versionID)
	}

	return &meta, nil
}

// DeleteObject deletes object metadata
func (p *PebbleStore) DeleteObject(ctx context.Context, bucket, key string, versionID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.db.Delete(objectKey(bucket, key), pebble.Sync)
}

// ListObjects lists objects with optional prefix
func (p *PebbleStore) ListObjects(ctx context.Context, bucket, prefix string, opts metadata.ListOptions) ([]metadata.ObjectMetadata, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	prefixKey := "object:" + bucket + "/" + prefix

	iter, err := p.db.NewIter(nil)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var objects []metadata.ObjectMetadata
	maxKeys := opts.MaxKeys
	if maxKeys == 0 {
		maxKeys = 1000
	}

	for iter.SeekGE([]byte(prefixKey)); iter.Valid() && len(objects) < maxKeys; iter.Next() {
		key := string(iter.Key())
		if len(key) < 8 || key[:8] != "object:" {
			break
		}

		// Extract bucket/key
		rest := key[8:]
		if len(rest) <= len(bucket)+1 || rest[:len(bucket)+1] != bucket+"/" {
			continue
		}
		objKey := rest[len(bucket)+1:]
		_ = objKey // Reserved for future use

		var meta metadata.ObjectMetadata
		if err := decodeMeta(iter.Value(), &meta); err != nil {
			continue
		}

		objects = append(objects, meta)
	}

	return objects, nil
}

// CreateMultipartUpload creates a new multipart upload
func (p *PebbleStore) CreateMultipartUpload(ctx context.Context, bucket, key, uploadID string, meta *metadata.ObjectMetadata) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if uploadID == "" {
		uploadID = uuid.New().String()
	}

	multiMeta := &metadata.MultipartUploadMetadata{
		UploadID:  uploadID,
		Key:       key,
		Bucket:    bucket,
		Initiated: nowUnix(),
		Metadata:  meta.Metadata,
	}

	data, err := encodeMeta(multiMeta)
	if err != nil {
		return err
	}

	return p.db.Set(multipartKey(bucket, key, uploadID), data, pebble.Sync)
}

// PutPart stores part metadata
func (p *PebbleStore) PutPart(ctx context.Context, bucket, key, uploadID string, partNumber int, partMeta *metadata.PartMetadata) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	data, err := encodeMeta(partMeta)
	if err != nil {
		return err
	}

	partKey := fmt.Sprintf("part:%s/%s/%s/%d", bucket, key, uploadID, partNumber)
	return p.db.Set([]byte(partKey), data, pebble.Sync)
}

// CompleteMultipartUpload completes a multipart upload
func (p *PebbleStore) CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []metadata.PartInfo) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Delete multipart upload metadata
	err := p.db.Delete(multipartKey(bucket, key, uploadID), pebble.Sync)
	if err != nil {
		return err
	}

	// Delete all parts
	for i := 1; i <= len(parts); i++ {
		partKey := fmt.Sprintf("part:%s/%s/%s/%d", bucket, key, uploadID, i)
		p.db.Delete([]byte(partKey), pebble.Sync)
	}

	return nil
}

// AbortMultipartUpload aborts a multipart upload
func (p *PebbleStore) AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.db.Delete(multipartKey(bucket, key, uploadID), pebble.Sync)
}

// ListParts lists parts of a multipart upload
func (p *PebbleStore) ListParts(ctx context.Context, bucket, key, uploadID string) ([]metadata.PartMetadata, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	prefix := fmt.Sprintf("part:%s/%s/%s/", bucket, key, uploadID)

	iter, err := p.db.NewIter(nil)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var parts []metadata.PartMetadata
	for iter.SeekGE([]byte(prefix)); iter.Valid(); iter.Next() {
		keyStr := string(iter.Key())
		if len(keyStr) < len(prefix) || keyStr[:len(prefix)] != prefix {
			break
		}

		var partMeta metadata.PartMetadata
		if err := decodeMeta(iter.Value(), &partMeta); err != nil {
			continue
		}

		parts = append(parts, partMeta)
	}

	return parts, nil
}

// ListMultipartUploads lists multipart uploads
func (p *PebbleStore) ListMultipartUploads(ctx context.Context, bucket, prefix string) ([]metadata.MultipartUploadMetadata, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	prefixKey := "multipart:" + bucket + "/" + prefix

	iter, err := p.db.NewIter(nil)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var uploads []metadata.MultipartUploadMetadata
	for iter.SeekGE([]byte(prefixKey)); iter.Valid(); iter.Next() {
		key := string(iter.Key())
		if len(key) < 11 || key[:11] != "multipart:" {
			break
		}

		var meta metadata.MultipartUploadMetadata
		if err := decodeMeta(iter.Value(), &meta); err != nil {
			continue
		}

		uploads = append(uploads, meta)
	}

	return uploads, nil
}

// PutLifecycleRule puts a lifecycle rule
func (p *PebbleStore) PutLifecycleRule(ctx context.Context, bucket string, rule *metadata.LifecycleRule) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Get existing rules
	rules, _ := p.GetLifecycleRules(ctx, bucket)

	// Add or update rule
	found := false
	for i, r := range rules {
		if r.ID == rule.ID {
			rules[i] = *rule
			found = true
			break
		}
	}
	if !found {
		rules = append(rules, *rule)
	}

	data, err := encodeMeta(rules)
	if err != nil {
		return err
	}

	return p.db.Set(lifecycleKey(bucket), data, pebble.Sync)
}

// GetLifecycleRules gets lifecycle rules for a bucket
func (p *PebbleStore) GetLifecycleRules(ctx context.Context, bucket string) ([]metadata.LifecycleRule, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	data, closer, err := p.db.Get(lifecycleKey(bucket))
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	defer closer.Close()

	var rules []metadata.LifecycleRule
	if err := decodeMeta(data, &rules); err != nil {
		return nil, err
	}

	return rules, nil
}

// DeleteLifecycleRule deletes a lifecycle rule from a bucket
func (p *PebbleStore) DeleteLifecycleRule(ctx context.Context, bucket, ruleID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	data, closer, err := p.db.Get(lifecycleKey(bucket))
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil
		}
		return err
	}
	defer closer.Close()

	var rules []metadata.LifecycleRule
	if err := decodeMeta(data, &rules); err != nil {
		return err
	}

	// Filter out the rule with the matching ID
	var newRules []metadata.LifecycleRule
	for _, rule := range rules {
		if rule.ID != ruleID {
			newRules = append(newRules, rule)
		}
	}

	// If no rules left, delete the key
	if len(newRules) == 0 {
		return p.db.Delete(lifecycleKey(bucket), pebble.Sync)
	}

	// Save remaining rules
	data, err = encodeMeta(newRules)
	if err != nil {
		return err
	}

	return p.db.Set(lifecycleKey(bucket), data, pebble.Sync)
}

// PutBucketVersioning puts bucket versioning configuration
func (p *PebbleStore) PutBucketVersioning(ctx context.Context, bucket string, versioning *metadata.BucketVersioning) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	data, err := encodeMeta(versioning)
	if err != nil {
		return err
	}

	return p.db.Set(versioningKey(bucket), data, pebble.Sync)
}

// GetBucketVersioning gets bucket versioning configuration
func (p *PebbleStore) GetBucketVersioning(ctx context.Context, bucket string) (*metadata.BucketVersioning, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	data, closer, err := p.db.Get(versioningKey(bucket))
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	defer closer.Close()

	var versioning metadata.BucketVersioning
	if err := decodeMeta(data, &versioning); err != nil {
		return nil, err
	}

	return &versioning, nil
}

// PutBucketCors stores CORS configuration
func (p *PebbleStore) PutBucketCors(ctx context.Context, bucket string, cors *metadata.CORSConfiguration) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	data, err := encodeMeta(cors)
	if err != nil {
		return err
	}

	return p.db.Set(corsKey(bucket), data, pebble.Sync)
}

// GetBucketCors gets CORS configuration
func (p *PebbleStore) GetBucketCors(ctx context.Context, bucket string) (*metadata.CORSConfiguration, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	data, closer, err := p.db.Get(corsKey(bucket))
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	defer closer.Close()

	var cors metadata.CORSConfiguration
	if err := decodeMeta(data, &cors); err != nil {
		return nil, err
	}

	return &cors, nil
}

// DeleteBucketCors deletes CORS configuration
func (p *PebbleStore) DeleteBucketCors(ctx context.Context, bucket string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.db.Delete(corsKey(bucket), pebble.Sync)
}

// PutBucketPolicy stores bucket policy
func (p *PebbleStore) PutBucketPolicy(ctx context.Context, bucket string, policy *string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if policy == nil {
		return fmt.Errorf("policy cannot be nil")
	}

	return p.db.Set(policyKey(bucket), []byte(*policy), pebble.Sync)
}

// GetBucketPolicy gets bucket policy
func (p *PebbleStore) GetBucketPolicy(ctx context.Context, bucket string) (*string, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	data, closer, err := p.db.Get(policyKey(bucket))
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	defer closer.Close()

	policy := string(data)
	return &policy, nil
}

// DeleteBucketPolicy deletes bucket policy
func (p *PebbleStore) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.db.Delete(policyKey(bucket), pebble.Sync)
}

// PutBucketEncryption stores bucket encryption configuration
func (p *PebbleStore) PutBucketEncryption(ctx context.Context, bucket string, encryption *metadata.BucketEncryption) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	data, err := encodeMeta(encryption)
	if err != nil {
		return err
	}

	return p.db.Set(encryptionKey(bucket), data, pebble.Sync)
}

// GetBucketEncryption gets bucket encryption configuration
func (p *PebbleStore) GetBucketEncryption(ctx context.Context, bucket string) (*metadata.BucketEncryption, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	data, closer, err := p.db.Get(encryptionKey(bucket))
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	defer closer.Close()

	var encryption metadata.BucketEncryption
	if err := decodeMeta(data, &encryption); err != nil {
		return nil, err
	}

	return &encryption, nil
}

// DeleteBucketEncryption deletes encryption configuration
func (p *PebbleStore) DeleteBucketEncryption(ctx context.Context, bucket string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.db.Delete(encryptionKey(bucket), pebble.Sync)
}

// PutReplicationConfig stores replication configuration
func (p *PebbleStore) PutReplicationConfig(ctx context.Context, bucket string, config *metadata.ReplicationConfig) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	data, err := encodeMeta(config)
	if err != nil {
		return err
	}

	return p.db.Set(replicationKey(bucket), data, pebble.Sync)
}

// GetReplicationConfig gets replication configuration
func (p *PebbleStore) GetReplicationConfig(ctx context.Context, bucket string) (*metadata.ReplicationConfig, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	data, closer, err := p.db.Get(replicationKey(bucket))
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	defer closer.Close()

	var config metadata.ReplicationConfig
	if err := decodeMeta(data, &config); err != nil {
		return nil, err
	}

	return &config, nil
}

// DeleteReplicationConfig deletes replication configuration
func (p *PebbleStore) DeleteReplicationConfig(ctx context.Context, bucket string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.db.Delete(replicationKey(bucket), pebble.Sync)
}

// PutBucketTags stores bucket tags
func (p *PebbleStore) PutBucketTags(ctx context.Context, bucket string, tags map[string]string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	data, err := encodeMeta(tags)
	if err != nil {
		return err
	}

	return p.db.Set(tagsKey(bucket), data, pebble.Sync)
}

// GetBucketTags gets bucket tags
func (p *PebbleStore) GetBucketTags(ctx context.Context, bucket string) (map[string]string, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	data, closer, err := p.db.Get(tagsKey(bucket))
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	defer closer.Close()

	var tags map[string]string
	if err := decodeMeta(data, &tags); err != nil {
		return nil, err
	}

	return tags, nil
}

// DeleteBucketTags deletes bucket tags
func (p *PebbleStore) DeleteBucketTags(ctx context.Context, bucket string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.db.Delete(tagsKey(bucket), pebble.Sync)
}

// PutObjectLock stores object lock configuration
func (p *PebbleStore) PutObjectLock(ctx context.Context, bucket string, config *metadata.ObjectLockConfig) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	data, err := encodeMeta(config)
	if err != nil {
		return err
	}

	return p.db.Set(objectLockKey(bucket), data, pebble.Sync)
}

// GetObjectLock gets object lock configuration
func (p *PebbleStore) GetObjectLock(ctx context.Context, bucket string) (*metadata.ObjectLockConfig, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	data, closer, err := p.db.Get(objectLockKey(bucket))
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	defer closer.Close()

	var config metadata.ObjectLockConfig
	if err := decodeMeta(data, &config); err != nil {
		return nil, err
	}

	return &config, nil
}

// DeleteObjectLock deletes object lock configuration
func (p *PebbleStore) DeleteObjectLock(ctx context.Context, bucket string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.db.Delete(objectLockKey(bucket), pebble.Sync)
}

// retentionKey generates an object retention key
func retentionKey(bucket, key string) []byte {
	return []byte("retention:" + bucket + ":" + key)
}

// PutObjectRetention stores object retention
func (p *PebbleStore) PutObjectRetention(ctx context.Context, bucket, key string, retention *metadata.ObjectRetention) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	data, err := encodeMeta(retention)
	if err != nil {
		return err
	}

	return p.db.Set(retentionKey(bucket, key), data, pebble.Sync)
}

// GetObjectRetention retrieves object retention
func (p *PebbleStore) GetObjectRetention(ctx context.Context, bucket, key string) (*metadata.ObjectRetention, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	data, closer, err := p.db.Get(retentionKey(bucket, key))
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	defer closer.Close()

	var retention metadata.ObjectRetention
	if err := decodeMeta(data, &retention); err != nil {
		return nil, err
	}

	return &retention, nil
}

// legalHoldKey generates an object legal hold key
func legalHoldKey(bucket, key string) []byte {
	return []byte("legalhold:" + bucket + ":" + key)
}

// PutObjectLegalHold stores object legal hold
func (p *PebbleStore) PutObjectLegalHold(ctx context.Context, bucket, key string, legalHold *metadata.ObjectLegalHold) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	data, err := encodeMeta(legalHold)
	if err != nil {
		return err
	}

	return p.db.Set(legalHoldKey(bucket, key), data, pebble.Sync)
}

// GetObjectLegalHold retrieves object legal hold
func (p *PebbleStore) GetObjectLegalHold(ctx context.Context, bucket, key string) (*metadata.ObjectLegalHold, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	data, closer, err := p.db.Get(legalHoldKey(bucket, key))
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	defer closer.Close()

	var legalHold metadata.ObjectLegalHold
	if err := decodeMeta(data, &legalHold); err != nil {
		return nil, err
	}

	return &legalHold, nil
}

// PutPublicAccessBlock stores public access block configuration
func (p *PebbleStore) PutPublicAccessBlock(ctx context.Context, bucket string, config *metadata.PublicAccessBlockConfiguration) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	data, err := encodeMeta(config)
	if err != nil {
		return err
	}

	return p.db.Set(publicAccessBlockKey(bucket), data, pebble.Sync)
}

// GetPublicAccessBlock gets public access block configuration
func (p *PebbleStore) GetPublicAccessBlock(ctx context.Context, bucket string) (*metadata.PublicAccessBlockConfiguration, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	data, closer, err := p.db.Get(publicAccessBlockKey(bucket))
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	defer closer.Close()

	var config metadata.PublicAccessBlockConfiguration
	if err := decodeMeta(data, &config); err != nil {
		return nil, err
	}

	return &config, nil
}

// DeletePublicAccessBlock deletes public access block configuration
func (p *PebbleStore) DeletePublicAccessBlock(ctx context.Context, bucket string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.db.Delete(publicAccessBlockKey(bucket), pebble.Sync)
}

// PutBucketAccelerate stores bucket accelerate configuration
func (p *PebbleStore) PutBucketAccelerate(ctx context.Context, bucket string, config *metadata.BucketAccelerateConfiguration) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	data, err := encodeMeta(config)
	if err != nil {
		return err
	}

	return p.db.Set(accelerateKey(bucket), data, pebble.Sync)
}

// GetBucketAccelerate gets bucket accelerate configuration
func (p *PebbleStore) GetBucketAccelerate(ctx context.Context, bucket string) (*metadata.BucketAccelerateConfiguration, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	data, closer, err := p.db.Get(accelerateKey(bucket))
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	defer closer.Close()

	var config metadata.BucketAccelerateConfiguration
	if err := decodeMeta(data, &config); err != nil {
		return nil, err
	}

	return &config, nil
}

// DeleteBucketAccelerate deletes bucket accelerate configuration
func (p *PebbleStore) DeleteBucketAccelerate(ctx context.Context, bucket string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.db.Delete(accelerateKey(bucket), pebble.Sync)
}

// PutBucketInventory stores bucket inventory configuration
func (p *PebbleStore) PutBucketInventory(ctx context.Context, bucket, id string, config *metadata.InventoryConfiguration) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	data, err := encodeMeta(config)
	if err != nil {
		return err
	}

	return p.db.Set(inventoryKey(bucket, id), data, pebble.Sync)
}

// GetBucketInventory gets bucket inventory configuration
func (p *PebbleStore) GetBucketInventory(ctx context.Context, bucket, id string) (*metadata.InventoryConfiguration, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	data, closer, err := p.db.Get(inventoryKey(bucket, id))
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	defer closer.Close()

	var config metadata.InventoryConfiguration
	if err := decodeMeta(data, &config); err != nil {
		return nil, err
	}

	return &config, nil
}

// ListBucketInventory lists all inventory configurations for a bucket
func (p *PebbleStore) ListBucketInventory(ctx context.Context, bucket string) ([]metadata.InventoryConfiguration, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	prefix := "inventory:" + bucket + "/"

	iter, err := p.db.NewIter(nil)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var configs []metadata.InventoryConfiguration
	for iter.SeekGE([]byte(prefix)); iter.Valid(); iter.Next() {
		key := string(iter.Key())
		if len(key) < len(prefix) || key[:len(prefix)] != prefix {
			break
		}

		var config metadata.InventoryConfiguration
		if err := decodeMeta(iter.Value(), &config); err != nil {
			continue
		}

		configs = append(configs, config)
	}

	return configs, nil
}

// DeleteBucketInventory deletes bucket inventory configuration
func (p *PebbleStore) DeleteBucketInventory(ctx context.Context, bucket, id string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.db.Delete(inventoryKey(bucket, id), pebble.Sync)
}

// PutBucketAnalytics stores bucket analytics configuration
func (p *PebbleStore) PutBucketAnalytics(ctx context.Context, bucket, id string, config *metadata.AnalyticsConfiguration) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	data, err := encodeMeta(config)
	if err != nil {
		return err
	}

	return p.db.Set(analyticsKey(bucket, id), data, pebble.Sync)
}

// GetBucketAnalytics gets bucket analytics configuration
func (p *PebbleStore) GetBucketAnalytics(ctx context.Context, bucket, id string) (*metadata.AnalyticsConfiguration, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	data, closer, err := p.db.Get(analyticsKey(bucket, id))
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	defer closer.Close()

	var config metadata.AnalyticsConfiguration
	if err := decodeMeta(data, &config); err != nil {
		return nil, err
	}

	return &config, nil
}

// ListBucketAnalytics lists all analytics configurations for a bucket
func (p *PebbleStore) ListBucketAnalytics(ctx context.Context, bucket string) ([]metadata.AnalyticsConfiguration, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	prefix := "analytics:" + bucket + "/"

	iter, err := p.db.NewIter(nil)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var configs []metadata.AnalyticsConfiguration
	for iter.SeekGE([]byte(prefix)); iter.Valid(); iter.Next() {
		key := string(iter.Key())
		if len(key) < len(prefix) || key[:len(prefix)] != prefix {
			break
		}

		var config metadata.AnalyticsConfiguration
		if err := decodeMeta(iter.Value(), &config); err != nil {
			continue
		}

		configs = append(configs, config)
	}

	return configs, nil
}

// DeleteBucketAnalytics deletes bucket analytics configuration
func (p *PebbleStore) DeleteBucketAnalytics(ctx context.Context, bucket, id string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.db.Delete(analyticsKey(bucket, id), pebble.Sync)
}

// presignedURLKey returns the key for a presigned URL
func presignedURLKey(url string) []byte {
	return []byte("presigned:" + url)
}

// PutPresignedURL stores a presigned URL request
func (p *PebbleStore) PutPresignedURL(ctx context.Context, url string, req *metadata.PresignedURLRequest) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	data, err := encodeMeta(req)
	if err != nil {
		return err
	}
	return p.db.Set(presignedURLKey(url), data, pebble.Sync)
}

// GetPresignedURL retrieves a presigned URL request
func (p *PebbleStore) GetPresignedURL(ctx context.Context, url string) (*metadata.PresignedURLRequest, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	data, _, err := p.db.Get(presignedURLKey(url))
	if err == pebble.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var req metadata.PresignedURLRequest
	if err := decodeMeta(data, &req); err != nil {
		return nil, err
	}
	return &req, nil
}

// DeletePresignedURL deletes a presigned URL
func (p *PebbleStore) DeletePresignedURL(ctx context.Context, url string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.db.Delete(presignedURLKey(url), pebble.Sync)
}

// websiteKey generates a website configuration key
func websiteKey(bucket string) []byte {
	return []byte("website:" + bucket)
}

// PutBucketWebsite stores bucket website configuration
func (p *PebbleStore) PutBucketWebsite(ctx context.Context, bucket string, config *metadata.WebsiteConfiguration) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	data, err := encodeMeta(config)
	if err != nil {
		return err
	}

	return p.db.Set(websiteKey(bucket), data, pebble.Sync)
}

// GetBucketWebsite gets bucket website configuration
func (p *PebbleStore) GetBucketWebsite(ctx context.Context, bucket string) (*metadata.WebsiteConfiguration, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	data, closer, err := p.db.Get(websiteKey(bucket))
	if err == pebble.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()

	var config metadata.WebsiteConfiguration
	if err := decodeMeta(data, &config); err != nil {
		return nil, err
	}

	return &config, nil
}

// DeleteBucketWebsite deletes bucket website configuration
func (p *PebbleStore) DeleteBucketWebsite(ctx context.Context, bucket string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.db.Delete(websiteKey(bucket), pebble.Sync)
}

// notificationKey generates a notification configuration key
func notificationKey(bucket string) []byte {
	return []byte("notification:" + bucket)
}

// PutBucketNotification stores bucket notification configuration
func (p *PebbleStore) PutBucketNotification(ctx context.Context, bucket string, config *metadata.NotificationConfiguration) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	data, err := encodeMeta(config)
	if err != nil {
		return err
	}

	return p.db.Set(notificationKey(bucket), data, pebble.Sync)
}

// GetBucketNotification gets bucket notification configuration
func (p *PebbleStore) GetBucketNotification(ctx context.Context, bucket string) (*metadata.NotificationConfiguration, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	data, closer, err := p.db.Get(notificationKey(bucket))
	if err == pebble.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()

	var config metadata.NotificationConfiguration
	if err := decodeMeta(data, &config); err != nil {
		return nil, err
	}

	return &config, nil
}

// DeleteBucketNotification deletes bucket notification configuration
func (p *PebbleStore) DeleteBucketNotification(ctx context.Context, bucket string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.db.Delete(notificationKey(bucket), pebble.Sync)
}

// loggingKey generates a logging configuration key
func loggingKey(bucket string) []byte {
	return []byte("logging:" + bucket)
}

// PutBucketLogging stores bucket logging configuration
func (p *PebbleStore) PutBucketLogging(ctx context.Context, bucket string, config *metadata.LoggingConfiguration) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	data, err := encodeMeta(config)
	if err != nil {
		return err
	}

	return p.db.Set(loggingKey(bucket), data, pebble.Sync)
}

// GetBucketLogging gets bucket logging configuration
func (p *PebbleStore) GetBucketLogging(ctx context.Context, bucket string) (*metadata.LoggingConfiguration, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	data, closer, err := p.db.Get(loggingKey(bucket))
	if err == pebble.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()

	var config metadata.LoggingConfiguration
	if err := decodeMeta(data, &config); err != nil {
		return nil, err
	}

	return &config, nil
}

// DeleteBucketLogging deletes bucket logging configuration
func (p *PebbleStore) DeleteBucketLogging(ctx context.Context, bucket string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.db.Delete(loggingKey(bucket), pebble.Sync)
}

// locationKey generates a location configuration key
func locationKey(bucket string) []byte {
	return []byte("location:" + bucket)
}

// PutBucketLocation stores bucket location
func (p *PebbleStore) PutBucketLocation(ctx context.Context, bucket string, location string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.db.Set(locationKey(bucket), []byte(location), pebble.Sync)
}

// GetBucketLocation retrieves bucket location
func (p *PebbleStore) GetBucketLocation(ctx context.Context, bucket string) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	data, closer, err := p.db.Get(locationKey(bucket))
	if err != nil {
		if err == pebble.ErrNotFound {
			return "", nil
		}
		return "", err
	}
	defer closer.Close()

	return string(data), nil
}

// ownershipKey generates an ownership controls key
func ownershipKey(bucket string) []byte {
	return []byte("ownership:" + bucket)
}

// PutBucketOwnershipControls stores bucket ownership controls
func (p *PebbleStore) PutBucketOwnershipControls(ctx context.Context, bucket string, config *metadata.OwnershipControls) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	data, err := encodeMeta(config)
	if err != nil {
		return err
	}

	return p.db.Set(ownershipKey(bucket), data, pebble.Sync)
}

// GetBucketOwnershipControls retrieves bucket ownership controls
func (p *PebbleStore) GetBucketOwnershipControls(ctx context.Context, bucket string) (*metadata.OwnershipControls, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	data, closer, err := p.db.Get(ownershipKey(bucket))
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	defer closer.Close()

	var config metadata.OwnershipControls
	if err := decodeMeta(data, &config); err != nil {
		return nil, err
	}

	return &config, nil
}

// DeleteBucketOwnershipControls deletes bucket ownership controls
func (p *PebbleStore) DeleteBucketOwnershipControls(ctx context.Context, bucket string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.db.Delete(ownershipKey(bucket), pebble.Sync)
}

// metricsKey generates a metrics configuration key
func metricsKey(bucket, id string) []byte {
	return []byte("metrics:" + bucket + ":" + id)
}

// metricsListKey generates a metrics list key for a bucket
func metricsListKey(bucket string) []byte {
	return []byte("metrics:list:" + bucket)
}

// PutBucketMetrics stores bucket metrics configuration
func (p *PebbleStore) PutBucketMetrics(ctx context.Context, bucket string, id string, config *metadata.MetricsConfiguration) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	data, err := encodeMeta(config)
	if err != nil {
		return err
	}

	return p.db.Set(metricsKey(bucket, id), data, pebble.Sync)
}

// GetBucketMetrics retrieves bucket metrics configuration
func (p *PebbleStore) GetBucketMetrics(ctx context.Context, bucket string, id string) (*metadata.MetricsConfiguration, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	data, closer, err := p.db.Get(metricsKey(bucket, id))
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	defer closer.Close()

	var config metadata.MetricsConfiguration
	if err := decodeMeta(data, &config); err != nil {
		return nil, err
	}

	return &config, nil
}

// DeleteBucketMetrics deletes bucket metrics configuration
func (p *PebbleStore) DeleteBucketMetrics(ctx context.Context, bucket string, id string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.db.Delete(metricsKey(bucket, id), pebble.Sync)
}

// ListBucketMetrics lists all metrics configurations for a bucket
func (p *PebbleStore) ListBucketMetrics(ctx context.Context, bucket string) ([]metadata.MetricsConfiguration, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	prefix := []byte("metrics:" + bucket + ":")
	var configs []metadata.MetricsConfiguration

	iter, err := p.db.NewIter(nil)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	for iter.SeekGE(prefix); iter.Valid(); iter.Next() {
		key := iter.Key()
		if !bytes.HasPrefix(key, prefix) {
			break
		}

		// Skip the list key
		if bytes.HasSuffix(key, []byte(":list")) {
			continue
		}

		var config metadata.MetricsConfiguration
		if err := decodeMeta(iter.Value(), &config); err != nil {
			continue
		}
		configs = append(configs, config)
	}

	return configs, nil
}

// Close closes the store
func (p *PebbleStore) Close() error {
	return p.db.Close()
}

// encodeMeta encodes metadata to bytes using Gob
func encodeMeta(v interface{}) ([]byte, error) {
	buf := new(bytes.Buffer)
	encoder := gob.NewEncoder(buf)
	err := encoder.Encode(v)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// decodeMeta decodes metadata from bytes
func decodeMeta(data []byte, v interface{}) error {
	reader := bytes.NewReader(data)
	decoder := gob.NewDecoder(reader)
	return decoder.Decode(v)
}

// nowUnix returns current Unix timestamp
func nowUnix() int64 {
	return time.Now().Unix()
}
