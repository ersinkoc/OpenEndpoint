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
