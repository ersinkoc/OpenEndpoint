package bbolt

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"time"

	"github.com/openendpoint/openendpoint/internal/metadata"
	bolt "go.etcd.io/bbolt"
)

// BBoltStore implements metadata.Store using bbolt
type BBoltStore struct {
	db *bolt.DB
}

// New creates a new bbolt metadata store
func New(rootDir string) (*BBoltStore, error) {
	dbPath := filepath.Join(rootDir, "metadata.db")

	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open bbolt database: %w", err)
	}

	// Create buckets
	err = db.Update(func(tx *bolt.Tx) error {
		// Buckets bucket
		if _, err := tx.CreateBucketIfNotExists([]byte("buckets")); err != nil {
			return err
		}
		// Objects bucket
		if _, err := tx.CreateBucketIfNotExists([]byte("objects")); err != nil {
			return err
		}
		// Multipart uploads bucket
		if _, err := tx.CreateBucketIfNotExists([]byte("multipart")); err != nil {
			return err
		}
		// Parts bucket
		if _, err := tx.CreateBucketIfNotExists([]byte("parts")); err != nil {
			return err
		}
		// Lifecycle bucket
		if _, err := tx.CreateBucketIfNotExists([]byte("lifecycle")); err != nil {
			return err
		}
		// Versioning bucket
		if _, err := tx.CreateBucketIfNotExists([]byte("versioning")); err != nil {
			return err
		}
		// Replication bucket
		if _, err := tx.CreateBucketIfNotExists([]byte("replication")); err != nil {
			return err
		}
		// Policy bucket
		if _, err := tx.CreateBucketIfNotExists([]byte("policy")); err != nil {
			return err
		}
		// CORS bucket
		if _, err := tx.CreateBucketIfNotExists([]byte("cors")); err != nil {
			return err
		}
		// Encryption bucket
		if _, err := tx.CreateBucketIfNotExists([]byte("encryption")); err != nil {
			return err
		}
		// Tags bucket
		if _, err := tx.CreateBucketIfNotExists([]byte("tags")); err != nil {
			return err
		}
		// ObjectLock bucket
		if _, err := tx.CreateBucketIfNotExists([]byte("objectlock")); err != nil {
			return err
		}
		// PublicAccessBlock bucket
		if _, err := tx.CreateBucketIfNotExists([]byte("publicaccessblock")); err != nil {
			return err
		}
		// Accelerate bucket
		if _, err := tx.CreateBucketIfNotExists([]byte("accelerate")); err != nil {
			return err
		}
		// Notification bucket
		if _, err := tx.CreateBucketIfNotExists([]byte("notification")); err != nil {
			return err
		}
		// Logging bucket
		if _, err := tx.CreateBucketIfNotExists([]byte("logging")); err != nil {
			return err
		}
		// Location bucket
		if _, err := tx.CreateBucketIfNotExists([]byte("location")); err != nil {
			return err
		}
		// Ownership bucket
		if _, err := tx.CreateBucketIfNotExists([]byte("ownership")); err != nil {
			return err
		}
		// Metrics bucket
		if _, err := tx.CreateBucketIfNotExists([]byte("metrics")); err != nil {
			return err
		}
		// Analytics bucket
		if _, err := tx.CreateBucketIfNotExists([]byte("analytics")); err != nil {
			return err
		}
		// Retention bucket
		if _, err := tx.CreateBucketIfNotExists([]byte("retention")); err != nil {
			return err
		}
		// Legal hold bucket
		if _, err := tx.CreateBucketIfNotExists([]byte("legalhold")); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create buckets: %w", err)
	}

	return &BBoltStore{db: db}, nil
}

// CreateBucket creates a new bucket
func (b *BBoltStore) CreateBucket(ctx context.Context, bucket string) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		buckets := tx.Bucket([]byte("buckets"))
		meta := &metadata.BucketMetadata{
			Name:         bucket,
			CreationDate: nowUnix(),
		}
		data, err := encode(meta)
		if err != nil {
			return err
		}
		return buckets.Put([]byte(bucket), data)
	})
}

// DeleteBucket deletes a bucket
func (b *BBoltStore) DeleteBucket(ctx context.Context, bucket string) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		buckets := tx.Bucket([]byte("buckets"))
		return buckets.Delete([]byte(bucket))
	})
}

// GetBucket gets bucket metadata
func (b *BBoltStore) GetBucket(ctx context.Context, bucket string) (*metadata.BucketMetadata, error) {
	var meta metadata.BucketMetadata
	err := b.db.View(func(tx *bolt.Tx) error {
		buckets := tx.Bucket([]byte("buckets"))
		data := buckets.Get([]byte(bucket))
		if data == nil {
			return fmt.Errorf("bucket not found: %s", bucket)
		}
		return mustDecode(data, &meta)
	})
	return &meta, err
}

// ListBuckets lists all buckets
func (b *BBoltStore) ListBuckets(ctx context.Context) ([]string, error) {
	var buckets []string
	err := b.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte("buckets"))
		return bkt.ForEach(func(k, v []byte) error {
			buckets = append(buckets, string(k))
			return nil
		})
	})
	return buckets, err
}

// PutObject stores object metadata
func (b *BBoltStore) PutObject(ctx context.Context, bucket, key string, meta *metadata.ObjectMetadata) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		objects := tx.Bucket([]byte("objects"))
		objKey := bucket + "/" + key
		data, err := encode(meta)
		if err != nil {
			return err
		}
		return objects.Put([]byte(objKey), data)
	})
}

// GetObject gets object metadata
func (b *BBoltStore) GetObject(ctx context.Context, bucket, key string, versionID string) (*metadata.ObjectMetadata, error) {
	var meta metadata.ObjectMetadata
	err := b.db.View(func(tx *bolt.Tx) error {
		objects := tx.Bucket([]byte("objects"))
		objKey := bucket + "/" + key
		data := objects.Get([]byte(objKey))
		if data == nil {
			return fmt.Errorf("object not found: %s/%s", bucket, key)
		}
		return mustDecode(data, &meta)
	})
	return &meta, err
}

// DeleteObject deletes object metadata
func (b *BBoltStore) DeleteObject(ctx context.Context, bucket, key string, versionID string) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		objects := tx.Bucket([]byte("objects"))
		objKey := bucket + "/" + key
		return objects.Delete([]byte(objKey))
	})
}

// ListObjects lists objects with optional prefix
func (b *BBoltStore) ListObjects(ctx context.Context, bucket, prefix string, opts metadata.ListOptions) ([]metadata.ObjectMetadata, error) {
	var objects []metadata.ObjectMetadata
	err := b.db.View(func(tx *bolt.Tx) error {
		objectsBkt := tx.Bucket([]byte("objects"))
		prefixKey := bucket + "/" + prefix

		maxKeys := opts.MaxKeys
		if maxKeys == 0 {
			maxKeys = 1000
		}

		cursor := objectsBkt.Cursor()
		for k, v := cursor.Seek([]byte(prefixKey)); k != nil && len(objects) < maxKeys; k, v = cursor.Next() {
			key := string(k)
			if len(key) < len(bucket)+1 || key[:len(bucket)+1] != bucket+"/" {
				break
			}

			var meta metadata.ObjectMetadata
			if err := mustDecode(v, &meta); err != nil {
				continue
			}
			objects = append(objects, meta)
		}
		return nil
	})
	return objects, err
}

// CreateMultipartUpload creates a new multipart upload
func (b *BBoltStore) CreateMultipartUpload(ctx context.Context, bucket, key, uploadID string, meta *metadata.ObjectMetadata) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		multipart := tx.Bucket([]byte("multipart"))
		multiMeta := &metadata.MultipartUploadMetadata{
			UploadID:  uploadID,
			Key:       key,
			Bucket:    bucket,
			Initiated: nowUnix(),
			Metadata:  meta.Metadata,
		}
		multiKey := bucket + "/" + key + "/" + uploadID
		return multipart.Put([]byte(multiKey), mustEncode(multiMeta))
	})
}

// PutPart stores part metadata
func (b *BBoltStore) PutPart(ctx context.Context, bucket, key, uploadID string, partNumber int, partMeta *metadata.PartMetadata) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		parts := tx.Bucket([]byte("parts"))
		partKey := fmt.Sprintf("%s/%s/%s/%d", bucket, key, uploadID, partNumber)
		return parts.Put([]byte(partKey), mustEncode(partMeta))
	})
}

// CompleteMultipartUpload completes a multipart upload
func (b *BBoltStore) CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []metadata.PartInfo) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		multipart := tx.Bucket([]byte("multipart"))
		partsBkt := tx.Bucket([]byte("parts"))

		multiKey := bucket + "/" + key + "/" + uploadID
		if err := multipart.Delete([]byte(multiKey)); err != nil {
			return err
		}

		// Delete parts
		for i := 1; i <= len(parts); i++ {
			partKey := fmt.Sprintf("%s/%s/%s/%d", bucket, key, uploadID, i)
			partsBkt.Delete([]byte(partKey))
		}
		return nil
	})
}

// AbortMultipartUpload aborts a multipart upload
func (b *BBoltStore) AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		multipart := tx.Bucket([]byte("multipart"))
		multiKey := bucket + "/" + key + "/" + uploadID
		return multipart.Delete([]byte(multiKey))
	})
}

// ListParts lists parts of a multipart upload
func (b *BBoltStore) ListParts(ctx context.Context, bucket, key, uploadID string) ([]metadata.PartMetadata, error) {
	var parts []metadata.PartMetadata
	err := b.db.View(func(tx *bolt.Tx) error {
		partsBkt := tx.Bucket([]byte("parts"))
		prefix := fmt.Sprintf("%s/%s/%s/", bucket, key, uploadID)

		cursor := partsBkt.Cursor()
		for k, v := cursor.Seek([]byte(prefix)); k != nil; k, v = cursor.Next() {
			key := string(k)
			if len(key) < len(prefix) || key[:len(prefix)] != prefix {
				break
			}

			var partMeta metadata.PartMetadata
			if err := mustDecode(v, &partMeta); err != nil {
				continue
			}
			parts = append(parts, partMeta)
		}
		return nil
	})
	return parts, err
}

// ListMultipartUploads lists multipart uploads
func (b *BBoltStore) ListMultipartUploads(ctx context.Context, bucket, prefix string) ([]metadata.MultipartUploadMetadata, error) {
	var uploads []metadata.MultipartUploadMetadata
	err := b.db.View(func(tx *bolt.Tx) error {
		multipart := tx.Bucket([]byte("multipart"))
		if multipart == nil {
			return nil
		}
		prefixKey := bucket + "/" + prefix

		cursor := multipart.Cursor()
		for k, v := cursor.Seek([]byte(prefixKey)); k != nil; k, v = cursor.Next() {
			key := string(k)
			// Check if still within the bucket prefix
			if !containsPrefix(key, bucket+"/") {
				break
			}

			var meta metadata.MultipartUploadMetadata
			if err := mustDecode(v, &meta); err != nil {
				continue
			}
			uploads = append(uploads, meta)
		}
		return nil
	})
	return uploads, err
}

// containsPrefix checks if string contains the given prefix
func containsPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

// PutLifecycleRule puts a lifecycle rule
func (b *BBoltStore) PutLifecycleRule(ctx context.Context, bucket string, rule *metadata.LifecycleRule) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		lifecycle := tx.Bucket([]byte("lifecycle"))
		return lifecycle.Put([]byte(bucket+"_"+rule.ID), mustEncode(rule))
	})
}

// GetLifecycleRules gets lifecycle rules for a bucket
func (b *BBoltStore) GetLifecycleRules(ctx context.Context, bucket string) ([]metadata.LifecycleRule, error) {
	var rules []metadata.LifecycleRule
	err := b.db.View(func(tx *bolt.Tx) error {
		lifecycle := tx.Bucket([]byte("lifecycle"))
		prefix := []byte(bucket + "_")

		cursor := lifecycle.Cursor()
		for k, v := cursor.Seek(prefix); k != nil; k, v = cursor.Next() {
			key := string(k)
			if len(key) < len(prefix) || key[:len(prefix)] != bucket+"_" {
				break
			}

			var rule metadata.LifecycleRule
			if err := mustDecode(v, &rule); err != nil {
				continue
			}
			rules = append(rules, rule)
		}
		return nil
	})
	return rules, err
}

// DeleteLifecycleRule deletes a lifecycle rule from a bucket
func (b *BBoltStore) DeleteLifecycleRule(ctx context.Context, bucket, ruleID string) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		lifecycle := tx.Bucket([]byte("lifecycle"))
		return lifecycle.Delete([]byte(bucket + "_" + ruleID))
	})
}

// PutBucketVersioning puts bucket versioning configuration
func (b *BBoltStore) PutBucketVersioning(ctx context.Context, bucket string, versioning *metadata.BucketVersioning) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		versioningBkt := tx.Bucket([]byte("versioning"))
		return versioningBkt.Put([]byte(bucket), mustEncode(versioning))
	})
}

// GetBucketVersioning gets bucket versioning configuration
func (b *BBoltStore) GetBucketVersioning(ctx context.Context, bucket string) (*metadata.BucketVersioning, error) {
	var versioning metadata.BucketVersioning
	err := b.db.View(func(tx *bolt.Tx) error {
		versioningBkt := tx.Bucket([]byte("versioning"))
		data := versioningBkt.Get([]byte(bucket))
		if data == nil {
			return nil
		}
		return mustDecode(data, &versioning)
	})
	return &versioning, err
}

// PutBucketPolicy stores bucket policy
func (b *BBoltStore) PutBucketPolicy(ctx context.Context, bucket string, policy *string) error {
	if policy == nil {
		return fmt.Errorf("policy cannot be nil")
	}
	return b.db.Update(func(tx *bolt.Tx) error {
		policyBkt := tx.Bucket([]byte("policy"))
		return policyBkt.Put([]byte(bucket), []byte(*policy))
	})
}

// GetBucketPolicy gets bucket policy
func (b *BBoltStore) GetBucketPolicy(ctx context.Context, bucket string) (*string, error) {
	var policy *string
	err := b.db.View(func(tx *bolt.Tx) error {
		policyBkt := tx.Bucket([]byte("policy"))
		data := policyBkt.Get([]byte(bucket))
		if data == nil {
			return nil
		}
		policyStr := string(data)
		policy = &policyStr
		return nil
	})
	return policy, err
}

// DeleteBucketPolicy deletes bucket policy
func (b *BBoltStore) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		policyBkt := tx.Bucket([]byte("policy"))
		return policyBkt.Delete([]byte(bucket))
	})
}

// PutBucketCors stores CORS configuration
func (b *BBoltStore) PutBucketCors(ctx context.Context, bucket string, cors *metadata.CORSConfiguration) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		corsBkt := tx.Bucket([]byte("cors"))
		return corsBkt.Put([]byte(bucket), mustEncode(cors))
	})
}

// GetBucketCors gets CORS configuration
func (b *BBoltStore) GetBucketCors(ctx context.Context, bucket string) (*metadata.CORSConfiguration, error) {
	var cors metadata.CORSConfiguration
	err := b.db.View(func(tx *bolt.Tx) error {
		corsBkt := tx.Bucket([]byte("cors"))
		data := corsBkt.Get([]byte(bucket))
		if data == nil {
			return nil
		}
		return mustDecode(data, &cors)
	})
	return &cors, err
}

// DeleteBucketCors deletes CORS configuration
func (b *BBoltStore) DeleteBucketCors(ctx context.Context, bucket string) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		corsBkt := tx.Bucket([]byte("cors"))
		return corsBkt.Delete([]byte(bucket))
	})
}

// PutBucketEncryption stores encryption configuration
func (b *BBoltStore) PutBucketEncryption(ctx context.Context, bucket string, encryption *metadata.BucketEncryption) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		encryptionBkt := tx.Bucket([]byte("encryption"))
		return encryptionBkt.Put([]byte(bucket), mustEncode(encryption))
	})
}

// GetBucketEncryption gets encryption configuration
func (b *BBoltStore) GetBucketEncryption(ctx context.Context, bucket string) (*metadata.BucketEncryption, error) {
	var encryption metadata.BucketEncryption
	err := b.db.View(func(tx *bolt.Tx) error {
		encryptionBkt := tx.Bucket([]byte("encryption"))
		data := encryptionBkt.Get([]byte(bucket))
		if data == nil {
			return nil
		}
		return mustDecode(data, &encryption)
	})
	return &encryption, err
}

// DeleteBucketEncryption deletes encryption configuration
func (b *BBoltStore) DeleteBucketEncryption(ctx context.Context, bucket string) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		encryptionBkt := tx.Bucket([]byte("encryption"))
		return encryptionBkt.Delete([]byte(bucket))
	})
}

// PutBucketTags stores bucket tags
func (b *BBoltStore) PutBucketTags(ctx context.Context, bucket string, tags map[string]string) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		tagsBkt := tx.Bucket([]byte("tags"))
		return tagsBkt.Put([]byte(bucket), mustEncode(tags))
	})
}

// GetBucketTags gets bucket tags
func (b *BBoltStore) GetBucketTags(ctx context.Context, bucket string) (map[string]string, error) {
	var tags map[string]string
	err := b.db.View(func(tx *bolt.Tx) error {
		tagsBkt := tx.Bucket([]byte("tags"))
		data := tagsBkt.Get([]byte(bucket))
		if data == nil {
			return nil
		}
		return mustDecode(data, &tags)
	})
	return tags, err
}

// DeleteBucketTags deletes bucket tags
func (b *BBoltStore) DeleteBucketTags(ctx context.Context, bucket string) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		tagsBkt := tx.Bucket([]byte("tags"))
		return tagsBkt.Delete([]byte(bucket))
	})
}

// PutObjectLock stores object lock configuration
func (b *BBoltStore) PutObjectLock(ctx context.Context, bucket string, config *metadata.ObjectLockConfig) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		objectLockBkt := tx.Bucket([]byte("objectlock"))
		return objectLockBkt.Put([]byte(bucket), mustEncode(config))
	})
}

// GetObjectLock gets object lock configuration
func (b *BBoltStore) GetObjectLock(ctx context.Context, bucket string) (*metadata.ObjectLockConfig, error) {
	var config metadata.ObjectLockConfig
	err := b.db.View(func(tx *bolt.Tx) error {
		objectLockBkt := tx.Bucket([]byte("objectlock"))
		data := objectLockBkt.Get([]byte(bucket))
		if data == nil {
			return nil
		}
		return mustDecode(data, &config)
	})
	return &config, err
}

// DeleteObjectLock deletes object lock configuration
func (b *BBoltStore) DeleteObjectLock(ctx context.Context, bucket string) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		objectLockBkt := tx.Bucket([]byte("objectlock"))
		return objectLockBkt.Delete([]byte(bucket))
	})
}

// PutObjectRetention stores object retention
func (b *BBoltStore) PutObjectRetention(ctx context.Context, bucket, key string, retention *metadata.ObjectRetention) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		retentionBkt := tx.Bucket([]byte("retention"))
		return retentionBkt.Put([]byte(bucket+"/"+key), mustEncode(retention))
	})
}

// GetObjectRetention retrieves object retention
func (b *BBoltStore) GetObjectRetention(ctx context.Context, bucket, key string) (*metadata.ObjectRetention, error) {
	var retention *metadata.ObjectRetention
	err := b.db.View(func(tx *bolt.Tx) error {
		retentionBkt := tx.Bucket([]byte("retention"))
		data := retentionBkt.Get([]byte(bucket + "/" + key))
		if data == nil {
			retention = nil
			return nil
		}
		return mustDecode(data, &retention)
	})
	return retention, err
}

// PutObjectLegalHold stores object legal hold
func (b *BBoltStore) PutObjectLegalHold(ctx context.Context, bucket, key string, legalHold *metadata.ObjectLegalHold) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		legalHoldBkt := tx.Bucket([]byte("legalhold"))
		return legalHoldBkt.Put([]byte(bucket+"/"+key), mustEncode(legalHold))
	})
}

// GetObjectLegalHold retrieves object legal hold
func (b *BBoltStore) GetObjectLegalHold(ctx context.Context, bucket, key string) (*metadata.ObjectLegalHold, error) {
	var legalHold *metadata.ObjectLegalHold
	err := b.db.View(func(tx *bolt.Tx) error {
		legalHoldBkt := tx.Bucket([]byte("legalhold"))
		data := legalHoldBkt.Get([]byte(bucket + "/" + key))
		if data == nil {
			legalHold = nil
			return nil
		}
		return mustDecode(data, &legalHold)
	})
	return legalHold, err
}

// PutPublicAccessBlock stores public access block configuration
func (b *BBoltStore) PutPublicAccessBlock(ctx context.Context, bucket string, config *metadata.PublicAccessBlockConfiguration) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		publicAccessBlockBkt := tx.Bucket([]byte("publicaccessblock"))
		return publicAccessBlockBkt.Put([]byte(bucket), mustEncode(config))
	})
}

// GetPublicAccessBlock gets public access block configuration
func (b *BBoltStore) GetPublicAccessBlock(ctx context.Context, bucket string) (*metadata.PublicAccessBlockConfiguration, error) {
	var config metadata.PublicAccessBlockConfiguration
	err := b.db.View(func(tx *bolt.Tx) error {
		publicAccessBlockBkt := tx.Bucket([]byte("publicaccessblock"))
		data := publicAccessBlockBkt.Get([]byte(bucket))
		if data == nil {
			return nil
		}
		return mustDecode(data, &config)
	})
	return &config, err
}

// DeletePublicAccessBlock deletes public access block configuration
func (b *BBoltStore) DeletePublicAccessBlock(ctx context.Context, bucket string) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		publicAccessBlockBkt := tx.Bucket([]byte("publicaccessblock"))
		return publicAccessBlockBkt.Delete([]byte(bucket))
	})
}

// PutBucketAccelerate stores bucket accelerate configuration
func (b *BBoltStore) PutBucketAccelerate(ctx context.Context, bucket string, config *metadata.BucketAccelerateConfiguration) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		accelerateBkt := tx.Bucket([]byte("accelerate"))
		return accelerateBkt.Put([]byte(bucket), mustEncode(config))
	})
}

// GetBucketAccelerate gets bucket accelerate configuration
func (b *BBoltStore) GetBucketAccelerate(ctx context.Context, bucket string) (*metadata.BucketAccelerateConfiguration, error) {
	var config metadata.BucketAccelerateConfiguration
	err := b.db.View(func(tx *bolt.Tx) error {
		accelerateBkt := tx.Bucket([]byte("accelerate"))
		data := accelerateBkt.Get([]byte(bucket))
		if data == nil {
			return nil
		}
		return mustDecode(data, &config)
	})
	return &config, err
}

// DeleteBucketAccelerate deletes bucket accelerate configuration
func (b *BBoltStore) DeleteBucketAccelerate(ctx context.Context, bucket string) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		accelerateBkt := tx.Bucket([]byte("accelerate"))
		return accelerateBkt.Delete([]byte(bucket))
	})
}

// PutReplicationConfig stores replication configuration
func (b *BBoltStore) PutReplicationConfig(ctx context.Context, bucket string, config *metadata.ReplicationConfig) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		replicationBkt := tx.Bucket([]byte("replication"))
		return replicationBkt.Put([]byte(bucket), mustEncode(config))
	})
}

// GetReplicationConfig gets replication configuration
func (b *BBoltStore) GetReplicationConfig(ctx context.Context, bucket string) (*metadata.ReplicationConfig, error) {
	var config metadata.ReplicationConfig
	err := b.db.View(func(tx *bolt.Tx) error {
		replicationBkt := tx.Bucket([]byte("replication"))
		data := replicationBkt.Get([]byte(bucket))
		if data == nil {
			return nil
		}
		return mustDecode(data, &config)
	})
	return &config, err
}

// DeleteReplicationConfig deletes replication configuration
func (b *BBoltStore) DeleteReplicationConfig(ctx context.Context, bucket string) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		replicationBkt := tx.Bucket([]byte("replication"))
		return replicationBkt.Delete([]byte(bucket))
	})
}

// PutBucketNotification stores bucket notification configuration
func (b *BBoltStore) PutBucketNotification(ctx context.Context, bucket string, config *metadata.NotificationConfiguration) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		notificationBkt := tx.Bucket([]byte("notification"))
		return notificationBkt.Put([]byte(bucket), mustEncode(config))
	})
}

// GetBucketNotification gets bucket notification configuration
func (b *BBoltStore) GetBucketNotification(ctx context.Context, bucket string) (*metadata.NotificationConfiguration, error) {
	var config metadata.NotificationConfiguration
	err := b.db.View(func(tx *bolt.Tx) error {
		notificationBkt := tx.Bucket([]byte("notification"))
		data := notificationBkt.Get([]byte(bucket))
		if data == nil {
			return nil
		}
		return mustDecode(data, &config)
	})
	return &config, err
}

// DeleteBucketNotification deletes bucket notification configuration
func (b *BBoltStore) DeleteBucketNotification(ctx context.Context, bucket string) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		notificationBkt := tx.Bucket([]byte("notification"))
		return notificationBkt.Delete([]byte(bucket))
	})
}

// PutBucketLogging stores bucket logging configuration
func (b *BBoltStore) PutBucketLogging(ctx context.Context, bucket string, config *metadata.LoggingConfiguration) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		loggingBkt := tx.Bucket([]byte("logging"))
		return loggingBkt.Put([]byte(bucket), mustEncode(config))
	})
}

// GetBucketLogging gets bucket logging configuration
func (b *BBoltStore) GetBucketLogging(ctx context.Context, bucket string) (*metadata.LoggingConfiguration, error) {
	var config metadata.LoggingConfiguration
	err := b.db.View(func(tx *bolt.Tx) error {
		loggingBkt := tx.Bucket([]byte("logging"))
		data := loggingBkt.Get([]byte(bucket))
		if data == nil {
			return nil
		}
		return mustDecode(data, &config)
	})
	return &config, err
}

// DeleteBucketLogging deletes bucket logging configuration
func (b *BBoltStore) DeleteBucketLogging(ctx context.Context, bucket string) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		loggingBkt := tx.Bucket([]byte("logging"))
		return loggingBkt.Delete([]byte(bucket))
	})
}

// PutBucketLocation stores bucket location
func (b *BBoltStore) PutBucketLocation(ctx context.Context, bucket string, location string) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		locationBkt := tx.Bucket([]byte("location"))
		return locationBkt.Put([]byte(bucket), []byte(location))
	})
}

// GetBucketLocation retrieves bucket location
func (b *BBoltStore) GetBucketLocation(ctx context.Context, bucket string) (string, error) {
	var location string
	err := b.db.View(func(tx *bolt.Tx) error {
		locationBkt := tx.Bucket([]byte("location"))
		data := locationBkt.Get([]byte(bucket))
		if data == nil {
			location = ""
			return nil
		}
		location = string(data)
		return nil
	})
	return location, err
}

// PutBucketOwnershipControls stores bucket ownership controls
func (b *BBoltStore) PutBucketOwnershipControls(ctx context.Context, bucket string, config *metadata.OwnershipControls) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		ownershipBkt := tx.Bucket([]byte("ownership"))
		return ownershipBkt.Put([]byte(bucket), mustEncode(config))
	})
}

// GetBucketOwnershipControls retrieves bucket ownership controls
func (b *BBoltStore) GetBucketOwnershipControls(ctx context.Context, bucket string) (*metadata.OwnershipControls, error) {
	var config *metadata.OwnershipControls
	err := b.db.View(func(tx *bolt.Tx) error {
		ownershipBkt := tx.Bucket([]byte("ownership"))
		data := ownershipBkt.Get([]byte(bucket))
		if data == nil {
			config = nil
			return nil
		}
		return mustDecode(data, &config)
	})
	return config, err
}

// DeleteBucketOwnershipControls deletes bucket ownership controls
func (b *BBoltStore) DeleteBucketOwnershipControls(ctx context.Context, bucket string) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		ownershipBkt := tx.Bucket([]byte("ownership"))
		return ownershipBkt.Delete([]byte(bucket))
	})
}

// PutBucketMetrics stores bucket metrics configuration
func (b *BBoltStore) PutBucketMetrics(ctx context.Context, bucket string, id string, config *metadata.MetricsConfiguration) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		metricsBkt := tx.Bucket([]byte("metrics"))
		key := bucket + ":" + id
		return metricsBkt.Put([]byte(key), mustEncode(config))
	})
}

// GetBucketMetrics retrieves bucket metrics configuration
func (b *BBoltStore) GetBucketMetrics(ctx context.Context, bucket string, id string) (*metadata.MetricsConfiguration, error) {
	var config *metadata.MetricsConfiguration
	err := b.db.View(func(tx *bolt.Tx) error {
		metricsBkt := tx.Bucket([]byte("metrics"))
		key := bucket + ":" + id
		data := metricsBkt.Get([]byte(key))
		if data == nil {
			config = nil
			return nil
		}
		return mustDecode(data, &config)
	})
	return config, err
}

// DeleteBucketMetrics deletes bucket metrics configuration
func (b *BBoltStore) DeleteBucketMetrics(ctx context.Context, bucket string, id string) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		metricsBkt := tx.Bucket([]byte("metrics"))
		key := bucket + ":" + id
		return metricsBkt.Delete([]byte(key))
	})
}

// ListBucketMetrics lists all metrics configurations for a bucket
func (b *BBoltStore) ListBucketMetrics(ctx context.Context, bucket string) ([]metadata.MetricsConfiguration, error) {
	var configs []metadata.MetricsConfiguration
	err := b.db.View(func(tx *bolt.Tx) error {
		metricsBkt := tx.Bucket([]byte("metrics"))
		prefix := bucket + ":"
		cursor := metricsBkt.Cursor()

		for k, v := cursor.Seek([]byte(prefix)); k != nil && string(k) >= prefix; k, v = cursor.Next() {
			var config metadata.MetricsConfiguration
			if err := json.Unmarshal(v, &config); err != nil {
				continue
			}
			configs = append(configs, config)
		}
		return nil
	})
	return configs, err
}

// PutBucketAnalytics stores bucket analytics configuration
func (b *BBoltStore) PutBucketAnalytics(ctx context.Context, bucket string, id string, config *metadata.AnalyticsConfiguration) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		analyticsBkt := tx.Bucket([]byte("analytics"))
		key := bucket + ":" + id
		return analyticsBkt.Put([]byte(key), mustEncode(config))
	})
}

// GetBucketAnalytics retrieves bucket analytics configuration
func (b *BBoltStore) GetBucketAnalytics(ctx context.Context, bucket string, id string) (*metadata.AnalyticsConfiguration, error) {
	var config *metadata.AnalyticsConfiguration
	err := b.db.View(func(tx *bolt.Tx) error {
		analyticsBkt := tx.Bucket([]byte("analytics"))
		key := bucket + ":" + id
		data := analyticsBkt.Get([]byte(key))
		if data == nil {
			config = nil
			return nil
		}
		return mustDecode(data, &config)
	})
	return config, err
}

// ListBucketAnalytics lists all analytics configurations for a bucket
func (b *BBoltStore) ListBucketAnalytics(ctx context.Context, bucket string) ([]metadata.AnalyticsConfiguration, error) {
	var configs []metadata.AnalyticsConfiguration
	err := b.db.View(func(tx *bolt.Tx) error {
		analyticsBkt := tx.Bucket([]byte("analytics"))
		prefix := bucket + ":"
		cursor := analyticsBkt.Cursor()

		for k, v := cursor.Seek([]byte(prefix)); k != nil && string(k) >= prefix; k, v = cursor.Next() {
			var config metadata.AnalyticsConfiguration
			if err := json.Unmarshal(v, &config); err != nil {
				continue
			}
			configs = append(configs, config)
		}
		return nil
	})
	return configs, err
}

// DeleteBucketAnalytics deletes bucket analytics configuration
func (b *BBoltStore) DeleteBucketAnalytics(ctx context.Context, bucket string, id string) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		analyticsBkt := tx.Bucket([]byte("analytics"))
		key := bucket + ":" + id
		return analyticsBkt.Delete([]byte(key))
	})
}

// Close closes the store
func (b *BBoltStore) Close() error {
	return b.db.Close()
}

// nowUnix returns current Unix timestamp
func nowUnix() int64 {
	return time.Now().Unix()
}

// encode marshals data to JSON, returns error on failure
func encode(v interface{}) ([]byte, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("failed to encode: %w", err)
	}
	return data, nil
}

// mustEncode panics on encode error - kept for backward compatibility
func mustEncode(v interface{}) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}

// mustDecode panics on decode error
func mustDecode(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}
