package metadata

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"time"
)

// CORSConfiguration represents S3 CORS configuration
type CORSConfiguration struct {
	XMLName   xml.Name    `xml:"CORSConfiguration"`
	CORSRules []CORSRule `xml:"CORSRule"`
}

// CORSRule represents a single CORS rule
type CORSRule struct {
	AllowedMethods []string `xml:"AllowedMethod"`
	AllowedOrigins []string `xml:"AllowedOrigin"`
	AllowedHeaders []string `xml:"AllowedHeader,omitempty"`
	ExposeHeaders  []string `xml:"ExposeHeader,omitempty"`
	MaxAgeSeconds  int      `xml:"MaxAgeSeconds,omitempty"`
}

// Store defines the interface for metadata storage
type Store interface {
	// Bucket operations
	CreateBucket(ctx context.Context, bucket string) error
	DeleteBucket(ctx context.Context, bucket string) error
	GetBucket(ctx context.Context, bucket string) (*BucketMetadata, error)
	ListBuckets(ctx context.Context) ([]string, error)

	// Object operations
	PutObject(ctx context.Context, bucket, key string, meta *ObjectMetadata) error
	GetObject(ctx context.Context, bucket, key string, versionID string) (*ObjectMetadata, error)
	DeleteObject(ctx context.Context, bucket, key string, versionID string) error
	ListObjects(ctx context.Context, bucket, prefix string, opts ListOptions) ([]ObjectMetadata, error)

	// Multipart upload operations
	CreateMultipartUpload(ctx context.Context, bucket, key, uploadID string, meta *ObjectMetadata) error
	PutPart(ctx context.Context, bucket, key, uploadID string, partNumber int, meta *PartMetadata) error
	CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []PartInfo) error
	AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error
	ListParts(ctx context.Context, bucket, key, uploadID string) ([]PartMetadata, error)
	ListMultipartUploads(ctx context.Context, bucket, prefix string) ([]MultipartUploadMetadata, error)

	// Lifecycle operations
	PutLifecycleRule(ctx context.Context, bucket string, rule *LifecycleRule) error
	GetLifecycleRules(ctx context.Context, bucket string) ([]LifecycleRule, error)

	// Replication operations
	PutReplicationConfig(ctx context.Context, bucket string, config *ReplicationConfig) error
	GetReplicationConfig(ctx context.Context, bucket string) (*ReplicationConfig, error)
	DeleteReplicationConfig(ctx context.Context, bucket string) error

	// Versioning operations
	PutBucketVersioning(ctx context.Context, bucket string, versioning *BucketVersioning) error
	GetBucketVersioning(ctx context.Context, bucket string) (*BucketVersioning, error)

	// CORS operations
	PutBucketCors(ctx context.Context, bucket string, cors *CORSConfiguration) error
	GetBucketCors(ctx context.Context, bucket string) (*CORSConfiguration, error)

	// Policy operations
	PutBucketPolicy(ctx context.Context, bucket string, policy *string) error
	GetBucketPolicy(ctx context.Context, bucket string) (*string, error)

	// Encryption operations
	PutBucketEncryption(ctx context.Context, bucket string, encryption *BucketEncryption) error
	GetBucketEncryption(ctx context.Context, bucket string) (*BucketEncryption, error)

	// Tagging operations
	PutBucketTags(ctx context.Context, bucket string, tags map[string]string) error
	GetBucketTags(ctx context.Context, bucket string) (map[string]string, error)

	// Object Lock operations
	PutObjectLock(ctx context.Context, bucket string, config *ObjectLockConfig) error
	GetObjectLock(ctx context.Context, bucket string) (*ObjectLockConfig, error)

	// PublicAccessBlock operations
	PutPublicAccessBlock(ctx context.Context, bucket string, config *PublicAccessBlockConfiguration) error
	GetPublicAccessBlock(ctx context.Context, bucket string) (*PublicAccessBlockConfiguration, error)

	// Close closes the store
	Close() error
}

// BucketMetadata contains bucket-level metadata
type BucketMetadata struct {
	Name          string    `json:"name"`
	CreationDate int64     `json:"creation_date"`
	Owner         string    `json:"owner"`
	Region        string    `json:"region"`
}

// ObjectMetadata contains object-level metadata
type ObjectMetadata struct {
	Key             string            `json:"key"`
	Bucket          string            `json:"bucket"`
	Size            int64             `json:"size"`
	ETag            string            `json:"etag"`
	ContentType     string            `json:"content_type"`
	ContentEncoding string            `json:"content_encoding"`
	CacheControl    string            `json:"cache_control"`
	Metadata        map[string]string `json:"metadata"`
	StorageClass    string            `json:"storage_class"`
	VersionID       string            `json:"version_id"`
	IsLatest        bool              `json:"is_latest"`
	IsDeleteMarker  bool              `json:"is_delete_marker"`
	LastModified    int64             `json:"last_modified"`
	Expires         int64             `json:"expires"`
	Parts           []PartInfo        `json:"parts,omitempty"`
}

// PartInfo represents a part in a multipart upload
type PartInfo struct {
	PartNumber int    `json:"part_number"`
	ETag       string `json:"etag"`
	Size       int64  `json:"size"`
}

// PartMetadata contains metadata for a part
type PartMetadata struct {
	UploadID     string `json:"upload_id"`
	Key          string `json:"key"`
	Bucket       string `json:"bucket"`
	PartNumber   int    `json:"part_number"`
	ETag         string `json:"etag"`
	Size         int64  `json:"size"`
	LastModified int64  `json:"last_modified"`
}

// MultipartUploadMetadata contains metadata for a multipart upload
type MultipartUploadMetadata struct {
	UploadID  string            `json:"upload_id"`
	Key      string            `json:"key"`
	Bucket   string            `json:"bucket"`
	Initiated int64            `json:"initiated"`
	Metadata map[string]string `json:"metadata"`
}

// LifecycleRule defines a lifecycle rule
type LifecycleRule struct {
	ID         string     `json:"id"`
	Prefix     string     `json:"prefix"`
	Status     string     `json:"status"` // Enabled or Disabled
	Expiration *Expiration `json:"expiration,omitempty"`
	Transitions []Transition `json:"transitions,omitempty"`
	NoncurrentVersionExpiration *NoncurrentVersionExpiration `json:"noncurrent_version_expiration,omitempty"`
}

type Expiration struct {
	Days          int  `json:"days"`
	Date          int64 `json:"date"`
	ExpiredObjectDeleteMarker bool `json:"expired_object_delete_marker"`
}

type Transition struct {
	Days          int    `json:"days"`
	StorageClass string `json:"storage_class"`
	Date         int64  `json:"date"`
}

type NoncurrentVersionExpiration struct {
	NoncurrentDays int `json:"noncurrent_days"`
}

// BucketEncryption contains bucket encryption configuration
type BucketEncryption struct {
	Rule        EncryptionRule `json:"Rule"`
}

// EncryptionRule contains encryption rule
type EncryptionRule struct {
	Apply       ApplyEncryptionConfiguration `json:"Apply"`
}

// ApplyEncryptionConfiguration applies encryption configuration
type ApplyEncryptionConfiguration struct {
	SSEAlgorithm         string `json:"SSEAlgorithm,omitempty"`
	KMSMasterKeyID      string `json:"KMSMasterKeyID,omitempty"`
}

// ObjectLockConfig contains object lock configuration
type ObjectLockConfig struct {
	Enabled bool `json:"Enabled"`
}

// PublicAccessBlockConfiguration contains public access block configuration
type PublicAccessBlockConfiguration struct {
	BlockPublicAcls       bool `json:"BlockPublicAcls"`
	BlockPublicPolicy     bool `json:"BlockPublicPolicy"`
	IgnorePublicAcls      bool `json:"IgnorePublicAcls"`
	RestrictPublicBuckets bool `json:"RestrictPublicBuckets"`
}

// BucketVersioning contains versioning configuration
type BucketVersioning struct {
	Status    string `json:"status"` // Enabled, Suspended, or ""
	MFADelete string `json:"mfa_delete"` // Enabled or Disabled
}

// ReplicationConfig contains bucket replication configuration
type ReplicationConfig struct {
	Role    string              `json:"role"`
	Rules   []ReplicationRule  `json:"rules"`
}

// ReplicationRule contains a replication rule
type ReplicationRule struct {
	ID        string `json:"id"`
	Status    string `json:"status"` // Enabled or Disabled
	Prefix    string `json:"prefix"`
	Destination Destination `json:"destination"`
}

// Destination contains replication destination
type Destination struct {
	Bucket       string `json:"bucket"`
	StorageClass string `json:"storage_class,omitempty"`
}

// ListOptions contains options for listing objects
type ListOptions struct {
	Prefix       string
	Delimiter    string
	MaxKeys      int
	Marker       string
	VersionIDMarker string
}

// MarshalJSON implements custom JSON marshaling
func (o *ObjectMetadata) MarshalJSON() ([]byte, error) {
	type Alias ObjectMetadata
	return json.Marshal(&struct {
		*Alias
		LastModified time.Time `json:"last_modified,omitempty"`
	}{
		Alias:        (*Alias)(o),
		LastModified: time.Unix(o.LastModified, 0).UTC(),
	})
}

// UnmarshalJSON implements custom JSON unmarshaling
func (o *ObjectMetadata) UnmarshalJSON(data []byte) error {
	type Alias ObjectMetadata
	aux := &struct {
		*Alias
		LastModified time.Time `json:"last_modified"`
	}{
		Alias: (*Alias)(o),
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	if !aux.LastModified.IsZero() {
		o.LastModified = aux.LastModified.Unix()
	}
	return nil
}
