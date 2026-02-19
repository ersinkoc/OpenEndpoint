package storage

import (
	"context"
	"io"
)

// Backend is an alias for StorageBackend
type Backend = StorageBackend

// StorageBackend defines the interface for object storage backends
type StorageBackend interface {
	// Put stores an object
	Put(ctx context.Context, bucket, key string, data io.Reader, size int64, opts PutOptions) error

	// Get retrieves an object
	Get(ctx context.Context, bucket, key string, opts GetOptions) (io.ReadCloser, error)

	// Delete removes an object
	Delete(ctx context.Context, bucket, key string) error

	// Head returns object metadata without reading the body
	Head(ctx context.Context, bucket, key string) (*ObjectInfo, error)

	// List lists objects in a bucket with optional prefix
	List(ctx context.Context, bucket, prefix string, opts ListOptions) (*ListResult, error)

	// CreateBucket creates a new bucket
	CreateBucket(ctx context.Context, bucket string) error

	// DeleteBucket deletes a bucket
	DeleteBucket(ctx context.Context, bucket string) error

	// ListBuckets lists all buckets
	ListBuckets(ctx context.Context) ([]BucketInfo, error)

	// Close closes the storage backend
	Close() error
}

// PutResult contains the result of a Put operation
type PutResult struct {
	ETag         string
	VersionID    string
	StorageClass string
}

// GetResult contains the result of a Get operation
type GetResult struct {
	Body         io.ReadCloser
	ObjectInfo   *ObjectInfo
	Range        *Range
	AcceptRanges string
}

// DeleteOptions contains options for Delete operation
type DeleteOptions struct {
	VersionID string
}

// PutOptions contains options for Put operation
type PutOptions struct {
	ContentType     string
	ContentEncoding string
	CacheControl   string
	Metadata       map[string]string
	StorageClass   string
}

// GetOptions contains options for Get operation
type GetOptions struct {
	Range *Range
	IfMatch           string
	IfNoneMatch       string
	IfModifiedSince   string
	IfUnmodifiedSince string
}

// Range represents a byte range for partial reads
type Range struct {
	Start int64
	End   int64
}

// ListOptions contains options for List operation
type ListOptions struct {
	Prefix    string
	Delimiter string
	MaxKeys   int
	Marker    string
}

// ObjectInfo contains metadata about an object
type ObjectInfo struct {
	Key          string
	Size         int64
	ETag         string
	LastModified int64
	ContentType  string
	Metadata     map[string]string
	StorageClass string
	VersionID    string
	IsDeleteMarker bool
}

// ListResult contains the result of a List operation
type ListResult struct {
	Objects       []ObjectInfo
	CommonPrefixes []string
}

// BucketInfo contains metadata about a bucket
type BucketInfo struct {
	Name         string
	CreationDate int64
}
