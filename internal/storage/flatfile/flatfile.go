package flatfile

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/openendpoint/openendpoint/internal/storage"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
)

var (
	bytesWritten = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "openendpoint_storage_bytes_written_total",
			Help: "Total bytes written to storage",
		},
	)
	bytesRead = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "openendpoint_storage_bytes_read_total",
			Help: "Total bytes read from storage",
		},
	)
	diskIOErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "openendpoint_storage_errors_total",
			Help: "Total storage errors",
		},
		[]string{"operation"},
	)
)

type FlatFile struct {
	rootDir    string
	logger     *zap.SugaredLogger
	mu         sync.RWMutex
	bufferPool sync.Pool
	readCache  *cache
	writeCache *cache
}

// cache is a simple in-memory cache for read/write optimization
type cache struct {
	mu          sync.RWMutex
	data        map[string][]byte
	accessTime  map[string]time.Time
	maxSize     int
	hits        int64
	misses      int64
}

// newCache creates a new cache
func newCache(maxSize int) *cache {
	return &cache{
		data:       make(map[string][]byte),
		accessTime: make(map[string]time.Time),
		maxSize:    maxSize,
	}
}

// get retrieves a value from cache
func (c *cache) get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	val, ok := c.data[key]
	if ok {
		c.hits++
		c.accessTime[key] = time.Now()
	} else {
		c.misses++
	}
	return val, ok
}

// set stores a value in cache
func (c *cache) set(key string, value []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Simple LRU eviction if over max size
	if len(c.data) >= c.maxSize {
		// Find and remove oldest entry (LRU)
		var oldestKey string
		var oldestTime time.Time
		first := true
		for k, t := range c.accessTime {
			if first || t.Before(oldestTime) {
				oldestKey = k
				oldestTime = t
				first = false
			}
		}
		if oldestKey != "" {
			delete(c.data, oldestKey)
			delete(c.accessTime, oldestKey)
		}
	}
	c.data[key] = value
	c.accessTime[key] = time.Now()
}

// invalidate removes a key from cache
func (c *cache) invalidate(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.data, key)
	delete(c.accessTime, key)
}

// New creates a new flat file storage backend
func New(rootDir string) (*FlatFile, error) {
	if err := os.MkdirAll(rootDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create root directory: %w", err)
	}

	logger, err := zap.NewDevelopment()
	if err != nil {
		return nil, err
	}

	ff := &FlatFile{
		rootDir: rootDir,
		logger:  logger.Sugar(),
	}

	// Initialize buffer pool for read/write optimization
	ff.bufferPool.New = func() interface{} {
		buf := make([]byte, 32*1024) // 32KB buffer
		return &buf
	}

	// Initialize caches
	ff.readCache = newCache(1000)  // Cache up to 1000 objects
	ff.writeCache = newCache(100)  // Small write cache

	// Create buckets directory
	if err := os.MkdirAll(filepath.Join(rootDir, "buckets"), 0755); err != nil {
		return nil, fmt.Errorf("failed to create buckets directory: %w", err)
	}

	return ff, nil
}

// bucketPath returns the filesystem path for a bucket
func (f *FlatFile) bucketPath(bucket string) string {
	// Sanitize bucket name to prevent path traversal
	safeBucket := sanitizePathComponent(bucket)
	return filepath.Join(f.rootDir, "buckets", safeBucket)
}

// objectPath returns the filesystem path for an object
func (f *FlatFile) objectPath(bucket, key string) string {
	// Sanitize inputs to prevent path traversal
	safeBucket := sanitizePathComponent(bucket)
	// Escape the key to be safe for filesystem
	safeKey := escapePath(key)
	return filepath.Join(f.rootDir, "buckets", safeBucket, safeKey)
}

// sanitizePathComponent removes potentially dangerous characters from a path component
func sanitizePathComponent(s string) string {
	// Remove path traversal attempts
	s = strings.ReplaceAll(s, "..", "")
	s = strings.ReplaceAll(s, "/", "")
	s = strings.ReplaceAll(s, "\\", "")
	return s
}

// validateKey validates an object key
func validateKey(key string) error {
	if key == "" {
		return errors.New("object key cannot be empty")
	}
	// Check for path traversal attempts
	if strings.Contains(key, "..") {
		return errors.New("object key cannot contain '..' (path traversal)")
	}
	// Key cannot start with / or \ (absolute paths)
	if key[0] == '/' || key[0] == '\\' {
		return errors.New("object key cannot start with '/' or '\\'")
	}
	// Normalize and verify no path traversal after normalization
	cleaned := filepath.Clean(key)
	if strings.HasPrefix(cleaned, "..") {
		return errors.New("invalid object key after normalization")
	}
	// Check for null bytes
	if strings.Contains(key, "\x00") {
		return errors.New("object key cannot contain null bytes")
	}
	return nil
}

// escapePath makes a key safe for filesystem
func escapePath(key string) string {
	// First sanitize for path traversal
	key = strings.ReplaceAll(key, "..", "")
	// Replace / with __ESCAPE__ to preserve directory structure
	key = strings.ReplaceAll(key, "/", "__ESCAPE__")
	// Also escape backslashes
	key = strings.ReplaceAll(key, "\\", "__BSLASH__")
	return key
}

// unescapePath reverses escapePath
func unescapePath(path string) string {
	path = strings.ReplaceAll(path, "__BSLASH__", "\\")
	path = strings.ReplaceAll(path, "__ESCAPE__", "/")
	return path
}

func (f *FlatFile) Put(ctx context.Context, bucket, key string, data io.Reader, size int64, opts storage.PutOptions) error {
	// Validate object key
	if err := validateKey(key); err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	bucketDir := f.bucketPath(bucket)
	if err := os.MkdirAll(bucketDir, 0755); err != nil {
		diskIOErrors.WithLabelValues("put_mkdir").Inc()
		return fmt.Errorf("failed to create bucket directory: %w", err)
	}

	objectPath := f.objectPath(bucket, key)

	// Create parent directories
	parentDir := filepath.Dir(objectPath)
	if err := os.MkdirAll(parentDir, 0755); err != nil {
		diskIOErrors.WithLabelValues("put_mkdir_parent").Inc()
		return fmt.Errorf("failed to create parent directory: %w", err)
	}

	// Create temp file for atomic write
	tmpPath := objectPath + ".tmp"
	fh, err := os.Create(tmpPath)
	if err != nil {
		diskIOErrors.WithLabelValues("put_create").Inc()
		return fmt.Errorf("failed to create temp file: %w", err)
	}

	// Copy data and calculate hash
	hasher := sha256.New()
	writer := io.MultiWriter(fh, hasher)

	written, err := io.Copy(writer, data)
	if err != nil {
		fh.Close()
		os.Remove(tmpPath)
		diskIOErrors.WithLabelValues("put_copy").Inc()
		return fmt.Errorf("failed to write data: %w", err)
	}

	if err := fh.Close(); err != nil {
		os.Remove(tmpPath)
		diskIOErrors.WithLabelValues("put_close").Inc()
		return fmt.Errorf("failed to close temp file: %w", err)
	}

	// Verify size
	if written != size && size > 0 {
		os.Remove(tmpPath)
		return fmt.Errorf("size mismatch: expected %d, got %d", size, written)
	}

	// Calculate and store hash for ETag
	hash := hex.EncodeToString(hasher.Sum(nil))
	hashPath := objectPath + ".hash"
	if err := os.WriteFile(hashPath, []byte(hash), 0644); err != nil {
		// Log warning but don't fail - hash is optional for ETag
		f.logger.Warnw("failed to write hash file", "error", err)
	}

	// Rename to final location (atomic on same filesystem)
	if err := os.Rename(tmpPath, objectPath); err != nil {
		os.Remove(tmpPath)
		os.Remove(hashPath)
		diskIOErrors.WithLabelValues("put_rename").Inc()
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	bytesWritten.Add(float64(written))
	f.logger.Debugw("object written",
		"bucket", bucket,
		"key", key,
		"size", written,
	)

	return nil
}

func (f *FlatFile) Get(ctx context.Context, bucket, key string, opts storage.GetOptions) (io.ReadCloser, error) {
	// Validate object key
	if err := validateKey(key); err != nil {
		return nil, err
	}

	f.mu.RLock()
	defer f.mu.RUnlock()

	objectPath := f.objectPath(bucket, key)

	// Check if file exists
	info, err := os.Stat(objectPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("object not found: %s/%s", bucket, key)
		}
		diskIOErrors.WithLabelValues("get_stat").Inc()
		return nil, fmt.Errorf("failed to stat object: %w", err)
	}

	file, err := os.Open(objectPath)
	if err != nil {
		diskIOErrors.WithLabelValues("get_open").Inc()
		return nil, fmt.Errorf("failed to open object: %w", err)
	}

	var reader io.Reader = file

	// Handle range requests
	if opts.Range != nil {
		reader = io.LimitReader(reader, opts.Range.End-opts.Range.Start)
		_, err := file.Seek(opts.Range.Start, io.SeekStart)
		if err != nil {
			file.Close()
			diskIOErrors.WithLabelValues("get_seek").Inc()
			return nil, fmt.Errorf("failed to seek: %w", err)
		}
	}

	bytesRead.Add(float64(info.Size()))

	return &readerWithSize{
		Reader: reader,
		Size:   info.Size(),
		Closer: file,
	}, nil
}

type readerWithSize struct {
	io.Reader
	Size int64
	io.Closer
}

func (f *FlatFile) Delete(ctx context.Context, bucket, key string) error {
	// Validate object key
	if err := validateKey(key); err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	objectPath := f.objectPath(bucket, key)

	if err := os.Remove(objectPath); err != nil {
		if os.IsNotExist(err) {
			return nil // Already deleted
		}
		diskIOErrors.WithLabelValues("delete").Inc()
		return fmt.Errorf("failed to delete object: %w", err)
	}

	// Also remove hash file if exists
	hashPath := objectPath + ".hash"
	os.Remove(hashPath) // Ignore error - file may not exist

	// Try to clean up empty parent directories
	parentDir := filepath.Dir(objectPath)
	f.cleanupEmptyDirs(parentDir)

	return nil
}

func (f *FlatFile) cleanupEmptyDirs(dir string) {
	for {
		entries, err := os.ReadDir(dir)
		if err != nil || len(entries) > 0 {
			break
		}
		if err := os.Remove(dir); err != nil {
			break
		}
		dir = filepath.Dir(dir)
		if dir == f.bucketPath("") {
			break
		}
	}
}

func (f *FlatFile) Head(ctx context.Context, bucket, key string) (*storage.ObjectInfo, error) {
	// Validate object key
	if err := validateKey(key); err != nil {
		return nil, err
	}

	f.mu.RLock()
	defer f.mu.RUnlock()

	objectPath := f.objectPath(bucket, key)

	info, err := os.Stat(objectPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("object not found: %s/%s", bucket, key)
		}
		return nil, fmt.Errorf("failed to stat object: %w", err)
	}

	// Try to read stored hash for ETag (preferred)
	var etag string
	hashPath := objectPath + ".hash"
	if hashData, err := os.ReadFile(hashPath); err == nil && len(hashData) > 0 {
		etag = fmt.Sprintf("\"%s\"", strings.TrimSpace(string(hashData)))
	} else {
		// Fallback to size + modtime hash if stored hash not available
		etagData := fmt.Sprintf("%d-%d", info.Size(), info.ModTime().UnixNano())
		etag = fmt.Sprintf("\"%x\"", sha256.Sum256([]byte(etagData)))
	}

	return &storage.ObjectInfo{
		Key:          key,
		Size:         info.Size(),
		ETag:         etag,
		LastModified: info.ModTime().Unix(),
	}, nil
}

func (f *FlatFile) List(ctx context.Context, bucket, prefix string, opts storage.ListOptions) (*storage.ListResult, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	bucketDir := f.bucketPath(bucket)

	// Check if bucket exists
	if _, err := os.Stat(bucketDir); err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("bucket not found: %s", bucket)
		}
		return nil, fmt.Errorf("failed to stat bucket: %w", err)
	}

	// Track common prefixes when delimiter is used
	var commonPrefixes []string
	commonPrefixSet := make(map[string]bool)

	// Walk the directory tree
	var objects []storage.ObjectInfo
	prefixPath := escapePath(prefix)
	_ = prefixPath // Reserved for future use

	err := filepath.Walk(bucketDir, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories
		if info.IsDir() {
			return nil
		}

		// Get relative path
		relPath, err := filepath.Rel(bucketDir, path)
		if err != nil {
			return err
		}

		// Unescape the path
		relPath = unescapePath(relPath)

		// Check if matches prefix
		if prefix != "" && !strings.HasPrefix(relPath, prefix) {
			return nil
		}

		// Skip if before marker
		if opts.Marker != "" && relPath <= opts.Marker {
			return nil
		}

		// Check delimiter for common prefix
		if opts.Delimiter != "" {
			// Check if this is a "directory" level
			afterPrefix := strings.TrimPrefix(relPath, prefix)
			if idx := strings.Index(afterPrefix, opts.Delimiter); idx >= 0 {
				// This is a common prefix - extract the folder part
				folderPart := relPath[:len(prefix)+idx+len(opts.Delimiter)]
				if !commonPrefixSet[folderPart] {
					commonPrefixSet[folderPart] = true
					commonPrefixes = append(commonPrefixes, folderPart)
				}
				// Skip the actual file
				return nil
			}
		}

		// Calculate etag
		etag := fmt.Sprintf("\"%s\"", hex.EncodeToString([]byte(info.Name())))

		objects = append(objects, storage.ObjectInfo{
			Key:          relPath,
			Size:         info.Size(),
			ETag:         etag,
			LastModified: info.ModTime().Unix(),
		})

		// Check max keys
		if opts.MaxKeys > 0 && len(objects) >= opts.MaxKeys {
			return io.EOF
		}

		return nil
	})

	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("failed to list objects: %w", err)
	}

	// Sort by key
	sort.Slice(objects, func(i, j int) bool {
		return objects[i].Key < objects[j].Key
	})

	// Sort common prefixes
	sort.Strings(commonPrefixes)

	return &storage.ListResult{
		Objects:       objects,
		CommonPrefixes: commonPrefixes,
	}, nil
}

func (f *FlatFile) CreateBucket(ctx context.Context, bucket string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	bucketDir := f.bucketPath(bucket)

	if err := os.MkdirAll(bucketDir, 0755); err != nil {
		diskIOErrors.WithLabelValues("create_bucket").Inc()
		return fmt.Errorf("failed to create bucket: %w", err)
	}

	return nil
}

func (f *FlatFile) DeleteBucket(ctx context.Context, bucket string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	bucketDir := f.bucketPath(bucket)

	// Check if bucket is empty
	entries, err := os.ReadDir(bucketDir)
	if err != nil {
		return fmt.Errorf("failed to read bucket: %w", err)
	}

	if len(entries) > 0 {
		return fmt.Errorf("bucket not empty: %s", bucket)
	}

	if err := os.Remove(bucketDir); err != nil {
		diskIOErrors.WithLabelValues("delete_bucket").Inc()
		return fmt.Errorf("failed to delete bucket: %w", err)
	}

	return nil
}

func (f *FlatFile) ListBuckets(ctx context.Context) ([]storage.BucketInfo, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	bucketsDir := filepath.Join(f.rootDir, "buckets")

	entries, err := os.ReadDir(bucketsDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read buckets: %w", err)
	}

	var buckets []storage.BucketInfo
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}

		buckets = append(buckets, storage.BucketInfo{
			Name:         entry.Name(),
			CreationDate: info.ModTime().Unix(),
		})
	}

	// Sort by name
	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].Name < buckets[j].Name
	})

	return buckets, nil
}

func (f *FlatFile) Close() error {
	return nil
}

// ComputeStorageMetrics computes total storage size and object count
func (f *FlatFile) ComputeStorageMetrics() (int64, int64, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	bucketsDir := filepath.Join(f.rootDir, "buckets")

	entries, err := os.ReadDir(bucketsDir)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to read buckets: %w", err)
	}

	var totalBytes int64
	var totalObjects int64

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		bucketDir := filepath.Join(bucketsDir, entry.Name())
		err := filepath.Walk(bucketDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return nil
			}
			if !info.IsDir() {
				totalBytes += info.Size()
				totalObjects++
			}
			return nil
		})
		if err != nil {
			continue
		}
	}

	return totalBytes, totalObjects, nil
}

// NewTestBackend creates a simple in-memory backend for testing
func NewTestBackend() *FlatFile {
	tmpDir, _ := os.MkdirTemp("", "openendpoint-test-*")
	ff, _ := New(tmpDir)
	return ff
}

// GetDataDir returns the root data directory
func (f *FlatFile) GetDataDir() string {
	return f.rootDir
}

func init() {
	// Override time functions for testing
	_ = time.Now()
}
