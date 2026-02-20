package dedup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"sync"

	"go.uber.org/zap"
)

// Fingerprint represents a content fingerprint
type Fingerprint string

// Store stores fingerprints and references
type Store struct {
	logger      *zap.Logger
	mu          sync.RWMutex
	fingerprints map[Fingerprint]*FingerprintInfo
	deduped     int64
	spaceSaved  int64
}

// FingerprintInfo contains information about a fingerprint
type FingerprintInfo struct {
	Fingerprint Fingerprint
	Size        int64
	RefCount    int64
	FirstSeen   int64
	Objects     []ObjectRef
}

// ObjectRef references an object that uses this fingerprint
type ObjectRef struct {
	Bucket string
	Key    string
}

// NewStore creates a new deduplication store
func NewStore(logger *zap.Logger) *Store {
	return &Store{
		logger:      logger,
		fingerprints: make(map[Fingerprint]*FingerprintInfo),
	}
}

// ComputeFingerprint computes the fingerprint of data
func ComputeFingerprint(data []byte) Fingerprint {
	hash := sha256.Sum256(data)
	return Fingerprint(hex.EncodeToString(hash[:]))
}

// ComputeFingerprintFromReader computes fingerprint from reader
func ComputeFingerprintFromReader(r io.Reader) (Fingerprint, int64, error) {
	hasher := sha256.New()
	buf := make([]byte, 32*1024)

	var total int64
	for {
		n, err := r.Read(buf)
		if n > 0 {
			hasher.Write(buf[:n])
			total += int64(n)
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return "", 0, err
		}
	}

	return Fingerprint(hex.EncodeToString(hasher.Sum(nil))), total, nil
}

// AddObject adds an object to the dedup store
func (s *Store) AddObject(bucket, key string, data []byte) (Fingerprint, bool, error) {
	fp := ComputeFingerprint(data)
	size := int64(len(data))

	s.mu.Lock()
	defer s.mu.Unlock()

	info, exists := s.fingerprints[fp]
	if exists {
		// Already exists - add reference
		info.RefCount++
		info.Objects = append(info.Objects, ObjectRef{Bucket: bucket, Key: key})

		s.deduped++
		s.spaceSaved += size

		s.logger.Debug("Object deduped",
			zap.String("fingerprint", string(fp)),
			zap.Int64("ref_count", info.RefCount),
			zap.Int64("space_saved", s.spaceSaved))

		return fp, true, nil
	}

	// New fingerprint
	s.fingerprints[fp] = &FingerprintInfo{
		Fingerprint: fp,
		Size:        size,
		RefCount:    1,
		FirstSeen:   nowUnix(),
		Objects:     []ObjectRef{{Bucket: bucket, Key: key}},
	}

	return fp, false, nil
}

// GetFingerprintInfo returns info about a fingerprint
func (s *Store) GetFingerprintInfo(fp Fingerprint) (*FingerprintInfo, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	info, ok := s.fingerprints[fp]
	return info, ok
}

// RemoveObject removes an object reference
func (s *Store) RemoveObject(bucket, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Find and remove reference
	for fp, info := range s.fingerprints {
		for i, ref := range info.Objects {
			if ref.Bucket == bucket && ref.Key == key {
				// Remove from slice
				info.Objects = append(info.Objects[:i], info.Objects[i+1:]...)
				info.RefCount--

				// If no more references, remove fingerprint
				if info.RefCount <= 0 {
					delete(s.fingerprints, fp)
					s.spaceSaved -= info.Size
				}

				return nil
			}
		}
	}

	return fmt.Errorf("object not found: %s/%s", bucket, key)
}

// GetStats returns deduplication statistics
func (s *Store) GetStats() Stats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var totalSize int64
	var totalObjects int64
	var duplicateObjects int64

	for _, info := range s.fingerprints {
		totalSize += info.Size
		totalObjects += info.RefCount
		if info.RefCount > 1 {
			duplicateObjects += info.RefCount - 1
		}
	}

	dedupRatio := 0.0
	if totalSize > 0 {
		dedupRatio = float64(s.spaceSaved) / float64(totalSize) * 100
	}

	return Stats{
		TotalObjects:      totalObjects,
		UniqueObjects:     int64(len(s.fingerprints)),
		DuplicateObjects: duplicateObjects,
		SpaceSaved:       s.spaceSaved,
		DedupRatio:       dedupRatio,
	}
}

// Stats contains deduplication statistics
type Stats struct {
	TotalObjects      int64   `json:"total_objects"`
	UniqueObjects     int64   `json:"unique_objects"`
	DuplicateObjects  int64   `json:"duplicate_objects"`
	SpaceSaved        int64   `json:"space_saved_bytes"`
	DedupRatio        float64 `json:"dedup_ratio_percent"`
}

// Deduplicator provides deduplication at read/write time
type Deduplicator struct {
	store  *Store
	logger *zap.Logger
}

// NewDeduplicator creates a new deduplicator
func NewDeduplicator(logger *zap.Logger) *Deduplicator {
	return &Deduplicator{
		store:  NewStore(logger),
		logger: logger,
	}
}

// ProcessWrite deduplicates data on write
func (d *Deduplicator) ProcessWrite(ctx context.Context, bucket, key string, data []byte) (*WriteResult, error) {
	fp, deduped, err := d.store.AddObject(bucket, key, data)
	if err != nil {
		return nil, err
	}

	return &WriteResult{
		Fingerprint: fp,
		Deduplicated: deduped,
		Size:        int64(len(data)),
	}, nil
}

// ProcessRead reads and reconstructs data
func (d *Deduplicator) ProcessRead(bucket, key string) ([]byte, error) {
	// In production, this would read the actual data
	// For now, return placeholder
	return nil, fmt.Errorf("not implemented: would read from storage")
}

// WriteResult contains the result of a deduplicated write
type WriteResult struct {
	Fingerprint  Fingerprint
	Deduplicated bool
	Size         int64
}

// nowUnix returns current Unix timestamp
func nowUnix() int64 {
	return 0 // Placeholder - would use time.Now().Unix()
}

// ChunkedFingerprinter fingerprints data using variable chunking
type ChunkedFingerprinter struct {
	logger    *zap.Logger
	minChunk  int
	maxChunk  int
	threshold int
}

// NewChunkedFingerprinter creates a new chunked fingerprinter
func NewChunkedFingerprinter(logger *zap.Logger, minChunk, maxChunk, threshold int) *ChunkedFingerprinter {
	return &ChunkedFingerprinter{
		logger:    logger,
		minChunk:  minChunk,
		maxChunk:  maxChunk,
		threshold: threshold,
	}
}

// FingerprintChunks fingerprints data using content-defined chunking
func (c *ChunkedFingerprinter) FingerprintChunks(data []byte) []Fingerprint {
	var chunks []Fingerprint

	// Simple fixed-size chunking for demonstration
	// In production, use content-defined chunking (CDC)
	chunkSize := 64 * 1024 // 64KB chunks

	for i := 0; i < len(data); i += chunkSize {
		end := i + chunkSize
		if end > len(data) {
			end = len(data)
		}

		chunk := data[i:end]
		fp := ComputeFingerprint(chunk)
		chunks = append(chunks, fp)
	}

	return chunks
}

// CompareChunks compares two sets of chunks
func (c *ChunkedFingerprinter) CompareChunks(a, b []Fingerprint) (int, int) {
	aSet := make(map[Fingerprint]bool)
	for _, fp := range a {
		aSet[fp] = true
	}

	common := 0
	uniqueB := 0
	for _, fp := range b {
		if aSet[fp] {
			common++
		} else {
			uniqueB++
		}
	}

	return common, uniqueB
}

// RollingFingerprinter implements rolling hash-based fingerprinting
type RollingFingerprinter struct {
	logger *zap.Logger
	window int
	prime  uint64
}

// NewRollingFingerprinter creates a new rolling fingerprinter
func NewRollingFingerprinter(logger *zap.Logger, window int) *RollingFingerprinter {
	return &RollingFingerprinter{
		logger: logger,
		window: window,
		prime:  91138233,
	}
}

// FindChunkBoundaries finds chunk boundaries using rolling hash
func (r *RollingFingerprinter) FindChunkBoundaries(data []byte) []int {
	var boundaries []int

	// Simple implementation: find boundaries at ~64KB intervals
	chunkSize := 64 * 1024

	for i := chunkSize; i < len(data); i += chunkSize {
		boundaries = append(boundaries, i)
	}

	return boundaries
}

// HashRange computes hash for a range
func (r *RollingFingerprinter) HashRange(data []byte, start, end int) uint64 {
	var hash uint64 = 0
	for i := start; i < end && i < len(data); i++ {
		hash = hash*r.prime + uint64(data[i])
	}
	return hash
}

// DeduplicationWriter wraps a writer with deduplication
type DeduplicationWriter struct {
	writer    io.Writer
	dedup     *Deduplicator
	bucket    string
	key       string
	buf       *bytes.Buffer
	threshold int
	logger    *zap.Logger
}

// NewDeduplicationWriter creates a new deduplication writer
func NewDeduplicationWriter(w io.Writer, dedup *Deduplicator, bucket, key string, threshold int, logger *zap.Logger) *DeduplicationWriter {
	return &DeduplicationWriter{
		writer:    w,
		dedup:     dedup,
		bucket:    bucket,
		key:       key,
		buf:       new(bytes.Buffer),
		threshold: threshold,
		logger:    logger,
	}
}

// Write implements io.Writer
func (d *DeduplicationWriter) Write(p []byte) (int, error) {
	d.buf.Write(p)

	// When buffer exceeds threshold, deduplicate
	if d.buf.Len() >= d.threshold {
		data := d.buf.Bytes()
		result, err := d.dedup.ProcessWrite(context.Background(), d.bucket, d.key, data)
		if err != nil {
			return 0, err
		}

		if result.Deduplicated {
			d.logger.Debug("Chunk deduplicated",
				zap.String("fingerprint", string(result.Fingerprint)))
		}

		d.buf.Reset()
	}

	return len(p), nil
}

// Close closes the writer
func (d *DeduplicationWriter) Close() error {
	// Process remaining data
	if d.buf.Len() > 0 {
		data := d.buf.Bytes()
		_, err := d.dedup.ProcessWrite(context.Background(), d.bucket, d.key, data)
		if err != nil {
			return err
		}
	}

	return nil
}
