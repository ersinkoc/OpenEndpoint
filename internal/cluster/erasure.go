package cluster

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/klauspost/reedsolomon"
	"go.uber.org/zap"
)

// ErasureConfig contains erasure coding configuration
type ErasureConfig struct {
	DataShards    int // Number of data shards
	ParityShards  int // Number of parity shards
	TotalShards   int // Total shards (data + parity)
}

// DefaultErasureConfig returns default configuration (4+2)
func DefaultErasureConfig() ErasureConfig {
	return ErasureConfig{
		DataShards:   4,
		ParityShards: 2,
		TotalShards:  6,
	}
}

// HighPerformanceConfig returns high performance config (8+2)
func HighPerformanceConfig() ErasureConfig {
	return ErasureConfig{
		DataShards:   8,
		ParityShards: 2,
		TotalShards: 10,
	}
}

// HighDurabilityConfig returns high durability config (4+4)
func HighDurabilityConfig() ErasureConfig {
	return ErasureConfig{
		DataShards:   4,
		ParityShards: 4,
		TotalShards:  8,
	}
}

// ErasureCoder handles erasure coding operations
type ErasureCoder struct {
	config  ErasureConfig
	enc     reedsolomon.Encoder
	logger  *zap.Logger
	pool    *sync.Pool
}

// NewErasureCoder creates a new erasure coder
func NewErasureCoder(config ErasureConfig, logger *zap.Logger) (*ErasureCoder, error) {
	enc, err := reedsolomon.New(config.DataShards, config.ParityShards)
	if err != nil {
		return nil, fmt.Errorf("failed to create encoder: %w", err)
	}

	c := &ErasureCoder{
		config: config,
		enc:    enc,
		logger: logger,
		pool: &sync.Pool{
			New: func() interface{} {
				return &bytes.Buffer{}
			},
		},
	}

	return c, nil
}

// Encode encodes data into shards
func (c *ErasureCoder) Encode(data []byte) ([][]byte, error) {
	// Calculate optimal shard size
	shardSize := (len(data) + c.config.DataShards - 1) / c.config.DataShards

	// Create data shards
	dataShards := make([][]byte, c.config.DataShards)
	for i := 0; i < c.config.DataShards; i++ {
		start := i * shardSize
		end := start + shardSize
		if end > len(data) {
			end = len(data)
		}

		if start < len(data) {
			dataShards[i] = make([]byte, shardSize)
			copy(dataShards[i], data[start:end])
		} else {
			dataShards[i] = make([]byte, shardSize)
		}
	}

	// Use encoder to create parity shards
	shards := make([][]byte, c.config.TotalShards)
	copy(shards, dataShards)

	err := c.enc.Encode(shards)
	if err != nil {
		return nil, fmt.Errorf("failed to encode: %w", err)
	}

	c.logger.Debug("Data encoded",
		zap.Int("data_size", len(data)),
		zap.Int("shard_count", c.config.TotalShards),
		zap.Int("shard_size", shardSize))

	return shards, nil
}

// Decode decodes shards back to data
func (c *ErasureCoder) Decode(shards [][]byte) ([]byte, error) {
	if len(shards) != c.config.TotalShards {
		return nil, fmt.Errorf("expected %d shards, got %d", c.config.TotalShards, len(shards))
	}

	// Check which shards are available
	available := make([]bool, c.config.TotalShards)
	for i, shard := range shards {
		available[i] = shard != nil && len(shard) > 0
	}

	// Check if we have enough shards to recover
	availableCount := 0
	for _, a := range available {
		if a {
			availableCount++
		}
	}

	if availableCount < c.config.DataShards {
		return nil, fmt.Errorf("not enough shards: have %d, need %d", availableCount, c.config.DataShards)
	}

	// Make a copy to avoid modifying original
	shardsCopy := make([][]byte, c.config.TotalShards)
	copy(shardsCopy, shards)

	// Decode
	err := c.enc.ReconstructData(shardsCopy)
	if err != nil {
		return nil, fmt.Errorf("failed to reconstruct: %w", err)
	}

	// Combine data shards
	var result []byte
	for _, shard := range shardsCopy[:c.config.DataShards] {
		result = append(result, shard...)
	}

	c.logger.Debug("Data decoded",
		zap.Int("result_size", len(result)),
		zap.Int("available_shards", availableCount))

	return result, nil
}

// Reconstruct reconstructs missing shards
func (c *ErasureCoder) Reconstruct(shards [][]byte) error {
	if len(shards) != c.config.TotalShards {
		return fmt.Errorf("expected %d shards, got %d", c.config.TotalShards, len(shards))
	}

	// Check which shards are available
	available := 0
	for _, shard := range shards {
		if shard != nil && len(shard) > 0 {
			available++
		}
	}

	if available < c.config.DataShards {
		return fmt.Errorf("not enough shards to reconstruct: have %d, need %d", available, c.config.DataShards)
	}

	err := c.enc.Reconstruct(shards)
	if err != nil {
		return fmt.Errorf("failed to reconstruct: %w", err)
	}

	return nil
}

// Verify verifies data integrity
func (c *ErasureCoder) Verify(shards [][]byte) (bool, error) {
	if len(shards) != c.config.TotalShards {
		return false, fmt.Errorf("expected %d shards, got %d", c.config.TotalShards, len(shards))
	}

	ok, err := c.enc.Verify(shards)
	if err != nil {
		return false, fmt.Errorf("failed to verify: %w", err)
	}

	return ok, nil
}

// GetConfig returns the erasure configuration
func (c *ErasureCoder) GetConfig() ErasureConfig {
	return c.config
}

// SplitSize calculates the shard size for a given data size
func (c *ErasureCoder) SplitSize(dataSize int) int {
	return (dataSize + c.config.DataShards - 1) / c.config.DataShards
}

// JoinSize calculates the original data size from shards
func (c *ErasureCoder) JoinSize(shardSize int) int {
	return shardSize * c.config.DataShards
}

// ErasureStripe represents a stripe of erasure-coded data
type ErasureStripe struct {
	ID        string    `json:"id"`
	Key       string    `json:"key"`
	Shards    [][]byte `json:"shards"`
	CreatedAt int64     `json:"created_at"`
}

// ErasureStripeStore manages erasure stripes
type ErasureStripeStore struct {
	mu      sync.RWMutex
	stripes map[string]*ErasureStripe
	coder   *ErasureCoder
	logger  *zap.Logger
}

// NewErasureStripeStore creates a new stripe store
func NewErasureStripeStore(coder *ErasureCoder, logger *zap.Logger) *ErasureStripeStore {
	return &ErasureStripeStore{
		stripes: make(map[string]*ErasureStripe),
		coder:   coder,
		logger:  logger,
	}
}

// Store stores a stripe
func (s *ErasureStripeStore) Store(stripe *ErasureStripe) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stripes[stripe.ID] = stripe
}

// Get retrieves a stripe
func (s *ErasureStripeStore) Get(id string) (*ErasureStripe, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	stripe, ok := s.stripes[id]
	return stripe, ok
}

// Delete deletes a stripe
func (s *ErasureStripeStore) Delete(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.stripes, id)
}

// List lists all stripes
func (s *ErasureStripeStore) List() []*ErasureStripe {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]*ErasureStripe, 0, len(s.stripes))
	for _, stripe := range s.stripes {
		result = append(result, stripe)
	}
	return result
}

// Stats returns store statistics
func (s *ErasureStripeStore) Stats() (int, int64) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	count := len(s.stripes)
	var size int64
	for _, stripe := range s.stripes {
		for _, shard := range stripe.Shards {
			size += int64(len(shard))
		}
	}
	return count, size
}

// ErasureWriter writes erasure-coded data
type ErasureWriter struct {
	coder   *ErasureCoder
	ring    *HashRing
	manager *Manager
	logger  *zap.Logger
}

// NewErasureWriter creates a new erasure writer
func NewErasureWriter(coder *ErasureCoder, ring *HashRing, manager *Manager, logger *zap.Logger) *ErasureWriter {
	return &ErasureWriter{
		coder:   coder,
		ring:    ring,
		manager: manager,
		logger:  logger,
	}
}

// Write writes erasure-coded data to nodes
func (w *ErasureWriter) Write(ctx context.Context, key string, data []byte) error {
	// Encode data into shards
	shards, err := w.coder.Encode(data)
	if err != nil {
		return fmt.Errorf("failed to encode: %w", err)
	}

	// Get target nodes
	targetNodes := w.ring.GetNNodes(key, w.coder.GetConfig().TotalShards)
	if len(targetNodes) < w.coder.GetConfig().TotalShards {
		return fmt.Errorf("not enough nodes: have %d, need %d", len(targetNodes), w.coder.GetConfig().TotalShards)
	}

	// Write shards to nodes
	var wg sync.WaitGroup
	var mu sync.Mutex
	writeErrors := make(map[string]error)

	for i, nodeID := range targetNodes {
		wg.Add(1)
		go func(shardIndex int, nid string, shardData []byte) {
			defer wg.Done()

			err := w.writeShardToNode(ctx, key, shardIndex, nid, shardData)
			mu.Lock()
			if err != nil {
				writeErrors[nid] = err
			}
			mu.Unlock()
		}(i, nodeID, shards[i])
	}

	wg.Wait()

	if len(writeErrors) > w.coder.GetConfig().ParityShards {
		return fmt.Errorf("too many write failures: %d", len(writeErrors))
	}

	w.logger.Info("Erasure write completed",
		zap.String("key", key),
		zap.Int("shards", len(shards)))

	return nil
}

// writeShardToNode writes a shard to a specific node
func (w *ErasureWriter) writeShardToNode(ctx context.Context, key string, shardIndex int, nodeID string, data []byte) error {
	w.logger.Debug("Writing shard to node",
		zap.String("key", key),
		zap.Int("shard_index", shardIndex),
		zap.String("node_id", nodeID),
		zap.Int("shard_size", len(data)))

	// In production, this would make an RPC call to the target node
	return nil
}

// Read reads erasure-coded data from nodes
func (w *ErasureWriter) Read(ctx context.Context, key string) ([]byte, error) {
	config := w.coder.GetConfig()
	targetNodes := w.ring.GetNNodes(key, config.TotalShards)

	// Read shards from nodes
	shards := make([][]byte, config.TotalShards)

	var wg sync.WaitGroup
	var mu sync.Mutex

	for i, nodeID := range targetNodes {
		wg.Add(1)
		go func(shardIndex int, nid string) {
			defer wg.Done()

			shard, err := w.readShardFromNode(ctx, key, shardIndex, nid)
			mu.Lock()
			if err == nil && shard != nil {
				shards[shardIndex] = shard
			}
			mu.Unlock()
		}(i, nodeID)
	}

	wg.Wait()

	// Check how many shards we got
	available := 0
	for _, shard := range shards {
		if shard != nil {
			available++
		}
	}

	if available < config.DataShards {
		return nil, fmt.Errorf("not enough shards to reconstruct: have %d, need %d", available, config.DataShards)
	}

	// Decode data
	return w.coder.Decode(shards)
}

// readShardFromNode reads a shard from a specific node
func (w *ErasureWriter) readShardFromNode(ctx context.Context, key string, shardIndex int, nodeID string) ([]byte, error) {
	w.logger.Debug("Reading shard from node",
		zap.String("key", key),
		zap.Int("shard_index", shardIndex),
		zap.String("node_id", nodeID))

	// In production, this would make an RPC call to the target node
	return nil, nil
}
