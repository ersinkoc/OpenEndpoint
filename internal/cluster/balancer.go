package cluster

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// RebalanceConfig contains rebalancing configuration
type RebalanceConfig struct {
	Enabled            bool          // Enable automatic rebalancing
	ThresholdPercent   float64       // Rebalance when distribution deviates by this %
	MaxConcurrentMoves int           // Maximum concurrent shard moves
	ThrottleMBps      int           // Throttle speed in MB/s
	CheckInterval     time.Duration // How often to check distribution
}

// DefaultRebalanceConfig returns default configuration
func DefaultRebalanceConfig() RebalanceConfig {
	return RebalanceConfig{
		Enabled:            true,
		ThresholdPercent:   10.0, // 10% deviation triggers rebalance
		MaxConcurrentMoves: 5,
		ThrottleMBps:       100, // 100 MB/s
		CheckInterval:      5 * time.Minute,
	}
}

// RebalanceOperation represents a rebalance move operation
type RebalanceOperation struct {
	ID          string         `json:"id"`
	ShardID     string         `json:"shard_id"`
	Key         string         `json:"key"`
	SourceNode  string         `json:"source_node"`
	TargetNode  string         `json:"target_node"`
	Status      RebalanceStatus `json:"status"`
	Progress    float64        `json:"progress"`
	StartTime   time.Time      `json:"start_time"`
	CompleteTime *time.Time    `json:"complete_time,omitempty"`
	Error       string        `json:"error,omitempty"`
}

// RebalanceStatus represents the status of a rebalance operation
type RebalanceStatus string

const (
	RebalancePending   RebalanceStatus = "pending"
	RebalanceRunning   RebalanceStatus = "running"
	RebalanceComplete  RebalanceStatus = "complete"
	RebalanceFailed    RebalanceStatus = "failed"
	RebalanceCancelled RebalanceStatus = "cancelled"
)

// Rebalancer handles data rebalancing across nodes
type Rebalancer struct {
	config    RebalanceConfig
	manager   *Manager
	ring      *HashRing
	logger    *zap.Logger
	mu        sync.RWMutex
	ops       map[string]*RebalanceOperation
	activeOps int32
	paused    atomic.Bool
	stopCh    chan struct{}
}

// NewRebalancer creates a new rebalancer
func NewRebalancer(config RebalanceConfig, manager *Manager, ring *HashRing, logger *zap.Logger) *Rebalancer {
	return &Rebalancer{
		config: config,
		manager: manager,
		ring:   ring,
		logger: logger,
		ops:    make(map[string]*RebalanceOperation),
		stopCh: make(chan struct{}),
	}
}

// Start starts the rebalancer
func (r *Rebalancer) Start(ctx context.Context) {
	if !r.config.Enabled {
		r.logger.Info("Rebalancer is disabled")
		return
	}

	r.logger.Info("Starting rebalancer",
		zap.Float64("threshold", r.config.ThresholdPercent),
		zap.Int("max_concurrent", r.config.MaxConcurrentMoves))

	go r.runChecker(ctx)
}

// Stop stops the rebalancer
func (r *Rebalancer) Stop() {
	r.logger.Info("Stopping rebalancer")
	close(r.stopCh)
}

// Pause pauses rebalancing
func (r *Rebalancer) Pause() {
	r.paused.Store(true)
	r.logger.Info("Rebalancer paused")
}

// Resume resumes rebalancing
func (r *Rebalancer) Resume() {
	r.paused.Store(false)
	r.logger.Info("Rebalancer resumed")
}

// IsPaused returns whether rebalancing is paused
func (r *Rebalancer) IsPaused() bool {
	return r.paused.Load()
}

// runChecker periodically checks and triggers rebalancing
func (r *Rebalancer) runChecker(ctx context.Context) {
	ticker := time.NewTicker(r.config.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-r.stopCh:
			return
		case <-ticker.C:
			if r.paused.Load() {
				continue
			}
			r.checkAndRebalance(ctx)
		}
	}
}

// checkAndRebalance checks distribution and triggers rebalancing if needed
func (r *Rebalancer) checkAndRebalance(ctx context.Context) {
	// Get current distribution
	distribution := r.ring.GetNodeDistribution()
	if len(distribution) == 0 {
		return
	}

	// Calculate ideal distribution
	nodeCount := r.ring.NodeCount()
	if nodeCount == 0 {
		return
	}

	totalVN := 0
	for _, count := range distribution {
		totalVN += count
	}
	idealVN := totalVN / nodeCount

	// Check for imbalance
	threshold := idealVN * int(r.config.ThresholdPercent) / 100
	var moves []RebalanceOperation

	for nodeID, count := range distribution {
		deviation := count - idealVN
		if abs(deviation) > threshold {
			// This node is imbalanced
			r.logger.Info("Node imbalance detected",
				zap.String("node_id", nodeID),
				zap.Int("current", count),
				zap.Int("ideal", idealVN),
				zap.Int("deviation", deviation))

			// Generate move operations
			if deviation > 0 {
				// Node has too many - need to move some shards out
				moveCount := deviation / 2
				for i := 0; i < moveCount; i++ {
					// Find a less-loaded node
					targetNode := r.findLeastLoadedNode(distribution, nodeID)
					if targetNode != "" {
						moves = append(moves, RebalanceOperation{
							ID:         fmt.Sprintf("rebalance-%d", time.Now().UnixNano()),
							SourceNode: nodeID,
							TargetNode: targetNode,
						})
					}
				}
			}
		}
	}

	// Execute moves
	if len(moves) > 0 {
		r.logger.Info("Starting rebalance",
			zap.Int("move_count", len(moves)))
		r.executeMoves(ctx, moves)
	}
}

// findLeastLoadedNode finds the least loaded node
func (r *Rebalancer) findLeastLoadedNode(distribution map[string]int, exclude string) string {
	var minNode string
	minCount := int(^uint(0) >> 1) // Max int

	for nodeID, count := range distribution {
		if nodeID == exclude {
			continue
		}
		if count < minCount {
			minCount = count
			minNode = nodeID
		}
	}

	return minNode
}

// executeMoves executes rebalance moves
func (r *Rebalancer) executeMoves(ctx context.Context, moves []RebalanceOperation) {
	var wg sync.WaitGroup
	sem := make(chan struct{}, r.config.MaxConcurrentMoves)

	for _, move := range moves {
		wg.Add(1)
		go func(op RebalanceOperation) {
			defer wg.Done()

			select {
			case <-ctx.Done():
				return
			case <-r.stopCh:
				return
			case sem <- struct{}{}:
				// Execute the move
				r.executeMove(ctx, &op)
				<-sem
			}
		}(move)
	}

	wg.Wait()
}

// executeMove executes a single move operation
func (r *Rebalancer) executeMove(ctx context.Context, op *RebalanceOperation) {
	op.Status = RebalanceRunning
	op.StartTime = time.Now()

	r.mu.Lock()
	r.ops[op.ID] = op
	r.mu.Unlock()

	atomic.AddInt32(&r.activeOps, 1)
	defer atomic.AddInt32(&r.activeOps, -1)

	r.logger.Info("Executing rebalance move",
		zap.String("op_id", op.ID),
		zap.String("from", op.SourceNode),
		zap.String("to", op.TargetNode))

	// Simulate shard move (in production, this would transfer data)
	time.Sleep(100 * time.Millisecond)

	// Update ring
	if op.Status == RebalanceRunning {
		op.Status = RebalanceComplete
		now := time.Now()
		op.CompleteTime = &now

		// Update hash ring
		r.logger.Debug("Updating hash ring after rebalance",
			zap.String("op_id", op.ID))
	}

	r.logger.Info("Rebalance move completed",
		zap.String("op_id", op.ID))
}

// TriggerManualRebalance triggers a manual rebalance
func (r *Rebalancer) TriggerManualRebalance(ctx context.Context) error {
	r.logger.Info("Manual rebalance triggered")
	r.checkAndRebalance(ctx)
	return nil
}

// GetStatus returns rebalancer status
func (r *Rebalancer) GetStatus() RebalancerStatus {
	distribution := r.ring.GetNodeDistribution()

	r.mu.RLock()
	defer r.mu.RUnlock()

	pending := 0
	running := 0
	for _, op := range r.ops {
		switch op.Status {
		case RebalancePending:
			pending++
		case RebalanceRunning:
			running++
		}
	}

	return RebalancerStatus{
		Paused:         r.paused.Load(),
		ActiveOps:      int(atomic.LoadInt32(&r.activeOps)),
		PendingOps:     pending,
		Distribution:   distribution,
	}
}

// RebalancerStatus contains rebalancer status information
type RebalancerStatus struct {
	Paused         bool
	ActiveOps      int
	PendingOps     int
	Distribution   map[string]int
}

// GetOperation returns a rebalance operation
func (r *Rebalancer) GetOperation(opID string) (*RebalanceOperation, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	op, ok := r.ops[opID]
	return op, ok
}

// GetOperations returns all rebalance operations
func (r *Rebalancer) GetOperations() []*RebalanceOperation {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]*RebalanceOperation, 0, len(r.ops))
	for _, op := range r.ops {
		result = append(result, op)
	}
	return result
}

// CancelOperation cancels a rebalance operation
func (r *Rebalancer) CancelOperation(opID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	op, ok := r.ops[opID]
	if !ok {
		return fmt.Errorf("operation not found: %s", opID)
	}

	if op.Status == RebalanceRunning {
		op.Status = RebalanceCancelled
		return nil
	}

	return fmt.Errorf("cannot cancel operation in status: %s", op.Status)
}

// abs returns absolute value of int
func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}
