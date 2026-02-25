package cluster

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
)

// ReplicationFactor is the number of replicas for each object
type ReplicationFactor int

const (
	RF1 ReplicationFactor = 1
	RF2 ReplicationFactor = 2
	RF3 ReplicationFactor = 3
	RF4 ReplicationFactor = 4
	RF5 ReplicationFactor = 5
)

// WriteQuorum returns the minimum number of successful writes needed
func (r ReplicationFactor) WriteQuorum() int {
	return int(r)/2 + 1
}

// ReadQuorum returns the minimum number of successful reads needed
func (r ReplicationFactor) ReadQuorum() int {
	return int(r)/2 + 1
}

// Replica represents a replica of data
type Replica struct {
	NodeID    string    `json:"node_id"`
	CreatedAt time.Time `json:"created_at"`
	Version   int64     `json:"version"`
	Status    string    `json:"status"`
}

// ReplicationStatus represents the status of a replication operation
type ReplicationStatus string

const (
	ReplicationPending    ReplicationStatus = "pending"
	ReplicationInProgress ReplicationStatus = "in_progress"
	ReplicationComplete   ReplicationStatus = "complete"
	ReplicationFailed     ReplicationStatus = "failed"
)

// Replicator handles data replication across nodes
type Replicator struct {
	manager           *Manager
	ring              *HashRing
	replicationFactor ReplicationFactor
	logger            *zap.Logger
	mu                sync.RWMutex
	pendingOps        map[string]*ReplicationOp
	completedOps      map[string]*ReplicationOp

	testWriteErr bool
}

// ReplicationOp represents a replication operation
type ReplicationOp struct {
	ID           string            `json:"id"`
	ObjectKey    string            `json:"object_key"`
	Bucket       string            `json:"bucket"`
	Operation    string            `json:"operation"` // write, delete
	SourceNode   string            `json:"source_node"`
	TargetNodes  []string          `json:"target_nodes"`
	Status       ReplicationStatus `json:"status"`
	Replicas     []Replica         `json:"replicas"`
	StartTime    time.Time         `json:"start_time"`
	CompleteTime *time.Time        `json:"complete_time,omitempty"`
	Error        string            `json:"error,omitempty"`
}

// NewReplicator creates a new replicator
func NewReplicator(manager *Manager, ring *HashRing, rf ReplicationFactor, logger *zap.Logger) *Replicator {
	return &Replicator{
		manager:           manager,
		ring:              ring,
		replicationFactor: rf,
		logger:            logger,
		pendingOps:        make(map[string]*ReplicationOp),
		completedOps:      make(map[string]*ReplicationOp),
	}
}

// GetTargetNodes returns the target nodes for an object key
func (r *Replicator) GetTargetNodes(key string) []string {
	return r.ring.GetNNodes(key, int(r.replicationFactor))
}

// ReplicateWrite replicates a write operation
func (r *Replicator) ReplicateWrite(ctx context.Context, op *ReplicationOp) error {
	op.Status = ReplicationInProgress
	op.StartTime = time.Now()

	targetNodes := r.GetTargetNodes(op.ObjectKey)
	op.TargetNodes = targetNodes

	r.mu.Lock()
	r.pendingOps[op.ID] = op
	r.mu.Unlock()

	// Perform write to all target nodes
	var wg sync.WaitGroup
	var mu sync.Mutex
	successCount := 0
	errors := make(map[string]error)

	for _, nodeID := range targetNodes {
		wg.Add(1)
		go func(nid string) {
			defer wg.Done()

			err := r.writeToNode(ctx, op, nid)
			mu.Lock()
			if err != nil {
				errors[nid] = err
			} else {
				successCount++
				op.Replicas = append(op.Replicas, Replica{
					NodeID:    nid,
					CreatedAt: time.Now(),
					Version:   1,
					Status:    "complete",
				})
			}
			mu.Unlock()
		}(nodeID)
	}

	wg.Wait()

	// Check write quorum
	if successCount < r.replicationFactor.WriteQuorum() {
		op.Status = ReplicationFailed
		op.Error = fmt.Sprintf("write quorum not met: %d/%d", successCount, r.replicationFactor.WriteQuorum())
		r.logger.Error("Replication write failed",
			zap.String("op_id", op.ID),
			zap.Int("success", successCount),
			zap.Int("quorum", r.replicationFactor.WriteQuorum()))

		// Rollback on failure
		r.rollbackWrite(ctx, op)

		return fmt.Errorf(op.Error)
	}

	now := time.Now()
	op.CompleteTime = &now
	op.Status = ReplicationComplete

	r.mu.Lock()
	delete(r.pendingOps, op.ID)
	r.completedOps[op.ID] = op
	r.mu.Unlock()

	r.logger.Info("Replication write completed",
		zap.String("op_id", op.ID),
		zap.Int("success", successCount),
		zap.Strings("nodes", targetNodes))

	return nil
}

// writeToNode writes data to a specific node
func (r *Replicator) writeToNode(ctx context.Context, op *ReplicationOp, nodeID string) error {
	if r.testWriteErr {
		return fmt.Errorf("test write error")
	}

	r.logger.Debug("Writing to node",
		zap.String("op_id", op.ID),
		zap.String("node_id", nodeID),
		zap.String("key", op.ObjectKey))

	time.Sleep(10 * time.Millisecond)

	return nil
}

// ReplicateDelete replicates a delete operation
func (r *Replicator) ReplicateDelete(ctx context.Context, op *ReplicationOp) error {
	op.Status = ReplicationInProgress
	op.StartTime = time.Now()

	targetNodes := r.GetTargetNodes(op.ObjectKey)
	op.TargetNodes = targetNodes

	r.mu.Lock()
	r.pendingOps[op.ID] = op
	r.mu.Unlock()

	// Perform delete on all target nodes
	var wg sync.WaitGroup
	var mu sync.Mutex
	successCount := 0

	for _, nodeID := range targetNodes {
		wg.Add(1)
		go func(nid string) {
			defer wg.Done()

			err := r.deleteFromNode(ctx, op, nid)
			mu.Lock()
			if err == nil {
				successCount++
			}
			mu.Unlock()
		}(nodeID)
	}

	wg.Wait()

	// Delete doesn't require quorum (idempotent)
	op.Status = ReplicationComplete
	now := time.Now()
	op.CompleteTime = &now

	r.mu.Lock()
	delete(r.pendingOps, op.ID)
	r.completedOps[op.ID] = op
	r.mu.Unlock()

	r.logger.Info("Replication delete completed",
		zap.String("op_id", op.ID),
		zap.Int("success", successCount))

	return nil
}

// deleteFromNode deletes data from a specific node
func (r *Replicator) deleteFromNode(ctx context.Context, op *ReplicationOp, nodeID string) error {
	r.logger.Debug("Deleting from node",
		zap.String("op_id", op.ID),
		zap.String("node_id", nodeID),
		zap.String("key", op.ObjectKey))

	// Simulate network delay
	time.Sleep(10 * time.Millisecond)

	return nil
}

// rollbackWrite rolls back a failed write operation
func (r *Replicator) rollbackWrite(ctx context.Context, op *ReplicationOp) {
	r.logger.Warn("Rolling back replication write",
		zap.String("op_id", op.ID))

	var wg sync.WaitGroup
	for _, replica := range op.Replicas {
		wg.Add(1)
		go func(nodeID string) {
			defer wg.Done()
			r.deleteFromNode(ctx, op, nodeID)
		}(replica.NodeID)
	}
	wg.Wait()
}

// GetReplicatedData reads data from quorum of nodes
func (r *Replicator) GetReplicatedData(ctx context.Context, key string) ([]byte, error) {
	targetNodes := r.GetTargetNodes(key)
	quorum := r.replicationFactor.ReadQuorum()

	type readResult struct {
		nodeID string
		data   []byte
		err    error
	}

	results := make(chan readResult, len(targetNodes))
	var wg sync.WaitGroup

	for _, nodeID := range targetNodes {
		wg.Add(1)
		go func(nid string) {
			defer wg.Done()

			data, err := r.readFromNode(ctx, key, nid)
			results <- readResult{
				nodeID: nid,
				data:   data,
				err:    err,
			}
		}(nodeID)
	}

	wg.Wait()
	close(results)

	// Collect successful reads
	successData := make([][]byte, 0, quorum)
	for result := range results {
		if result.err == nil && result.data != nil {
			successData = append(successData, result.data)
		}
	}

	if len(successData) < quorum {
		return nil, fmt.Errorf("read quorum not met: %d/%d", len(successData), quorum)
	}

	// Simple merge: return first data (in production, use versioning/CRDT)
	return successData[0], nil
}

// readFromNode reads data from a specific node
func (r *Replicator) readFromNode(ctx context.Context, key, nodeID string) ([]byte, error) {
	r.logger.Debug("Reading from node",
		zap.String("key", key),
		zap.String("node_id", nodeID))

	// Simulate network delay
	time.Sleep(10 * time.Millisecond)

	return []byte("simulated-data-" + key), nil
}

// GetOperation returns a replication operation
func (r *Replicator) GetOperation(opID string) (*ReplicationOp, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if op, ok := r.pendingOps[opID]; ok {
		return op, true
	}
	if op, ok := r.completedOps[opID]; ok {
		return op, true
	}
	return nil, false
}

// GetPendingOperations returns all pending operations
func (r *Replicator) GetPendingOperations() []*ReplicationOp {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]*ReplicationOp, 0, len(r.pendingOps))
	for _, op := range r.pendingOps {
		result = append(result, op)
	}
	return result
}

// SetReplicationFactor changes the replication factor
func (r *Replicator) SetReplicationFactor(rf ReplicationFactor) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.replicationFactor = rf
	r.logger.Info("Replication factor updated", zap.Int("rf", int(rf)))
}

// GetReplicationFactor returns the current replication factor
func (r *Replicator) GetReplicationFactor() ReplicationFactor {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.replicationFactor
}
