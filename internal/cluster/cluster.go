package cluster

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
)

// Cluster represents the main cluster manager
type Cluster struct {
	config       ClusterConfig
	logger       *zap.Logger
	manager      *Manager
	ring         *HashRing
	replicator  *Replicator
	erasurer    *ErasureCoder
	rebalancer  *Rebalancer
	backupMgr   *BackupManager
	mu          sync.RWMutex
	initialized bool
	startTime   time.Time
}

// ClusterOption is a function that modifies cluster config
type ClusterOption func(*ClusterConfig)

// WithNodeID sets the node ID
func WithNodeID(id string) ClusterOption {
	return func(c *ClusterConfig) {
		c.NodeID = id
	}
}

// WithNodeName sets the node name
func WithNodeName(name string) ClusterOption {
	return func(c *ClusterConfig) {
		c.NodeName = name
	}
}

// WithBindAddress sets the bind address
func WithBindAddress(addr string) ClusterOption {
	return func(c *ClusterConfig) {
		c.BindAddr = addr
	}
}

// WithBindPort sets the bind port
func WithBindPort(port int) ClusterOption {
	return func(c *ClusterConfig) {
		c.BindPort = port
	}
}

// WithSeedNodes sets the seed nodes
func WithSeedNodes(nodes []string) ClusterOption {
	return func(c *ClusterConfig) {
		c.SeedNodes = nodes
	}
}

// WithMetadata sets node metadata
func WithMetadata(meta NodeMetadata) ClusterOption {
	return func(c *ClusterConfig) {
		c.Metadata = meta
	}
}

// NewCluster creates a new cluster instance
func NewCluster(logger *zap.Logger, opts ...ClusterOption) *Cluster {
	cfg := ClusterConfig{
		NodeID:          GenerateNodeID(),
		NodeName:        fmt.Sprintf("node-%s", time.Now().Format("20060102")),
		BindAddr:        GetOutboundIP(),
		BindPort:        9001,
		ProtocolVersion: 2,
		SeedNodes:       []string{},
		Metadata: NodeMetadata{
			Region:  "default",
			Zone:    "default",
			DiskType: "SSD",
		},
	}

	for _, opt := range opts {
		opt(&cfg)
	}

	return &Cluster{
		config: cfg,
		logger: logger,
		startTime: time.Now(),
	}
}

// Initialize initializes the cluster
func (c *Cluster) Initialize(ctx context.Context, rf ReplicationFactor) error {
	c.logger.Info("Initializing cluster",
		zap.String("node_id", c.config.NodeID),
		zap.String("address", c.config.BindAddr),
		zap.Int("port", c.config.BindPort))

	// Create manager
	c.manager = NewManager(c.config, c.logger)

	// Start manager
	if err := c.manager.Start(ctx); err != nil {
		return fmt.Errorf("failed to start cluster manager: %w", err)
	}

	// Create hash ring
	c.ring = NewHashRing()

	// Add local node to ring
	localNode := c.manager.GetLocalNode()
	c.ring.AddNode(localNode)

	// Create replicator
	c.replicator = NewReplicator(c.manager, c.ring, rf, c.logger)

	// Create erasure coder
	erasureCfg := DefaultErasureConfig()
	erasurer, err := NewErasureCoder(erasureCfg, c.logger)
	if err != nil {
		return fmt.Errorf("failed to create erasure coder: %w", err)
	}
	c.erasurer = erasurer

	// Create rebalancer
	rebalanceCfg := DefaultRebalanceConfig()
	c.rebalancer = NewRebalancer(rebalanceCfg, c.manager, c.ring, c.logger)
	c.rebalancer.Start(ctx)

	// Create backup manager
	c.backupMgr = NewBackupManager(c.logger)

	c.initialized = true

	c.logger.Info("Cluster initialized successfully")
	return nil
}

// Start starts the cluster
func (c *Cluster) Start(ctx context.Context) error {
	if !c.initialized {
		return fmt.Errorf("cluster not initialized")
	}

	c.logger.Info("Starting cluster services")
	return nil
}

// Stop stops the cluster
func (c *Cluster) Stop() error {
	c.logger.Info("Stopping cluster")

	if c.rebalancer != nil {
		c.rebalancer.Stop()
	}

	if c.manager != nil {
		c.manager.Stop()
	}

	c.logger.Info("Cluster stopped")
	return nil
}

// GetManager returns the cluster manager
func (c *Cluster) GetManager() *Manager {
	return c.manager
}

// GetHashRing returns the hash ring
func (c *Cluster) GetHashRing() *HashRing {
	return c.ring
}

// GetReplicator returns the replicator
func (c *Cluster) GetReplicator() *Replicator {
	return c.replicator
}

// GetErasureCoder returns the erasure coder
func (c *Cluster) GetErasureCoder() *ErasureCoder {
	return c.erasurer
}

// GetRebalancer returns the rebalancer
func (c *Cluster) GetRebalancer() *Rebalancer {
	return c.rebalancer
}

// GetBackupManager returns the backup manager
func (c *Cluster) GetBackupManager() *BackupManager {
	return c.backupMgr
}

// AddNode adds a node to the cluster
func (c *Cluster) AddNode(node *Node) {
	c.ring.AddNode(node)
	c.logger.Info("Node added to cluster",
		zap.String("node_id", node.ID),
		zap.String("name", node.Name))
}

// RemoveNode removes a node from the cluster
func (c *Cluster) RemoveNode(nodeID string) {
	c.ring.RemoveNode(nodeID)
	c.logger.Info("Node removed from cluster",
		zap.String("node_id", nodeID))
}

// GetNodes returns all nodes in the cluster
func (c *Cluster) GetNodes() []*Node {
	return c.manager.Members()
}

// GetNodeCount returns the number of nodes
func (c *Cluster) GetNodeCount() int {
	return c.manager.NodeCount()
}

// IsReady returns whether the cluster is ready
func (c *Cluster) IsReady() bool {
	return c.initialized && c.manager.IsReady()
}

// GetClusterInfo returns cluster information
func (c *Cluster) GetClusterInfo() ClusterInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return ClusterInfo{
		NodeID:        c.config.NodeID,
		NodeName:      c.config.NodeName,
		ClusterSize:   c.manager.NodeCount(),
		IsLeader:      c.manager.IsLeader(),
		Uptime:        time.Since(c.startTime).String(),
		ReplicationFactor: int(c.replicator.GetReplicationFactor()),
	}
}

// GetRingDistribution returns the hash ring distribution
func (c *Cluster) GetRingDistribution() map[string]int {
	return c.ring.GetNodeDistribution()
}

// ClusterInfo contains cluster information
type ClusterInfo struct {
	NodeID             string `json:"node_id"`
	NodeName           string `json:"node_name"`
	ClusterSize        int    `json:"cluster_size"`
	IsLeader           bool   `json:"is_leader"`
	Uptime             string `json:"uptime"`
	ReplicationFactor  int    `json:"replication_factor"`
}
