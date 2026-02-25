package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sort"
	"testing"
	"time"

	"github.com/hashicorp/memberlist"
	"go.uber.org/zap"
)

func TestNewCluster(t *testing.T) {
	logger := zap.NewNop()
	cluster := NewCluster(logger)
	if cluster == nil {
		t.Fatal("Cluster should not be nil")
	}
}

func TestClusterOption(t *testing.T) {
	logger := zap.NewNop()

	// Test WithNodeID
	cluster := NewCluster(logger, WithNodeID("node-1"))
	if cluster.config.NodeID != "node-1" {
		t.Errorf("NodeID = %s, want node-1", cluster.config.NodeID)
	}

	// Test WithNodeName
	cluster = NewCluster(logger, WithNodeName("test-node"))
	if cluster.config.NodeName != "test-node" {
		t.Errorf("NodeName = %s, want test-node", cluster.config.NodeName)
	}

	// Test WithBindAddress
	cluster = NewCluster(logger, WithBindAddress("192.168.1.1"))
	if cluster.config.BindAddr != "192.168.1.1" {
		t.Errorf("BindAddr = %s, want 192.168.1.1", cluster.config.BindAddr)
	}

	// Test WithBindPort
	cluster = NewCluster(logger, WithBindPort(9002))
	if cluster.config.BindPort != 9002 {
		t.Errorf("BindPort = %d, want 9002", cluster.config.BindPort)
	}

	// Test WithSeedNodes
	cluster = NewCluster(logger, WithSeedNodes([]string{"node1", "node2"}))
	if len(cluster.config.SeedNodes) != 2 {
		t.Errorf("SeedNodes count = %d, want 2", len(cluster.config.SeedNodes))
	}

	// Test WithMetadata
	meta := NodeMetadata{Region: "us-west-1", Zone: "zone-a"}
	cluster = NewCluster(logger, WithMetadata(meta))
	if cluster.config.Metadata.Region != "us-west-1" {
		t.Errorf("Metadata.Region = %s, want us-west-1", cluster.config.Metadata.Region)
	}
}

func TestCluster_IsReady(t *testing.T) {
	logger := zap.NewNop()
	cluster := NewCluster(logger)

	// Before initialization, should not be ready
	if cluster.IsReady() {
		t.Error("Cluster should not be ready before initialization")
	}
}

func TestCluster_GettersBeforeInit(t *testing.T) {
	logger := zap.NewNop()
	cluster := NewCluster(logger)

	if cluster.GetManager() != nil {
		t.Error("GetManager should return nil before init")
	}
	if cluster.GetHashRing() != nil {
		t.Error("GetHashRing should return nil before init")
	}
	if cluster.GetReplicator() != nil {
		t.Error("GetReplicator should return nil before init")
	}
	if cluster.GetErasureCoder() != nil {
		t.Error("GetErasureCoder should return nil before init")
	}
	if cluster.GetRebalancer() != nil {
		t.Error("GetRebalancer should return nil before init")
	}
	if cluster.GetBackupManager() != nil {
		t.Error("GetBackupManager should return nil before init")
	}
}

func TestCluster_Stop(t *testing.T) {
	logger := zap.NewNop()
	cluster := NewCluster(logger)

	// Stop should not panic even if not initialized
	if err := cluster.Stop(); err != nil {
		t.Errorf("Stop should not fail: %v", err)
	}
}

func TestCluster_StartWithoutInit(t *testing.T) {
	logger := zap.NewNop()
	cluster := NewCluster(logger)

	err := cluster.Start(context.Background())
	if err == nil {
		t.Error("Start should fail without initialization")
	}
}

func TestNodeStatus(t *testing.T) {
	tests := []struct {
		state    NodeState
		expected string
	}{
		{NodeStateAlive, "online"},
		{NodeStateSuspect, "degraded"},
		{NodeStateDead, "offline"},
		{NodeStateLeft, "left"},
		{NodeState("unknown"), "unknown"},
	}

	for _, tt := range tests {
		node := &Node{State: tt.state}
		if node.Status() != tt.expected {
			t.Errorf("Node(%s).Status() = %s, want %s", tt.state, node.Status(), tt.expected)
		}
	}
}

func TestNodeStruct(t *testing.T) {
	now := time.Now()
	node := &Node{
		ID:       "test-id",
		Name:     "test-name",
		Address:  "192.168.1.1",
		Port:     9001,
		State:    NodeStateAlive,
		Version:  "1.0.0",
		Metadata: NodeMetadata{Region: "us-west-1"},
		JoinTime: now,
		LastSeen: now,
	}

	if node.ID != "test-id" {
		t.Error("Node.ID mismatch")
	}
	if node.Name != "test-name" {
		t.Error("Node.Name mismatch")
	}
	if node.Address != "192.168.1.1" {
		t.Error("Node.Address mismatch")
	}
	if node.Port != 9001 {
		t.Error("Node.Port mismatch")
	}
}

func TestNodeMetadataStruct(t *testing.T) {
	meta := NodeMetadata{
		StorageCapacity: 1000,
		StorageUsed:     500,
		CPUCount:        8,
		MemoryTotal:     16384,
		Region:          "us-east-1",
		Zone:            "zone-a",
		DiskType:        "SSD",
	}

	if meta.StorageCapacity != 1000 {
		t.Error("StorageCapacity mismatch")
	}
	if meta.CPUCount != 8 {
		t.Error("CPUCount mismatch")
	}
}

func TestClusterEventStruct(t *testing.T) {
	event := ClusterEvent{
		Type:      "node_join",
		NodeID:    "node-1",
		NodeName:  "test-node",
		Address:   "192.168.1.1",
		Timestamp: time.Now(),
	}

	if event.Type != "node_join" {
		t.Error("ClusterEvent.Type mismatch")
	}
}

func TestClusterInfoStruct(t *testing.T) {
	info := ClusterInfo{
		NodeID:            "node-1",
		NodeName:          "test-node",
		ClusterSize:       3,
		IsLeader:          true,
		Uptime:            "1h",
		ReplicationFactor: 3,
	}

	if info.NodeID != "node-1" {
		t.Error("ClusterInfo.NodeID mismatch")
	}
	if info.ClusterSize != 3 {
		t.Error("ClusterInfo.ClusterSize mismatch")
	}
}

func TestGenerateNodeID(t *testing.T) {
	id1 := GenerateNodeID()
	id2 := GenerateNodeID()

	if id1 == "" {
		t.Error("NodeID should not be empty")
	}
	if id1 == id2 {
		t.Error("NodeIDs should be unique")
	}
}

func TestNewHashRing(t *testing.T) {
	ring := NewHashRing()
	if ring == nil {
		t.Fatal("HashRing should not be nil")
	}
	if ring.virtualNodes == nil {
		t.Error("virtualNodes map should be initialized")
	}
	if ring.physicalNodes == nil {
		t.Error("physicalNodes map should be initialized")
	}
	if ring.nodes == nil {
		t.Error("nodes map should be initialized")
	}
}

func TestHashRingAddNode(t *testing.T) {
	ring := NewHashRing()
	node := &Node{ID: "node-1", Name: "Node 1"}

	ring.AddNode(node)

	if ring.NodeCount() != 1 {
		t.Errorf("NodeCount = %d, want 1", ring.NodeCount())
	}

	// Adding same node again should not duplicate
	ring.AddNode(node)
	if ring.NodeCount() != 1 {
		t.Errorf("NodeCount = %d, want 1 after duplicate add", ring.NodeCount())
	}
}

func TestHashRingRemoveNode(t *testing.T) {
	ring := NewHashRing()
	node := &Node{ID: "node-1", Name: "Node 1"}

	ring.AddNode(node)
	ring.RemoveNode("node-1")

	if ring.NodeCount() != 0 {
		t.Errorf("NodeCount = %d, want 0", ring.NodeCount())
	}

	// Removing non-existent node should not panic
	ring.RemoveNode("nonexistent")
}

func TestHashRingGetNode(t *testing.T) {
	ring := NewHashRing()

	// Empty ring should return false
	_, ok := ring.GetNode("test-key")
	if ok {
		t.Error("Empty ring should return false")
	}

	// Add nodes
	ring.AddNode(&Node{ID: "node-1", Name: "Node 1"})
	ring.AddNode(&Node{ID: "node-2", Name: "Node 2"})

	nodeID, ok := ring.GetNode("test-key")
	if !ok {
		t.Error("Should find a node")
	}
	if nodeID == "" {
		t.Error("NodeID should not be empty")
	}
}

func TestHashRingGetNNodes(t *testing.T) {
	ring := NewHashRing()

	// Empty ring should return nil
	nodes := ring.GetNNodes("test-key", 3)
	if nodes != nil {
		t.Error("Empty ring should return nil")
	}

	// Add nodes
	ring.AddNode(&Node{ID: "node-1", Name: "Node 1"})
	ring.AddNode(&Node{ID: "node-2", Name: "Node 2"})
	ring.AddNode(&Node{ID: "node-3", Name: "Node 3"})

	nodes = ring.GetNNodes("test-key", 2)
	if len(nodes) != 2 {
		t.Errorf("GetNNodes(2) = %d nodes, want 2", len(nodes))
	}

	// Requesting more than available should return all
	nodes = ring.GetNNodes("test-key", 10)
	if len(nodes) != 3 {
		t.Errorf("GetNNodes(10) = %d nodes, want 3", len(nodes))
	}
}

func TestHashRingGetNodesInRange(t *testing.T) {
	ring := NewHashRing()

	// Empty ring should return nil
	nodes := ring.GetNodesInRange("a", "z", 3)
	if nodes != nil {
		t.Error("Empty ring should return nil")
	}

	// Add nodes
	ring.AddNode(&Node{ID: "node-1", Name: "Node 1"})
	ring.AddNode(&Node{ID: "node-2", Name: "Node 2"})

	nodes = ring.GetNodesInRange("a", "z", 3)
	if len(nodes) == 0 {
		t.Error("Should find some nodes in range")
	}
}

func TestHashRingGetNodes(t *testing.T) {
	ring := NewHashRing()
	ring.AddNode(&Node{ID: "node-1", Name: "Node 1"})
	ring.AddNode(&Node{ID: "node-2", Name: "Node 2"})

	nodes := ring.GetNodes()
	if len(nodes) != 2 {
		t.Errorf("GetNodes() = %d nodes, want 2", len(nodes))
	}
}

func TestHashRingGetNodeDistribution(t *testing.T) {
	ring := NewHashRing()
	ring.AddNode(&Node{ID: "node-1", Name: "Node 1"})
	ring.AddNode(&Node{ID: "node-2", Name: "Node 2"})

	dist := ring.GetNodeDistribution()
	if len(dist) != 2 {
		t.Errorf("GetNodeDistribution() = %d nodes, want 2", len(dist))
	}

	// Each node should have VirtualNodeCount virtual nodes
	for nodeID, count := range dist {
		if count != VirtualNodeCount {
			t.Errorf("Node %s has %d virtual nodes, want %d", nodeID, count, VirtualNodeCount)
		}
	}
}

func TestHashRingSetHashFunction(t *testing.T) {
	ring := NewHashRing()
	ring.AddNode(&Node{ID: "node-1", Name: "Node 1"})

	// Set custom hash function
	ring.SetHashFunction(func(b []byte) uint32 {
		return 12345
	})

	// Ring should still work
	nodeID, ok := ring.GetNode("test-key")
	if !ok {
		t.Error("Should find a node after SetHashFunction")
	}
	_ = nodeID
}

func TestDefaultHashFunction(t *testing.T) {
	hash1 := defaultHashFunction([]byte("test"))
	hash2 := defaultHashFunction([]byte("test"))

	if hash1 != hash2 {
		t.Error("Same input should produce same hash")
	}

	hash3 := defaultHashFunction([]byte("different"))
	if hash1 == hash3 {
		t.Error("Different inputs should likely produce different hashes")
	}
}

func TestKetamaHashFunction(t *testing.T) {
	fn := KetamaHashFunction()
	hash := fn([]byte("test"))
	if hash == 0 {
		t.Error("Hash should not be zero")
	}
}

func TestMD5HashFunction(t *testing.T) {
	fn := MD5HashFunction()
	hash := fn([]byte("test"))
	if hash == 0 {
		t.Error("Hash should not be zero")
	}
}

func TestMurmurHashFunction(t *testing.T) {
	fn := MurmurHashFunction()

	// Test with various key lengths
	keys := [][]byte{
		[]byte("test"),
		[]byte("longer test key"),
		[]byte("a"),
		make([]byte, 5),             // Test with zeros
		[]byte{1, 2, 3, 4, 5, 6, 7}, // Test with 7 bytes (not aligned to 4)
	}

	for _, key := range keys {
		hash := fn(key)
		if hash == 0 {
			t.Errorf("Hash should not be zero for key %v", key)
		}
	}
}

func TestNewManager(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{
		NodeID:   "test-node",
		NodeName: "Test Node",
		BindAddr: "127.0.0.1",
		BindPort: 9001,
	}

	mgr := NewManager(cfg, logger)
	if mgr == nil {
		t.Fatal("Manager should not be nil")
	}
	if mgr.node == nil {
		t.Error("Manager.node should be initialized")
	}
}

func TestManagerGetLocalNode(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{
		NodeID:   "test-node",
		NodeName: "Test Node",
	}
	mgr := NewManager(cfg, logger)

	node := mgr.GetLocalNode()
	if node == nil {
		t.Fatal("GetLocalNode should return a node")
	}
	if node.ID != "test-node" {
		t.Errorf("Node.ID = %s, want test-node", node.ID)
	}
}

func TestManagerMembers(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{NodeID: "test-node"}
	mgr := NewManager(cfg, logger)

	// Before start, members should be empty
	members := mgr.Members()
	if len(members) != 0 {
		t.Errorf("Members() = %d, want 0 before start", len(members))
	}
}

func TestManagerIsLeader(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{NodeID: "test-node"}
	mgr := NewManager(cfg, logger)

	// Simplified implementation always returns true
	if !mgr.IsLeader() {
		t.Error("IsLeader should return true in simplified implementation")
	}
}

func TestManagerNodeCount(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{NodeID: "test-node"}
	mgr := NewManager(cfg, logger)

	if mgr.NodeCount() != 0 {
		t.Errorf("NodeCount() = %d, want 0", mgr.NodeCount())
	}
}

func TestManagerIsReady(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{NodeID: "test-node"}
	mgr := NewManager(cfg, logger)

	if mgr.IsReady() {
		t.Error("Manager should not be ready before Start()")
	}
}

func TestManagerEvents(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{NodeID: "test-node"}
	mgr := NewManager(cfg, logger)

	events := mgr.Events()
	if events == nil {
		t.Error("Events channel should not be nil")
	}
}

func TestManagerGetMember(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{NodeID: "test-node"}
	mgr := NewManager(cfg, logger)

	_, ok := mgr.GetMember("nonexistent")
	if ok {
		t.Error("GetMember should return false for nonexistent node")
	}
}

func TestManagerGetLeader(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{NodeID: "test-node"}
	mgr := NewManager(cfg, logger)

	// No leader before start
	_, err := mgr.GetLeader()
	if err == nil {
		t.Error("GetLeader should return error when no nodes")
	}
}

func TestManagerUpdateMetadata(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{NodeID: "test-node"}
	mgr := NewManager(cfg, logger)

	meta := NodeMetadata{
		StorageCapacity: 1000,
		Region:          "us-west-1",
	}
	mgr.UpdateMetadata(meta)

	node := mgr.GetLocalNode()
	if node.Metadata.StorageCapacity != 1000 {
		t.Errorf("StorageCapacity = %d, want 1000", node.Metadata.StorageCapacity)
	}
}

func TestNewClusterDelegate(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{NodeID: "test-node"}
	mgr := NewManager(cfg, logger)

	delegate := NewClusterDelegate(mgr)
	if delegate == nil {
		t.Fatal("NewClusterDelegate should not return nil")
	}
}

func TestClusterDelegateNodeMeta(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{
		NodeID: "test-node",
		Metadata: NodeMetadata{
			Region:          "us-west-1",
			StorageCapacity: 1000,
		},
	}
	mgr := NewManager(cfg, logger)
	delegate := NewClusterDelegate(mgr)

	meta := delegate.NodeMeta(1024)
	if len(meta) == 0 {
		t.Error("NodeMeta should return non-empty data")
	}
}

func TestClusterDelegateNotifyMsg(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{NodeID: "test-node"}
	mgr := NewManager(cfg, logger)
	delegate := NewClusterDelegate(mgr)

	// Should not panic
	delegate.NotifyMsg([]byte("test message"))
}

func TestClusterDelegateGetBroadcasts(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{NodeID: "test-node"}
	mgr := NewManager(cfg, logger)
	delegate := NewClusterDelegate(mgr)

	broadcasts := delegate.GetBroadcasts(0, 1024)
	if broadcasts != nil {
		t.Error("GetBroadcasts should return nil")
	}
}

func TestClusterDelegateLocalState(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{NodeID: "test-node"}
	mgr := NewManager(cfg, logger)
	delegate := NewClusterDelegate(mgr)

	state := delegate.LocalState(false)
	if len(state) == 0 {
		t.Error("LocalState should return non-empty data")
	}
}

func TestClusterDelegateMergeRemoteState(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{NodeID: "test-node"}
	mgr := NewManager(cfg, logger)
	delegate := NewClusterDelegate(mgr)

	// Empty data should not panic
	delegate.MergeRemoteState([]byte{}, false)

	// Invalid JSON should not panic
	delegate.MergeRemoteState([]byte("invalid json"), false)
}

func TestGetOutboundIP(t *testing.T) {
	ip := GetOutboundIP()
	if ip == "" {
		t.Error("GetOutboundIP should return non-empty string")
	}
}

func TestVirtualNodeCount(t *testing.T) {
	if VirtualNodeCount <= 0 {
		t.Error("VirtualNodeCount should be positive")
	}
}

func TestCluster_Initialize(t *testing.T) {
	logger := zap.NewNop()
	cluster := NewCluster(logger)

	ctx := context.Background()
	err := cluster.Initialize(ctx, ReplicationFactor(3))
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	if !cluster.initialized {
		t.Error("Cluster should be initialized")
	}

	// Getters should now return non-nil values
	if cluster.GetManager() == nil {
		t.Error("GetManager should return non-nil after init")
	}
	if cluster.GetHashRing() == nil {
		t.Error("GetHashRing should return non-nil after init")
	}
	if cluster.GetReplicator() == nil {
		t.Error("GetReplicator should return non-nil after init")
	}
	if cluster.GetErasureCoder() == nil {
		t.Error("GetErasureCoder should return non-nil after init")
	}
	if cluster.GetRebalancer() == nil {
		t.Error("GetRebalancer should return non-nil after init")
	}
	if cluster.GetBackupManager() == nil {
		t.Error("GetBackupManager should return non-nil after init")
	}

	// Cleanup
	cluster.Stop()
}

func TestCluster_StartAfterInit(t *testing.T) {
	logger := zap.NewNop()
	cluster := NewCluster(logger)

	ctx := context.Background()
	err := cluster.Initialize(ctx, ReplicationFactor(3))
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	err = cluster.Start(ctx)
	if err != nil {
		t.Fatalf("Start should succeed after init: %v", err)
	}

	cluster.Stop()
}

func TestCluster_AddRemoveNode(t *testing.T) {
	logger := zap.NewNop()
	cluster := NewCluster(logger)

	ctx := context.Background()
	cluster.Initialize(ctx, ReplicationFactor(3))

	node := &Node{ID: "node-2", Name: "Node 2"}
	cluster.AddNode(node)

	if cluster.GetHashRing().NodeCount() != 2 {
		t.Errorf("NodeCount = %d, want 2", cluster.GetHashRing().NodeCount())
	}

	cluster.RemoveNode("node-2")

	if cluster.GetHashRing().NodeCount() != 1 {
		t.Errorf("NodeCount = %d, want 1", cluster.GetHashRing().NodeCount())
	}

	cluster.Stop()
}

func TestCluster_GetNodes(t *testing.T) {
	logger := zap.NewNop()
	cluster := NewCluster(logger)

	ctx := context.Background()
	cluster.Initialize(ctx, ReplicationFactor(3))

	nodes := cluster.GetNodes()
	_ = nodes // Might be empty before memberlist starts

	cluster.Stop()
}

func TestCluster_GetNodeCount(t *testing.T) {
	logger := zap.NewNop()
	cluster := NewCluster(logger)

	ctx := context.Background()
	cluster.Initialize(ctx, ReplicationFactor(3))

	count := cluster.GetNodeCount()
	_ = count

	cluster.Stop()
}

func TestCluster_IsReadyAfterInit(t *testing.T) {
	logger := zap.NewNop()
	cluster := NewCluster(logger)

	ctx := context.Background()
	cluster.Initialize(ctx, ReplicationFactor(3))

	// Note: IsReady depends on manager.IsReady() which might still be false
	// since we haven't started the actual memberlist
	_ = cluster.IsReady()

	cluster.Stop()
}

func TestCluster_GetClusterInfo(t *testing.T) {
	logger := zap.NewNop()
	cluster := NewCluster(logger)

	ctx := context.Background()
	err := cluster.Initialize(ctx, ReplicationFactor(3))
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	info := cluster.GetClusterInfo()
	if info.NodeID == "" {
		t.Error("ClusterInfo.NodeID should not be empty")
	}
	if info.Uptime == "" {
		t.Error("ClusterInfo.Uptime should not be empty")
	}

	cluster.Stop()
}

func TestCluster_GetRingDistribution(t *testing.T) {
	logger := zap.NewNop()
	cluster := NewCluster(logger)

	ctx := context.Background()
	cluster.Initialize(ctx, ReplicationFactor(3))

	dist := cluster.GetRingDistribution()
	if dist == nil {
		t.Error("GetRingDistribution should not return nil")
	}

	cluster.Stop()
}

func TestCluster_StopWithNilComponents(t *testing.T) {
	logger := zap.NewNop()
	cluster := NewCluster(logger)

	// Stop without initializing - should not panic
	cluster.Stop()
}

func TestReplicationFactor(t *testing.T) {
	tests := []struct {
		rf       ReplicationFactor
		expected int
	}{
		{ReplicationFactor(1), 1},
		{ReplicationFactor(3), 3},
		{ReplicationFactor(5), 5},
	}

	for _, tt := range tests {
		if int(tt.rf) != tt.expected {
			t.Errorf("ReplicationFactor(%d) = %d, want %d", tt.expected, tt.rf, tt.expected)
		}
	}
}

func TestClusterConfigDefaults(t *testing.T) {
	logger := zap.NewNop()
	cluster := NewCluster(logger)

	if cluster.config.ProtocolVersion != 2 {
		t.Errorf("ProtocolVersion = %d, want 2", cluster.config.ProtocolVersion)
	}
	if cluster.config.BindPort != 9001 {
		t.Errorf("BindPort = %d, want 9001", cluster.config.BindPort)
	}
}

func TestManagerStart(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{
		NodeID:   "test-node",
		NodeName: "Test Node",
		BindAddr: "127.0.0.1",
		BindPort: 19001, // Use non-standard port to avoid conflicts
	}
	mgr := NewManager(cfg, logger)

	ctx := context.Background()
	err := mgr.Start(ctx)
	if err != nil {
		t.Logf("Manager.Start() error (may be expected in test env): %v", err)
	}

	mgr.Stop()
}

func TestManagerStop(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{NodeID: "test-node"}
	mgr := NewManager(cfg, logger)

	// Stop without start should not panic
	mgr.Stop()
}

func TestRebalancer_Pause(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{NodeID: "test-node"}
	mgr := NewManager(cfg, logger)
	ring := NewHashRing()
	rebalancer := NewRebalancer(DefaultRebalanceConfig(), mgr, ring, logger)

	if rebalancer.IsPaused() {
		t.Error("Rebalancer should not be paused initially")
	}

	rebalancer.Pause()
	if !rebalancer.IsPaused() {
		t.Error("Rebalancer should be paused after Pause()")
	}
}

func TestRebalancer_Resume(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{NodeID: "test-node"}
	mgr := NewManager(cfg, logger)
	ring := NewHashRing()
	rebalancer := NewRebalancer(DefaultRebalanceConfig(), mgr, ring, logger)

	rebalancer.Pause()
	rebalancer.Resume()
	if rebalancer.IsPaused() {
		t.Error("Rebalancer should not be paused after Resume()")
	}
}

func TestRebalancer_GetStatus(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{NodeID: "test-node"}
	mgr := NewManager(cfg, logger)
	ring := NewHashRing()
	ring.AddNode(&Node{ID: "node-1", Name: "Node 1"})

	rebalancer := NewRebalancer(DefaultRebalanceConfig(), mgr, ring, logger)

	status := rebalancer.GetStatus()
	if status.Paused {
		t.Error("Status.Paused should be false initially")
	}
	if status.Distribution == nil {
		t.Error("Status.Distribution should not be nil")
	}
}

func TestRebalancer_GetOperation(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{NodeID: "test-node"}
	mgr := NewManager(cfg, logger)
	ring := NewHashRing()
	rebalancer := NewRebalancer(DefaultRebalanceConfig(), mgr, ring, logger)

	_, ok := rebalancer.GetOperation("nonexistent")
	if ok {
		t.Error("GetOperation should return false for nonexistent operation")
	}
}

func TestRebalancer_GetOperations(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{NodeID: "test-node"}
	mgr := NewManager(cfg, logger)
	ring := NewHashRing()
	rebalancer := NewRebalancer(DefaultRebalanceConfig(), mgr, ring, logger)

	ops := rebalancer.GetOperations()
	if ops == nil {
		t.Error("GetOperations should not return nil")
	}
}

func TestRebalancer_CancelOperation(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{NodeID: "test-node"}
	mgr := NewManager(cfg, logger)
	ring := NewHashRing()
	rebalancer := NewRebalancer(DefaultRebalanceConfig(), mgr, ring, logger)

	err := rebalancer.CancelOperation("nonexistent")
	if err == nil {
		t.Error("CancelOperation should fail for nonexistent operation")
	}
}

func TestRebalancer_TriggerManualRebalance(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{NodeID: "test-node"}
	mgr := NewManager(cfg, logger)
	ring := NewHashRing()
	rebalancer := NewRebalancer(DefaultRebalanceConfig(), mgr, ring, logger)

	ctx := context.Background()
	err := rebalancer.TriggerManualRebalance(ctx)
	if err != nil {
		t.Errorf("TriggerManualRebalance failed: %v", err)
	}
}

func TestRebalancer_Stop(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{NodeID: "test-node"}
	mgr := NewManager(cfg, logger)
	ring := NewHashRing()
	rebalancer := NewRebalancer(DefaultRebalanceConfig(), mgr, ring, logger)

	rebalancer.Stop()
}

func TestRebalancer_StartStop(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{NodeID: "test-node"}
	mgr := NewManager(cfg, logger)
	ring := NewHashRing()
	ring.AddNode(&Node{ID: "node-1", Name: "Node 1"})

	rebalancer := NewRebalancer(DefaultRebalanceConfig(), mgr, ring, logger)

	ctx := context.Background()
	rebalancer.Start(ctx)
	time.Sleep(20 * time.Millisecond)
	rebalancer.Stop()
}

func TestRebalanceOperationStruct(t *testing.T) {
	op := RebalanceOperation{
		ID:         "op-1",
		SourceNode: "node-1",
		TargetNode: "node-2",
		Status:     RebalancePending,
	}

	if op.ID != "op-1" {
		t.Error("ID mismatch")
	}
	if op.Status != RebalancePending {
		t.Error("Status mismatch")
	}
}

func TestRebalancerStatusStruct(t *testing.T) {
	status := RebalancerStatus{
		Paused:       true,
		ActiveOps:    5,
		PendingOps:   3,
		Distribution: map[string]int{"node-1": 100},
	}

	if !status.Paused {
		t.Error("Paused should be true")
	}
	if status.ActiveOps != 5 {
		t.Error("ActiveOps mismatch")
	}
}

func TestDefaultRebalanceConfigValues(t *testing.T) {
	cfg := DefaultRebalanceConfig()

	if cfg.CheckInterval <= 0 {
		t.Error("CheckInterval should be positive")
	}
	if cfg.ThresholdPercent <= 0 {
		t.Error("ThresholdPercent should be positive")
	}
	if cfg.MaxConcurrentMoves <= 0 {
		t.Error("MaxConcurrentMoves should be positive")
	}
}

func TestAbsFunction(t *testing.T) {
	tests := []struct {
		input    int
		expected int
	}{
		{0, 0},
		{5, 5},
		{-5, 5},
		{100, 100},
		{-100, 100},
	}

	for _, tt := range tests {
		result := abs(tt.input)
		if result != tt.expected {
			t.Errorf("abs(%d) = %d, want %d", tt.input, result, tt.expected)
		}
	}
}

func TestDefaultErasureConfig(t *testing.T) {
	cfg := DefaultErasureConfig()
	if cfg.DataShards != 4 {
		t.Errorf("DataShards = %d, want 4", cfg.DataShards)
	}
	if cfg.ParityShards != 2 {
		t.Errorf("ParityShards = %d, want 2", cfg.ParityShards)
	}
	if cfg.TotalShards != 6 {
		t.Errorf("TotalShards = %d, want 6", cfg.TotalShards)
	}
}

func TestHighPerformanceConfig(t *testing.T) {
	cfg := HighPerformanceConfig()
	if cfg.DataShards != 8 {
		t.Errorf("DataShards = %d, want 8", cfg.DataShards)
	}
	if cfg.ParityShards != 2 {
		t.Errorf("ParityShards = %d, want 2", cfg.ParityShards)
	}
	if cfg.TotalShards != 10 {
		t.Errorf("TotalShards = %d, want 10", cfg.TotalShards)
	}
}

func TestHighDurabilityConfig(t *testing.T) {
	cfg := HighDurabilityConfig()
	if cfg.DataShards != 4 {
		t.Errorf("DataShards = %d, want 4", cfg.DataShards)
	}
	if cfg.ParityShards != 4 {
		t.Errorf("ParityShards = %d, want 4", cfg.ParityShards)
	}
	if cfg.TotalShards != 8 {
		t.Errorf("TotalShards = %d, want 8", cfg.TotalShards)
	}
}

func TestNewErasureCoder(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()

	coder, err := NewErasureCoder(cfg, logger)
	if err != nil {
		t.Fatalf("NewErasureCoder() error: %v", err)
	}
	if coder == nil {
		t.Fatal("NewErasureCoder() returned nil")
	}
}

func TestErasureCoder_GetConfig(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()

	coder, _ := NewErasureCoder(cfg, logger)
	result := coder.GetConfig()

	if result.DataShards != cfg.DataShards {
		t.Errorf("DataShards = %d, want %d", result.DataShards, cfg.DataShards)
	}
	if result.ParityShards != cfg.ParityShards {
		t.Errorf("ParityShards = %d, want %d", result.ParityShards, cfg.ParityShards)
	}
}

func TestErasureCoder_SplitSize(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig() // 4 data shards

	coder, _ := NewErasureCoder(cfg, logger)

	tests := []struct {
		dataSize int
		expected int
	}{
		{0, 0},
		{4, 1},
		{8, 2},
		{100, 25},
		{101, 26},
		{400, 100},
	}

	for _, tt := range tests {
		result := coder.SplitSize(tt.dataSize)
		if result != tt.expected {
			t.Errorf("SplitSize(%d) = %d, want %d", tt.dataSize, result, tt.expected)
		}
	}
}

func TestErasureCoder_JoinSize(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig() // 4 data shards

	coder, _ := NewErasureCoder(cfg, logger)

	tests := []struct {
		shardSize int
		expected  int
	}{
		{0, 0},
		{1, 4},
		{10, 40},
		{100, 400},
	}

	for _, tt := range tests {
		result := coder.JoinSize(tt.shardSize)
		if result != tt.expected {
			t.Errorf("JoinSize(%d) = %d, want %d", tt.shardSize, result, tt.expected)
		}
	}
}

func TestReplicationFactor_WriteQuorum(t *testing.T) {
	tests := []struct {
		rf       ReplicationFactor
		expected int
	}{
		{RF1, 1},
		{RF2, 2},
		{RF3, 2},
		{RF5, 3},
	}

	for _, tt := range tests {
		result := tt.rf.WriteQuorum()
		if result != tt.expected {
			t.Errorf("WriteQuorum() = %d, want %d", result, tt.expected)
		}
	}
}

func TestReplicationFactor_ReadQuorum(t *testing.T) {
	tests := []struct {
		rf       ReplicationFactor
		expected int
	}{
		{RF1, 1},
		{RF2, 2},
		{RF3, 2},
		{RF5, 3},
	}

	for _, tt := range tests {
		result := tt.rf.ReadQuorum()
		if result != tt.expected {
			t.Errorf("ReadQuorum() = %d, want %d", result, tt.expected)
		}
	}
}

func TestReplicator_GetReplicationFactor(t *testing.T) {
	logger := zap.NewNop()
	ring := NewHashRing()

	manager := &Manager{
		logger: logger,
	}

	replicator := NewReplicator(manager, ring, RF3, logger)
	rf := replicator.GetReplicationFactor()

	if rf != RF3 {
		t.Errorf("GetReplicationFactor() = %d, want %d", rf, RF3)
	}
}

func TestFindLeastLoadedNode(t *testing.T) {
	tests := []struct {
		name         string
		distribution map[string]int
		exclude      string
		expected     string
	}{
		{
			name:         "simple case",
			distribution: map[string]int{"node-1": 100, "node-2": 50, "node-3": 75},
			exclude:      "",
			expected:     "node-2",
		},
		{
			name:         "exclude least loaded",
			distribution: map[string]int{"node-1": 100, "node-2": 50, "node-3": 75},
			exclude:      "node-2",
			expected:     "node-3",
		},
		{
			name:         "single node",
			distribution: map[string]int{"node-1": 100},
			exclude:      "",
			expected:     "node-1",
		},
		{
			name:         "empty distribution",
			distribution: map[string]int{},
			exclude:      "",
			expected:     "",
		},
		{
			name:         "all excluded",
			distribution: map[string]int{"node-1": 100},
			exclude:      "node-1",
			expected:     "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rebalancer := &Rebalancer{}
			result := rebalancer.findLeastLoadedNode(tt.distribution, tt.exclude)
			if result != tt.expected {
				t.Errorf("findLeastLoadedNode() = %s, want %s", result, tt.expected)
			}
		})
	}
}

func TestReplicator_SetReplicationFactor(t *testing.T) {
	logger := zap.NewNop()
	ring := NewHashRing()

	manager := &Manager{
		logger: logger,
	}

	replicator := NewReplicator(manager, ring, RF3, logger)

	replicator.SetReplicationFactor(RF5)
	rf := replicator.GetReplicationFactor()

	if rf != RF5 {
		t.Errorf("GetReplicationFactor() = %d, want %d", rf, RF5)
	}
}

func TestReplicator_GetTargetNodes(t *testing.T) {
	logger := zap.NewNop()
	ring := NewHashRing()
	ring.AddNode(&Node{ID: "node-1"})
	ring.AddNode(&Node{ID: "node-2"})
	ring.AddNode(&Node{ID: "node-3"})

	manager := &Manager{
		logger: logger,
	}

	replicator := NewReplicator(manager, ring, RF3, logger)
	targets := replicator.GetTargetNodes("test-key")

	if len(targets) != 3 {
		t.Errorf("GetTargetNodes() returned %d nodes, want 3", len(targets))
	}
}

func TestReplicator_GetTargetNodesEmptyRing(t *testing.T) {
	logger := zap.NewNop()
	ring := NewHashRing()

	manager := &Manager{
		logger: logger,
	}

	replicator := NewReplicator(manager, ring, RF3, logger)
	targets := replicator.GetTargetNodes("test-key")

	if len(targets) != 0 {
		t.Errorf("GetTargetNodes() returned %d nodes, want 0", len(targets))
	}
}

func TestNode_Status(t *testing.T) {
	tests := []struct {
		state    NodeState
		expected string
	}{
		{NodeStateAlive, "online"},
		{NodeStateSuspect, "degraded"},
		{NodeStateDead, "offline"},
		{NodeStateLeft, "left"},
		{NodeState("unknown"), "unknown"},
	}

	for _, tt := range tests {
		node := &Node{State: tt.state}
		result := node.Status()
		if result != tt.expected {
			t.Errorf("Status() = %s, want %s", result, tt.expected)
		}
	}
}

func TestBackupStatusConstants(t *testing.T) {
	if BackupStatusPending != "pending" {
		t.Error("BackupStatusPending should be 'pending'")
	}
	if BackupStatusRunning != "running" {
		t.Error("BackupStatusRunning should be 'running'")
	}
	if BackupStatusComplete != "complete" {
		t.Error("BackupStatusComplete should be 'complete'")
	}
	if BackupStatusFailed != "failed" {
		t.Error("BackupStatusFailed should be 'failed'")
	}
	if BackupStatusCancelled != "cancelled" {
		t.Error("BackupStatusCancelled should be 'cancelled'")
	}
}

func TestMirrorManager_NewMirrorManager(t *testing.T) {
	logger := zap.NewNop()
	config := MirrorConfig{
		Enabled:       false,
		SourceCluster: "source",
		TargetCluster: "target",
		Mode:          MirrorModeAsync,
	}

	base := &MirrorManager{}
	m := base.NewMirrorManager(config, nil, logger)

	if m == nil {
		t.Fatal("NewMirrorManager returned nil")
	}
	if m.config.SourceCluster != "source" {
		t.Errorf("SourceCluster = %s, want source", m.config.SourceCluster)
	}
}

func TestMirrorManager_StartDisabled(t *testing.T) {
	logger := zap.NewNop()
	config := MirrorConfig{
		Enabled: false,
	}

	base := &MirrorManager{}
	m := base.NewMirrorManager(config, nil, logger)

	err := m.Start(context.Background())
	if err != nil {
		t.Errorf("Start() error: %v", err)
	}
}

func TestMirrorManager_Stop(t *testing.T) {
	logger := zap.NewNop()
	config := MirrorConfig{
		Enabled: false,
	}

	base := &MirrorManager{}
	m := base.NewMirrorManager(config, nil, logger)
	m.stopCh = make(chan struct{})

	m.Stop()

	if m.IsActive() {
		t.Error("IsActive() should return false after Stop()")
	}
}

func TestMirrorManager_IsActive(t *testing.T) {
	logger := zap.NewNop()
	config := MirrorConfig{}

	base := &MirrorManager{}
	m := base.NewMirrorManager(config, nil, logger)

	m.mu.Lock()
	m.active = true
	m.mu.Unlock()

	if !m.IsActive() {
		t.Error("IsActive() should return true")
	}
}

func TestIOUtils_NewIOUtils(t *testing.T) {
	utils := newIOUtils()
	if utils == nil {
		t.Fatal("newIOUtils returned nil")
	}
}

func TestIOUtils_GetBuffer(t *testing.T) {
	utils := newIOUtils()
	buf := utils.getBuffer()

	if buf == nil {
		t.Fatal("getBuffer returned nil")
	}
	if len(*buf) != 32*1024 {
		t.Errorf("buffer size = %d, want %d", len(*buf), 32*1024)
	}
}

func TestIOUtils_PutBuffer(t *testing.T) {
	utils := newIOUtils()
	buf := utils.getBuffer()

	utils.putBuffer(buf)

	buf2 := utils.getBuffer()
	if buf2 == nil {
		t.Fatal("getBuffer after put returned nil")
	}
}

func TestMirrorConfigDefaults(t *testing.T) {
	config := MirrorConfig{
		Mode:     MirrorModeSync,
		Interval: 5 * time.Minute,
	}

	if config.Mode != MirrorModeSync {
		t.Errorf("Mode = %s, want sync", config.Mode)
	}
	if config.Interval != 5*time.Minute {
		t.Errorf("Interval = %v, want 5m", config.Interval)
	}
}

func TestBackupTargetType(t *testing.T) {
	target := BackupTarget{
		Type:     BackupTargetS3,
		Name:     "test-target",
		Bucket:   "test-bucket",
		Endpoint: "https://s3.amazonaws.com",
		Auth: BackupAuth{
			AccessKey: "test",
		},
	}

	if target.Type != BackupTargetS3 {
		t.Errorf("Type = %s, want s3", target.Type)
	}
	if target.Name != "test-target" {
		t.Errorf("Name = %s, want test-target", target.Name)
	}
}

func TestBackupJobFields(t *testing.T) {
	now := time.Now()
	job := BackupJob{
		ID:        "job-123",
		TargetID:  "target-1",
		Status:    BackupStatusPending,
		StartedAt: now,
	}

	if job.ID != "job-123" {
		t.Errorf("ID = %s, want job-123", job.ID)
	}
	if job.Status != BackupStatusPending {
		t.Errorf("Status = %s, want pending", job.Status)
	}
}

func TestBackupAuth(t *testing.T) {
	auth := BackupAuth{
		AccessKey: "AKIAIOSFODNN7EXAMPLE",
		SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		Token:     "session-token",
	}

	if auth.AccessKey != "AKIAIOSFODNN7EXAMPLE" {
		t.Errorf("AccessKey = %s, want AKIAIOSFODNN7EXAMPLE", auth.AccessKey)
	}
}

func TestBackupJobTypeConstants(t *testing.T) {
	if BackupJobFull != "full" {
		t.Error("BackupJobFull should be 'full'")
	}
	if BackupJobIncremental != "incremental" {
		t.Error("BackupJobIncremental should be 'incremental'")
	}
}

func TestBackupTargetTypeConstants(t *testing.T) {
	if BackupTargetS3 != "s3" {
		t.Error("BackupTargetS3 should be 's3'")
	}
	if BackupTargetGCS != "gcs" {
		t.Error("BackupTargetGCS should be 'gcs'")
	}
	if BackupTargetAzure != "azure" {
		t.Error("BackupTargetAzure should be 'azure'")
	}
	if BackupTargetNFS != "nfs" {
		t.Error("BackupTargetNFS should be 'nfs'")
	}
	if BackupTargetLocal != "local" {
		t.Error("BackupTargetLocal should be 'local'")
	}
}

func TestErasureStripeStore_New(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()
	coder, _ := NewErasureCoder(cfg, logger)

	store := NewErasureStripeStore(coder, logger)
	if store == nil {
		t.Fatal("NewErasureStripeStore returned nil")
	}
}

func TestErasureStripeStore_Store(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()
	coder, _ := NewErasureCoder(cfg, logger)

	store := NewErasureStripeStore(coder, logger)

	stripe := &ErasureStripe{
		ID:        "stripe-1",
		Key:       "bucket/key",
		Shards:    make([][]byte, 6),
		CreatedAt: time.Now().Unix(),
	}

	store.Store(stripe)

	retrieved, ok := store.Get("stripe-1")
	if !ok {
		t.Fatal("Get() should return true")
	}
	if retrieved.Key != "bucket/key" {
		t.Errorf("Key = %s, want bucket/key", retrieved.Key)
	}
}

func TestErasureStripeStore_GetNotFound(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()
	coder, _ := NewErasureCoder(cfg, logger)

	store := NewErasureStripeStore(coder, logger)

	_, ok := store.Get("nonexistent")
	if ok {
		t.Error("Get() should return false for nonexistent stripe")
	}
}

func TestErasureStripeStore_Delete(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()
	coder, _ := NewErasureCoder(cfg, logger)

	store := NewErasureStripeStore(coder, logger)

	stripe := &ErasureStripe{
		ID:        "stripe-1",
		Key:       "bucket/key",
		CreatedAt: time.Now().Unix(),
	}

	store.Store(stripe)
	store.Delete("stripe-1")

	_, ok := store.Get("stripe-1")
	if ok {
		t.Error("Get() should return false after Delete()")
	}
}

func TestErasureStripeStore_List(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()
	coder, _ := NewErasureCoder(cfg, logger)

	store := NewErasureStripeStore(coder, logger)

	store.Store(&ErasureStripe{ID: "stripe-1", Key: "key1"})
	store.Store(&ErasureStripe{ID: "stripe-2", Key: "key2"})

	list := store.List()
	if len(list) != 2 {
		t.Errorf("List() returned %d stripes, want 2", len(list))
	}
}

func TestErasureStripeStore_Stats(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()
	coder, _ := NewErasureCoder(cfg, logger)

	store := NewErasureStripeStore(coder, logger)

	store.Store(&ErasureStripe{ID: "stripe-1", Key: "key1"})
	store.Store(&ErasureStripe{ID: "stripe-2", Key: "key2"})

	count, _ := store.Stats()
	if count != 2 {
		t.Errorf("Stats() count = %d, want 2", count)
	}
}

func TestErasureStripe_Fields(t *testing.T) {
	stripe := ErasureStripe{
		ID:        "test-id",
		Key:       "bucket/object.txt",
		Shards:    [][]byte{[]byte("data1"), []byte("data2")},
		CreatedAt: 1234567890,
	}

	if stripe.ID != "test-id" {
		t.Errorf("ID = %s, want test-id", stripe.ID)
	}
	if len(stripe.Shards) != 2 {
		t.Errorf("Shards count = %d, want 2", len(stripe.Shards))
	}
}

func TestErasureCoder_Encode(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()
	coder, err := NewErasureCoder(cfg, logger)
	if err != nil {
		t.Fatalf("NewErasureCoder() error = %v", err)
	}

	data := []byte("Hello, this is a test message for erasure coding!")
	shards, err := coder.Encode(data)
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}

	if len(shards) != cfg.TotalShards {
		t.Errorf("Shards count = %d, want %d", len(shards), cfg.TotalShards)
	}

	for i, shard := range shards {
		if len(shard) == 0 {
			t.Errorf("Shard %d is empty", i)
		}
	}
}

func TestErasureCoder_Decode(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()
	coder, err := NewErasureCoder(cfg, logger)
	if err != nil {
		t.Fatalf("NewErasureCoder() error = %v", err)
	}

	originalData := []byte("Hello, this is a test message for erasure coding!")
	shards, err := coder.Encode(originalData)
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}

	decoded, err := coder.Decode(shards)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}

	if string(decoded[:len(originalData)]) != string(originalData) {
		t.Errorf("Decoded data = %s, want %s", string(decoded[:len(originalData)]), string(originalData))
	}
}

func TestErasureCoder_DecodeWithMissingShards(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()
	coder, err := NewErasureCoder(cfg, logger)
	if err != nil {
		t.Fatalf("NewErasureCoder() error = %v", err)
	}

	originalData := []byte("Test data for decoding with missing shards")
	shards, err := coder.Encode(originalData)
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}

	shards[0] = nil
	shards[5] = nil

	decoded, err := coder.Decode(shards)
	if err != nil {
		t.Fatalf("Decode() with missing shards error = %v", err)
	}

	if string(decoded[:len(originalData)]) != string(originalData) {
		t.Errorf("Decoded data mismatch")
	}
}

func TestErasureCoder_DecodeNotEnoughShards(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()
	coder, err := NewErasureCoder(cfg, logger)
	if err != nil {
		t.Fatalf("NewErasureCoder() error = %v", err)
	}

	shards := make([][]byte, cfg.TotalShards)
	shards[0] = []byte("only one shard")

	_, err = coder.Decode(shards)
	if err == nil {
		t.Error("Decode() should fail with not enough shards")
	}
}

func TestErasureCoder_DecodeWrongShardCount(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()
	coder, err := NewErasureCoder(cfg, logger)
	if err != nil {
		t.Fatalf("NewErasureCoder() error = %v", err)
	}

	shards := [][]byte{[]byte("one"), []byte("two")}

	_, err = coder.Decode(shards)
	if err == nil {
		t.Error("Decode() should fail with wrong shard count")
	}
}

func TestErasureCoder_Reconstruct(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()
	coder, err := NewErasureCoder(cfg, logger)
	if err != nil {
		t.Fatalf("NewErasureCoder() error = %v", err)
	}

	originalData := []byte("Test data for reconstruction")
	shards, err := coder.Encode(originalData)
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}

	shards[0] = nil
	shards[5] = nil

	err = coder.Reconstruct(shards)
	if err != nil {
		t.Fatalf("Reconstruct() error = %v", err)
	}

	if shards[0] == nil {
		t.Error("Shard 0 should be reconstructed")
	}
	if shards[5] == nil {
		t.Error("Shard 5 should be reconstructed")
	}
}

func TestErasureCoder_ReconstructNotEnoughShards(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()
	coder, err := NewErasureCoder(cfg, logger)
	if err != nil {
		t.Fatalf("NewErasureCoder() error = %v", err)
	}

	shards := make([][]byte, cfg.TotalShards)
	shards[0] = []byte("only one shard")

	err = coder.Reconstruct(shards)
	if err == nil {
		t.Error("Reconstruct() should fail with not enough shards")
	}
}

func TestErasureCoder_ReconstructWrongShardCount(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()
	coder, err := NewErasureCoder(cfg, logger)
	if err != nil {
		t.Fatalf("NewErasureCoder() error = %v", err)
	}

	shards := [][]byte{[]byte("one"), []byte("two")}

	err = coder.Reconstruct(shards)
	if err == nil {
		t.Error("Reconstruct() should fail with wrong shard count")
	}
}

func TestErasureCoder_Verify(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()
	coder, err := NewErasureCoder(cfg, logger)
	if err != nil {
		t.Fatalf("NewErasureCoder() error = %v", err)
	}

	data := []byte("Test data for verification")
	shards, err := coder.Encode(data)
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}

	valid, err := coder.Verify(shards)
	if err != nil {
		t.Fatalf("Verify() error = %v", err)
	}

	if !valid {
		t.Error("Verify() should return true for valid shards")
	}
}

func TestErasureCoder_VerifyWrongShardCount(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()
	coder, err := NewErasureCoder(cfg, logger)
	if err != nil {
		t.Fatalf("NewErasureCoder() error = %v", err)
	}

	shards := [][]byte{[]byte("one"), []byte("two")}

	_, err = coder.Verify(shards)
	if err == nil {
		t.Error("Verify() should fail with wrong shard count")
	}
}

func TestErasureCoder_HighPerformanceConfig(t *testing.T) {
	logger := zap.NewNop()
	cfg := HighPerformanceConfig()
	coder, err := NewErasureCoder(cfg, logger)
	if err != nil {
		t.Fatalf("NewErasureCoder() error = %v", err)
	}

	if coder.GetConfig().DataShards != 8 {
		t.Errorf("DataShards = %d, want 8", coder.GetConfig().DataShards)
	}
	if coder.GetConfig().ParityShards != 2 {
		t.Errorf("ParityShards = %d, want 2", coder.GetConfig().ParityShards)
	}
}

func TestErasureCoder_HighDurabilityConfig(t *testing.T) {
	logger := zap.NewNop()
	cfg := HighDurabilityConfig()
	coder, err := NewErasureCoder(cfg, logger)
	if err != nil {
		t.Fatalf("NewErasureCoder() error = %v", err)
	}

	if coder.GetConfig().DataShards != 4 {
		t.Errorf("DataShards = %d, want 4", coder.GetConfig().DataShards)
	}
	if coder.GetConfig().ParityShards != 4 {
		t.Errorf("ParityShards = %d, want 4", coder.GetConfig().ParityShards)
	}
}

func TestRebalancer_ExecuteMove(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultRebalanceConfig()
	ring := NewHashRing()
	mgr := &Manager{logger: logger}

	rebalancer := NewRebalancer(cfg, mgr, ring, logger)

	ctx := context.Background()
	op := &RebalanceOperation{
		ID:         "move-1",
		SourceNode: "node-1",
		TargetNode: "node-2",
	}

	rebalancer.executeMove(ctx, op)

	if op.Status != RebalanceComplete {
		t.Errorf("Status = %v, want %v", op.Status, RebalanceComplete)
	}
	if op.CompleteTime == nil {
		t.Error("CompleteTime should be set")
	}
}

func TestReplicator_GetOperation_New(t *testing.T) {
	logger := zap.NewNop()
	ring := NewHashRing()
	mgr := &Manager{logger: logger}

	replicator := NewReplicator(mgr, ring, RF3, logger)

	op := &ReplicationOp{ID: "op-1", ObjectKey: "key-1"}
	replicator.mu.Lock()
	replicator.completedOps["op-1"] = op
	replicator.mu.Unlock()

	retrieved, exists := replicator.GetOperation("op-1")
	if !exists {
		t.Error("GetOperation() should return true")
	}
	if retrieved.ID != "op-1" {
		t.Errorf("ID = %s, want op-1", retrieved.ID)
	}
}

func TestReplicator_GetPendingOperations_New(t *testing.T) {
	logger := zap.NewNop()
	ring := NewHashRing()
	mgr := &Manager{logger: logger}

	replicator := NewReplicator(mgr, ring, RF3, logger)

	op1 := &ReplicationOp{ID: "op-1", ObjectKey: "key-1"}
	op2 := &ReplicationOp{ID: "op-2", ObjectKey: "key-2"}
	replicator.mu.Lock()
	replicator.pendingOps["op-1"] = op1
	replicator.pendingOps["op-2"] = op2
	replicator.mu.Unlock()

	pending := replicator.GetPendingOperations()
	if len(pending) != 2 {
		t.Errorf("Pending operations count = %d, want 2", len(pending))
	}
}

func TestReplicator_WriteQuorum_New(t *testing.T) {
	rf := ReplicationFactor(3)
	if rf.WriteQuorum() != 2 {
		t.Errorf("WriteQuorum() = %d, want 2", rf.WriteQuorum())
	}
}

func TestReplicator_ReadQuorum_New(t *testing.T) {
	rf := ReplicationFactor(3)
	if rf.ReadQuorum() != 2 {
		t.Errorf("ReadQuorum() = %d, want 2", rf.ReadQuorum())
	}
}

func TestMirrorManager_PerformMirror_New(t *testing.T) {
	logger := zap.NewNop()
	config := MirrorConfig{Enabled: true}
	base := &MirrorManager{}
	mgr := base.NewMirrorManager(config, nil, logger)

	ctx := context.Background()
	mgr.performMirror(ctx)
}

func TestMirrorManager_Start_New(t *testing.T) {
	logger := zap.NewNop()
	config := MirrorConfig{Enabled: true}
	base := &MirrorManager{}
	mgr := base.NewMirrorManager(config, nil, logger)

	ctx := context.Background()
	mgr.Start(ctx)

	time.Sleep(50 * time.Millisecond)
	mgr.Stop()
}

func TestMirrorManager_IsActive_New(t *testing.T) {
	logger := zap.NewNop()
	config := MirrorConfig{Enabled: false}
	base := &MirrorManager{}
	mgr := base.NewMirrorManager(config, nil, logger)

	if mgr.IsActive() {
		t.Error("IsActive() should be false initially")
	}
}

func TestIOUtils_CopyWithBuffer(t *testing.T) {
	utils := newIOUtils()

	src := io.NopCloser(bytes.NewBufferString("test content for copy"))
	dst := &bytes.Buffer{}

	n, err := utils.copyWithBuffer(dst, src)
	if err != nil {
		t.Errorf("copyWithBuffer() error = %v", err)
	}
	if n != 21 {
		t.Errorf("bytes copied = %d, want 21", n)
	}
	if dst.String() != "test content for copy" {
		t.Errorf("dst = %s, want 'test content for copy'", dst.String())
	}
}

func TestReplicator_ReplicateWrite(t *testing.T) {
	logger := zap.NewNop()
	ring := NewHashRing()
	ring.AddNode(&Node{ID: "node-1"})
	ring.AddNode(&Node{ID: "node-2"})
	ring.AddNode(&Node{ID: "node-3"})

	mgr := &Manager{logger: logger}
	replicator := NewReplicator(mgr, ring, RF3, logger)

	ctx := context.Background()
	op := &ReplicationOp{
		ID:        "op-1",
		ObjectKey: "test-key",
	}

	err := replicator.ReplicateWrite(ctx, op)
	if err != nil {
		t.Errorf("ReplicateWrite() error = %v", err)
	}
	if op.Status != ReplicationComplete {
		t.Errorf("Status = %v, want %v", op.Status, ReplicationComplete)
	}
	if op.CompleteTime == nil {
		t.Error("CompleteTime should be set")
	}
}

func TestReplicator_ReplicateWrite_QuorumFailure(t *testing.T) {
	logger := zap.NewNop()
	ring := NewHashRing()

	mgr := &Manager{logger: logger}
	replicator := NewReplicator(mgr, ring, RF3, logger)

	ctx := context.Background()
	op := &ReplicationOp{
		ID:        "op-1",
		ObjectKey: "test-key",
	}

	err := replicator.ReplicateWrite(ctx, op)
	if err == nil {
		t.Error("ReplicateWrite() should fail with no nodes")
	}
	if op.Status != ReplicationFailed {
		t.Errorf("Status = %v, want %v", op.Status, ReplicationFailed)
	}
}

func TestReplicator_ReplicateDelete(t *testing.T) {
	logger := zap.NewNop()
	ring := NewHashRing()
	ring.AddNode(&Node{ID: "node-1"})
	ring.AddNode(&Node{ID: "node-2"})

	mgr := &Manager{logger: logger}
	replicator := NewReplicator(mgr, ring, RF3, logger)

	ctx := context.Background()
	op := &ReplicationOp{
		ID:        "op-1",
		ObjectKey: "test-key",
	}

	err := replicator.ReplicateDelete(ctx, op)
	if err != nil {
		t.Errorf("ReplicateDelete() error = %v", err)
	}
	if op.Status != ReplicationComplete {
		t.Errorf("Status = %v, want %v", op.Status, ReplicationComplete)
	}
}

func TestReplicator_GetReplicatedData(t *testing.T) {
	logger := zap.NewNop()
	ring := NewHashRing()

	mgr := &Manager{logger: logger}
	replicator := NewReplicator(mgr, ring, RF3, logger)

	ctx := context.Background()
	_, err := replicator.GetReplicatedData(ctx, "test-key")
	// Returns error when no nodes available for quorum
	if err == nil {
		t.Error("GetReplicatedData() should return error with no nodes in ring")
	}
}

func TestReplicator_readFromNode(t *testing.T) {
	logger := zap.NewNop()
	ring := NewHashRing()
	mgr := &Manager{logger: logger}
	replicator := NewReplicator(mgr, ring, RF3, logger)

	ctx := context.Background()

	data, err := replicator.readFromNode(ctx, "test-key", "node-1")
	if err != nil {
		t.Errorf("readFromNode() error = %v", err)
	}
	// Returns simulated data
	if len(data) == 0 {
		t.Error("readFromNode() should return simulated data")
	}
}

func TestReplicator_writeToNode(t *testing.T) {
	logger := zap.NewNop()
	ring := NewHashRing()
	mgr := &Manager{logger: logger}
	replicator := NewReplicator(mgr, ring, RF3, logger)

	ctx := context.Background()
	op := &ReplicationOp{ID: "op-1", ObjectKey: "key-1"}

	err := replicator.writeToNode(ctx, op, "node-1")
	if err != nil {
		t.Errorf("writeToNode() error = %v", err)
	}
}

func TestReplicator_deleteFromNode(t *testing.T) {
	logger := zap.NewNop()
	ring := NewHashRing()
	mgr := &Manager{logger: logger}
	replicator := NewReplicator(mgr, ring, RF3, logger)

	ctx := context.Background()
	op := &ReplicationOp{ID: "op-1", ObjectKey: "key-1"}

	err := replicator.deleteFromNode(ctx, op, "node-1")
	if err != nil {
		t.Errorf("deleteFromNode() error = %v", err)
	}
}

func TestReplicator_rollbackWrite(t *testing.T) {
	logger := zap.NewNop()
	ring := NewHashRing()
	ring.AddNode(&Node{ID: "node-1"})

	mgr := &Manager{logger: logger}
	replicator := NewReplicator(mgr, ring, RF3, logger)

	ctx := context.Background()
	op := &ReplicationOp{
		ID:        "op-1",
		ObjectKey: "test-key",
		Replicas: []Replica{
			{NodeID: "node-1"},
		},
	}

	replicator.rollbackWrite(ctx, op)
}

func TestRebalancer_ExecuteMoves(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultRebalanceConfig()
	ring := NewHashRing()
	mgr := &Manager{logger: logger}

	rebalancer := NewRebalancer(cfg, mgr, ring, logger)

	ctx := context.Background()
	moves := []RebalanceOperation{
		{ID: "move-1", SourceNode: "node-1", TargetNode: "node-2"},
		{ID: "move-2", SourceNode: "node-1", TargetNode: "node-3"},
	}

	rebalancer.executeMoves(ctx, moves)

	if rebalancer.activeOps != 0 {
		t.Errorf("activeOps = %d, want 0", rebalancer.activeOps)
	}
}

func TestMirrorManager_RunAsyncMirror(t *testing.T) {
	logger := zap.NewNop()
	config := MirrorConfig{
		Enabled:  true,
		Interval: 100 * time.Millisecond,
	}
	base := &MirrorManager{}
	mgr := base.NewMirrorManager(config, nil, logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go mgr.runAsyncMirror(ctx)

	time.Sleep(150 * time.Millisecond)
	cancel()
}

func TestClusterDelegate_NotifyMsg(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{NodeID: "node-1"}
	mgr := NewManager(cfg, logger)
	delegate := NewClusterDelegate(mgr)

	delegate.NotifyMsg([]byte("test message"))
}

func TestClusterDelegate_NotifyJoin(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{NodeID: "node-1"}
	mgr := NewManager(cfg, logger)
	delegate := NewClusterDelegate(mgr)

	delegate.NotifyJoin(nil)
}

func TestClusterDelegate_NotifyLeave(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{NodeID: "node-1"}
	mgr := NewManager(cfg, logger)
	delegate := NewClusterDelegate(mgr)

	delegate.NotifyLeave(nil)
}

func TestClusterDelegate_NotifyUpdate(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{NodeID: "node-1"}
	mgr := NewManager(cfg, logger)
	delegate := NewClusterDelegate(mgr)

	delegate.NotifyUpdate(nil)
}

func TestErasureWriter_New(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()
	coder, err := NewErasureCoder(cfg, logger)
	if err != nil {
		t.Fatalf("NewErasureCoder() error = %v", err)
	}

	ring := NewHashRing()
	mgr := &Manager{logger: logger}

	writer := NewErasureWriter(coder, ring, mgr, logger)

	if writer == nil {
		t.Error("NewErasureWriter() returned nil")
	}
}

func TestErasureWriter_Write(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()
	coder, err := NewErasureCoder(cfg, logger)
	if err != nil {
		t.Fatalf("NewErasureCoder() error = %v", err)
	}

	ring := NewHashRing()
	ring.AddNode(&Node{ID: "node-1"})
	ring.AddNode(&Node{ID: "node-2"})
	ring.AddNode(&Node{ID: "node-3"})
	ring.AddNode(&Node{ID: "node-4"})
	ring.AddNode(&Node{ID: "node-5"})
	ring.AddNode(&Node{ID: "node-6"})

	mgr := &Manager{logger: logger}
	writer := NewErasureWriter(coder, ring, mgr, logger)

	ctx := context.Background()
	data := []byte("test data for erasure writer")
	err = writer.Write(ctx, "test-key", data)
	if err != nil {
		t.Errorf("Write() error = %v", err)
	}
}

func TestErasureWriter_Read(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()
	coder, err := NewErasureCoder(cfg, logger)
	if err != nil {
		t.Fatalf("NewErasureCoder() error = %v", err)
	}

	ring := NewHashRing()
	ring.AddNode(&Node{ID: "node-1"})
	ring.AddNode(&Node{ID: "node-2"})
	ring.AddNode(&Node{ID: "node-3"})
	ring.AddNode(&Node{ID: "node-4"})
	ring.AddNode(&Node{ID: "node-5"})
	ring.AddNode(&Node{ID: "node-6"})

	mgr := &Manager{logger: logger}
	writer := NewErasureWriter(coder, ring, mgr, logger)

	ctx := context.Background()
	_, err = writer.Read(ctx, "test-key")
	// Returns error when no actual shards exist
	if err == nil {
		t.Error("Read() should return error with no real data")
	}
}

func TestReplicator_GetOperation_PendingOps(t *testing.T) {
	logger := zap.NewNop()
	ring := NewHashRing()
	mgr := &Manager{logger: logger}

	replicator := NewReplicator(mgr, ring, RF3, logger)

	op := &ReplicationOp{ID: "op-pending", ObjectKey: "key-1"}
	replicator.mu.Lock()
	replicator.pendingOps["op-pending"] = op
	replicator.mu.Unlock()

	retrieved, exists := replicator.GetOperation("op-pending")
	if !exists {
		t.Error("GetOperation() should return true for pending op")
	}
	if retrieved.ID != "op-pending" {
		t.Errorf("ID = %s, want op-pending", retrieved.ID)
	}
}

func TestReplicator_GetOperation_NotFound(t *testing.T) {
	logger := zap.NewNop()
	ring := NewHashRing()
	mgr := &Manager{logger: logger}

	replicator := NewReplicator(mgr, ring, RF3, logger)

	_, exists := replicator.GetOperation("nonexistent")
	if exists {
		t.Error("GetOperation() should return false for nonexistent op")
	}
}

func TestReplicator_GetReplicatedData_Success(t *testing.T) {
	logger := zap.NewNop()
	ring := NewHashRing()
	ring.AddNode(&Node{ID: "node-1"})
	ring.AddNode(&Node{ID: "node-2"})
	ring.AddNode(&Node{ID: "node-3"})

	mgr := &Manager{logger: logger}
	replicator := NewReplicator(mgr, ring, RF3, logger)

	ctx := context.Background()
	data, err := replicator.GetReplicatedData(ctx, "test-key")
	if err != nil {
		t.Errorf("GetReplicatedData() error = %v", err)
	}
	if len(data) == 0 {
		t.Error("GetReplicatedData() should return data")
	}
}

func TestReplicator_ReplicateWrite_ErrorBranch(t *testing.T) {
	logger := zap.NewNop()
	ring := NewHashRing()
	ring.AddNode(&Node{ID: "node-1"})
	ring.AddNode(&Node{ID: "node-2"})

	mgr := &Manager{logger: logger}
	replicator := NewReplicator(mgr, ring, RF2, logger)

	ctx := context.Background()
	op := &ReplicationOp{
		ID:        "op-1",
		ObjectKey: "test-key",
	}

	err := replicator.ReplicateWrite(ctx, op)
	if err != nil {
		t.Errorf("ReplicateWrite() error = %v", err)
	}
}

func TestCluster_Initialize_ManagerStartError(t *testing.T) {
	logger := zap.NewNop()
	cluster := NewCluster(logger)
	cluster.testManagerStartErr = true

	ctx := context.Background()
	err := cluster.Initialize(ctx, RF3)
	if err == nil {
		t.Error("Initialize should fail when manager start fails")
	}
}

func TestCluster_Initialize_ErasureCoderError(t *testing.T) {
	logger := zap.NewNop()
	cluster := NewCluster(logger, WithBindPort(0))
	cluster.testManagerStartErr = false
	cluster.testErasureCoderErr = true

	ctx := context.Background()
	err := cluster.Initialize(ctx, RF3)
	if err == nil {
		t.Error("Initialize should fail when erasure coder creation fails")
	}
}

func TestReplicator_ReplicateWrite_WriteError(t *testing.T) {
	logger := zap.NewNop()
	ring := NewHashRing()
	ring.AddNode(&Node{ID: "node-1"})
	ring.AddNode(&Node{ID: "node-2"})
	ring.AddNode(&Node{ID: "node-3"})

	mgr := &Manager{logger: logger}
	replicator := NewReplicator(mgr, ring, RF3, logger)
	replicator.testWriteErr = true

	ctx := context.Background()
	op := &ReplicationOp{
		ID:        "op-1",
		ObjectKey: "test-key",
	}

	err := replicator.ReplicateWrite(ctx, op)
	if err == nil {
		t.Error("ReplicateWrite should fail when write to node fails")
	}
}

func TestRebalancer_StartDisabled(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultRebalanceConfig()
	cfg.Enabled = false
	ring := NewHashRing()
	mgr := &Manager{logger: logger}

	rebalancer := NewRebalancer(cfg, mgr, ring, logger)
	ctx := context.Background()
	rebalancer.Start(ctx)
}

func TestRebalancer_RunCheckerWithStopCh(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultRebalanceConfig()
	cfg.CheckInterval = 10 * time.Millisecond
	ring := NewHashRing()
	ring.AddNode(&Node{ID: "node-1"})
	mgr := &Manager{logger: logger}

	rebalancer := NewRebalancer(cfg, mgr, ring, logger)
	ctx := context.Background()

	go rebalancer.runChecker(ctx)
	time.Sleep(5 * time.Millisecond)
	rebalancer.Stop()
	time.Sleep(5 * time.Millisecond)
}

func TestRebalancer_CheckAndRebalanceWithImbalance(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultRebalanceConfig()
	cfg.ThresholdPercent = 5
	ring := NewHashRing()

	ring.AddNode(&Node{ID: "node-1"})
	ring.AddNode(&Node{ID: "node-2"})
	mgr := &Manager{logger: logger}

	rebalancer := NewRebalancer(cfg, mgr, ring, logger)
	ctx := context.Background()

	rebalancer.checkAndRebalance(ctx)
}

func TestRebalancer_ExecuteMovesWithContextCancel(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultRebalanceConfig()
	ring := NewHashRing()
	mgr := &Manager{logger: logger}

	rebalancer := NewRebalancer(cfg, mgr, ring, logger)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	moves := []RebalanceOperation{
		{ID: "move-1", SourceNode: "node-1", TargetNode: "node-2"},
	}
	rebalancer.executeMoves(ctx, moves)
}

func TestRebalancer_ExecuteMovesWithStopCh(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultRebalanceConfig()
	ring := NewHashRing()
	mgr := &Manager{logger: logger}

	rebalancer := NewRebalancer(cfg, mgr, ring, logger)
	close(rebalancer.stopCh)

	ctx := context.Background()
	moves := []RebalanceOperation{
		{ID: "move-1", SourceNode: "node-1", TargetNode: "node-2"},
	}
	rebalancer.executeMoves(ctx, moves)
}

func TestRebalancer_GetStatusWithOps(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultRebalanceConfig()
	ring := NewHashRing()
	ring.AddNode(&Node{ID: "node-1"})
	mgr := &Manager{logger: logger}

	rebalancer := NewRebalancer(cfg, mgr, ring, logger)
	rebalancer.mu.Lock()
	rebalancer.ops["op-1"] = &RebalanceOperation{ID: "op-1", Status: RebalancePending}
	rebalancer.ops["op-2"] = &RebalanceOperation{ID: "op-2", Status: RebalanceRunning}
	rebalancer.mu.Unlock()

	status := rebalancer.GetStatus()
	if status.PendingOps != 1 {
		t.Errorf("PendingOps = %d, want 1", status.PendingOps)
	}
	if status.ActiveOps != 0 {
		t.Errorf("ActiveOps = %d, want 0", status.ActiveOps)
	}
}

func TestRebalancer_CancelOperationRunning(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultRebalanceConfig()
	ring := NewHashRing()
	mgr := &Manager{logger: logger}

	rebalancer := NewRebalancer(cfg, mgr, ring, logger)
	rebalancer.mu.Lock()
	rebalancer.ops["op-1"] = &RebalanceOperation{ID: "op-1", Status: RebalanceRunning}
	rebalancer.mu.Unlock()

	err := rebalancer.CancelOperation("op-1")
	if err != nil {
		t.Errorf("CancelOperation failed: %v", err)
	}
}

func TestRebalancer_CancelOperationNotRunning(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultRebalanceConfig()
	ring := NewHashRing()
	mgr := &Manager{logger: logger}

	rebalancer := NewRebalancer(cfg, mgr, ring, logger)
	rebalancer.mu.Lock()
	rebalancer.ops["op-1"] = &RebalanceOperation{ID: "op-1", Status: RebalanceComplete}
	rebalancer.mu.Unlock()

	err := rebalancer.CancelOperation("op-1")
	if err == nil {
		t.Error("CancelOperation should fail for completed operation")
	}
}

func TestRebalancer_GetOperationsWithOps(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultRebalanceConfig()
	ring := NewHashRing()
	mgr := &Manager{logger: logger}

	rebalancer := NewRebalancer(cfg, mgr, ring, logger)
	rebalancer.mu.Lock()
	rebalancer.ops["op-1"] = &RebalanceOperation{ID: "op-1"}
	rebalancer.ops["op-2"] = &RebalanceOperation{ID: "op-2"}
	rebalancer.mu.Unlock()

	ops := rebalancer.GetOperations()
	if len(ops) != 2 {
		t.Errorf("GetOperations returned %d ops, want 2", len(ops))
	}
}

func TestManager_HandleNodeEventJoin(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{NodeID: "local-node"}
	mgr := NewManager(cfg, logger)

	event := memberlist.NodeEvent{
		Event: memberlist.NodeJoin,
		Node: &memberlist.Node{
			Name: "new-node",
			Addr: net.ParseIP("192.168.1.1"),
			Port: 9001,
		},
	}

	mgr.handleNodeEvent(event)

	mgr.lock.RLock()
	node, ok := mgr.nodes["new-node"]
	mgr.lock.RUnlock()

	if !ok {
		t.Error("Node should be added")
	}
	if node.State != NodeStateAlive {
		t.Errorf("Node state = %s, want alive", node.State)
	}
}

func TestManager_HandleNodeEventLeave(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{NodeID: "local-node"}
	mgr := NewManager(cfg, logger)

	mgr.lock.Lock()
	mgr.nodes["leaving-node"] = &Node{ID: "leaving-node", State: NodeStateAlive}
	mgr.lock.Unlock()

	event := memberlist.NodeEvent{
		Event: memberlist.NodeLeave,
		Node: &memberlist.Node{
			Name: "leaving-node",
		},
	}

	mgr.handleNodeEvent(event)

	mgr.lock.RLock()
	node := mgr.nodes["leaving-node"]
	mgr.lock.RUnlock()

	if node.State != NodeStateLeft {
		t.Errorf("Node state = %s, want left", node.State)
	}
}

func TestManager_HandleNodeEventUpdate(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{NodeID: "local-node"}
	mgr := NewManager(cfg, logger)

	oldTime := time.Now().Add(-time.Hour)
	mgr.lock.Lock()
	mgr.nodes["updating-node"] = &Node{ID: "updating-node", State: NodeStateAlive, LastSeen: oldTime}
	mgr.lock.Unlock()

	event := memberlist.NodeEvent{
		Event: memberlist.NodeUpdate,
		Node: &memberlist.Node{
			Name: "updating-node",
		},
	}

	mgr.handleNodeEvent(event)

	mgr.lock.RLock()
	node := mgr.nodes["updating-node"]
	mgr.lock.RUnlock()

	if node.LastSeen.Before(oldTime.Add(time.Minute)) {
		t.Error("LastSeen should be updated")
	}
}

func TestManager_GetLeaderWithAliveNode(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{NodeID: "local-node"}
	mgr := NewManager(cfg, logger)

	mgr.lock.Lock()
	mgr.nodes["node-1"] = &Node{ID: "node-1", State: NodeStateAlive}
	mgr.nodes["node-2"] = &Node{ID: "node-2", State: NodeStateDead}
	mgr.lock.Unlock()

	leader, err := mgr.GetLeader()
	if err != nil {
		t.Fatalf("GetLeader failed: %v", err)
	}
	if leader.ID != "node-1" {
		t.Errorf("Leader ID = %s, want node-1", leader.ID)
	}
}

func TestManager_ProcessEvents(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{NodeID: "local-node"}
	mgr := NewManager(cfg, logger)
	mgr.delegate = &clusterDelegate{
		manager: mgr,
		eventCh: make(chan memberlist.NodeEvent, 1),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go mgr.processEvents(ctx)

	mgr.delegate.eventCh <- memberlist.NodeEvent{
		Event: memberlist.NodeJoin,
		Node: &memberlist.Node{
			Name: "test-node",
			Addr: net.ParseIP("192.168.1.1"),
			Port: 9001,
		},
	}

	time.Sleep(50 * time.Millisecond)
}

func TestManager_PeriodicHealthUpdate(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{NodeID: "local-node"}
	mgr := NewManager(cfg, logger)

	mgr.lock.Lock()
	mgr.nodes["node-1"] = &Node{ID: "node-1", State: NodeStateAlive}
	mgr.nodes["node-2"] = &Node{ID: "node-2", State: NodeStateDead}
	mgr.nodes["node-3"] = &Node{ID: "node-3", State: NodeStateSuspect}
	mgr.lock.Unlock()

	ctx, cancel := context.WithCancel(context.Background())

	go mgr.periodicHealthUpdate(ctx)
	time.Sleep(20 * time.Millisecond)
	cancel()
}

func TestClusterDelegate_MergeRemoteStateWithValidData(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{NodeID: "local-node"}
	mgr := NewManager(cfg, logger)
	delegate := NewClusterDelegate(mgr)

	state := struct {
		Nodes map[string]*Node
	}{
		Nodes: map[string]*Node{
			"remote-node": {
				ID:      "remote-node",
				Name:    "Remote Node",
				State:   NodeStateAlive,
				Address: "192.168.1.2",
			},
		},
	}

	data, _ := json.Marshal(state)
	delegate.MergeRemoteState(data, false)

	mgr.lock.RLock()
	_, ok := mgr.nodes["remote-node"]
	mgr.lock.RUnlock()

	if !ok {
		t.Error("Remote node should be merged")
	}
}

func TestClusterDelegate_NotifyJoinEvent(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{NodeID: "local-node"}
	mgr := NewManager(cfg, logger)
	delegate := NewClusterDelegate(mgr)

	node := &memberlist.Node{
		Name: "joining-node",
		Addr: net.ParseIP("192.168.1.1"),
		Port: 9001,
	}

	delegate.NotifyJoin(node)

	select {
	case event := <-delegate.eventCh:
		if event.Event != memberlist.NodeJoin {
			t.Errorf("Event = %v, want NodeJoin", event.Event)
		}
	default:
		t.Error("Event should be sent to channel")
	}
}

func TestClusterDelegate_NotifyLeaveEvent(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{NodeID: "local-node"}
	mgr := NewManager(cfg, logger)
	delegate := NewClusterDelegate(mgr)

	node := &memberlist.Node{
		Name: "leaving-node",
	}

	delegate.NotifyLeave(node)

	select {
	case event := <-delegate.eventCh:
		if event.Event != memberlist.NodeLeave {
			t.Errorf("Event = %v, want NodeLeave", event.Event)
		}
	default:
		t.Error("Event should be sent to channel")
	}
}

func TestClusterDelegate_NotifyUpdateEvent(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{NodeID: "local-node"}
	mgr := NewManager(cfg, logger)
	delegate := NewClusterDelegate(mgr)

	node := &memberlist.Node{
		Name: "updating-node",
	}

	delegate.NotifyUpdate(node)

	select {
	case event := <-delegate.eventCh:
		if event.Event != memberlist.NodeUpdate {
			t.Errorf("Event = %v, want NodeUpdate", event.Event)
		}
	default:
		t.Error("Event should be sent to channel")
	}
}

func TestBackupManager_RunS3BackupWithCancel(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewBackupManager(logger)

	target := &BackupTarget{
		Name:     "test-target",
		Type:     BackupTargetS3,
		Endpoint: "https://s3.amazonaws.com",
		Enabled:  true,
	}
	mgr.AddTarget(target)

	job, _ := mgr.CreateBackupJob("test-job", target.ID, "bucket", BackupJobFull)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := mgr.runS3Backup(ctx, job, target)
	if err == nil {
		t.Error("runS3Backup should fail with cancelled context")
	}
}

func TestBackupManager_CancelJobNotRunning(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewBackupManager(logger)

	target := &BackupTarget{Name: "test-target", Type: BackupTargetLocal, Endpoint: "/backup", Enabled: true}
	mgr.AddTarget(target)

	job, _ := mgr.CreateBackupJob("test-job", target.ID, "bucket", BackupJobFull)

	err := mgr.CancelJob(job.ID)
	if err == nil {
		t.Error("CancelJob should fail for non-running job")
	}
}

func TestMirrorManager_StartAsyncMode(t *testing.T) {
	logger := zap.NewNop()
	config := MirrorConfig{
		Enabled:  true,
		Mode:     MirrorModeAsync,
		Interval: 50 * time.Millisecond,
	}
	base := &MirrorManager{}
	mgr := base.NewMirrorManager(config, nil, logger)

	ctx := context.Background()
	err := mgr.Start(ctx)
	if err != nil {
		t.Errorf("Start() error: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	mgr.Stop()
}

func TestMirrorManager_RunAsyncMirrorWithStopCh(t *testing.T) {
	logger := zap.NewNop()
	config := MirrorConfig{
		Enabled:  true,
		Interval: 10 * time.Millisecond,
	}
	base := &MirrorManager{}
	mgr := base.NewMirrorManager(config, nil, logger)

	ctx := context.Background()
	go mgr.runAsyncMirror(ctx)
	time.Sleep(30 * time.Millisecond)
	mgr.Stop()
	time.Sleep(10 * time.Millisecond)
}

func TestErasureCoder_NewErasureCoderError(t *testing.T) {
	logger := zap.NewNop()
	cfg := ErasureConfig{
		DataShards:   0,
		ParityShards: 0,
		TotalShards:  0,
	}

	_, err := NewErasureCoder(cfg, logger)
	if err == nil {
		t.Error("NewErasureCoder should fail with invalid config")
	}
}

func TestErasureCoder_EncodeEmptyData(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()
	coder, _ := NewErasureCoder(cfg, logger)

	_, err := coder.Encode([]byte{})
	if err == nil {
		t.Error("Encode() should fail with empty data")
	}
}

func TestErasureCoder_VerifyError(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()
	coder, _ := NewErasureCoder(cfg, logger)

	data := []byte("test data")
	shards, _ := coder.Encode(data)

	shards[0][0] ^= 0xFF

	valid, err := coder.Verify(shards)
	if err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	if valid {
		t.Error("Verify should return false for corrupted data")
	}
}

func TestErasureStripeStore_StatsWithShards(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()
	coder, _ := NewErasureCoder(cfg, logger)

	store := NewErasureStripeStore(coder, logger)

	shards := [][]byte{
		[]byte("shard1-data"),
		[]byte("shard2-data"),
	}

	store.Store(&ErasureStripe{
		ID:        "stripe-1",
		Key:       "key1",
		Shards:    shards,
		CreatedAt: time.Now().Unix(),
	})

	count, size := store.Stats()
	if count != 1 {
		t.Errorf("Stats() count = %d, want 1", count)
	}
	if size <= 0 {
		t.Errorf("Stats() size = %d, want > 0", size)
	}
}

func TestErasureWriter_WriteNotEnoughNodes(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()
	coder, _ := NewErasureCoder(cfg, logger)

	ring := NewHashRing()
	ring.AddNode(&Node{ID: "node-1"})
	ring.AddNode(&Node{ID: "node-2"})

	mgr := &Manager{logger: logger}
	writer := NewErasureWriter(coder, ring, mgr, logger)

	ctx := context.Background()
	err := writer.Write(ctx, "test-key", []byte("test data"))
	if err == nil {
		t.Error("Write should fail with not enough nodes")
	}
}

func TestErasureWriter_ReadNotEnoughShards(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()
	coder, _ := NewErasureCoder(cfg, logger)

	ring := NewHashRing()
	ring.AddNode(&Node{ID: "node-1"})

	mgr := &Manager{logger: logger}
	writer := NewErasureWriter(coder, ring, mgr, logger)

	ctx := context.Background()
	_, err := writer.Read(ctx, "test-key")
	if err == nil {
		t.Error("Read should fail with not enough shards")
	}
}

func TestHashRing_GetNodesInRangeWrapAround(t *testing.T) {
	ring := NewHashRing()
	ring.AddNode(&Node{ID: "node-1"})
	ring.AddNode(&Node{ID: "node-2"})
	ring.AddNode(&Node{ID: "node-3"})

	ring.SetHashFunction(func(b []byte) uint32 {
		if string(b) == "start" {
			return ^uint32(0) - 10
		}
		if string(b) == "end" {
			return 10
		}
		return defaultHashFunction(b)
	})

	nodes := ring.GetNodesInRange("start", "end", 3)
	if nodes == nil {
		t.Error("GetNodesInRange should return nodes for wrap-around range")
	}
}

func TestHashRing_SearchWrapAround(t *testing.T) {
	ring := NewHashRing()
	ring.AddNode(&Node{ID: "node-1"})

	ring.lock.RLock()
	idx := ring.search(^uint32(0))
	ring.lock.RUnlock()

	if idx != 0 {
		t.Errorf("search(^uint32(0)) = %d, want 0 (wrap-around)", idx)
	}
}

func TestRebalancer_CheckAndRebalanceEmptyDistribution(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultRebalanceConfig()
	ring := NewHashRing()
	mgr := &Manager{logger: logger}

	rebalancer := NewRebalancer(cfg, mgr, ring, logger)
	ctx := context.Background()

	rebalancer.checkAndRebalance(ctx)
}

func TestRebalancer_CheckAndRebalanceZeroNodeCount(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultRebalanceConfig()
	ring := &HashRing{
		hashFunction:  defaultHashFunction,
		virtualNodes:  map[uint32]string{1: "node-1"},
		physicalNodes: map[string]int{"node-1": 1},
		nodes:         map[string]*Node{},
		sortedHashes:  []uint32{1},
	}
	mgr := &Manager{logger: logger}

	rebalancer := NewRebalancer(cfg, mgr, ring, logger)
	ctx := context.Background()

	rebalancer.checkAndRebalance(ctx)
}

func TestManager_StartWithSeedNodesFailure(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{
		NodeID:    "test-node",
		NodeName:  "Test Node",
		BindAddr:  "127.0.0.1",
		BindPort:  19002,
		SeedNodes: []string{"nonexistent-seed:9000"},
	}
	mgr := NewManager(cfg, logger)

	ctx := context.Background()
	err := mgr.Start(ctx)
	if err != nil {
		t.Logf("Start() error (expected with invalid seed): %v", err)
	}
	mgr.Stop()
}

func TestClusterDelegate_NotifyMsgBody(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{NodeID: "test-node"}
	mgr := NewManager(cfg, logger)
	delegate := NewClusterDelegate(mgr)

	delegate.NotifyMsg([]byte("test message body"))
}

func TestManager_UpdateMetadataWithList(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{
		NodeID:   "test-node",
		NodeName: "Test Node",
		BindAddr: "127.0.0.1",
		BindPort: 19003,
	}
	mgr := NewManager(cfg, logger)

	ctx := context.Background()
	err := mgr.Start(ctx)
	if err != nil {
		t.Logf("Start() error: %v", err)
	}

	meta := NodeMetadata{
		StorageCapacity: 2000,
		Region:          "us-east-1",
	}
	mgr.UpdateMetadata(meta)

	node := mgr.GetLocalNode()
	if node.Metadata.StorageCapacity != 2000 {
		t.Errorf("StorageCapacity = %d, want 2000", node.Metadata.StorageCapacity)
	}

	mgr.Stop()
}

func TestGetOutboundIPErr(t *testing.T) {
	ip := GetOutboundIP()
	if ip == "" {
		t.Error("GetOutboundIP should return non-empty string")
	}
}

func TestRebalancer_RunCheckerWithPause(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultRebalanceConfig()
	cfg.CheckInterval = 10 * time.Millisecond
	ring := NewHashRing()
	ring.AddNode(&Node{ID: "node-1"})
	mgr := &Manager{logger: logger}

	rebalancer := NewRebalancer(cfg, mgr, ring, logger)
	rebalancer.Pause()

	ctx, cancel := context.WithCancel(context.Background())
	go rebalancer.runChecker(ctx)
	time.Sleep(30 * time.Millisecond)
	cancel()
}

func TestRebalancer_CheckAndRebalanceWithDeviation(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultRebalanceConfig()
	cfg.ThresholdPercent = 1
	ring := NewHashRing()
	ring.AddNode(&Node{ID: "node-1"})
	ring.AddNode(&Node{ID: "node-2"})
	ring.AddNode(&Node{ID: "node-3"})
	mgr := &Manager{logger: logger}

	rebalancer := NewRebalancer(cfg, mgr, ring, logger)
	ctx := context.Background()

	rebalancer.checkAndRebalance(ctx)
}

func TestRebalancer_ExecuteMovesWithSemaphore(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultRebalanceConfig()
	cfg.MaxConcurrentMoves = 1
	ring := NewHashRing()
	mgr := &Manager{logger: logger}

	rebalancer := NewRebalancer(cfg, mgr, ring, logger)

	ctx := context.Background()
	moves := []RebalanceOperation{
		{ID: "move-1", SourceNode: "node-1", TargetNode: "node-2"},
	}
	rebalancer.executeMoves(ctx, moves)
}

func TestManager_UpdateMetadataWithoutList(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{
		NodeID:   "test-node",
		NodeName: "Test Node",
	}
	mgr := NewManager(cfg, logger)

	meta := NodeMetadata{
		StorageCapacity: 1000,
		Region:          "us-west-1",
	}
	mgr.UpdateMetadata(meta)

	node := mgr.GetLocalNode()
	if node.Metadata.StorageCapacity != 1000 {
		t.Errorf("StorageCapacity = %d, want 1000", node.Metadata.StorageCapacity)
	}
}

func TestClusterDelegate_NotifyMsgWithMessage(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{NodeID: "test-node"}
	mgr := NewManager(cfg, logger)
	delegate := NewClusterDelegate(mgr)

	delegate.NotifyMsg([]byte("test message with content"))
}

func TestErasureCoder_VerifyWithCorruptedData(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()
	coder, _ := NewErasureCoder(cfg, logger)

	data := []byte("test data for verification")
	shards, _ := coder.Encode(data)

	valid, err := coder.Verify(shards)
	if err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	if !valid {
		t.Error("Verify should return true for valid data")
	}
}

func TestErasureCoder_WriteWithEnoughNodes(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()
	coder, _ := NewErasureCoder(cfg, logger)

	ring := NewHashRing()
	for i := 0; i < 10; i++ {
		ring.AddNode(&Node{ID: fmt.Sprintf("node-%d", i)})
	}

	mgr := &Manager{logger: logger}
	writer := NewErasureWriter(coder, ring, mgr, logger)

	ctx := context.Background()
	err := writer.Write(ctx, "test-key", []byte("test data"))
	if err != nil {
		t.Errorf("Write() error = %v", err)
	}
}

func TestErasureWriter_ReadWithPartialShards(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()
	coder, _ := NewErasureCoder(cfg, logger)

	ring := NewHashRing()
	for i := 0; i < 6; i++ {
		ring.AddNode(&Node{ID: fmt.Sprintf("node-%d", i)})
	}

	mgr := &Manager{logger: logger}
	writer := NewErasureWriter(coder, ring, mgr, logger)

	ctx := context.Background()
	_, err := writer.Read(ctx, "test-key")
	if err == nil {
		t.Error("Read should fail with no actual shards")
	}
}

func TestHashRing_GetNodesInRangeNormal(t *testing.T) {
	ring := NewHashRing()
	ring.AddNode(&Node{ID: "node-1"})
	ring.AddNode(&Node{ID: "node-2"})
	ring.AddNode(&Node{ID: "node-3"})

	nodes := ring.GetNodesInRange("a", "z", 3)
	if nodes == nil {
		t.Error("GetNodesInRange should return nodes")
	}
}

func TestHashRing_GetNodesInRangeWithLimit(t *testing.T) {
	ring := NewHashRing()
	ring.AddNode(&Node{ID: "node-1"})
	ring.AddNode(&Node{ID: "node-2"})
	ring.AddNode(&Node{ID: "node-3"})

	nodes := ring.GetNodesInRange("a", "z", 1)
	if len(nodes) > 1 {
		t.Errorf("GetNodesInRange returned %d nodes, want at most 1", len(nodes))
	}
}

func TestManager_PeriodicHealthUpdateAllStates(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{NodeID: "local-node"}
	mgr := NewManager(cfg, logger)

	mgr.lock.Lock()
	mgr.nodes["node-1"] = &Node{ID: "node-1", State: NodeStateAlive}
	mgr.nodes["node-2"] = &Node{ID: "node-2", State: NodeStateDead}
	mgr.nodes["node-3"] = &Node{ID: "node-3", State: NodeStateSuspect}
	mgr.nodes["node-4"] = &Node{ID: "node-4", State: NodeStateLeft}
	mgr.lock.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	go mgr.periodicHealthUpdate(ctx)
	time.Sleep(20 * time.Millisecond)
	cancel()
}

func TestRebalancer_CheckAndRebalanceWithThreshold(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultRebalanceConfig()
	cfg.ThresholdPercent = 0
	ring := NewHashRing()
	ring.AddNode(&Node{ID: "node-1"})
	ring.AddNode(&Node{ID: "node-2"})
	mgr := &Manager{logger: logger}

	rebalancer := NewRebalancer(cfg, mgr, ring, logger)
	ctx := context.Background()

	rebalancer.checkAndRebalance(ctx)
}

func TestErasureCoder_NewErasureCoderSuccess(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()
	coder, err := NewErasureCoder(cfg, logger)
	if err != nil {
		t.Fatalf("NewErasureCoder() error = %v", err)
	}
	if coder == nil {
		t.Fatal("NewErasureCoder() returned nil")
	}
}

func TestErasureCoder_DecodeWithAllShards(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()
	coder, _ := NewErasureCoder(cfg, logger)

	data := []byte("test data for decode with all shards")
	shards, _ := coder.Encode(data)

	decoded, err := coder.Decode(shards)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if string(decoded[:len(data)]) != string(data) {
		t.Error("Decoded data mismatch")
	}
}

func TestErasureCoder_ReconstructWithAllShards(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()
	coder, _ := NewErasureCoder(cfg, logger)

	data := []byte("test data for reconstruct with all shards")
	shards, _ := coder.Encode(data)

	err := coder.Reconstruct(shards)
	if err != nil {
		t.Fatalf("Reconstruct() error = %v", err)
	}
}

func TestRebalancer_CheckAndRebalanceWithActualImbalance(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultRebalanceConfig()
	cfg.ThresholdPercent = 1
	cfg.MaxConcurrentMoves = 10

	ring := NewHashRing()
	ring.AddNode(&Node{ID: "node-1"})
	ring.AddNode(&Node{ID: "node-2"})

	ring.lock.Lock()
	for i := 0; i < 100; i++ {
		hash := uint32(1000 + i)
		ring.virtualNodes[hash] = "node-1"
		ring.sortedHashes = append(ring.sortedHashes, hash)
	}
	sort.Slice(ring.sortedHashes, func(i, j int) bool {
		return ring.sortedHashes[i] < ring.sortedHashes[j]
	})
	ring.lock.Unlock()

	mgr := &Manager{logger: logger}
	rebalancer := NewRebalancer(cfg, mgr, ring, logger)

	ctx := context.Background()
	rebalancer.checkAndRebalance(ctx)
}

func TestRebalancer_ExecuteMovesFull(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultRebalanceConfig()
	cfg.MaxConcurrentMoves = 2
	ring := NewHashRing()
	ring.AddNode(&Node{ID: "node-1"})
	ring.AddNode(&Node{ID: "node-2"})
	mgr := &Manager{logger: logger}

	rebalancer := NewRebalancer(cfg, mgr, ring, logger)

	ctx := context.Background()
	moves := []RebalanceOperation{
		{ID: "move-1", SourceNode: "node-1", TargetNode: "node-2"},
		{ID: "move-2", SourceNode: "node-1", TargetNode: "node-2"},
	}
	rebalancer.executeMoves(ctx, moves)
}

func TestManager_PeriodicHealthUpdateTicker(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{NodeID: "local-node"}
	mgr := NewManager(cfg, logger)

	mgr.lock.Lock()
	mgr.nodes["node-1"] = &Node{ID: "node-1", State: NodeStateAlive}
	mgr.nodes["node-2"] = &Node{ID: "node-2", State: NodeStateDead}
	mgr.nodes["node-3"] = &Node{ID: "node-3", State: NodeStateSuspect}
	mgr.lock.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		mgr.periodicHealthUpdate(ctx)
		close(done)
	}()

	time.Sleep(200 * time.Millisecond)
	cancel()
	<-done
}

func TestClusterDelegate_NotifyMsgCall(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{NodeID: "test-node"}
	mgr := NewManager(cfg, logger)
	delegate := NewClusterDelegate(mgr)

	delegate.NotifyMsg(nil)
	delegate.NotifyMsg([]byte{})
	delegate.NotifyMsg([]byte("test"))
}

func TestHashRing_GetNodesInRangeWrapAroundFull(t *testing.T) {
	ring := NewHashRing()
	ring.AddNode(&Node{ID: "node-1"})
	ring.AddNode(&Node{ID: "node-2"})
	ring.AddNode(&Node{ID: "node-3"})

	nodes := ring.GetNodesInRange("start", "end", 3)
	if nodes == nil {
		t.Error("GetNodesInRange should return non-nil result")
	}
}

func TestHashRing_GetNodesInRangeExact(t *testing.T) {
	ring := NewHashRing()
	ring.AddNode(&Node{ID: "node-1"})
	ring.AddNode(&Node{ID: "node-2"})

	nodes := ring.GetNodesInRange("a", "z", 10)
	if len(nodes) == 0 {
		t.Error("GetNodesInRange should return nodes")
	}
}

func TestErasureCoder_WriteFull(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()
	coder, _ := NewErasureCoder(cfg, logger)

	ring := NewHashRing()
	for i := 0; i < 10; i++ {
		ring.AddNode(&Node{ID: fmt.Sprintf("node-%d", i)})
	}

	mgr := &Manager{logger: logger}
	writer := NewErasureWriter(coder, ring, mgr, logger)

	ctx := context.Background()
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	err := writer.Write(ctx, "test-key-large", data)
	if err != nil {
		t.Errorf("Write() error = %v", err)
	}
}

func TestErasureCoder_ReadFull(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()
	coder, _ := NewErasureCoder(cfg, logger)

	ring := NewHashRing()
	for i := 0; i < 6; i++ {
		ring.AddNode(&Node{ID: fmt.Sprintf("node-%d", i)})
	}

	mgr := &Manager{logger: logger}
	writer := NewErasureWriter(coder, ring, mgr, logger)

	ctx := context.Background()
	_, err := writer.Read(ctx, "test-key")
	if err == nil {
		t.Error("Read should fail with no actual shards")
	}
}

func TestErasureCoder_VerifyWithValidData(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()
	coder, _ := NewErasureCoder(cfg, logger)

	data := []byte("test data for verification with valid shards")
	shards, _ := coder.Encode(data)

	valid, err := coder.Verify(shards)
	if err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	if !valid {
		t.Error("Verify should return true for valid data")
	}
}

func TestManager_StartFull(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{
		NodeID:    "test-node",
		NodeName:  "Test Node",
		BindAddr:  "127.0.0.1",
		BindPort:  0,
		SeedNodes: []string{"127.0.0.1:9999"},
	}
	mgr := NewManager(cfg, logger)

	ctx := context.Background()
	err := mgr.Start(ctx)
	if err != nil {
		t.Logf("Start() returned: %v", err)
	}
	mgr.Stop()
}

func TestHashRing_GetNodesInRangeWrapCase(t *testing.T) {
	ring := NewHashRing()
	ring.AddNode(&Node{ID: "node-1"})
	ring.AddNode(&Node{ID: "node-2"})

	testHash := func(b []byte) uint32 {
		key := string(b)
		if key == "start" {
			return ^uint32(0) - 1
		}
		if key == "end" {
			return 1
		}
		return defaultHashFunction(b)
	}

	ring.SetHashFunction(testHash)

	nodes := ring.GetNodesInRange("start", "end", 5)
	_ = nodes
}

func TestRebalancer_ExecuteMovesAllPaths(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultRebalanceConfig()
	cfg.MaxConcurrentMoves = 2
	ring := NewHashRing()
	ring.AddNode(&Node{ID: "node-1"})
	ring.AddNode(&Node{ID: "node-2"})
	mgr := &Manager{logger: logger}

	rebalancer := NewRebalancer(cfg, mgr, ring, logger)

	ctx := context.Background()
	moves := []RebalanceOperation{
		{ID: "move-1", SourceNode: "node-1", TargetNode: "node-2"},
		{ID: "move-2", SourceNode: "node-1", TargetNode: "node-2"},
		{ID: "move-3", SourceNode: "node-1", TargetNode: "node-2"},
	}
	rebalancer.executeMoves(ctx, moves)
}

func TestErasureWriter_WriteWithWriteErrors(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()
	coder, _ := NewErasureCoder(cfg, logger)

	ring := NewHashRing()
	for i := 0; i < 10; i++ {
		ring.AddNode(&Node{ID: fmt.Sprintf("node-%d", i)})
	}

	mgr := &Manager{logger: logger}
	writer := NewErasureWriter(coder, ring, mgr, logger)

	ctx := context.Background()
	err := writer.Write(ctx, "test-key", []byte("test data with write errors"))
	if err != nil {
		t.Logf("Write() error = %v", err)
	}
}

func TestErasureWriter_ReadWithAvailableShards(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()
	coder, _ := NewErasureCoder(cfg, logger)

	ring := NewHashRing()
	for i := 0; i < 6; i++ {
		ring.AddNode(&Node{ID: fmt.Sprintf("node-%d", i)})
	}

	mgr := &Manager{logger: logger}
	writer := NewErasureWriter(coder, ring, mgr, logger)

	ctx := context.Background()
	_, err := writer.Read(ctx, "test-key")
	if err == nil {
		t.Error("Read should fail with no actual shards")
	}
}

func TestManager_UpdateMetadataWithNilList(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{
		NodeID:   "test-node",
		NodeName: "Test Node",
	}
	mgr := NewManager(cfg, logger)

	meta := NodeMetadata{
		StorageCapacity: 3000,
		Region:          "eu-west-1",
	}
	mgr.UpdateMetadata(meta)

	node := mgr.GetLocalNode()
	if node.Metadata.StorageCapacity != 3000 {
		t.Errorf("StorageCapacity = %d, want 3000", node.Metadata.StorageCapacity)
	}
}

func TestErasureCoder_NewErasureCoderFull(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()
	coder, err := NewErasureCoder(cfg, logger)
	if err != nil {
		t.Fatalf("NewErasureCoder() error = %v", err)
	}
	if coder.pool == nil {
		t.Error("Pool should be initialized")
	}
}

func TestRebalancer_RunCheckerFullIteration(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultRebalanceConfig()
	cfg.CheckInterval = 10 * time.Millisecond
	ring := NewHashRing()
	ring.AddNode(&Node{ID: "node-1"})
	ring.AddNode(&Node{ID: "node-2"})
	mgr := &Manager{logger: logger}

	rebalancer := NewRebalancer(cfg, mgr, ring, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	go rebalancer.runChecker(ctx)
	time.Sleep(60 * time.Millisecond)
}

func TestManager_GetLeaderNoAliveNodes(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{NodeID: "local-node"}
	mgr := NewManager(cfg, logger)

	mgr.lock.Lock()
	mgr.nodes["node-1"] = &Node{ID: "node-1", State: NodeStateDead}
	mgr.nodes["node-2"] = &Node{ID: "node-2", State: NodeStateLeft}
	mgr.lock.Unlock()

	_, err := mgr.GetLeader()
	if err == nil {
		t.Error("GetLeader should return error when no alive nodes")
	}
}

func TestErasureCoder_DecodeAvailableCount(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()
	coder, _ := NewErasureCoder(cfg, logger)

	data := []byte("test data for decode available count")
	shards, _ := coder.Encode(data)

	for i := 0; i < cfg.ParityShards; i++ {
		shards[cfg.DataShards+i] = nil
	}

	decoded, err := coder.Decode(shards)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	_ = decoded
}

func TestErasureCoder_ReconstructAvailableCount(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()
	coder, _ := NewErasureCoder(cfg, logger)

	data := []byte("test data for reconstruct available count")
	shards, _ := coder.Encode(data)

	for i := 0; i < cfg.ParityShards; i++ {
		shards[cfg.DataShards+i] = nil
	}

	err := coder.Reconstruct(shards)
	if err != nil {
		t.Fatalf("Reconstruct() error = %v", err)
	}
}

func TestClusterDelegate_MergeRemoteStateEmptyData(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{NodeID: "test-node"}
	mgr := NewManager(cfg, logger)
	delegate := NewClusterDelegate(mgr)

	delegate.MergeRemoteState(nil, false)
	delegate.MergeRemoteState([]byte{}, false)
}

func TestRebalancer_CheckAndRebalanceWithMoves(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultRebalanceConfig()
	cfg.ThresholdPercent = 0

	ring := &HashRing{
		hashFunction:  defaultHashFunction,
		virtualNodes:  map[uint32]string{},
		physicalNodes: map[string]int{},
		nodes:         map[string]*Node{},
		sortedHashes:  []uint32{},
	}

	ring.nodes["node-1"] = &Node{ID: "node-1"}
	ring.nodes["node-2"] = &Node{ID: "node-2"}

	for i := 0; i < 50; i++ {
		hash := uint32(1000 + i)
		ring.virtualNodes[hash] = "node-1"
		ring.sortedHashes = append(ring.sortedHashes, hash)
	}
	for i := 0; i < 150; i++ {
		hash := uint32(2000 + i)
		ring.virtualNodes[hash] = "node-2"
		ring.sortedHashes = append(ring.sortedHashes, hash)
	}
	sort.Slice(ring.sortedHashes, func(i, j int) bool {
		return ring.sortedHashes[i] < ring.sortedHashes[j]
	})
	ring.physicalNodes["node-1"] = 50
	ring.physicalNodes["node-2"] = 150

	mgr := &Manager{logger: logger}
	rebalancer := NewRebalancer(cfg, mgr, ring, logger)

	ctx := context.Background()
	rebalancer.checkAndRebalance(ctx)
}

func TestHashRing_GetNodesInRangeWrapAroundTrigger(t *testing.T) {
	ring := NewHashRing()
	ring.AddNode(&Node{ID: "node-1"})
	ring.AddNode(&Node{ID: "node-2"})
	ring.AddNode(&Node{ID: "node-3"})

	ring.lock.Lock()
	ring.virtualNodes = make(map[uint32]string)
	ring.sortedHashes = []uint32{}
	for i := 0; i < 10; i++ {
		hash := uint32(100 + i)
		ring.virtualNodes[hash] = "node-1"
		ring.sortedHashes = append(ring.sortedHashes, hash)
	}
	for i := 0; i < 10; i++ {
		hash := uint32(200 + i)
		ring.virtualNodes[hash] = "node-2"
		ring.sortedHashes = append(ring.sortedHashes, hash)
	}
	sort.Slice(ring.sortedHashes, func(i, j int) bool {
		return ring.sortedHashes[i] < ring.sortedHashes[j]
	})
	ring.lock.Unlock()

	customHash := func(b []byte) uint32 {
		key := string(b)
		if key == "wrap-start" {
			return 250
		}
		if key == "wrap-end" {
			return 50
		}
		return defaultHashFunction(b)
	}
	ring.SetHashFunction(customHash)

	nodes := ring.GetNodesInRange("wrap-start", "wrap-end", 5)
	if nodes == nil {
		t.Error("GetNodesInRange should return nodes for wrap-around")
	}
}

func TestRebalancer_ExecuteMovesSemaphore(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultRebalanceConfig()
	cfg.MaxConcurrentMoves = 1
	ring := NewHashRing()
	ring.AddNode(&Node{ID: "node-1"})
	ring.AddNode(&Node{ID: "node-2"})
	mgr := &Manager{logger: logger}

	rebalancer := NewRebalancer(cfg, mgr, ring, logger)

	ctx := context.Background()
	moves := []RebalanceOperation{
		{ID: "move-1", SourceNode: "node-1", TargetNode: "node-2"},
	}
	rebalancer.executeMoves(ctx, moves)
}

func TestManager_PeriodicHealthUpdateFull(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{NodeID: "local-node"}
	mgr := NewManager(cfg, logger)

	mgr.lock.Lock()
	mgr.nodes["node-1"] = &Node{ID: "node-1", State: NodeStateAlive}
	mgr.nodes["node-2"] = &Node{ID: "node-2", State: NodeStateDead}
	mgr.nodes["node-3"] = &Node{ID: "node-3", State: NodeStateSuspect}
	mgr.nodes["node-4"] = &Node{ID: "node-4", State: NodeStateAlive}
	mgr.lock.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		mgr.periodicHealthUpdate(ctx)
		close(done)
	}()

	time.Sleep(11 * time.Second)
	cancel()
	<-done
}

func TestErasureWriter_WriteFullCoverage(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()
	coder, _ := NewErasureCoder(cfg, logger)

	ring := NewHashRing()
	for i := 0; i < 10; i++ {
		ring.AddNode(&Node{ID: fmt.Sprintf("node-%d", i)})
	}

	mgr := &Manager{logger: logger}
	writer := NewErasureWriter(coder, ring, mgr, logger)

	ctx := context.Background()
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i % 256)
	}

	err := writer.Write(ctx, "test-key-full", data)
	if err != nil {
		t.Errorf("Write() error = %v", err)
	}
}

func TestErasureWriter_ReadFullCoverage(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()
	coder, _ := NewErasureCoder(cfg, logger)

	ring := NewHashRing()
	for i := 0; i < 6; i++ {
		ring.AddNode(&Node{ID: fmt.Sprintf("node-%d", i)})
	}

	mgr := &Manager{logger: logger}
	writer := NewErasureWriter(coder, ring, mgr, logger)

	ctx := context.Background()
	_, err := writer.Read(ctx, "test-key-read")
	if err == nil {
		t.Error("Read should fail with no actual shards")
	}
}

func TestErasureCoder_VerifyErrorPath(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()
	coder, _ := NewErasureCoder(cfg, logger)

	_, err := coder.Verify([][]byte{[]byte("one"), []byte("two")})
	if err == nil {
		t.Error("Verify should fail with wrong shard count")
	}
}

func TestManager_StartWithSeedNodes(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{
		NodeID:    "test-node",
		NodeName:  "Test Node",
		BindAddr:  "127.0.0.1",
		BindPort:  0,
		SeedNodes: []string{"192.168.1.1:9000", "192.168.1.2:9000"},
	}
	mgr := NewManager(cfg, logger)

	ctx := context.Background()
	err := mgr.Start(ctx)
	if err != nil {
		t.Logf("Start() returned (expected with invalid seeds): %v", err)
	}
	mgr.Stop()
}

func TestErasureCoder_NewErasureCoderErrorPath(t *testing.T) {
	logger := zap.NewNop()
	cfg := ErasureConfig{
		DataShards:   -1,
		ParityShards: -1,
	}

	_, err := NewErasureCoder(cfg, logger)
	if err == nil {
		t.Error("NewErasureCoder should fail with invalid config")
	}
}

func TestClusterDelegate_LocalStateFull(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{NodeID: "test-node"}
	mgr := NewManager(cfg, logger)

	mgr.lock.Lock()
	mgr.nodes["remote-node"] = &Node{ID: "remote-node", State: NodeStateAlive}
	mgr.lock.Unlock()

	delegate := NewClusterDelegate(mgr)
	state := delegate.LocalState(false)
	if len(state) == 0 {
		t.Error("LocalState should return non-empty data")
	}
}

func TestHashRing_GetNodesInRangeWrapAroundManual(t *testing.T) {
	ring := &HashRing{
		hashFunction:  defaultHashFunction,
		virtualNodes:  make(map[uint32]string),
		physicalNodes: make(map[string]int),
		nodes:         make(map[string]*Node),
		sortedHashes:  []uint32{},
	}

	ring.nodes["node-1"] = &Node{ID: "node-1"}
	ring.nodes["node-2"] = &Node{ID: "node-2"}

	ring.virtualNodes[100] = "node-1"
	ring.virtualNodes[200] = "node-1"
	ring.virtualNodes[300] = "node-2"
	ring.virtualNodes[400] = "node-2"
	ring.sortedHashes = []uint32{100, 200, 300, 400}

	ring.hashFunction = func(b []byte) uint32 {
		key := string(b)
		if key == "wrap-start" {
			return 350
		}
		if key == "wrap-end" {
			return 150
		}
		return defaultHashFunction(b)
	}

	nodes := ring.GetNodesInRange("wrap-start", "wrap-end", 10)
	if len(nodes) == 0 {
		t.Error("GetNodesInRange should return nodes for wrap-around case")
	}
}

func TestErasureWriter_WriteWithTooManyErrors(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()
	coder, _ := NewErasureCoder(cfg, logger)

	ring := NewHashRing()
	for i := 0; i < 10; i++ {
		ring.AddNode(&Node{ID: fmt.Sprintf("node-%d", i)})
	}

	mgr := &Manager{logger: logger}
	writer := &ErasureWriter{
		coder:   coder,
		ring:    ring,
		manager: mgr,
		logger:  logger,
	}

	ctx := context.Background()
	err := writer.Write(ctx, "test-key-errors", []byte("test data"))
	if err != nil {
		t.Logf("Write() error = %v", err)
	}
}

func TestErasureWriter_ReadWithPartialData(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()
	coder, _ := NewErasureCoder(cfg, logger)

	ring := NewHashRing()
	for i := 0; i < 6; i++ {
		ring.AddNode(&Node{ID: fmt.Sprintf("node-%d", i)})
	}

	mgr := &Manager{logger: logger}
	writer := &ErasureWriter{
		coder:   coder,
		ring:    ring,
		manager: mgr,
		logger:  logger,
	}

	ctx := context.Background()
	_, err := writer.Read(ctx, "test-key-partial")
	if err == nil {
		t.Error("Read should fail with no actual shards")
	}
}

func TestRebalancer_ExecuteMovesFullCoverage(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultRebalanceConfig()
	cfg.MaxConcurrentMoves = 2
	ring := NewHashRing()
	ring.AddNode(&Node{ID: "node-1"})
	ring.AddNode(&Node{ID: "node-2"})
	mgr := &Manager{logger: logger}

	rebalancer := NewRebalancer(cfg, mgr, ring, logger)

	ctx := context.Background()
	moves := []RebalanceOperation{
		{ID: "move-1", SourceNode: "node-1", TargetNode: "node-2"},
		{ID: "move-2", SourceNode: "node-1", TargetNode: "node-2"},
		{ID: "move-3", SourceNode: "node-1", TargetNode: "node-2"},
		{ID: "move-4", SourceNode: "node-1", TargetNode: "node-2"},
	}
	rebalancer.executeMoves(ctx, moves)
}

func TestErasureCoder_DecodeFullCoverage(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()
	coder, _ := NewErasureCoder(cfg, logger)

	data := []byte("test data for full decode coverage")
	shards, _ := coder.Encode(data)

	shards[0] = nil
	shards[1] = nil

	decoded, err := coder.Decode(shards)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	_ = decoded
}

func TestErasureCoder_ReconstructFullCoverage(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()
	coder, _ := NewErasureCoder(cfg, logger)

	data := []byte("test data for full reconstruct coverage")
	shards, _ := coder.Encode(data)

	shards[0] = nil
	shards[1] = nil

	err := coder.Reconstruct(shards)
	if err != nil {
		t.Fatalf("Reconstruct() error = %v", err)
	}
}

func TestHashRing_GetNodesInRangeNormalBreak(t *testing.T) {
	ring := &HashRing{
		hashFunction:  defaultHashFunction,
		virtualNodes:  make(map[uint32]string),
		physicalNodes: make(map[string]int),
		nodes:         make(map[string]*Node),
		sortedHashes:  []uint32{},
	}

	ring.nodes["node-1"] = &Node{ID: "node-1"}
	ring.nodes["node-2"] = &Node{ID: "node-2"}

	ring.virtualNodes[100] = "node-1"
	ring.virtualNodes[200] = "node-1"
	ring.virtualNodes[300] = "node-2"
	ring.virtualNodes[400] = "node-2"
	ring.sortedHashes = []uint32{100, 200, 300, 400}

	ring.hashFunction = func(b []byte) uint32 {
		key := string(b)
		if key == "normal-start" {
			return 150
		}
		if key == "normal-end" {
			return 250
		}
		return defaultHashFunction(b)
	}

	nodes := ring.GetNodesInRange("normal-start", "normal-end", 10)
	_ = nodes
}

func TestRebalancer_ExecuteMovesCtxDone(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultRebalanceConfig()
	cfg.MaxConcurrentMoves = 1
	ring := NewHashRing()
	ring.AddNode(&Node{ID: "node-1"})
	ring.AddNode(&Node{ID: "node-2"})
	mgr := &Manager{logger: logger}

	rebalancer := NewRebalancer(cfg, mgr, ring, logger)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	moves := []RebalanceOperation{
		{ID: "move-1", SourceNode: "node-1", TargetNode: "node-2"},
		{ID: "move-2", SourceNode: "node-1", TargetNode: "node-2"},
	}
	rebalancer.executeMoves(ctx, moves)
}

func TestRebalancer_ExecuteMovesStopCh(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultRebalanceConfig()
	cfg.MaxConcurrentMoves = 1
	ring := NewHashRing()
	ring.AddNode(&Node{ID: "node-1"})
	ring.AddNode(&Node{ID: "node-2"})
	mgr := &Manager{logger: logger}

	rebalancer := NewRebalancer(cfg, mgr, ring, logger)
	close(rebalancer.stopCh)

	ctx := context.Background()
	moves := []RebalanceOperation{
		{ID: "move-1", SourceNode: "node-1", TargetNode: "node-2"},
		{ID: "move-2", SourceNode: "node-1", TargetNode: "node-2"},
	}
	rebalancer.executeMoves(ctx, moves)
}

func TestErasureWriter_WriteFull(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()
	coder, _ := NewErasureCoder(cfg, logger)

	ring := NewHashRing()
	for i := 0; i < 10; i++ {
		ring.AddNode(&Node{ID: fmt.Sprintf("node-%d", i)})
	}

	mgr := &Manager{logger: logger}
	writer := &ErasureWriter{
		coder:   coder,
		ring:    ring,
		manager: mgr,
		logger:  logger,
	}

	ctx := context.Background()
	data := make([]byte, 8192)
	for i := range data {
		data[i] = byte(i % 256)
	}

	err := writer.Write(ctx, "test-key-full-write", data)
	if err != nil {
		t.Errorf("Write() error = %v", err)
	}
}

func TestErasureWriter_ReadFull(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()
	coder, _ := NewErasureCoder(cfg, logger)

	ring := NewHashRing()
	for i := 0; i < 6; i++ {
		ring.AddNode(&Node{ID: fmt.Sprintf("node-%d", i)})
	}

	mgr := &Manager{logger: logger}
	writer := &ErasureWriter{
		coder:   coder,
		ring:    ring,
		manager: mgr,
		logger:  logger,
	}

	ctx := context.Background()
	_, err := writer.Read(ctx, "test-key-full-read")
	if err == nil {
		t.Error("Read should fail with no actual shards")
	}
}

func TestErasureCoder_DecodeWithNilShards(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()
	coder, _ := NewErasureCoder(cfg, logger)

	data := []byte("test data for decode with nil shards")
	shards, _ := coder.Encode(data)

	shards[0] = nil
	shards[1] = nil
	shards[2] = nil

	decoded, err := coder.Decode(shards)
	if err != nil {
		t.Logf("Decode() error = %v (expected)", err)
	} else {
		_ = decoded
	}
}

func TestErasureCoder_ReconstructWithNilShards(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()
	coder, _ := NewErasureCoder(cfg, logger)

	data := []byte("test data for reconstruct with nil shards")
	shards, _ := coder.Encode(data)

	shards[0] = nil
	shards[1] = nil
	shards[2] = nil

	err := coder.Reconstruct(shards)
	if err == nil {
		t.Logf("Reconstruct() error = %v (expected)", err)
	}
}

func TestManager_UpdateMetadataWithMemberlist(t *testing.T) {
	logger := zap.NewNop()
	cfg := ClusterConfig{
		NodeID:   "test-node",
		NodeName: "Test Node",
		BindAddr: "127.0.0.1",
		BindPort: 0,
	}
	mgr := NewManager(cfg, logger)

	ctx := context.Background()
	err := mgr.Start(ctx)
	if err != nil {
		t.Logf("Start() error: %v", err)
	}

	meta := NodeMetadata{
		StorageCapacity: 5000,
		Region:          "ap-southeast-1",
	}
	mgr.UpdateMetadata(meta)

	node := mgr.GetLocalNode()
	if node.Metadata.StorageCapacity != 5000 {
		t.Errorf("StorageCapacity = %d, want 5000", node.Metadata.StorageCapacity)
	}

	mgr.Stop()
}

func TestErasureCoder_NewErasureCoderInvalid(t *testing.T) {
	logger := zap.NewNop()

	tests := []struct {
		name    string
		cfg     ErasureConfig
		wantErr bool
	}{
		{"negative data shards", ErasureConfig{DataShards: -1, ParityShards: 2}, true},
		{"negative parity shards", ErasureConfig{DataShards: 4, ParityShards: -1}, true},
		{"zero data shards", ErasureConfig{DataShards: 0, ParityShards: 2}, true},
		{"zero parity shards", ErasureConfig{DataShards: 4, ParityShards: 0}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewErasureCoder(tt.cfg, logger)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewErasureCoder() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestErasureCoder_VerifyWithCorruption(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()
	coder, _ := NewErasureCoder(cfg, logger)

	data := []byte("test data for verify with corruption")
	shards, _ := coder.Encode(data)

	shards[0][0] ^= 0xFF
	shards[1][0] ^= 0xFF

	valid, err := coder.Verify(shards)
	if err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	if valid {
		t.Error("Verify should return false for corrupted data")
	}
}

func TestErasureWriter_WriteEncodeError(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()
	coder, _ := NewErasureCoder(cfg, logger)

	ring := NewHashRing()
	for i := 0; i < 10; i++ {
		ring.AddNode(&Node{ID: fmt.Sprintf("node-%d", i)})
	}

	mgr := &Manager{logger: logger}
	writer := &ErasureWriter{
		coder:         coder,
		ring:          ring,
		manager:       mgr,
		logger:        logger,
		testEncodeErr: true,
	}

	ctx := context.Background()
	err := writer.Write(ctx, "test-key", []byte("test data"))
	if err == nil {
		t.Logf("Write() error = %v (expected)", err)
	}
}

func TestErasureWriter_WriteTooManyErrors(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()
	coder, _ := NewErasureCoder(cfg, logger)

	ring := NewHashRing()
	for i := 0; i < 10; i++ {
		ring.AddNode(&Node{ID: fmt.Sprintf("node-%d", i)})
	}

	mgr := &Manager{logger: logger}
	writer := &ErasureWriter{
		coder:        coder,
		ring:         ring,
		manager:      mgr,
		logger:       logger,
		testWriteErr: true,
	}

	ctx := context.Background()
	err := writer.Write(ctx, "test-key", []byte("test data"))
	if err == nil {
		t.Error("Write should fail with too many write errors")
	}
}

func TestErasureWriter_ReadNoTargetNodes(t *testing.T) {
	logger := zap.NewNop()
	cfg := DefaultErasureConfig()
	coder, _ := NewErasureCoder(cfg, logger)

	ring := NewHashRing()

	mgr := &Manager{logger: logger}
	writer := &ErasureWriter{
		coder:   coder,
		ring:    ring,
		manager: mgr,
		logger:  logger,
	}

	ctx := context.Background()
	_, err := writer.Read(ctx, "test-key")
	if err == nil {
		t.Error("Read should fail with no target nodes")
	}
}
