package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/memberlist"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
)

// NodeState represents the state of a cluster node
type NodeState string

const (
	NodeStateAlive  NodeState = "alive"
	NodeStateSuspect NodeState = "suspect"
	NodeStateDead    NodeState = "dead"
	NodeStateLeft   NodeState = "left"
)

// Node represents a node in the cluster
type Node struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Address   string    `json:"address"`
	Port      int      `json:"port"`
	State     NodeState `json:"state"`
	Version   string    `json:"version"`
	Metadata  NodeMetadata `json:"metadata"`
	JoinTime  time.Time `json:"join_time"`
	LastSeen  time.Time `json:"last_seen"`
}

// Status returns the node status as a string for dashboard display
func (n *Node) Status() string {
	switch n.State {
	case NodeStateAlive:
		return "online"
	case NodeStateSuspect:
		return "degraded"
	case NodeStateDead:
		return "offline"
	case NodeStateLeft:
		return "left"
	default:
		return "unknown"
	}
}

// NodeMetadata contains additional node information
type NodeMetadata struct {
	StorageCapacity  int64   `json:"storage_capacity"`
	StorageUsed      int64   `json:"storage_used"`
	CPUCount         int     `json:"cpu_count"`
	MemoryTotal      int64   `json:"memory_total"`
	Region           string  `json:"region"`
	Zone             string  `json:"zone"`
	DiskType         string  `json:"disk_type"` // SSD, NVMe, HDD
}

// ClusterConfig contains cluster configuration
type ClusterConfig struct {
	NodeID          string
	NodeName        string
	BindAddr        string
	BindPort        int
	ProtocolVersion int
	SeedNodes       []string
	Metadata        NodeMetadata
}

// Metrics
var (
	clusterNodesGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "openendpoint_cluster_nodes_total",
		Help: "Total number of nodes in the cluster",
	})
	clusterHealthGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "openendpoint_cluster_node_health",
		Help: "Health status of the current node (1=healthy, 0=unhealthy",
	}, []string{"node_id"})
	clusterMembersGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "openendpoint_cluster_members",
		Help: "Cluster members by state",
	}, []string{"state"})
)

// Manager handles cluster operations
type Manager struct {
	config      ClusterConfig
	node        *Node
	list        *memberlist.Memberlist
	delegate    *clusterDelegate
	lock        sync.RWMutex
	logger      *zap.Logger
	nodes       map[string]*Node
	events      chan ClusterEvent
	ready       bool
}

// ClusterEvent represents a cluster event
type ClusterEvent struct {
	Type      string    `json:"type"`
	NodeID    string    `json:"node_id"`
	NodeName  string    `json:"node_name"`
	Address   string    `json:"address"`
	Timestamp time.Time `json:"timestamp"`
}

// NewManager creates a new cluster manager
func NewManager(config ClusterConfig, logger *zap.Logger) *Manager {
	return &Manager{
		config: config,
		node: &Node{
			ID:       config.NodeID,
			Name:     config.NodeName,
			Address:  config.BindAddr,
			Port:     config.BindPort,
			State:    NodeStateAlive,
			Version:  "2.0.0",
			Metadata: config.Metadata,
			JoinTime: time.Now(),
			LastSeen: time.Now(),
		},
		logger: logger,
		nodes:  make(map[string]*Node),
		events: make(chan ClusterEvent, 100),
	}
}

// Start starts the cluster manager
func (m *Manager) Start(ctx context.Context) error {
	m.logger.Info("Starting cluster manager",
		zap.String("node_id", m.node.ID),
		zap.String("address", m.node.Address),
		zap.Int("port", m.node.Port))

	// Create delegate
	m.delegate = &clusterDelegate{
		manager: m,
	}

	// Create memberlist config
	cfg := memberlist.DefaultLocalConfig()
	cfg.Name = m.node.Name
	cfg.BindAddr = m.config.BindAddr
	cfg.BindPort = m.config.BindPort
	cfg.Delegate = m.delegate
	cfg.ProtocolVersion = uint8(m.config.ProtocolVersion)

	// Set up push/pull for metadata sync
	cfg.PushPullInterval = 30 * time.Second

	// Enable compression
	cfg.EnableCompression = true

	// Create memberlist
	list, err := memberlist.Create(cfg)
	if err != nil {
		return fmt.Errorf("failed to create memberlist: %w", err)
	}
	m.list = list

	// Join cluster with seed nodes
	if len(m.config.SeedNodes) > 0 {
		n, err := list.Join(m.config.SeedNodes)
		if err != nil {
			m.logger.Warn("Failed to join seed nodes, starting standalone",
				zap.Error(err),
				zap.Strings("seeds", m.config.SeedNodes))
		} else {
			m.logger.Info("Joined cluster",
				zap.Int("nodes_joined", n))
		}
	}

	// Add self to nodes
	m.lock.Lock()
	m.nodes[m.node.ID] = m.node
	m.lock.Unlock()

	m.ready = true
	clusterNodesGauge.Set(1)
	clusterHealthGauge.WithLabelValues(m.node.ID).Set(1)

	// Start event processor
	go m.processEvents(ctx)

	// Start periodic health updates
	go m.periodicHealthUpdate(ctx)

	m.logger.Info("Cluster manager started", zap.String("node_id", m.node.ID))
	return nil
}

// Stop stops the cluster manager
func (m *Manager) Stop() error {
	m.logger.Info("Stopping cluster manager")
	m.ready = false

	if m.list != nil {
		m.list.Leave(5 * time.Second)
		m.list.Shutdown()
	}
	close(m.events)

	clusterHealthGauge.WithLabelValues(m.node.ID).Set(0)
	m.logger.Info("Cluster manager stopped")
	return nil
}

// Members returns cluster members
func (m *Manager) Members() []*Node {
	m.lock.RLock()
	defer m.lock.RUnlock()

	result := make([]*Node, 0, len(m.nodes))
	for _, node := range m.nodes {
		result = append(result, node)
	}
	return result
}

// GetMember returns a specific member by ID
func (m *Manager) GetMember(nodeID string) (*Node, bool) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	node, ok := m.nodes[nodeID]
	return node, ok
}

// IsLeader checks if this node is the leader (simplified - single leader)
func (m *Manager) IsLeader() bool {
	return true
}

// GetLeader returns the current leader node
func (m *Manager) GetLeader() (*Node, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	// Simplified: return first alive node
	for _, node := range m.nodes {
		if node.State == NodeStateAlive {
			return node, nil
		}
	}
	return nil, fmt.Errorf("no leader found")
}

// Events returns the event channel
func (m *Manager) Events() <-chan ClusterEvent {
	return m.events
}

// IsReady returns whether the cluster is ready
func (m *Manager) IsReady() bool {
	return m.ready
}

// NodeCount returns the number of nodes in the cluster
func (m *Manager) NodeCount() int {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return len(m.nodes)
}

// UpdateMetadata updates the node metadata
func (m *Manager) UpdateMetadata(meta NodeMetadata) {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.node.Metadata = meta
	m.node.LastSeen = time.Now()

	// Broadcast updated metadata
	if m.list != nil {
		m.list.UpdateNode(1 * time.Second)
	}
}

// GetLocalNode returns the local node
func (m *Manager) GetLocalNode() *Node {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return m.node
}

// periodicHealthUpdate updates node health metrics periodically
func (m *Manager) periodicHealthUpdate(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.lock.RLock()
			count := len(m.nodes)
			alive := 0
			dead := 0
			suspect := 0
			for _, n := range m.nodes {
				switch n.State {
				case NodeStateAlive:
					alive++
				case NodeStateDead:
					dead++
				case NodeStateSuspect:
					suspect++
				}
			}
			m.lock.RUnlock()

			clusterNodesGauge.Set(float64(count))
			clusterMembersGauge.WithLabelValues("alive").Set(float64(alive))
			clusterMembersGauge.WithLabelValues("dead").Set(float64(dead))
			clusterMembersGauge.WithLabelValues("suspect").Set(float64(suspect))
		}
	}
}

// processEvents processes cluster events
func (m *Manager) processEvents(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-m.delegate.eventCh:
			m.handleNodeEvent(event)
		}
	}
}

// handleNodeEvent handles node join/leave/update events
func (m *Manager) handleNodeEvent(event memberlist.NodeEvent) {
	node := event.Node

	m.lock.Lock()
	defer m.lock.Unlock()

	switch event.Event {
	case memberlist.NodeJoin:
		m.logger.Info("Node joined cluster",
			zap.String("node", node.Name),
			zap.String("addr", node.Address()))

		// Create node entry
		newNode := &Node{
			ID:        node.Name,
			Name:      node.Name,
			Address:   node.Address(),
			Port:      int(node.Port),
			State:     NodeStateAlive,
			Version:   "2.0.0",
			JoinTime:  time.Now(),
			LastSeen:  time.Now(),
		}
		m.nodes[newNode.ID] = newNode

		m.events <- ClusterEvent{
			Type:      "node_join",
			NodeID:    newNode.ID,
			NodeName:  newNode.Name,
			Address:   newNode.Address,
			Timestamp: time.Now(),
		}

	case memberlist.NodeLeave:
		m.logger.Info("Node left cluster",
			zap.String("node", node.Name))

		if n, ok := m.nodes[node.Name]; ok {
			n.State = NodeStateLeft
			n.LastSeen = time.Now()
		}

		m.events <- ClusterEvent{
			Type:      "node_leave",
			NodeID:    node.Name,
			NodeName:  node.Name,
			Timestamp: time.Now(),
		}

	case memberlist.NodeUpdate:
		if n, ok := m.nodes[node.Name]; ok {
			n.LastSeen = time.Now()
		}
	}
}

// clusterDelegate implements memberlist.Delegate
type clusterDelegate struct {
	manager  *Manager
	eventCh  chan memberlist.NodeEvent
	nodeMeta []byte
}

// NewClusterDelegate creates a new cluster delegate
func NewClusterDelegate(m *Manager) *clusterDelegate {
	return &clusterDelegate{
		manager: m,
		eventCh: make(chan memberlist.NodeEvent, 100),
	}
}

// NodeMeta returns metadata for local node
func (d *clusterDelegate) NodeMeta(limit int) []byte {
	d.manager.lock.RLock()
	meta := NodeMetadata{
		StorageCapacity: d.manager.node.Metadata.StorageCapacity,
		StorageUsed:     d.manager.node.Metadata.StorageUsed,
		CPUCount:        d.manager.node.Metadata.CPUCount,
		MemoryTotal:     d.manager.node.Metadata.MemoryTotal,
		Region:          d.manager.node.Metadata.Region,
		Zone:            d.manager.node.Metadata.Zone,
		DiskType:        d.manager.node.Metadata.DiskType,
	}
	d.manager.lock.RUnlock()

	data, _ := json.Marshal(meta)
	return data
}

// NotifyMsg handles incoming messages
func (d *clusterDelegate) NotifyMsg(msg []byte) {
	// Handle replication/同步 messages here
}

// GetBroadcasts handles broadcast messages
func (d *clusterDelegate) GetBroadcasts(overhead, limit int) [][]byte {
	return nil
}

// LocalState returns local state for push/pull
func (d *clusterDelegate) LocalState(join bool) []byte {
	d.manager.lock.RLock()
	defer d.manager.lock.RUnlock()

	state := struct {
		Nodes map[string]*Node
	}{
		Nodes: d.manager.nodes,
	}

	data, _ := json.Marshal(state)
	return data
}

// MergeRemoteState merges remote state
func (d *clusterDelegate) MergeRemoteState(data []byte, join bool) {
	if len(data) == 0 {
		return
	}

	var state struct {
		Nodes map[string]*Node
	}
	if err := json.Unmarshal(data, &state); err != nil {
		return
	}

	d.manager.lock.Lock()
	defer d.manager.lock.Unlock()

	for id, node := range state.Nodes {
		if _, ok := d.manager.nodes[id]; !ok {
			d.manager.nodes[id] = node
		}
	}
}

// NotifyJoin handles node join event
func (d *clusterDelegate) NotifyJoin(node *memberlist.Node) {
	d.eventCh <- memberlist.NodeEvent{
		Event: memberlist.NodeJoin,
		Node:  node,
	}
}

// NotifyLeave handles node leave event
func (d *clusterDelegate) NotifyLeave(node *memberlist.Node) {
	d.eventCh <- memberlist.NodeEvent{
		Event: memberlist.NodeLeave,
		Node:  node,
	}
}

// NotifyUpdate handles node update event
func (d *clusterDelegate) NotifyUpdate(node *memberlist.Node) {
	d.eventCh <- memberlist.NodeEvent{
		Event: memberlist.NodeUpdate,
		Node:  node,
	}
}

// GenerateNodeID generates a new node ID
func GenerateNodeID() string {
	return uuid.New().String()
}

// GetOutboundIP gets the outbound IP address
func GetOutboundIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return "127.0.0.1"
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP.String()
}
