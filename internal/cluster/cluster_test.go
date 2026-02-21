package cluster

import (
	"testing"
	"time"
)

func TestNewCluster(t *testing.T) {
	cfg := Config{
		NodeID:   "node-1",
		NodeName: "test-node",
		BindAddr: "127.0.0.1",
		BindPort: 7946,
	}

	cluster, err := New(cfg)
	if err != nil {
		t.Fatalf("NewCluster failed: %v", err)
	}

	if cluster == nil {
		t.Fatal("Cluster should not be nil")
	}
}

func TestCluster_Join(t *testing.T) {
	cfg := Config{
		NodeID:   "node-1",
		NodeName: "test-node",
		BindAddr: "127.0.0.1",
		BindPort: 7947,
	}

	cluster, _ := New(cfg)

	// Join with no peers should not fail
	err := cluster.Join([]string{})
	if err != nil {
		t.Errorf("Join with empty peers failed: %v", err)
	}
}

func TestCluster_GetNodes(t *testing.T) {
	cfg := Config{
		NodeID:   "node-1",
		NodeName: "test-node",
		BindAddr: "127.0.0.1",
		BindPort: 7948,
	}

	cluster, _ := New(cfg)

	nodes := cluster.GetNodes()
	if nodes == nil {
		t.Error("GetNodes should not return nil")
	}
}

func TestCluster_GetClusterInfo(t *testing.T) {
	cfg := Config{
		NodeID:   "node-1",
		NodeName: "test-node",
		BindAddr: "127.0.0.1",
		BindPort: 7949,
	}

	cluster, _ := New(cfg)

	info := cluster.GetClusterInfo()
	if info == nil {
		t.Error("GetClusterInfo should not return nil")
	}
}

func TestCluster_Leave(t *testing.T) {
	cfg := Config{
		NodeID:   "node-1",
		NodeName: "test-node",
		BindAddr: "127.0.0.1",
		BindPort: 7950,
	}

	cluster, _ := New(cfg)

	err := cluster.Leave()
	if err != nil {
		t.Errorf("Leave failed: %v", err)
	}
}

func TestCluster_Start(t *testing.T) {
	cfg := Config{
		NodeID:   "node-1",
		NodeName: "test-node",
		BindAddr: "127.0.0.1",
		BindPort: 7951,
	}

	cluster, _ := New(cfg)

	// Start should not block
	go cluster.Start()
	time.Sleep(100 * time.Millisecond)

	cluster.Leave()
}

func TestNode_State(t *testing.T) {
	node := &Node{
		ID:    "node-1",
		Name:  "test-node",
		State: StateAlive,
	}

	if node.State != StateAlive {
		t.Errorf("State = %v, want %v", node.State, StateAlive)
	}
}

func TestNode_IsAlive(t *testing.T) {
	node := &Node{
		ID:    "node-1",
		State: StateAlive,
	}

	if !node.IsAlive() {
		t.Error("Node should be alive")
	}

	node.State = StateDead
	if node.IsAlive() {
		t.Error("Node should be dead")
	}
}

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name: "valid config",
			config: Config{
				NodeID:   "node-1",
				BindAddr: "127.0.0.1",
				BindPort: 7946,
			},
			wantErr: false,
		},
		{
			name: "missing node ID",
			config: Config{
				BindAddr: "127.0.0.1",
				BindPort: 7946,
			},
			wantErr: true,
		},
		{
			name: "missing bind address",
			config: Config{
				NodeID:   "node-1",
				BindPort: 7946,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestMemberList(t *testing.T) {
	cluster := &Cluster{
		nodes: make(map[string]*Node),
	}

	// Add nodes
	cluster.nodes["node-1"] = &Node{ID: "node-1", State: StateAlive}
	cluster.nodes["node-2"] = &Node{ID: "node-2", State: StateAlive}
	cluster.nodes["node-3"] = &Node{ID: "node-3", State: StateDead}

	alive := 0
	for _, node := range cluster.nodes {
		if node.IsAlive() {
			alive++
		}
	}

	if alive != 2 {
		t.Errorf("Alive nodes = %d, want 2", alive)
	}
}

func TestCluster_Broadcast(t *testing.T) {
	cfg := Config{
		NodeID:   "node-1",
		NodeName: "test-node",
		BindAddr: "127.0.0.1",
		BindPort: 7952,
	}

	cluster, _ := New(cfg)

	msg := &Message{
		Type: "test",
		Data: []byte("test data"),
	}

	// Broadcast should not fail even with no peers
	err := cluster.Broadcast(msg)
	if err != nil {
		t.Errorf("Broadcast failed: %v", err)
	}
}

func TestCluster_Send(t *testing.T) {
	cfg := Config{
		NodeID:   "node-1",
		NodeName: "test-node",
		BindAddr: "127.0.0.1",
		BindPort: 7953,
	}

	cluster, _ := New(cfg)

	msg := &Message{
		Type: "test",
		Data: []byte("test data"),
	}

	// Send to non-existent node should fail
	err := cluster.Send("non-existent", msg)
	if err == nil {
		t.Error("Send to non-existent node should fail")
	}
}

func TestMessage(t *testing.T) {
	msg := &Message{
		Type:      "test",
		Data:      []byte("test data"),
		Timestamp: time.Now(),
		NodeID:    "node-1",
	}

	if msg.Type != "test" {
		t.Errorf("Type = %s, want test", msg.Type)
	}

	if string(msg.Data) != "test data" {
		t.Errorf("Data = %s, want test data", string(msg.Data))
	}
}
