package federation

import (
	"testing"
	"time"
)

func TestNewManager(t *testing.T) {
	mgr := NewManager()
	if mgr == nil {
		t.Fatal("Manager should not be nil")
	}
}

func TestManager_AddEndpoint(t *testing.T) {
	mgr := NewManager()

	endpoint := &Endpoint{
		ID:      "endpoint-1",
		Name:    "remote-site",
		URL:     "https://remote.example.com",
		Enabled: true,
	}

	err := mgr.AddEndpoint(endpoint)
	if err != nil {
		t.Fatalf("AddEndpoint failed: %v", err)
	}
}

func TestManager_GetEndpoint(t *testing.T) {
	mgr := NewManager()

	endpoint := &Endpoint{ID: "endpoint-1", Name: "remote"}
	mgr.AddEndpoint(endpoint)

	ep, err := mgr.GetEndpoint("endpoint-1")
	if err != nil {
		t.Fatalf("GetEndpoint failed: %v", err)
	}

	if ep.Name != "remote" {
		t.Errorf("Name = %s, want remote", ep.Name)
	}
}

func TestManager_GetEndpoint_NotFound(t *testing.T) {
	mgr := NewManager()

	_, err := mgr.GetEndpoint("non-existent")
	if err == nil {
		t.Error("Should fail for non-existent endpoint")
	}
}

func TestManager_RemoveEndpoint(t *testing.T) {
	mgr := NewManager()

	endpoint := &Endpoint{ID: "endpoint-1"}
	mgr.AddEndpoint(endpoint)

	err := mgr.RemoveEndpoint("endpoint-1")
	if err != nil {
		t.Fatalf("RemoveEndpoint failed: %v", err)
	}

	_, err = mgr.GetEndpoint("endpoint-1")
	if err == nil {
		t.Error("Endpoint should be removed")
	}
}

func TestManager_ListEndpoints(t *testing.T) {
	mgr := NewManager()

	endpoints := mgr.ListEndpoints()
	if len(endpoints) != 0 {
		t.Errorf("Empty list = %d, want 0", len(endpoints))
	}

	mgr.AddEndpoint(&Endpoint{ID: "ep1"})
	mgr.AddEndpoint(&Endpoint{ID: "ep2"})

	endpoints = mgr.ListEndpoints()
	if len(endpoints) != 2 {
		t.Errorf("Endpoint count = %d, want 2", len(endpoints))
	}
}

func TestManager_EnableEndpoint(t *testing.T) {
	mgr := NewManager()

	endpoint := &Endpoint{ID: "endpoint-1", Enabled: false}
	mgr.AddEndpoint(endpoint)

	err := mgr.EnableEndpoint("endpoint-1")
	if err != nil {
		t.Fatalf("EnableEndpoint failed: %v", err)
	}

	ep, _ := mgr.GetEndpoint("endpoint-1")
	if !ep.Enabled {
		t.Error("Endpoint should be enabled")
	}
}

func TestManager_DisableEndpoint(t *testing.T) {
	mgr := NewManager()

	endpoint := &Endpoint{ID: "endpoint-1", Enabled: true}
	mgr.AddEndpoint(endpoint)

	err := mgr.DisableEndpoint("endpoint-1")
	if err != nil {
		t.Fatalf("DisableEndpoint failed: %v", err)
	}

	ep, _ := mgr.GetEndpoint("endpoint-1")
	if ep.Enabled {
		t.Error("Endpoint should be disabled")
	}
}

func TestManager_ReplicateObject(t *testing.T) {
	mgr := NewManager()

	endpoint := &Endpoint{ID: "endpoint-1", URL: "https://remote.example.com"}
	mgr.AddEndpoint(endpoint)

	err := mgr.ReplicateObject("endpoint-1", "bucket", "key", []byte("data"))
	if err != nil {
		t.Fatalf("ReplicateObject failed: %v", err)
	}
}

func TestEndpoint(t *testing.T) {
	endpoint := &Endpoint{
		ID:         "endpoint-1",
		Name:       "remote-site",
		URL:        "https://remote.example.com",
		Enabled:    true,
		LastSync:   time.Now(),
		Statistics: &EndpointStats{TotalObjects: 100},
	}

	if endpoint.ID != "endpoint-1" {
		t.Errorf("ID = %s, want endpoint-1", endpoint.ID)
	}

	if !endpoint.Enabled {
		t.Error("Endpoint should be enabled")
	}
}

func TestEndpointStats(t *testing.T) {
	stats := &EndpointStats{
		TotalObjects:  1000,
		TotalBytes:    1024 * 1024,
		LastSyncTime:  time.Now(),
		ReplicationLag: 5 * time.Second,
	}

	if stats.TotalObjects != 1000 {
		t.Errorf("TotalObjects = %d, want 1000", stats.TotalObjects)
	}
}

func TestFederationConfig(t *testing.T) {
	config := &FederationConfig{
		Name:         "federation-1",
		Primary:      true,
		Endpoints:    []string{"ep1", "ep2"},
		SyncInterval: 60 * time.Second,
	}

	if config.Name != "federation-1" {
		t.Errorf("Name = %s, want federation-1", config.Name)
	}

	if !config.Primary {
		t.Error("Should be primary")
	}
}

func TestManager_Concurrent(t *testing.T) {
	mgr := NewManager()
	done := make(chan bool)

	for i := 0; i < 10; i++ {
		go func(id int) {
			ep := &Endpoint{ID: string(rune('A' + id))}
			mgr.AddEndpoint(ep)
			mgr.GetEndpoint(ep.ID)
			done <- true
		}(i)
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}
