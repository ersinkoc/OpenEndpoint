package cdn

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

func TestManager_AddProvider(t *testing.T) {
	mgr := NewManager()

	provider := &MockProvider{}
	err := mgr.AddProvider("cloudflare", provider)
	if err != nil {
		t.Fatalf("AddProvider failed: %v", err)
	}
}

func TestManager_GetProvider(t *testing.T) {
	mgr := NewManager()

	provider := &MockProvider{}
	mgr.AddProvider("cloudflare", provider)

	p, ok := mgr.GetProvider("cloudflare")
	if !ok {
		t.Fatal("Provider should exist")
	}

	if p == nil {
		t.Fatal("Provider should not be nil")
	}
}

func TestManager_GetProvider_NotFound(t *testing.T) {
	mgr := NewManager()

	_, ok := mgr.GetProvider("non-existent")
	if ok {
		t.Error("Provider should not exist")
	}
}

func TestManager_RemoveProvider(t *testing.T) {
	mgr := NewManager()

	provider := &MockProvider{}
	mgr.AddProvider("cloudflare", provider)
	mgr.RemoveProvider("cloudflare")

	_, ok := mgr.GetProvider("cloudflare")
	if ok {
		t.Error("Provider should be removed")
	}
}

func TestManager_CreateDistribution(t *testing.T) {
	mgr := NewManager()

	provider := &MockProvider{}
	mgr.AddProvider("cloudflare", provider)

	config := DistributionConfig{
		Origins: []Origin{
			{Domain: "example.com"},
		},
	}

	id, err := mgr.CreateDistribution("cloudflare", config)
	if err != nil {
		t.Fatalf("CreateDistribution failed: %v", err)
	}

	if id == "" {
		t.Error("Distribution ID should not be empty")
	}
}

func TestManager_InvalidateCache(t *testing.T) {
	mgr := NewManager()

	provider := &MockProvider{}
	mgr.AddProvider("cloudflare", provider)

	err := mgr.InvalidateCache("cloudflare", "dist-123", []string{"/path/*"})
	if err != nil {
		t.Fatalf("InvalidateCache failed: %v", err)
	}
}

func TestManager_ListDistributions(t *testing.T) {
	mgr := NewManager()

	provider := &MockProvider{
		Distributions: []DistributionInfo{
			{ID: "dist-1", Status: "Deployed"},
		},
	}
	mgr.AddProvider("cloudflare", provider)

	distributions, err := mgr.ListDistributions("cloudflare")
	if err != nil {
		t.Fatalf("ListDistributions failed: %v", err)
	}

	if len(distributions) != 1 {
		t.Errorf("Distribution count = %d, want 1", len(distributions))
	}
}

func TestDistributionConfig(t *testing.T) {
	config := DistributionConfig{
		Origins: []Origin{
			{Domain: "origin.example.com"},
		},
		Enabled: true,
	}

	if len(config.Origins) != 1 {
		t.Errorf("Origin count = %d, want 1", len(config.Origins))
	}

	if !config.Enabled {
		t.Error("Should be enabled")
	}
}

func TestDistributionInfo(t *testing.T) {
	info := DistributionInfo{
		ID:         "dist-123",
		Status:     "Deployed",
		DomainName: "dist.example.com",
		LastModified: time.Now(),
	}

	if info.ID != "dist-123" {
		t.Errorf("ID = %s, want dist-123", info.ID)
	}

	if info.Status != "Deployed" {
		t.Errorf("Status = %s, want Deployed", info.Status)
	}
}

func TestOrigin(t *testing.T) {
	origin := Origin{
		Domain:      "origin.example.com",
		Port:        443,
		Protocol:    "https",
	}

	if origin.Domain != "origin.example.com" {
		t.Errorf("Domain = %s, want origin.example.com", origin.Domain)
	}

	if origin.Port != 443 {
		t.Errorf("Port = %d, want 443", origin.Port)
	}
}

// Mock Provider for testing
type MockProvider struct {
	Distributions []DistributionInfo
}

func (m *MockProvider) CreateDistribution(ctx interface{}, config DistributionConfig) (string, error) {
	return "mock-dist-id", nil
}

func (m *MockProvider) GetDistribution(ctx interface{}, id string) (DistributionInfo, error) {
	return DistributionInfo{ID: id, Status: "Deployed"}, nil
}

func (m *MockProvider) DeleteDistribution(ctx interface{}, id string) error {
	return nil
}

func (m *MockProvider) ListDistributions(ctx interface{}) ([]DistributionInfo, error) {
	return m.Distributions, nil
}

func (m *MockProvider) InvalidateCache(ctx interface{}, distributionID string, paths []string) error {
	return nil
}

func (m *MockProvider) GetInvalidationStatus(ctx interface{}, distributionID, invalidationID string) (string, error) {
	return "Complete", nil
}

func TestManager_Concurrent(t *testing.T) {
	mgr := NewManager()
	provider := &MockProvider{}
	mgr.AddProvider("test", provider)

	done := make(chan bool)

	for i := 0; i < 10; i++ {
		go func() {
			mgr.GetProvider("test")
			mgr.ListDistributions("test")
			done <- true
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}
