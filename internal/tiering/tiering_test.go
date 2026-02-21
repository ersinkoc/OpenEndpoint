package tiering

import (
	"testing"
	"time"
)

func TestNewManager(t *testing.T) {
	config := &Config{
		Tiers: []TierConfig{
			{Name: "STANDARD", Backend: "local"},
			{Name: "GLACIER", Backend: "local"},
		},
	}

	mgr, err := NewManager(config)
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}

	if mgr == nil {
		t.Fatal("Manager should not be nil")
	}
}

func TestManager_GetTier(t *testing.T) {
	config := &Config{
		Tiers: []TierConfig{
			{Name: "STANDARD", Backend: "local"},
			{Name: "GLACIER", Backend: "local"},
		},
	}

	mgr, _ := NewManager(config)

	tier, err := mgr.GetTier("STANDARD")
	if err != nil {
		t.Fatalf("GetTier failed: %v", err)
	}

	if tier.Name != "STANDARD" {
		t.Errorf("Tier name = %s, want STANDARD", tier.Name)
	}
}

func TestManager_GetTier_NotFound(t *testing.T) {
	config := &Config{
		Tiers: []TierConfig{
			{Name: "STANDARD", Backend: "local"},
		},
	}

	mgr, _ := NewManager(config)

	_, err := mgr.GetTier("NON_EXISTENT")
	if err == nil {
		t.Error("GetTier should fail for non-existent tier")
	}
}

func TestManager_TransitionObject(t *testing.T) {
	config := &Config{
		Tiers: []TierConfig{
			{Name: "STANDARD", Backend: "local"},
			{Name: "GLACIER", Backend: "local"},
		},
	}

	mgr, _ := NewManager(config)

	object := &ObjectInfo{
		Bucket:    "test-bucket",
		Key:       "test-key",
		Size:      1024,
		Tier:      "STANDARD",
		UpdatedAt: time.Now(),
	}

	err := mgr.TransitionObject(object, "GLACIER")
	if err != nil {
		t.Fatalf("TransitionObject failed: %v", err)
	}
}

func TestManager_TransitionObject_SameTier(t *testing.T) {
	config := &Config{
		Tiers: []TierConfig{
			{Name: "STANDARD", Backend: "local"},
		},
	}

	mgr, _ := NewManager(config)

	object := &ObjectInfo{
		Bucket: "test-bucket",
		Key:    "test-key",
		Tier:   "STANDARD",
	}

	// Transition to same tier should succeed
	err := mgr.TransitionObject(object, "STANDARD")
	if err != nil {
		t.Errorf("Transition to same tier failed: %v", err)
	}
}

func TestManager_GetTransitionCandidates(t *testing.T) {
	config := &Config{
		Tiers: []TierConfig{
			{Name: "STANDARD", Backend: "local"},
			{Name: "GLACIER", Backend: "local"},
		},
	}

	mgr, _ := NewManager(config)

	candidates, err := mgr.GetTransitionCandidates("test-bucket", "GLACIER", 30*24*time.Hour)
	if err != nil {
		t.Fatalf("GetTransitionCandidates failed: %v", err)
	}

	// May be empty if no objects match criteria
	if candidates == nil {
		t.Error("Candidates should not be nil")
	}
}

func TestManager_ListTiers(t *testing.T) {
	config := &Config{
		Tiers: []TierConfig{
			{Name: "STANDARD", Backend: "local"},
			{Name: "GLACIER", Backend: "local"},
		},
	}

	mgr, _ := NewManager(config)

	tiers := mgr.ListTiers()
	if len(tiers) != 2 {
		t.Errorf("Tier count = %d, want 2", len(tiers))
	}
}

func TestManager_GetObjectTier(t *testing.T) {
	config := &Config{
		Tiers: []TierConfig{
			{Name: "STANDARD", Backend: "local"},
		},
	}

	mgr, _ := NewManager(config)

	// Should return default tier
	tier := mgr.GetObjectTier("test-bucket", "test-key")
	if tier != "STANDARD" {
		t.Errorf("Object tier = %s, want STANDARD", tier)
	}
}

func TestManager_CalculateSavings(t *testing.T) {
	config := &Config{
		Tiers: []TierConfig{
			{Name: "STANDARD", CostPerGB: 0.023},
			{Name: "GLACIER", CostPerGB: 0.004},
		},
	}

	mgr, _ := NewManager(config)

	savings := mgr.CalculateSavings(1024*1024*1024, "STANDARD", "GLACIER")

	if savings <= 0 {
		t.Error("Savings should be positive when moving to cheaper tier")
	}
}

func TestTierConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  TierConfig
		wantErr bool
	}{
		{"valid config", TierConfig{Name: "STANDARD", Backend: "local"}, false},
		{"empty name", TierConfig{Backend: "local"}, true},
		{"empty backend", TierConfig{Name: "STANDARD"}, true},
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

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  *Config
		wantErr bool
	}{
		{"valid config", &Config{Tiers: []TierConfig{{Name: "STANDARD", Backend: "local"}}}, false},
		{"empty tiers", &Config{Tiers: []TierConfig{}}, true},
		{"nil config", nil, true},
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

func TestObjectInfo(t *testing.T) {
	now := time.Now()
	obj := &ObjectInfo{
		Bucket:    "test-bucket",
		Key:       "test-key",
		Size:      1024,
		Tier:      "STANDARD",
		UpdatedAt: now,
	}

	if obj.Bucket != "test-bucket" {
		t.Errorf("Bucket = %s, want test-bucket", obj.Bucket)
	}

	if obj.Size != 1024 {
		t.Errorf("Size = %d, want 1024", obj.Size)
	}
}

func TestManager_Concurrent(t *testing.T) {
	config := &Config{
		Tiers: []TierConfig{
			{Name: "STANDARD", Backend: "local"},
			{Name: "GLACIER", Backend: "local"},
		},
	}

	mgr, _ := NewManager(config)
	done := make(chan bool)

	for i := 0; i < 10; i++ {
		go func() {
			mgr.GetTier("STANDARD")
			mgr.ListTiers()
			done <- true
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}
