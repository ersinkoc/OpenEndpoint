package quota

import (
	"testing"
)

func TestNewManager(t *testing.T) {
	mgr := NewManager()
	if mgr == nil {
		t.Fatal("Manager should not be nil")
	}
}

func TestManager_SetQuota(t *testing.T) {
	mgr := NewManager()

	quota := &Quota{
		MaxSize:   1024 * 1024 * 1024, // 1GB
		MaxObjects: 1000,
	}

	err := mgr.SetQuota("test-bucket", quota)
	if err != nil {
		t.Fatalf("SetQuota failed: %v", err)
	}
}

func TestManager_GetQuota(t *testing.T) {
	mgr := NewManager()

	quota := &Quota{
		MaxSize:    1024 * 1024,
		MaxObjects: 100,
	}
	mgr.SetQuota("test-bucket", quota)

	result, err := mgr.GetQuota("test-bucket")
	if err != nil {
		t.Fatalf("GetQuota failed: %v", err)
	}

	if result.MaxSize != 1024*1024 {
		t.Errorf("MaxSize = %d, want %d", result.MaxSize, 1024*1024)
	}
}

func TestManager_GetQuota_NotFound(t *testing.T) {
	mgr := NewManager()

	_, err := mgr.GetQuota("non-existent")
	if err == nil {
		t.Error("GetQuota should fail for non-existent bucket")
	}
}

func TestManager_DeleteQuota(t *testing.T) {
	mgr := NewManager()

	quota := &Quota{MaxSize: 1024}
	mgr.SetQuota("test-bucket", quota)

	err := mgr.DeleteQuota("test-bucket")
	if err != nil {
		t.Fatalf("DeleteQuota failed: %v", err)
	}

	_, err = mgr.GetQuota("test-bucket")
	if err == nil {
		t.Error("Quota should be deleted")
	}
}

func TestManager_CheckQuota(t *testing.T) {
	mgr := NewManager()

	quota := &Quota{
		MaxSize:    1000,
		MaxObjects: 10,
	}
	mgr.SetQuota("test-bucket", quota)

	tests := []struct {
		name       string
		size       int64
		objects    int
		shouldPass bool
	}{
		{"within limits", 500, 5, true},
		{"at size limit", 1000, 5, true},
		{"over size limit", 1001, 5, false},
		{"at object limit", 500, 10, true},
		{"over object limit", 500, 11, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := mgr.CheckQuota("test-bucket", tt.size, tt.objects)
			if (err == nil) != tt.shouldPass {
				t.Errorf("CheckQuota() pass = %v, want %v", err == nil, tt.shouldPass)
			}
		})
	}
}

func TestManager_UpdateUsage(t *testing.T) {
	mgr := NewManager()

	err := mgr.UpdateUsage("test-bucket", 1024, 5)
	if err != nil {
		t.Fatalf("UpdateUsage failed: %v", err)
	}
}

func TestManager_GetUsage(t *testing.T) {
	mgr := NewManager()

	mgr.UpdateUsage("test-bucket", 2048, 10)

	usage, err := mgr.GetUsage("test-bucket")
	if err != nil {
		t.Fatalf("GetUsage failed: %v", err)
	}

	if usage.Size != 2048 {
		t.Errorf("Size = %d, want 2048", usage.Size)
	}

	if usage.Objects != 10 {
		t.Errorf("Objects = %d, want 10", usage.Objects)
	}
}

func TestManager_ResetUsage(t *testing.T) {
	mgr := NewManager()

	mgr.UpdateUsage("test-bucket", 1024, 5)

	err := mgr.ResetUsage("test-bucket")
	if err != nil {
		t.Fatalf("ResetUsage failed: %v", err)
	}

	usage, _ := mgr.GetUsage("test-bucket")
	if usage.Size != 0 || usage.Objects != 0 {
		t.Error("Usage should be reset to 0")
	}
}

func TestManager_ListQuotas(t *testing.T) {
	mgr := NewManager()

	// Empty list
	quotas := mgr.ListQuotas()
	if len(quotas) != 0 {
		t.Errorf("Empty list should have 0 quotas, got %d", len(quotas))
	}

	// Add quotas
	mgr.SetQuota("bucket1", &Quota{MaxSize: 1000})
	mgr.SetQuota("bucket2", &Quota{MaxSize: 2000})

	quotas = mgr.ListQuotas()
	if len(quotas) != 2 {
		t.Errorf("List should have 2 quotas, got %d", len(quotas))
	}
}

func TestQuota_Validate(t *testing.T) {
	tests := []struct {
		name    string
		quota   *Quota
		wantErr bool
	}{
		{"valid quota", &Quota{MaxSize: 1000, MaxObjects: 100}, false},
		{"zero limits", &Quota{MaxSize: 0, MaxObjects: 0}, false}, // 0 means unlimited
		{"negative size", &Quota{MaxSize: -1, MaxObjects: 100}, true},
		{"negative objects", &Quota{MaxSize: 1000, MaxObjects: -1}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.quota.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestManager_Concurrent(t *testing.T) {
	mgr := NewManager()
	done := make(chan bool)

	for i := 0; i < 10; i++ {
		go func(id int) {
			bucket := string(rune('A' + id))
			mgr.SetQuota(bucket, &Quota{MaxSize: int64(id)})
			mgr.GetQuota(bucket)
			mgr.UpdateUsage(bucket, int64(id), id)
			done <- true
		}(i)
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}
