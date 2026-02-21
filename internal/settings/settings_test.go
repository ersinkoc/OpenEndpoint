package settings

import (
	"testing"
)

func TestNewManager(t *testing.T) {
	mgr := NewManager()
	if mgr == nil {
		t.Fatal("Manager should not be nil")
	}
}

func TestManager_Get(t *testing.T) {
	mgr := NewManager()

	// Get non-existent setting
	val := mgr.Get("non-existent")
	if val != "" {
		t.Errorf("Non-existent setting = %s, want empty", val)
	}
}

func TestManager_Set(t *testing.T) {
	mgr := NewManager()

	mgr.Set("key1", "value1")

	val := mgr.Get("key1")
	if val != "value1" {
		t.Errorf("Get = %s, want value1", val)
	}
}

func TestManager_Delete(t *testing.T) {
	mgr := NewManager()

	mgr.Set("key1", "value1")
	mgr.Delete("key1")

	val := mgr.Get("key1")
	if val != "" {
		t.Error("Setting should be deleted")
	}
}

func TestManager_GetAll(t *testing.T) {
	mgr := NewManager()

	mgr.Set("key1", "value1")
	mgr.Set("key2", "value2")

	all := mgr.GetAll()
	if len(all) != 2 {
		t.Errorf("Settings count = %d, want 2", len(all))
	}
}

func TestManager_Reset(t *testing.T) {
	mgr := NewManager()

	mgr.Set("key1", "value1")
	mgr.Set("key2", "value2")
	mgr.Reset()

	if mgr.Count() != 0 {
		t.Error("Settings should be empty after reset")
	}
}

func TestManager_Count(t *testing.T) {
	mgr := NewManager()

	if mgr.Count() != 0 {
		t.Error("New manager should have 0 settings")
	}

	mgr.Set("key1", "value1")
	mgr.Set("key2", "value2")

	if mgr.Count() != 2 {
		t.Errorf("Count = %d, want 2", mgr.Count())
	}
}

func TestManager_Has(t *testing.T) {
	mgr := NewManager()

	if mgr.Has("key1") {
		t.Error("Has should return false for non-existent key")
	}

	mgr.Set("key1", "value1")

	if !mgr.Has("key1") {
		t.Error("Has should return true for existing key")
	}
}

func TestManager_SetDefault(t *testing.T) {
	mgr := NewManager()

	mgr.SetDefault("key1", "default")

	if mgr.Get("key1") != "default" {
		t.Error("SetDefault should set value")
	}

	// Should not overwrite existing value
	mgr.Set("key1", "custom")
	mgr.SetDefault("key1", "default")

	if mgr.Get("key1") != "custom" {
		t.Error("SetDefault should not overwrite existing value")
	}
}

func TestManager_GetWithDefault(t *testing.T) {
	mgr := NewManager()

	// Non-existent should return default
	val := mgr.GetWithDefault("non-existent", "default")
	if val != "default" {
		t.Errorf("GetWithDefault = %s, want default", val)
	}

	// Existing should return actual value
	mgr.Set("key1", "actual")
	val = mgr.GetWithDefault("key1", "default")
	if val != "actual" {
		t.Errorf("GetWithDefault = %s, want actual", val)
	}
}

func TestManager_Export(t *testing.T) {
	mgr := NewManager()

	mgr.Set("key1", "value1")
	mgr.Set("key2", "value2")

	data := mgr.Export()
	if len(data) != 2 {
		t.Errorf("Export count = %d, want 2", len(data))
	}
}

func TestManager_Import(t *testing.T) {
	mgr := NewManager()

	data := map[string]string{
		"key1": "value1",
		"key2": "value2",
	}

	err := mgr.Import(data)
	if err != nil {
		t.Fatalf("Import failed: %v", err)
	}

	if mgr.Count() != 2 {
		t.Errorf("Count after import = %d, want 2", mgr.Count())
	}
}

func TestManager_Concurrent(t *testing.T) {
	mgr := NewManager()
	done := make(chan bool)

	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				key := string(rune('A' + id))
				mgr.Set(key, "value")
				mgr.Get(key)
			}
			done <- true
		}(i)
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}
