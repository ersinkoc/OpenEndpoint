package settings

import (
	"os"
	"testing"
)

func TestNewManager(t *testing.T) {
	// Create temp file for testing
	mgr := NewManager("/tmp/test_settings.json")
	if mgr == nil {
		t.Fatal("Manager should not be nil")
	}
}

func TestNewManagerWithTempFile(t *testing.T) {
	tmpFile := t.TempDir() + "/settings.json"
	mgr := NewManager(tmpFile)
	if mgr == nil {
		t.Fatal("Manager should not be nil")
	}
}

func TestManager_GetString(t *testing.T) {
	mgr := NewManager("/tmp/test_settings.json")

	// Get non-existent setting with default
	val := mgr.GetString("non-existent", "default-value")
	if val != "default-value" {
		t.Errorf("GetString = %s, want default-value", val)
	}

	// Set and get
	mgr.Set("key1", "value1")
	val = mgr.GetString("key1", "")
	if val != "value1" {
		t.Errorf("GetString = %s, want value1", val)
	}
}

func TestManager_GetBool(t *testing.T) {
	mgr := NewManager("/tmp/test_settings.json")

	// Get non-existent with default
	val := mgr.GetBool("non-existent", true)
	if val != true {
		t.Errorf("GetBool = %v, want true", val)
	}

	// Set and get
	mgr.Set("enabled", false)
	val = mgr.GetBool("enabled", true)
	if val != false {
		t.Errorf("GetBool = %v, want false", val)
	}
}

func TestManager_SetMultiple(t *testing.T) {
	mgr := NewManager("/tmp/test_settings.json")

	settings := map[string]interface{}{
		"key1": "value1",
		"key2": 123,
		"key3": true,
	}

	mgr.SetMultiple(settings)

	if mgr.GetString("key1", "") != "value1" {
		t.Error("key1 should be value1")
	}
	if mgr.GetBool("key3", false) != true {
		t.Error("key3 should be true")
	}
}

func TestManager_GetAll(t *testing.T) {
	mgr := NewManager("/tmp/test_settings.json")

	mgr.Set("key1", "value1")
	mgr.Set("key2", "value2")

	all := mgr.GetAll()
	if len(all) < 2 {
		t.Errorf("GetAll count = %d, want at least 2", len(all))
	}
}

func TestManager_Get(t *testing.T) {
	mgr := NewManager("/tmp/test_settings.json")

	// Get non-existent
	_, ok := mgr.Get("non-existent")
	if ok {
		t.Error("Get should return false for non-existent key")
	}

	// Set and get
	mgr.Set("key1", "value1")
	val, ok := mgr.Get("key1")
	if !ok {
		t.Error("Get should return true for existing key")
	}
	if val != "value1" {
		t.Errorf("Get = %v, want value1", val)
	}
}

func TestManager_Save(t *testing.T) {
	tmpFile := t.TempDir() + "/settings.json"
	mgr := NewManager(tmpFile)

	mgr.Set("key1", "value1")
	mgr.Set("key2", 123)

	err := mgr.Save()
	if err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// Verify file was created
	if _, err := os.Stat(tmpFile); os.IsNotExist(err) {
		t.Error("Settings file should be created")
	}
}

func TestManager_Load(t *testing.T) {
	tmpFile := t.TempDir() + "/settings.json"

	// Create initial settings
	mgr1 := NewManager(tmpFile)
	mgr1.Set("key1", "value1")
	mgr1.Save()

	// Load settings in new manager
	mgr2 := NewManager(tmpFile)
	val := mgr2.GetString("key1", "")
	if val != "value1" {
		t.Errorf("Loaded value = %s, want value1", val)
	}
}

func TestManager_LoadNonExistent(t *testing.T) {
	mgr := NewManager("/nonexistent/path/settings.json")

	// Should have defaults
	val := mgr.GetString("region", "")
	if val != "us-east-1" {
		t.Errorf("Default region = %s, want us-east-1", val)
	}
}

func TestManager_Defaults(t *testing.T) {
	mgr := NewManager("/nonexistent/settings.json")

	// Check defaults
	if mgr.GetString("region", "") != "us-east-1" {
		t.Error("Default region should be us-east-1")
	}
	if mgr.GetString("storageClass", "") != "STANDARD" {
		t.Error("Default storageClass should be STANDARD")
	}
	if mgr.GetBool("objectLock", true) != false {
		t.Error("Default objectLock should be false")
	}
}

func TestManager_GetStringTypeMismatch(t *testing.T) {
	mgr := NewManager("/tmp/test_settings.json")

	// Set non-string value
	mgr.Set("number", 123)

	// Should return default when type doesn't match
	val := mgr.GetString("number", "default")
	if val != "default" {
		t.Errorf("GetString with type mismatch = %s, want default", val)
	}
}

func TestManager_GetBoolTypeMismatch(t *testing.T) {
	mgr := NewManager("/tmp/test_settings.json")

	// Set non-bool value
	mgr.Set("string", "true")

	// Should return default when type doesn't match
	val := mgr.GetBool("string", false)
	if val != false {
		t.Errorf("GetBool with type mismatch = %v, want false", val)
	}
}

func TestManager_LoadInvalidJSON(t *testing.T) {
	tmpFile := t.TempDir() + "/settings.json"

	// Write invalid JSON
	os.WriteFile(tmpFile, []byte("invalid json"), 0644)

	mgr := NewManager(tmpFile)

	// Should use defaults when JSON is invalid
	val := mgr.GetString("region", "")
	if val != "us-east-1" {
		t.Errorf("Should use defaults for invalid JSON, got %s", val)
	}
}

func TestManager_SaveMarshalError(t *testing.T) {
	tmpFile := t.TempDir() + "/settings.json"
	mgr := NewManager(tmpFile)

	mgr.Set("unmarshallable", make(chan int))

	err := mgr.Save()
	if err == nil {
		t.Error("Save should fail with unmarshallable value")
	}
}

func TestManager_SaveInvalidPath(t *testing.T) {
	mgr := NewManager("Z:\\nonexistent\\path\\that\\does\\not\\exist\\settings.json")

	mgr.Set("key1", "value1")

	err := mgr.Save()
	if err == nil {
		t.Error("Save should fail with invalid path")
	}
}

func TestManager_PublicLoad(t *testing.T) {
	tmpFile := t.TempDir() + "/settings.json"

	mgr1 := NewManager(tmpFile)
	mgr1.Set("key1", "value1")
	mgr1.Save()

	mgr2 := NewManager(tmpFile)
	mgr2.Set("key2", "value2") // Add a new setting

	// Reload from file
	mgr2.Load()

	// key2 should be gone after reload
	_, ok := mgr2.Get("key2")
	if ok {
		t.Error("key2 should not exist after Load")
	}
}

func TestManager_Concurrent(t *testing.T) {
	mgr := NewManager(t.TempDir() + "/settings.json")
	done := make(chan bool)

	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				mgr.Set("key", "value")
				mgr.Get("key")
				mgr.GetString("key", "")
				mgr.GetBool("key", false)
			}
			done <- true
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}
