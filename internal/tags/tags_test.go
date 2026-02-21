package tags

import (
	"testing"
)

func TestNewManager(t *testing.T) {
	mgr := NewManager()
	if mgr == nil {
		t.Fatal("Manager should not be nil")
	}
}

func TestManager_SetTags(t *testing.T) {
	mgr := NewManager()

	tags := map[string]string{
		"environment": "production",
		"team":        "backend",
	}

	err := mgr.SetTags("test-bucket", "test-key", tags)
	if err != nil {
		t.Fatalf("SetTags failed: %v", err)
	}
}

func TestManager_GetTags(t *testing.T) {
	mgr := NewManager()

	tags := map[string]string{
		"environment": "production",
	}

	mgr.SetTags("test-bucket", "test-key", tags)

	result, err := mgr.GetTags("test-bucket", "test-key")
	if err != nil {
		t.Fatalf("GetTags failed: %v", err)
	}

	if result["environment"] != "production" {
		t.Errorf("environment tag = %s, want production", result["environment"])
	}
}

func TestManager_GetTags_NotFound(t *testing.T) {
	mgr := NewManager()

	_, err := mgr.GetTags("non-existent", "non-existent")
	if err == nil {
		t.Error("GetTags should fail for non-existent object")
	}
}

func TestManager_DeleteTags(t *testing.T) {
	mgr := NewManager()

	tags := map[string]string{"key": "value"}
	mgr.SetTags("test-bucket", "test-key", tags)

	err := mgr.DeleteTags("test-bucket", "test-key")
	if err != nil {
		t.Fatalf("DeleteTags failed: %v", err)
	}

	_, err = mgr.GetTags("test-bucket", "test-key")
	if err == nil {
		t.Error("Tags should be deleted")
	}
}

func TestManager_AddTag(t *testing.T) {
	mgr := NewManager()

	err := mgr.AddTag("test-bucket", "test-key", "environment", "production")
	if err != nil {
		t.Fatalf("AddTag failed: %v", err)
	}

	result, _ := mgr.GetTags("test-bucket", "test-key")
	if result["environment"] != "production" {
		t.Errorf("environment tag = %s, want production", result["environment"])
	}
}

func TestManager_RemoveTag(t *testing.T) {
	mgr := NewManager()

	tags := map[string]string{"key1": "value1", "key2": "value2"}
	mgr.SetTags("test-bucket", "test-key", tags)

	err := mgr.RemoveTag("test-bucket", "test-key", "key1")
	if err != nil {
		t.Fatalf("RemoveTag failed: %v", err)
	}

	result, _ := mgr.GetTags("test-bucket", "test-key")
	if _, exists := result["key1"]; exists {
		t.Error("key1 should be removed")
	}

	if result["key2"] != "value2" {
		t.Error("key2 should still exist")
	}
}

func TestManager_ListTagsByPrefix(t *testing.T) {
	mgr := NewManager()

	// Add tags with prefix
	mgr.AddTag("test-bucket", "photos/2023/1.jpg", "type", "photo")
	mgr.AddTag("test-bucket", "photos/2023/2.jpg", "type", "photo")
	mgr.AddTag("test-bucket", "documents/report.pdf", "type", "document")

	result, err := mgr.ListTagsByPrefix("test-bucket", "photos/")
	if err != nil {
		t.Fatalf("ListTagsByPrefix failed: %v", err)
	}

	if len(result) != 2 {
		t.Errorf("Found %d objects, want 2", len(result))
	}
}

func TestManager_ListTagsByTag(t *testing.T) {
	mgr := NewManager()

	mgr.AddTag("test-bucket", "obj1", "env", "prod")
	mgr.AddTag("test-bucket", "obj2", "env", "prod")
	mgr.AddTag("test-bucket", "obj3", "env", "dev")

	result, err := mgr.ListByTag("test-bucket", "env", "prod")
	if err != nil {
		t.Fatalf("ListByTag failed: %v", err)
	}

	if len(result) != 2 {
		t.Errorf("Found %d objects, want 2", len(result))
	}
}

func TestManager_GetBucketTags(t *testing.T) {
	mgr := NewManager()

	tags := map[string]string{"project": "myproject"}
	mgr.SetBucketTags("test-bucket", tags)

	result, err := mgr.GetBucketTags("test-bucket")
	if err != nil {
		t.Fatalf("GetBucketTags failed: %v", err)
	}

	if result["project"] != "myproject" {
		t.Errorf("project tag = %s, want myproject", result["project"])
	}
}

func TestManager_DeleteBucketTags(t *testing.T) {
	mgr := NewManager()

	tags := map[string]string{"project": "myproject"}
	mgr.SetBucketTags("test-bucket", tags)

	err := mgr.DeleteBucketTags("test-bucket")
	if err != nil {
		t.Fatalf("DeleteBucketTags failed: %v", err)
	}

	_, err = mgr.GetBucketTags("test-bucket")
	if err == nil {
		t.Error("Bucket tags should be deleted")
	}
}

func TestManager_Concurrent(t *testing.T) {
	mgr := NewManager()
	done := make(chan bool)

	for i := 0; i < 10; i++ {
		go func(id int) {
			key := string(rune('A' + id))
			mgr.AddTag("bucket", key, "id", string(rune('0'+id)))
			mgr.GetTags("bucket", key)
			done <- true
		}(i)
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestTagValidation(t *testing.T) {
	tests := []struct {
		key    string
		value  string
		valid  bool
	}{
		{"valid-key", "valid-value", true},
		{"", "value", false},  // Empty key
		{"key", "", true},      // Empty value is allowed
		{"key-with-dashes", "value", true},
		{"key_with_underscores", "value", true},
		{"KeyWithMixedCase", "Value", true},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			err := ValidateTag(tt.key, tt.value)
			if (err == nil) != tt.valid {
				t.Errorf("ValidateTag(%s, %s) valid = %v, want %v", tt.key, tt.value, err == nil, tt.valid)
			}
		})
	}
}
