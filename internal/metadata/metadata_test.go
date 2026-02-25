package metadata

import (
	"testing"
	"time"
)

func TestBucketMetadata(t *testing.T) {
	meta := &BucketMetadata{
		Name:         "test-bucket",
		CreationDate: time.Now().Unix(),
		Owner:        "owner123",
		Region:       "us-east-1",
	}

	if meta.Name != "test-bucket" {
		t.Errorf("Name = %s, want test-bucket", meta.Name)
	}
	if meta.Region != "us-east-1" {
		t.Errorf("Region = %s, want us-east-1", meta.Region)
	}
}

func TestObjectMetadata(t *testing.T) {
	meta := &ObjectMetadata{
		Key:          "test-key",
		Bucket:       "test-bucket",
		Size:         1024,
		ETag:         "abc123",
		ContentType:  "application/json",
		StorageClass: "STANDARD",
		LastModified: time.Now().Unix(),
	}

	if meta.Key != "test-key" {
		t.Errorf("Key = %s, want test-key", meta.Key)
	}
	if meta.Size != 1024 {
		t.Errorf("Size = %d, want 1024", meta.Size)
	}
}

func TestCORSConfiguration(t *testing.T) {
	cors := &CORSConfiguration{
		CORSRules: []CORSRule{
			{
				AllowedOrigins: []string{"*"},
				AllowedMethods: []string{"GET", "PUT"},
				AllowedHeaders: []string{"*"},
			},
		},
	}

	if len(cors.CORSRules) != 1 {
		t.Errorf("Rules count = %d, want 1", len(cors.CORSRules))
	}
	if len(cors.CORSRules[0].AllowedMethods) != 2 {
		t.Errorf("AllowedMethods count = %d, want 2", len(cors.CORSRules[0].AllowedMethods))
	}
}

func TestLifecycleRule(t *testing.T) {
	rule := &LifecycleRule{
		ID:     "rule-1",
		Prefix: "logs/",
		Status: "Enabled",
		Expiration: &Expiration{
			Days: 30,
		},
	}

	if rule.ID != "rule-1" {
		t.Errorf("ID = %s, want rule-1", rule.ID)
	}
	if rule.Status != "Enabled" {
		t.Errorf("Status = %s, want Enabled", rule.Status)
	}
	if rule.Expiration == nil {
		t.Fatal("Expiration should not be nil")
	}
	if rule.Expiration.Days != 30 {
		t.Errorf("Days = %d, want 30", rule.Expiration.Days)
	}
}

func TestBucketVersioning(t *testing.T) {
	versioning := &BucketVersioning{
		Status:    "Enabled",
		MFADelete: "Disabled",
	}

	if versioning.Status != "Enabled" {
		t.Errorf("Status = %s, want Enabled", versioning.Status)
	}
	if versioning.MFADelete != "Disabled" {
		t.Errorf("MFADelete = %s, want Disabled", versioning.MFADelete)
	}
}

func TestObjectLockConfig(t *testing.T) {
	config := &ObjectLockConfig{
		Enabled: true,
	}

	if !config.Enabled {
		t.Error("Enabled should be true")
	}
}

func TestReplicationConfig(t *testing.T) {
	config := &ReplicationConfig{
		Role: "arn:aws:iam::123456789012:role/replication-role",
		Rules: []ReplicationRule{
			{
				ID:     "rule-1",
				Status: "Enabled",
				Prefix: "",
				Destination: Destination{
					Bucket:       "dest-bucket",
					StorageClass: "STANDARD",
				},
			},
		},
	}

	if config.Role == "" {
		t.Error("Role should not be empty")
	}
	if len(config.Rules) != 1 {
		t.Errorf("Rules count = %d, want 1", len(config.Rules))
	}
}

func TestPartInfo(t *testing.T) {
	part := &PartInfo{
		PartNumber: 1,
		ETag:       "etag123",
		Size:       1024,
	}

	if part.PartNumber != 1 {
		t.Errorf("PartNumber = %d, want 1", part.PartNumber)
	}
	if part.ETag != "etag123" {
		t.Errorf("ETag = %s, want etag123", part.ETag)
	}
}

func TestObjectMetadata_MarshalJSON(t *testing.T) {
	now := time.Now().Unix()
	meta := &ObjectMetadata{
		Key:          "test-key",
		Bucket:       "test-bucket",
		Size:         1024,
		ETag:         "abc123",
		ContentType:  "application/json",
		StorageClass: "STANDARD",
		LastModified: now,
	}

	data, err := meta.MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON() error = %v", err)
	}
	if len(data) == 0 {
		t.Error("MarshalJSON() returned empty data")
	}
}

func TestObjectMetadata_UnmarshalJSON(t *testing.T) {
	jsonData := `{"key":"test-key","bucket":"test-bucket","size":1024,"etag":"abc123","content_type":"application/json","storage_class":"STANDARD","last_modified":"2024-01-01T00:00:00Z"}`

	var meta ObjectMetadata
	err := meta.UnmarshalJSON([]byte(jsonData))
	if err != nil {
		t.Fatalf("UnmarshalJSON() error = %v", err)
	}
	if meta.Key != "test-key" {
		t.Errorf("Key = %s, want test-key", meta.Key)
	}
	if meta.Bucket != "test-bucket" {
		t.Errorf("Bucket = %s, want test-bucket", meta.Bucket)
	}
}

func TestObjectMetadata_UnmarshalJSON_EmptyLastModified(t *testing.T) {
	jsonData := `{"key":"test-key","bucket":"test-bucket","size":1024,"etag":"abc123"}`

	var meta ObjectMetadata
	err := meta.UnmarshalJSON([]byte(jsonData))
	if err != nil {
		t.Fatalf("UnmarshalJSON() error = %v", err)
	}
	if meta.LastModified != 0 {
		t.Errorf("LastModified = %d, want 0", meta.LastModified)
	}
}

func TestObjectMetadata_UnmarshalJSON_Invalid(t *testing.T) {
	var meta ObjectMetadata
	err := meta.UnmarshalJSON([]byte(`invalid json`))
	if err == nil {
		t.Error("UnmarshalJSON() should return error for invalid JSON")
	}
}

func TestObjectMetadata_RoundTrip(t *testing.T) {
	now := time.Now().Unix()
	original := &ObjectMetadata{
		Key:          "test-key",
		Bucket:       "test-bucket",
		Size:         1024,
		ETag:         "abc123",
		ContentType:  "application/json",
		StorageClass: "STANDARD",
		LastModified: now,
		Metadata:     map[string]string{"custom": "value"},
	}

	data, err := original.MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON() error = %v", err)
	}

	var decoded ObjectMetadata
	err = decoded.UnmarshalJSON(data)
	if err != nil {
		t.Fatalf("UnmarshalJSON() error = %v", err)
	}

	if decoded.Key != original.Key {
		t.Errorf("Key = %s, want %s", decoded.Key, original.Key)
	}
	if decoded.Bucket != original.Bucket {
		t.Errorf("Bucket = %s, want %s", decoded.Bucket, original.Bucket)
	}
	if decoded.Size != original.Size {
		t.Errorf("Size = %d, want %d", decoded.Size, original.Size)
	}
}
