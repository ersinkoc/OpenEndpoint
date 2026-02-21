package metadata

import (
	"testing"
	"time"
)

func TestBucketMetadata(t *testing.T) {
	meta := &BucketMetadata{
		Name:      "test-bucket",
		CreatedAt: time.Now(),
	}

	if meta.Name != "test-bucket" {
		t.Errorf("Name = %s, want test-bucket", meta.Name)
	}
}

func TestObjectMetadata(t *testing.T) {
	meta := &ObjectMetadata{
		Key:         "test-key",
		Size:        1024,
		ContentType: "application/json",
		ETag:        "abc123",
	}

	if meta.Key != "test-key" {
		t.Errorf("Key = %s, want test-key", meta.Key)
	}

	if meta.Size != 1024 {
		t.Errorf("Size = %d, want 1024", meta.Size)
	}
}

func TestVersionInfo(t *testing.T) {
	version := &VersionInfo{
		VersionID:     "v1",
		IsLatest:      true,
		LastModified:  time.Now(),
		Size:          1024,
	}

	if version.VersionID != "v1" {
		t.Errorf("VersionID = %s, want v1", version.VersionID)
	}

	if !version.IsLatest {
		t.Error("IsLatest should be true")
	}
}

func TestLifecycleRule(t *testing.T) {
	days := 30
	rule := &LifecycleRule{
		ID:         "rule-1",
		Status:     "Enabled",
		Expiration: &LifecycleExpiration{Days: &days},
	}

	if rule.ID != "rule-1" {
		t.Errorf("ID = %s, want rule-1", rule.ID)
	}

	if rule.Status != "Enabled" {
		t.Errorf("Status = %s, want Enabled", rule.Status)
	}
}

func TestReplicationConfig(t *testing.T) {
	config := &ReplicationConfig{
		Role:    "arn:aws:iam::123456789012:role/replication-role",
		Rules:   []ReplicationRule{},
	}

	if config.Role == "" {
		t.Error("Role should not be empty")
	}
}

func TestCORSConfiguration(t *testing.T) {
	cors := &CORSConfiguration{
		CORSRules: []CORSRule{
			{
				AllowedOrigins: []string{"*"},
				AllowedMethods: []string{"GET", "PUT"},
			},
		},
	}

	if len(cors.CORSRules) != 1 {
		t.Errorf("Rules count = %d, want 1", len(cors.CORSRules))
	}
}

func TestBucketPolicy(t *testing.T) {
	policy := &BucketPolicy{
		Version: "2012-10-17",
		Statement: []PolicyStatement{
			{
				Effect:    "Allow",
				Actions:   []string{"s3:GetObject"},
				Resources: []string{"arn:aws:s3:::my-bucket/*"},
			},
		},
	}

	if policy.Version != "2012-10-17" {
		t.Errorf("Version = %s, want 2012-10-17", policy.Version)
	}
}

func TestBucketEncryption(t *testing.T) {
	encryption := &BucketEncryption{
		Algorithm: "AES256",
	}

	if encryption.Algorithm != "AES256" {
		t.Errorf("Algorithm = %s, want AES256", encryption.Algorithm)
	}
}

func TestBucketVersioning(t *testing.T) {
	versioning := &BucketVersioning{
		Status:    "Enabled",
		MfaDelete: "Disabled",
	}

	if versioning.Status != "Enabled" {
		t.Errorf("Status = %s, want Enabled", versioning.Status)
	}
}

func TestObjectLockConfig(t *testing.T) {
	days := 30
	config := &ObjectLockConfig{
		ObjectLockEnabled: "Enabled",
		DefaultRetention: &DefaultRetention{
			Mode: "COMPLIANCE",
			Days: &days,
		},
	}

	if config.ObjectLockEnabled != "Enabled" {
		t.Errorf("ObjectLockEnabled = %s, want Enabled", config.ObjectLockEnabled)
	}
}

func TestTag(t *testing.T) {
	tag := &Tag{
		Key:   "environment",
		Value: "production",
	}

	if tag.Key != "environment" {
		t.Errorf("Key = %s, want environment", tag.Key)
	}
}

func TestPartMetadata(t *testing.T) {
	part := &PartMetadata{
		PartNumber:   1,
		Size:         1024,
		ETag:         "part-etag",
		LastModified: time.Now(),
	}

	if part.PartNumber != 1 {
		t.Errorf("PartNumber = %d, want 1", part.PartNumber)
	}
}

func TestMultipartUploadMetadata(t *testing.T) {
	upload := &MultipartUploadMetadata{
		UploadID:    "upload-123",
		Key:         "test-key",
		InitiatedAt: time.Now(),
	}

	if upload.UploadID != "upload-123" {
		t.Errorf("UploadID = %s, want upload-123", upload.UploadID)
	}
}
