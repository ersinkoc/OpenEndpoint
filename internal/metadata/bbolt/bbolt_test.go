package bbolt

import (
	"context"
	"os"
	"testing"

	"github.com/openendpoint/openendpoint/internal/metadata"
	bolt "go.etcd.io/bbolt"
)

func TestNew(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	if store == nil {
		t.Fatal("New() returned nil")
	}
	defer store.Close()
}

func TestCreateBucket(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	err = store.CreateBucket(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("CreateBucket() error: %v", err)
	}
}

func TestGetBucket(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	_ = store.CreateBucket(ctx, "test-bucket")

	meta, err := store.GetBucket(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("GetBucket() error: %v", err)
	}
	if meta == nil {
		t.Fatal("GetBucket() returned nil")
	}
	if meta.Name != "test-bucket" {
		t.Errorf("Name = %s, expected test-bucket", meta.Name)
	}
}

func TestGetBucketNotFound(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	_, err = store.GetBucket(ctx, "nonexistent")
	if err == nil {
		t.Error("GetBucket() expected error for nonexistent bucket")
	}
}

func TestDeleteBucket(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	_ = store.CreateBucket(ctx, "test-bucket")

	err = store.DeleteBucket(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("DeleteBucket() error: %v", err)
	}

	_, err = store.GetBucket(ctx, "test-bucket")
	if err == nil {
		t.Error("GetBucket() expected error after delete")
	}
}

func TestListBuckets(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	_ = store.CreateBucket(ctx, "bucket1")
	_ = store.CreateBucket(ctx, "bucket2")

	buckets, err := store.ListBuckets(ctx)
	if err != nil {
		t.Fatalf("ListBuckets() error: %v", err)
	}
	if len(buckets) != 2 {
		t.Errorf("ListBuckets() returned %d buckets, expected 2", len(buckets))
	}
}

func TestPutGetObject(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	_ = store.CreateBucket(ctx, "test-bucket")

	meta := &metadata.ObjectMetadata{
		Key:    "test-key",
		Bucket: "test-bucket",
		Size:   1024,
		ETag:   "\"abc123\"",
	}

	err = store.PutObject(ctx, "test-bucket", "test-key", meta)
	if err != nil {
		t.Fatalf("PutObject() error: %v", err)
	}

	retrieved, err := store.GetObject(ctx, "test-bucket", "test-key", "")
	if err != nil {
		t.Fatalf("GetObject() error: %v", err)
	}
	if retrieved.Key != "test-key" {
		t.Errorf("Key = %s, expected test-key", retrieved.Key)
	}
}

func TestGetObjectNotFound(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	_ = store.CreateBucket(ctx, "test-bucket")

	_, err = store.GetObject(ctx, "test-bucket", "nonexistent", "")
	if err == nil {
		t.Error("GetObject() expected error for nonexistent object")
	}
}

func TestDeleteObject(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	_ = store.CreateBucket(ctx, "test-bucket")

	meta := &metadata.ObjectMetadata{Key: "test-key", Bucket: "test-bucket"}
	_ = store.PutObject(ctx, "test-bucket", "test-key", meta)

	err = store.DeleteObject(ctx, "test-bucket", "test-key", "")
	if err != nil {
		t.Fatalf("DeleteObject() error: %v", err)
	}

	_, err = store.GetObject(ctx, "test-bucket", "test-key", "")
	if err == nil {
		t.Error("GetObject() expected error after delete")
	}
}

func TestListObjects(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	_ = store.CreateBucket(ctx, "test-bucket")

	for i := 0; i < 3; i++ {
		key := string(rune('a' + i))
		_ = store.PutObject(ctx, "test-bucket", key, &metadata.ObjectMetadata{
			Key:    key,
			Bucket: "test-bucket",
		})
	}

	objects, err := store.ListObjects(ctx, "test-bucket", "", metadata.ListOptions{})
	if err != nil {
		t.Fatalf("ListObjects() error: %v", err)
	}
	if len(objects) != 3 {
		t.Errorf("ListObjects() returned %d objects, expected 3", len(objects))
	}
}

func TestMultipartUpload(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	_ = store.CreateBucket(ctx, "test-bucket")

	err = store.CreateMultipartUpload(ctx, "test-bucket", "test-key", "upload-123", &metadata.ObjectMetadata{})
	if err != nil {
		t.Fatalf("CreateMultipartUpload() error: %v", err)
	}

	uploads, err := store.ListMultipartUploads(ctx, "test-bucket", "")
	if err != nil {
		t.Fatalf("ListMultipartUploads() error: %v", err)
	}
	if len(uploads) != 1 {
		t.Errorf("ListMultipartUploads() returned %d uploads, expected 1", len(uploads))
	}
}

func TestPutPart(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	_ = store.CreateBucket(ctx, "test-bucket")

	partMeta := &metadata.PartMetadata{
		UploadID:   "upload-123",
		Key:        "test-key",
		Bucket:     "test-bucket",
		PartNumber: 1,
		Size:       1024,
	}

	err = store.PutPart(ctx, "test-bucket", "test-key", "upload-123", 1, partMeta)
	if err != nil {
		t.Fatalf("PutPart() error: %v", err)
	}

	parts, err := store.ListParts(ctx, "test-bucket", "test-key", "upload-123")
	if err != nil {
		t.Fatalf("ListParts() error: %v", err)
	}
	if len(parts) != 1 {
		t.Errorf("ListParts() returned %d parts, expected 1", len(parts))
	}
}

func TestCompleteMultipartUpload(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	_ = store.CreateBucket(ctx, "test-bucket")

	_ = store.CreateMultipartUpload(ctx, "test-bucket", "test-key", "upload-123", &metadata.ObjectMetadata{})
	_ = store.PutPart(ctx, "test-bucket", "test-key", "upload-123", 1, &metadata.PartMetadata{})

	parts := []metadata.PartInfo{{PartNumber: 1}}
	err = store.CompleteMultipartUpload(ctx, "test-bucket", "test-key", "upload-123", parts)
	if err != nil {
		t.Fatalf("CompleteMultipartUpload() error: %v", err)
	}

	uploads, _ := store.ListMultipartUploads(ctx, "test-bucket", "")
	if len(uploads) != 0 {
		t.Errorf("Expected 0 uploads after complete, got %d", len(uploads))
	}
}

func TestAbortMultipartUpload(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	_ = store.CreateBucket(ctx, "test-bucket")

	_ = store.CreateMultipartUpload(ctx, "test-bucket", "test-key", "upload-123", &metadata.ObjectMetadata{})

	err = store.AbortMultipartUpload(ctx, "test-bucket", "test-key", "upload-123")
	if err != nil {
		t.Fatalf("AbortMultipartUpload() error: %v", err)
	}

	uploads, _ := store.ListMultipartUploads(ctx, "test-bucket", "")
	if len(uploads) != 0 {
		t.Errorf("Expected 0 uploads after abort, got %d", len(uploads))
	}
}

func TestLifecycleRules(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()

	rule := &metadata.LifecycleRule{
		ID:     "test-rule",
		Prefix: "logs/",
		Status: "Enabled",
	}

	err = store.PutLifecycleRule(ctx, "test-bucket", rule)
	if err != nil {
		t.Fatalf("PutLifecycleRule() error: %v", err)
	}

	rules, err := store.GetLifecycleRules(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("GetLifecycleRules() error: %v", err)
	}
	if len(rules) != 1 {
		t.Errorf("Expected 1 rule, got %d", len(rules))
	}

	err = store.DeleteLifecycleRule(ctx, "test-bucket", "test-rule")
	if err != nil {
		t.Fatalf("DeleteLifecycleRule() error: %v", err)
	}

	rules, _ = store.GetLifecycleRules(ctx, "test-bucket")
	if len(rules) != 0 {
		t.Errorf("Expected 0 rules after delete, got %d", len(rules))
	}
}

func TestBucketVersioning(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()

	versioning := &metadata.BucketVersioning{Status: "Enabled"}
	err = store.PutBucketVersioning(ctx, "test-bucket", versioning)
	if err != nil {
		t.Fatalf("PutBucketVersioning() error: %v", err)
	}

	retrieved, err := store.GetBucketVersioning(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("GetBucketVersioning() error: %v", err)
	}
	if retrieved.Status != "Enabled" {
		t.Errorf("Status = %s, expected Enabled", retrieved.Status)
	}
}

func TestBucketPolicy(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()

	policy := `{"Version":"2012-10-17"}`
	policyPtr := &policy

	err = store.PutBucketPolicy(ctx, "test-bucket", policyPtr)
	if err != nil {
		t.Fatalf("PutBucketPolicy() error: %v", err)
	}

	retrieved, err := store.GetBucketPolicy(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("GetBucketPolicy() error: %v", err)
	}
	if retrieved == nil || *retrieved != policy {
		t.Error("Policy mismatch")
	}

	err = store.DeleteBucketPolicy(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("DeleteBucketPolicy() error: %v", err)
	}
}

func TestBucketPolicyNil(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()

	err = store.PutBucketPolicy(ctx, "test-bucket", nil)
	if err == nil {
		t.Error("PutBucketPolicy() expected error for nil policy")
	}
}

func TestBucketCors(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()

	cors := &metadata.CORSConfiguration{
		CORSRules: []metadata.CORSRule{
			{AllowedMethods: []string{"GET"}, AllowedOrigins: []string{"*"}},
		},
	}

	err = store.PutBucketCors(ctx, "test-bucket", cors)
	if err != nil {
		t.Fatalf("PutBucketCors() error: %v", err)
	}

	retrieved, err := store.GetBucketCors(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("GetBucketCors() error: %v", err)
	}
	if len(retrieved.CORSRules) != 1 {
		t.Errorf("Expected 1 CORS rule, got %d", len(retrieved.CORSRules))
	}

	err = store.DeleteBucketCors(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("DeleteBucketCors() error: %v", err)
	}
}

func TestBucketEncryption(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()

	encryption := &metadata.BucketEncryption{
		Rule: metadata.EncryptionRule{
			Apply: metadata.ApplyEncryptionConfiguration{
				SSEAlgorithm: "AES256",
			},
		},
	}

	err = store.PutBucketEncryption(ctx, "test-bucket", encryption)
	if err != nil {
		t.Fatalf("PutBucketEncryption() error: %v", err)
	}

	retrieved, err := store.GetBucketEncryption(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("GetBucketEncryption() error: %v", err)
	}
	if retrieved.Rule.Apply.SSEAlgorithm != "AES256" {
		t.Errorf("SSEAlgorithm = %s, expected AES256", retrieved.Rule.Apply.SSEAlgorithm)
	}

	err = store.DeleteBucketEncryption(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("DeleteBucketEncryption() error: %v", err)
	}
}

func TestBucketTags(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()

	tags := map[string]string{"env": "test", "team": "dev"}

	err = store.PutBucketTags(ctx, "test-bucket", tags)
	if err != nil {
		t.Fatalf("PutBucketTags() error: %v", err)
	}

	retrieved, err := store.GetBucketTags(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("GetBucketTags() error: %v", err)
	}
	if retrieved["env"] != "test" {
		t.Errorf("env tag = %s, expected test", retrieved["env"])
	}

	err = store.DeleteBucketTags(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("DeleteBucketTags() error: %v", err)
	}
}

func TestObjectLock(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()

	config := &metadata.ObjectLockConfig{Enabled: true}

	err = store.PutObjectLock(ctx, "test-bucket", config)
	if err != nil {
		t.Fatalf("PutObjectLock() error: %v", err)
	}

	retrieved, err := store.GetObjectLock(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("GetObjectLock() error: %v", err)
	}
	if !retrieved.Enabled {
		t.Error("ObjectLock should be enabled")
	}

	err = store.DeleteObjectLock(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("DeleteObjectLock() error: %v", err)
	}
}

func TestObjectRetention(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()

	retention := &metadata.ObjectRetention{Mode: "GOVERNANCE"}

	err = store.PutObjectRetention(ctx, "test-bucket", "test-key", retention)
	if err != nil {
		t.Fatalf("PutObjectRetention() error: %v", err)
	}

	retrieved, err := store.GetObjectRetention(ctx, "test-bucket", "test-key")
	if err != nil {
		t.Fatalf("GetObjectRetention() error: %v", err)
	}
	if retrieved == nil || retrieved.Mode != "GOVERNANCE" {
		t.Error("Retention mode mismatch")
	}
}

func TestObjectLegalHold(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()

	legalHold := &metadata.ObjectLegalHold{Status: "ON"}

	err = store.PutObjectLegalHold(ctx, "test-bucket", "test-key", legalHold)
	if err != nil {
		t.Fatalf("PutObjectLegalHold() error: %v", err)
	}

	retrieved, err := store.GetObjectLegalHold(ctx, "test-bucket", "test-key")
	if err != nil {
		t.Fatalf("GetObjectLegalHold() error: %v", err)
	}
	if retrieved == nil || retrieved.Status != "ON" {
		t.Error("Legal hold status mismatch")
	}
}

func TestPublicAccessBlock(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()

	config := &metadata.PublicAccessBlockConfiguration{
		BlockPublicAcls:       true,
		BlockPublicPolicy:     true,
		IgnorePublicAcls:      true,
		RestrictPublicBuckets: true,
	}

	err = store.PutPublicAccessBlock(ctx, "test-bucket", config)
	if err != nil {
		t.Fatalf("PutPublicAccessBlock() error: %v", err)
	}

	retrieved, err := store.GetPublicAccessBlock(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("GetPublicAccessBlock() error: %v", err)
	}
	if !retrieved.BlockPublicAcls {
		t.Error("BlockPublicAcls should be true")
	}

	err = store.DeletePublicAccessBlock(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("DeletePublicAccessBlock() error: %v", err)
	}
}

func TestBucketAccelerate(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()

	config := &metadata.BucketAccelerateConfiguration{Status: "Enabled"}

	err = store.PutBucketAccelerate(ctx, "test-bucket", config)
	if err != nil {
		t.Fatalf("PutBucketAccelerate() error: %v", err)
	}

	retrieved, err := store.GetBucketAccelerate(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("GetBucketAccelerate() error: %v", err)
	}
	if retrieved.Status != "Enabled" {
		t.Errorf("Status = %s, expected Enabled", retrieved.Status)
	}

	err = store.DeleteBucketAccelerate(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("DeleteBucketAccelerate() error: %v", err)
	}
}

func TestReplicationConfig(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()

	config := &metadata.ReplicationConfig{
		Role: "arn:aws:iam::123:role/replication",
	}

	err = store.PutReplicationConfig(ctx, "test-bucket", config)
	if err != nil {
		t.Fatalf("PutReplicationConfig() error: %v", err)
	}

	retrieved, err := store.GetReplicationConfig(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("GetReplicationConfig() error: %v", err)
	}
	if retrieved.Role != config.Role {
		t.Error("Role mismatch")
	}

	err = store.DeleteReplicationConfig(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("DeleteReplicationConfig() error: %v", err)
	}
}

func TestBucketNotification(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()

	config := &metadata.NotificationConfiguration{}

	err = store.PutBucketNotification(ctx, "test-bucket", config)
	if err != nil {
		t.Fatalf("PutBucketNotification() error: %v", err)
	}

	_, err = store.GetBucketNotification(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("GetBucketNotification() error: %v", err)
	}

	err = store.DeleteBucketNotification(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("DeleteBucketNotification() error: %v", err)
	}
}

func TestBucketLogging(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()

	config := &metadata.LoggingConfiguration{
		TargetBucket: "log-bucket",
	}

	err = store.PutBucketLogging(ctx, "test-bucket", config)
	if err != nil {
		t.Fatalf("PutBucketLogging() error: %v", err)
	}

	retrieved, err := store.GetBucketLogging(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("GetBucketLogging() error: %v", err)
	}
	if retrieved.TargetBucket != "log-bucket" {
		t.Errorf("TargetBucket = %s, expected log-bucket", retrieved.TargetBucket)
	}

	err = store.DeleteBucketLogging(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("DeleteBucketLogging() error: %v", err)
	}
}

func TestBucketLocation(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()

	err = store.PutBucketLocation(ctx, "test-bucket", "us-west-2")
	if err != nil {
		t.Fatalf("PutBucketLocation() error: %v", err)
	}

	location, err := store.GetBucketLocation(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("GetBucketLocation() error: %v", err)
	}
	if location != "us-west-2" {
		t.Errorf("Location = %s, expected us-west-2", location)
	}
}

func TestBucketOwnershipControls(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()

	config := &metadata.OwnershipControls{}

	err = store.PutBucketOwnershipControls(ctx, "test-bucket", config)
	if err != nil {
		t.Fatalf("PutBucketOwnershipControls() error: %v", err)
	}

	_, err = store.GetBucketOwnershipControls(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("GetBucketOwnershipControls() error: %v", err)
	}

	err = store.DeleteBucketOwnershipControls(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("DeleteBucketOwnershipControls() error: %v", err)
	}
}

func TestBucketMetrics(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()

	config := &metadata.MetricsConfiguration{ID: "metrics-1"}

	err = store.PutBucketMetrics(ctx, "test-bucket", "metrics-1", config)
	if err != nil {
		t.Fatalf("PutBucketMetrics() error: %v", err)
	}

	retrieved, err := store.GetBucketMetrics(ctx, "test-bucket", "metrics-1")
	if err != nil {
		t.Fatalf("GetBucketMetrics() error: %v", err)
	}
	if retrieved == nil || retrieved.ID != "metrics-1" {
		t.Error("Metrics ID mismatch")
	}

	list, err := store.ListBucketMetrics(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("ListBucketMetrics() error: %v", err)
	}
	if len(list) != 1 {
		t.Errorf("Expected 1 metrics config, got %d", len(list))
	}

	err = store.DeleteBucketMetrics(ctx, "test-bucket", "metrics-1")
	if err != nil {
		t.Fatalf("DeleteBucketMetrics() error: %v", err)
	}
}

func TestBucketAnalytics(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()

	config := &metadata.AnalyticsConfiguration{ID: "analytics-1"}

	err = store.PutBucketAnalytics(ctx, "test-bucket", "analytics-1", config)
	if err != nil {
		t.Fatalf("PutBucketAnalytics() error: %v", err)
	}

	retrieved, err := store.GetBucketAnalytics(ctx, "test-bucket", "analytics-1")
	if err != nil {
		t.Fatalf("GetBucketAnalytics() error: %v", err)
	}
	if retrieved == nil || retrieved.ID != "analytics-1" {
		t.Error("Analytics ID mismatch")
	}

	list, err := store.ListBucketAnalytics(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("ListBucketAnalytics() error: %v", err)
	}
	if len(list) != 1 {
		t.Errorf("Expected 1 analytics config, got %d", len(list))
	}

	err = store.DeleteBucketAnalytics(ctx, "test-bucket", "analytics-1")
	if err != nil {
		t.Fatalf("DeleteBucketAnalytics() error: %v", err)
	}
}

func TestContainsPrefix(t *testing.T) {
	tests := []struct {
		s, prefix string
		expected  bool
	}{
		{"bucket/key", "bucket/", true},
		{"bucket/key", "other/", false},
		{"bucket", "bucket/", false},
		{"bucket/", "bucket/", true},
	}

	for _, tt := range tests {
		result := containsPrefix(tt.s, tt.prefix)
		if result != tt.expected {
			t.Errorf("containsPrefix(%s, %s) = %v, expected %v", tt.s, tt.prefix, result, tt.expected)
		}
	}
}

func TestGetBucketVersioningNotFound(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	versioning, err := store.GetBucketVersioning(ctx, "nonexistent-bucket")
	if err != nil {
		t.Fatalf("GetBucketVersioning() error: %v", err)
	}
	if versioning == nil {
		t.Fatal("Expected non-nil versioning struct")
	}
}

func TestGetBucketPolicyNotFound(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	policy, err := store.GetBucketPolicy(ctx, "nonexistent-bucket")
	if err != nil {
		t.Fatalf("GetBucketPolicy() error: %v", err)
	}
	if policy != nil {
		t.Error("Expected nil policy for nonexistent bucket")
	}
}

func TestGetBucketCorsNotFound(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	cors, err := store.GetBucketCors(ctx, "nonexistent-bucket")
	if err != nil {
		t.Fatalf("GetBucketCors() error: %v", err)
	}
	if cors == nil {
		t.Fatal("Expected non-nil cors struct")
	}
}

func TestGetBucketEncryptionNotFound(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	encryption, err := store.GetBucketEncryption(ctx, "nonexistent-bucket")
	if err != nil {
		t.Fatalf("GetBucketEncryption() error: %v", err)
	}
	if encryption == nil {
		t.Fatal("Expected non-nil encryption struct")
	}
}

func TestGetBucketTagsNotFound(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	tags, err := store.GetBucketTags(ctx, "nonexistent-bucket")
	if err != nil {
		t.Fatalf("GetBucketTags() error: %v", err)
	}
	if tags != nil {
		t.Error("Expected nil tags for nonexistent bucket")
	}
}

func TestGetObjectLockNotFound(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	config, err := store.GetObjectLock(ctx, "nonexistent-bucket")
	if err != nil {
		t.Fatalf("GetObjectLock() error: %v", err)
	}
	if config == nil {
		t.Fatal("Expected non-nil config struct")
	}
}

func TestGetObjectRetentionNotFound(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	retention, err := store.GetObjectRetention(ctx, "nonexistent-bucket", "nonexistent-key")
	if err != nil {
		t.Fatalf("GetObjectRetention() error: %v", err)
	}
	if retention != nil {
		t.Error("Expected nil retention for nonexistent object")
	}
}

func TestGetObjectLegalHoldNotFound(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	legalHold, err := store.GetObjectLegalHold(ctx, "nonexistent-bucket", "nonexistent-key")
	if err != nil {
		t.Fatalf("GetObjectLegalHold() error: %v", err)
	}
	if legalHold != nil {
		t.Error("Expected nil legalHold for nonexistent object")
	}
}

func TestGetPublicAccessBlockNotFound(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	config, err := store.GetPublicAccessBlock(ctx, "nonexistent-bucket")
	if err != nil {
		t.Fatalf("GetPublicAccessBlock() error: %v", err)
	}
	if config == nil {
		t.Fatal("Expected non-nil config struct")
	}
}

func TestGetBucketAccelerateNotFound(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	config, err := store.GetBucketAccelerate(ctx, "nonexistent-bucket")
	if err != nil {
		t.Fatalf("GetBucketAccelerate() error: %v", err)
	}
	if config == nil {
		t.Fatal("Expected non-nil config struct")
	}
}

func TestGetReplicationConfigNotFound(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	config, err := store.GetReplicationConfig(ctx, "nonexistent-bucket")
	if err != nil {
		t.Fatalf("GetReplicationConfig() error: %v", err)
	}
	if config == nil {
		t.Fatal("Expected non-nil config struct")
	}
}

func TestGetBucketNotificationNotFound(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	config, err := store.GetBucketNotification(ctx, "nonexistent-bucket")
	if err != nil {
		t.Fatalf("GetBucketNotification() error: %v", err)
	}
	if config == nil {
		t.Fatal("Expected non-nil config struct")
	}
}

func TestGetBucketLoggingNotFound(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	config, err := store.GetBucketLogging(ctx, "nonexistent-bucket")
	if err != nil {
		t.Fatalf("GetBucketLogging() error: %v", err)
	}
	if config == nil {
		t.Fatal("Expected non-nil config struct")
	}
}

func TestGetBucketLocationNotFound(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	location, err := store.GetBucketLocation(ctx, "nonexistent-bucket")
	if err != nil {
		t.Fatalf("GetBucketLocation() error: %v", err)
	}
	if location != "" {
		t.Errorf("Expected empty location, got %s", location)
	}
}

func TestGetBucketOwnershipControlsNotFound(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	config, err := store.GetBucketOwnershipControls(ctx, "nonexistent-bucket")
	if err != nil {
		t.Fatalf("GetBucketOwnershipControls() error: %v", err)
	}
	if config != nil {
		t.Error("Expected nil config for nonexistent bucket")
	}
}

func TestGetBucketMetricsNotFound(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	config, err := store.GetBucketMetrics(ctx, "nonexistent-bucket", "nonexistent-id")
	if err != nil {
		t.Fatalf("GetBucketMetrics() error: %v", err)
	}
	if config != nil {
		t.Error("Expected nil config for nonexistent metrics")
	}
}

func TestGetBucketAnalyticsNotFound(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	config, err := store.GetBucketAnalytics(ctx, "nonexistent-bucket", "nonexistent-id")
	if err != nil {
		t.Fatalf("GetBucketAnalytics() error: %v", err)
	}
	if config != nil {
		t.Error("Expected nil config for nonexistent analytics")
	}
}

func TestListObjectsWithMaxKeys(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	_ = store.CreateBucket(ctx, "test-bucket")

	for i := 0; i < 5; i++ {
		key := string(rune('a' + i))
		_ = store.PutObject(ctx, "test-bucket", key, &metadata.ObjectMetadata{
			Key:    key,
			Bucket: "test-bucket",
		})
	}

	objects, err := store.ListObjects(ctx, "test-bucket", "", metadata.ListOptions{MaxKeys: 2})
	if err != nil {
		t.Fatalf("ListObjects() error: %v", err)
	}
	if len(objects) != 2 {
		t.Errorf("ListObjects() returned %d objects, expected 2", len(objects))
	}
}

func TestListBucketMetricsEmpty(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	configs, err := store.ListBucketMetrics(ctx, "nonexistent-bucket")
	if err != nil {
		t.Fatalf("ListBucketMetrics() error: %v", err)
	}
	if len(configs) != 0 {
		t.Errorf("Expected 0 configs, got %d", len(configs))
	}
}

func TestListBucketAnalyticsEmpty(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	configs, err := store.ListBucketAnalytics(ctx, "nonexistent-bucket")
	if err != nil {
		t.Fatalf("ListBucketAnalytics() error: %v", err)
	}
	if len(configs) != 0 {
		t.Errorf("Expected 0 configs, got %d", len(configs))
	}
}

func TestGetLifecycleRulesEmpty(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	rules, err := store.GetLifecycleRules(ctx, "nonexistent-bucket")
	if err != nil {
		t.Fatalf("GetLifecycleRules() error: %v", err)
	}
	if len(rules) != 0 {
		t.Errorf("Expected 0 rules, got %d", len(rules))
	}
}

func TestListMultipartUploadsEmpty(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	uploads, err := store.ListMultipartUploads(ctx, "nonexistent-bucket", "")
	if err != nil {
		t.Fatalf("ListMultipartUploads() error: %v", err)
	}
	if len(uploads) != 0 {
		t.Errorf("Expected 0 uploads, got %d", len(uploads))
	}
}

func TestListPartsEmpty(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	parts, err := store.ListParts(ctx, "nonexistent-bucket", "nonexistent-key", "nonexistent-upload")
	if err != nil {
		t.Fatalf("ListParts() error: %v", err)
	}
	if len(parts) != 0 {
		t.Errorf("Expected 0 parts, got %d", len(parts))
	}
}

func TestListObjectsEmpty(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	_ = store.CreateBucket(ctx, "test-bucket")
	objects, err := store.ListObjects(ctx, "test-bucket", "", metadata.ListOptions{})
	if err != nil {
		t.Fatalf("ListObjects() error: %v", err)
	}
	if len(objects) != 0 {
		t.Errorf("Expected 0 objects, got %d", len(objects))
	}
}

func TestListBucketsEmpty(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	buckets, err := store.ListBuckets(ctx)
	if err != nil {
		t.Fatalf("ListBuckets() error: %v", err)
	}
	if len(buckets) != 0 {
		t.Errorf("Expected 0 buckets, got %d", len(buckets))
	}
}

func TestListObjectsWithInvalidData(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	_ = store.CreateBucket(ctx, "test-bucket")

	store.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte("objects"))
		return bkt.Put([]byte("test-bucket/bad"), []byte("invalid-json"))
	})

	objects, err := store.ListObjects(ctx, "test-bucket", "", metadata.ListOptions{})
	if err != nil {
		t.Fatalf("ListObjects() error: %v", err)
	}
	if len(objects) != 0 {
		t.Errorf("Expected 0 objects (invalid data skipped), got %d", len(objects))
	}
}

func TestListPartsWithInvalidData(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	_ = store.CreateBucket(ctx, "test-bucket")

	store.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte("parts"))
		return bkt.Put([]byte("test-bucket/key/upload-123/1"), []byte("invalid-json"))
	})

	parts, err := store.ListParts(ctx, "test-bucket", "key", "upload-123")
	if err != nil {
		t.Fatalf("ListParts() error: %v", err)
	}
	if len(parts) != 0 {
		t.Errorf("Expected 0 parts (invalid data skipped), got %d", len(parts))
	}
}

func TestListMultipartUploadsWithInvalidData(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	_ = store.CreateBucket(ctx, "test-bucket")

	store.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte("multipart"))
		return bkt.Put([]byte("test-bucket/key/upload-123"), []byte("invalid-json"))
	})

	uploads, err := store.ListMultipartUploads(ctx, "test-bucket", "")
	if err != nil {
		t.Fatalf("ListMultipartUploads() error: %v", err)
	}
	if len(uploads) != 0 {
		t.Errorf("Expected 0 uploads (invalid data skipped), got %d", len(uploads))
	}
}

func TestGetLifecycleRulesWithInvalidData(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()

	store.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte("lifecycle"))
		return bkt.Put([]byte("test-bucket_bad-rule"), []byte("invalid-json"))
	})

	rules, err := store.GetLifecycleRules(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("GetLifecycleRules() error: %v", err)
	}
	if len(rules) != 0 {
		t.Errorf("Expected 0 rules (invalid data skipped), got %d", len(rules))
	}
}

func TestListBucketMetricsWithInvalidData(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()

	store.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte("metrics"))
		return bkt.Put([]byte("test-bucket:metrics-1"), []byte("invalid-json"))
	})

	configs, err := store.ListBucketMetrics(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("ListBucketMetrics() error: %v", err)
	}
	if len(configs) != 0 {
		t.Errorf("Expected 0 configs (invalid data skipped), got %d", len(configs))
	}
}

func TestListBucketAnalyticsWithInvalidData(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()

	store.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte("analytics"))
		return bkt.Put([]byte("test-bucket:analytics-1"), []byte("invalid-json"))
	})

	configs, err := store.ListBucketAnalytics(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("ListBucketAnalytics() error: %v", err)
	}
	if len(configs) != 0 {
		t.Errorf("Expected 0 configs (invalid data skipped), got %d", len(configs))
	}
}

func TestMustEncode(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic for mustEncode with channel")
		}
	}()
	mustEncode(make(chan int))
}

func TestNewError(t *testing.T) {
	file, err := os.CreateTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	filePath := file.Name()
	file.Close()
	defer os.RemoveAll(filePath)

	_, err = New(filePath)
	if err == nil {
		t.Error("Expected error when path is a file instead of directory")
	}
}

func TestNewWithInvalidPath(t *testing.T) {
	invalidPath := string([]byte{0x00, 'i', 'n', 'v', 'a', 'l', 'i', 'd'})
	_, err := New(invalidPath)
	if err == nil {
		t.Error("Expected error for invalid path with null bytes")
	}
}

func TestNewWithPermissionDenied(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("Skipping test when running as root")
	}

	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	os.Chmod(dir, 0o000)
	defer func() {
		os.Chmod(dir, 0o755)
		os.RemoveAll(dir)
	}()

	err = os.Chmod(dir, 0o000)
	if err != nil {
		t.Skipf("Could not change permissions: %v", err)
	}

	_, err = New(dir)
	if err == nil {
		t.Skip("Permission denied not enforced on this platform")
	}
}

func TestNewWithLongPath(t *testing.T) {
	longPath := ""
	for i := 0; i < 1000; i++ {
		longPath += "a"
	}
	_, err := New(longPath)
	if err == nil {
		t.Error("Expected error for path that is too long")
	}
}

func TestListObjectsWithDifferentBuckets(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	_ = store.CreateBucket(ctx, "bucket-a")
	_ = store.CreateBucket(ctx, "bucket-b")

	_ = store.PutObject(ctx, "bucket-a", "key1", &metadata.ObjectMetadata{Key: "key1", Bucket: "bucket-a"})
	_ = store.PutObject(ctx, "bucket-b", "key2", &metadata.ObjectMetadata{Key: "key2", Bucket: "bucket-b"})

	objects, err := store.ListObjects(ctx, "bucket-a", "", metadata.ListOptions{})
	if err != nil {
		t.Fatalf("ListObjects() error: %v", err)
	}
	if len(objects) != 1 {
		t.Errorf("Expected 1 object, got %d", len(objects))
	}
}

func TestListPartsWithDifferentUploads(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	_ = store.CreateBucket(ctx, "test-bucket")

	_ = store.PutPart(ctx, "test-bucket", "key", "upload-123", 1, &metadata.PartMetadata{PartNumber: 1})
	_ = store.PutPart(ctx, "test-bucket", "key", "upload-456", 1, &metadata.PartMetadata{PartNumber: 1})

	parts, err := store.ListParts(ctx, "test-bucket", "key", "upload-123")
	if err != nil {
		t.Fatalf("ListParts() error: %v", err)
	}
	if len(parts) != 1 {
		t.Errorf("Expected 1 part, got %d", len(parts))
	}
}

func TestListMultipartUploadsWithDifferentBuckets(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	_ = store.CreateBucket(ctx, "bucket-a")
	_ = store.CreateBucket(ctx, "bucket-b")

	_ = store.CreateMultipartUpload(ctx, "bucket-a", "key1", "upload-123", &metadata.ObjectMetadata{})
	_ = store.CreateMultipartUpload(ctx, "bucket-b", "key2", "upload-456", &metadata.ObjectMetadata{})

	uploads, err := store.ListMultipartUploads(ctx, "bucket-a", "")
	if err != nil {
		t.Fatalf("ListMultipartUploads() error: %v", err)
	}
	if len(uploads) != 1 {
		t.Errorf("Expected 1 upload, got %d", len(uploads))
	}
}

func TestListMultipartUploadsWithNilBucket(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()

	store.db.Update(func(tx *bolt.Tx) error {
		tx.DeleteBucket([]byte("multipart"))
		return nil
	})

	uploads, err := store.ListMultipartUploads(ctx, "test-bucket", "")
	if err != nil {
		t.Fatalf("ListMultipartUploads() error: %v", err)
	}
	if len(uploads) != 0 {
		t.Errorf("Expected 0 uploads, got %d", len(uploads))
	}
}

func TestGetLifecycleRulesWithDifferentBuckets(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()

	rule1 := &metadata.LifecycleRule{ID: "rule1", Prefix: "a/"}
	rule2 := &metadata.LifecycleRule{ID: "rule2", Prefix: "b/"}

	_ = store.PutLifecycleRule(ctx, "bucket-a", rule1)
	_ = store.PutLifecycleRule(ctx, "bucket-b", rule2)

	rules, err := store.GetLifecycleRules(ctx, "bucket-a")
	if err != nil {
		t.Fatalf("GetLifecycleRules() error: %v", err)
	}
	if len(rules) != 1 {
		t.Errorf("Expected 1 rule, got %d", len(rules))
	}
}

func TestCompleteMultipartUploadNonExistent(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	_ = store.CreateBucket(ctx, "test-bucket")

	parts := []metadata.PartInfo{{PartNumber: 1}}
	err = store.CompleteMultipartUpload(ctx, "test-bucket", "test-key", "nonexistent-upload", parts)
	if err != nil {
		t.Logf("CompleteMultipartUpload returned error (expected): %v", err)
	}
}

func TestNewBoltOpenError(t *testing.T) {
	invalidPath := "/dev/null/somepath"
	_, err := New(invalidPath)
	if err == nil {
		t.Error("Expected error when bolt.Open fails")
	}
}

func TestNewBucketCreationError(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("Skipping test when running as root")
	}

	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}

	store, err := New(dir)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatal(err)
	}
	store.Close()

	dbPath := dir + "/metadata.db"
	err = os.Chmod(dbPath, 0o000)
	if err != nil {
		os.RemoveAll(dir)
		t.Skipf("Could not change file permissions: %v", err)
	}
	defer func() {
		os.Chmod(dbPath, 0o644)
		os.RemoveAll(dir)
	}()

	_, err = New(dir)
	if err == nil {
		t.Error("Expected error when bucket creation fails")
	}
}

func TestNewWithReadOnlyFilesystem(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("Skipping test when running as root")
	}

	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}

	err = os.Chmod(dir, 0o444)
	if err != nil {
		os.RemoveAll(dir)
		t.Skipf("Could not change directory permissions: %v", err)
	}
	defer func() {
		os.Chmod(dir, 0o755)
		os.RemoveAll(dir)
	}()

	_, err = New(dir)
	if err != nil {
		t.Logf("Got expected error: %v", err)
	} else {
		t.Log("No error returned (permission not enforced on this platform)")
	}
}

func TestNewWithCorruptedDatabase(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	dbPath := dir + "/metadata.db"
	err = os.WriteFile(dbPath, []byte("corrupted data that is not a valid bbolt database"), 0o644)
	if err != nil {
		t.Fatal(err)
	}

	_, err = New(dir)
	if err == nil {
		t.Error("Expected error when opening corrupted database")
	}
}

func TestCompleteMultipartUploadWithClosedDB(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	_ = store.CreateBucket(ctx, "test-bucket")
	_ = store.CreateMultipartUpload(ctx, "test-bucket", "test-key", "upload-123", &metadata.ObjectMetadata{})
	_ = store.PutPart(ctx, "test-bucket", "test-key", "upload-123", 1, &metadata.PartMetadata{})

	store.Close()

	parts := []metadata.PartInfo{{PartNumber: 1}}
	err = store.CompleteMultipartUpload(ctx, "test-bucket", "test-key", "upload-123", parts)
	if err == nil {
		t.Error("Expected error when DB is closed")
	}
}

func TestAbortMultipartUploadWithClosedDB(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	_ = store.CreateBucket(ctx, "test-bucket")
	_ = store.CreateMultipartUpload(ctx, "test-bucket", "test-key", "upload-123", &metadata.ObjectMetadata{})

	store.Close()

	err = store.AbortMultipartUpload(ctx, "test-bucket", "test-key", "upload-123")
	if err == nil {
		t.Error("Expected error when DB is closed")
	}
}

func TestCreateBucketWithClosedDB(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	store.Close()

	ctx := context.Background()
	err = store.CreateBucket(ctx, "test-bucket")
	if err == nil {
		t.Error("Expected error when DB is closed")
	}
}

func TestDeleteBucketWithClosedDB(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	store.Close()

	ctx := context.Background()
	err = store.DeleteBucket(ctx, "test-bucket")
	if err == nil {
		t.Error("Expected error when DB is closed")
	}
}

func TestGetBucketWithClosedDB(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	store.Close()

	ctx := context.Background()
	_, err = store.GetBucket(ctx, "test-bucket")
	if err == nil {
		t.Error("Expected error when DB is closed")
	}
}

func TestListBucketsWithClosedDB(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	store.Close()

	ctx := context.Background()
	_, err = store.ListBuckets(ctx)
	if err == nil {
		t.Error("Expected error when DB is closed")
	}
}

func TestPutObjectWithClosedDB(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	store.Close()

	ctx := context.Background()
	err = store.PutObject(ctx, "test-bucket", "test-key", &metadata.ObjectMetadata{})
	if err == nil {
		t.Error("Expected error when DB is closed")
	}
}

func TestGetObjectWithClosedDB(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	store.Close()

	ctx := context.Background()
	_, err = store.GetObject(ctx, "test-bucket", "test-key", "")
	if err == nil {
		t.Error("Expected error when DB is closed")
	}
}

func TestDeleteObjectWithClosedDB(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	store.Close()

	ctx := context.Background()
	err = store.DeleteObject(ctx, "test-bucket", "test-key", "")
	if err == nil {
		t.Error("Expected error when DB is closed")
	}
}

func TestListObjectsWithClosedDB(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	store.Close()

	ctx := context.Background()
	_, err = store.ListObjects(ctx, "test-bucket", "", metadata.ListOptions{})
	if err == nil {
		t.Error("Expected error when DB is closed")
	}
}

func TestCreateMultipartUploadWithClosedDB(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	store.Close()

	ctx := context.Background()
	err = store.CreateMultipartUpload(ctx, "test-bucket", "test-key", "upload-123", &metadata.ObjectMetadata{})
	if err == nil {
		t.Error("Expected error when DB is closed")
	}
}

func TestPutPartWithClosedDB(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	store.Close()

	ctx := context.Background()
	err = store.PutPart(ctx, "test-bucket", "test-key", "upload-123", 1, &metadata.PartMetadata{})
	if err == nil {
		t.Error("Expected error when DB is closed")
	}
}

func TestListPartsWithClosedDB(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	store.Close()

	ctx := context.Background()
	_, err = store.ListParts(ctx, "test-bucket", "test-key", "upload-123")
	if err == nil {
		t.Error("Expected error when DB is closed")
	}
}

func TestListMultipartUploadsWithClosedDB(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	store.Close()

	ctx := context.Background()
	_, err = store.ListMultipartUploads(ctx, "test-bucket", "")
	if err == nil {
		t.Error("Expected error when DB is closed")
	}
}

func TestPutLifecycleRuleWithClosedDB(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	store.Close()

	ctx := context.Background()
	err = store.PutLifecycleRule(ctx, "test-bucket", &metadata.LifecycleRule{ID: "rule1"})
	if err == nil {
		t.Error("Expected error when DB is closed")
	}
}

func TestGetLifecycleRulesWithClosedDB(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	store.Close()

	ctx := context.Background()
	_, err = store.GetLifecycleRules(ctx, "test-bucket")
	if err == nil {
		t.Error("Expected error when DB is closed")
	}
}

func TestDeleteLifecycleRuleWithClosedDB(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	store.Close()

	ctx := context.Background()
	err = store.DeleteLifecycleRule(ctx, "test-bucket", "rule1")
	if err == nil {
		t.Error("Expected error when DB is closed")
	}
}

func TestPutBucketVersioningWithClosedDB(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	store.Close()

	ctx := context.Background()
	err = store.PutBucketVersioning(ctx, "test-bucket", &metadata.BucketVersioning{Status: "Enabled"})
	if err == nil {
		t.Error("Expected error when DB is closed")
	}
}

func TestGetBucketVersioningWithClosedDB(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	store.Close()

	ctx := context.Background()
	_, err = store.GetBucketVersioning(ctx, "test-bucket")
	if err == nil {
		t.Error("Expected error when DB is closed")
	}
}

func TestPutBucketPolicyWithClosedDB(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	store.Close()

	ctx := context.Background()
	policy := "test"
	err = store.PutBucketPolicy(ctx, "test-bucket", &policy)
	if err == nil {
		t.Error("Expected error when DB is closed")
	}
}

func TestGetBucketPolicyWithClosedDB(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	store.Close()

	ctx := context.Background()
	_, err = store.GetBucketPolicy(ctx, "test-bucket")
	if err == nil {
		t.Error("Expected error when DB is closed")
	}
}

func TestDeleteBucketPolicyWithClosedDB(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	store.Close()

	ctx := context.Background()
	err = store.DeleteBucketPolicy(ctx, "test-bucket")
	if err == nil {
		t.Error("Expected error when DB is closed")
	}
}

func TestCloseTwice(t *testing.T) {
	dir, err := os.MkdirTemp("", "bbolt-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}

	err = store.Close()
	if err != nil {
		t.Fatalf("First close error: %v", err)
	}

	err = store.Close()
	if err == nil {
		t.Log("Second close returned nil (acceptable)")
	}
}
