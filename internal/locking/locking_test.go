package locking

import (
	"context"
	"testing"
	"time"
)

func TestNewObjectLock(t *testing.T) {
	ol := NewObjectLock()
	if ol == nil {
		t.Fatal("ObjectLock should not be nil")
	}
	if ol.locks == nil {
		t.Error("locks map should be initialized")
	}
	if ol.retention == nil {
		t.Error("retention map should be initialized")
	}
	if ol.legalHolds == nil {
		t.Error("legalHolds map should be initialized")
	}
}

func TestLockTypeConstants(t *testing.T) {
	if LockTypeExclusive != "exclusive" {
		t.Errorf("LockTypeExclusive = %v, want exclusive", LockTypeExclusive)
	}
	if LockTypeShared != "shared" {
		t.Errorf("LockTypeShared = %v, want shared", LockTypeShared)
	}
}

func TestObjectLockConfigStruct(t *testing.T) {
	config := ObjectLockConfig{
		Enabled:        true,
		RetentionMode:  "GOVERNANCE",
		RetentionDays:  30,
		RetentionYears: 1,
	}

	if !config.Enabled {
		t.Error("Enabled should be true")
	}
	if config.RetentionMode != "GOVERNANCE" {
		t.Errorf("RetentionMode = %v, want GOVERNANCE", config.RetentionMode)
	}
}

func TestRetentionPolicyStruct(t *testing.T) {
	policy := RetentionPolicy{
		Bucket:      "test-bucket",
		Key:         "test-key",
		Mode:        "GOVERNANCE",
		RetainUntil: time.Now().Add(24 * time.Hour),
		CreatedAt:   time.Now(),
		CreatedBy:   "user1",
	}

	if policy.Bucket != "test-bucket" {
		t.Errorf("Bucket = %v, want test-bucket", policy.Bucket)
	}
}

func TestLegalHoldStruct(t *testing.T) {
	hold := LegalHold{
		Bucket:    "test-bucket",
		Key:       "test-key",
		Status:    "ON",
		CreatedAt: time.Now(),
		CreatedBy: "user1",
	}

	if hold.Status != "ON" {
		t.Errorf("Status = %v, want ON", hold.Status)
	}
}

func TestEnableObjectLock(t *testing.T) {
	ol := NewObjectLock()

	err := ol.EnableObjectLock("test-bucket", "GOVERNANCE", 30, 0)
	if err != nil {
		t.Fatalf("EnableObjectLock failed: %v", err)
	}

	if !ol.IsObjectLockEnabled("test-bucket") {
		t.Error("Object lock should be enabled")
	}

	config, ok := ol.GetLockConfig("test-bucket")
	if !ok {
		t.Fatal("Should find lock config")
	}
	if config.RetentionMode != "GOVERNANCE" {
		t.Errorf("RetentionMode = %s, want GOVERNANCE", config.RetentionMode)
	}
	if config.RetentionDays != 30 {
		t.Errorf("RetentionDays = %d, want 30", config.RetentionDays)
	}
}

func TestEnableObjectLockCompliance(t *testing.T) {
	ol := NewObjectLock()

	err := ol.EnableObjectLock("test-bucket", "COMPLIANCE", 0, 1)
	if err != nil {
		t.Fatalf("EnableObjectLock failed: %v", err)
	}

	config, _ := ol.GetLockConfig("test-bucket")
	if config.RetentionMode != "COMPLIANCE" {
		t.Errorf("RetentionMode = %v, want COMPLIANCE", config.RetentionMode)
	}
	if config.RetentionYears != 1 {
		t.Errorf("RetentionYears = %d, want 1", config.RetentionYears)
	}
}

func TestDisableObjectLock(t *testing.T) {
	ol := NewObjectLock()

	ol.EnableObjectLock("test-bucket", "GOVERNANCE", 30, 0)
	err := ol.DisableObjectLock("test-bucket")
	if err != nil {
		t.Fatalf("DisableObjectLock failed: %v", err)
	}

	if ol.IsObjectLockEnabled("test-bucket") {
		t.Error("Object lock should be disabled")
	}
}

func TestDisableObjectLockNotEnabled(t *testing.T) {
	ol := NewObjectLock()

	err := ol.DisableObjectLock("nonexistent-bucket")
	if err != nil {
		t.Fatalf("DisableObjectLock should not fail for nonexistent bucket")
	}
}

func TestIsObjectLockEnabled(t *testing.T) {
	ol := NewObjectLock()

	if ol.IsObjectLockEnabled("nonexistent") {
		t.Error("Object lock should not be enabled for nonexistent bucket")
	}

	ol.EnableObjectLock("test-bucket", "GOVERNANCE", 30, 0)
	if !ol.IsObjectLockEnabled("test-bucket") {
		t.Error("Object lock should be enabled")
	}
}

func TestGetLockConfig(t *testing.T) {
	ol := NewObjectLock()

	_, ok := ol.GetLockConfig("nonexistent")
	if ok {
		t.Error("Should not find config for nonexistent bucket")
	}

	ol.EnableObjectLock("test-bucket", "GOVERNANCE", 30, 0)
	config, ok := ol.GetLockConfig("test-bucket")
	if !ok {
		t.Fatal("Should find lock config")
	}
	if config.RetentionDays != 30 {
		t.Errorf("RetentionDays = %d, want 30", config.RetentionDays)
	}
}

func TestSetRetention(t *testing.T) {
	ol := NewObjectLock()
	ol.EnableObjectLock("test-bucket", "GOVERNANCE", 30, 0)

	retainUntil := time.Now().Add(24 * time.Hour)
	err := ol.SetRetention(context.Background(), "test-bucket", "test-key", "GOVERNANCE", retainUntil, "user1")
	if err != nil {
		t.Fatalf("SetRetention failed: %v", err)
	}

	policy, ok := ol.GetRetention("test-bucket", "test-key")
	if !ok {
		t.Fatal("Should find retention policy")
	}
	if policy.Mode != "GOVERNANCE" {
		t.Errorf("Mode = %s, want GOVERNANCE", policy.Mode)
	}
	if policy.CreatedBy != "user1" {
		t.Errorf("CreatedBy = %s, want user1", policy.CreatedBy)
	}
}

func TestSetRetentionCompliance(t *testing.T) {
	ol := NewObjectLock()
	ol.EnableObjectLock("test-bucket", "COMPLIANCE", 30, 0)

	retainUntil := time.Now().Add(24 * time.Hour)
	err := ol.SetRetention(context.Background(), "test-bucket", "test-key", "COMPLIANCE", retainUntil, "user1")
	if err != nil {
		t.Fatalf("SetRetention failed: %v", err)
	}

	policy, _ := ol.GetRetention("test-bucket", "test-key")
	if policy.Mode != "COMPLIANCE" {
		t.Errorf("Mode = %v, want COMPLIANCE", policy.Mode)
	}
}

func TestSetRetentionLockNotEnabled(t *testing.T) {
	ol := NewObjectLock()

	retainUntil := time.Now().Add(24 * time.Hour)
	err := ol.SetRetention(context.Background(), "test-bucket", "test-key", "GOVERNANCE", retainUntil, "user1")
	if err == nil {
		t.Error("SetRetention should fail when lock not enabled")
	}
}

func TestSetRetentionInvalidMode(t *testing.T) {
	ol := NewObjectLock()
	ol.EnableObjectLock("test-bucket", "GOVERNANCE", 30, 0)

	retainUntil := time.Now().Add(24 * time.Hour)
	err := ol.SetRetention(context.Background(), "test-bucket", "test-key", "INVALID", retainUntil, "user1")
	if err == nil {
		t.Error("SetRetention should fail for invalid mode")
	}
}

func TestGetRetention(t *testing.T) {
	ol := NewObjectLock()
	ol.EnableObjectLock("test-bucket", "GOVERNANCE", 30, 0)

	_, ok := ol.GetRetention("test-bucket", "nonexistent")
	if ok {
		t.Error("Should not find retention for nonexistent key")
	}

	retainUntil := time.Now().Add(24 * time.Hour)
	ol.SetRetention(context.Background(), "test-bucket", "test-key", "GOVERNANCE", retainUntil, "user1")

	policy, ok := ol.GetRetention("test-bucket", "test-key")
	if !ok {
		t.Fatal("Should find retention policy")
	}
	if policy.Key != "test-key" {
		t.Errorf("Key = %v, want test-key", policy.Key)
	}
}

func TestRemoveRetention(t *testing.T) {
	ol := NewObjectLock()
	ol.EnableObjectLock("test-bucket", "GOVERNANCE", 30, 0)

	// Set expired retention
	retainUntil := time.Now().Add(-1 * time.Hour)
	ol.SetRetention(context.Background(), "test-bucket", "test-key", "GOVERNANCE", retainUntil, "user1")

	err := ol.RemoveRetention("test-bucket", "test-key", "user1")
	if err != nil {
		t.Fatalf("RemoveRetention failed: %v", err)
	}

	_, ok := ol.GetRetention("test-bucket", "test-key")
	if ok {
		t.Error("Retention should be removed")
	}
}

func TestRemoveRetentionUnderRetention(t *testing.T) {
	ol := NewObjectLock()
	ol.EnableObjectLock("test-bucket", "GOVERNANCE", 30, 0)

	// Set active retention
	retainUntil := time.Now().Add(24 * time.Hour)
	ol.SetRetention(context.Background(), "test-bucket", "test-key", "GOVERNANCE", retainUntil, "user1")

	err := ol.RemoveRetention("test-bucket", "test-key", "user1")
	if err == nil {
		t.Error("RemoveRetention should fail when retention is active")
	}
}

func TestRemoveRetentionCompliance(t *testing.T) {
	ol := NewObjectLock()
	ol.EnableObjectLock("test-bucket", "COMPLIANCE", 30, 0)

	retainUntil := time.Now().Add(-1 * time.Hour)
	ol.SetRetention(context.Background(), "test-bucket", "test-key", "COMPLIANCE", retainUntil, "user1")

	err := ol.RemoveRetention("test-bucket", "test-key", "user1")
	if err == nil {
		t.Error("RemoveRetention should fail for COMPLIANCE mode")
	}
}

func TestRemoveRetentionNonexistent(t *testing.T) {
	ol := NewObjectLock()

	err := ol.RemoveRetention("test-bucket", "nonexistent", "user1")
	if err != nil {
		t.Fatalf("RemoveRetention should succeed for nonexistent retention")
	}
}

func TestSetLegalHold(t *testing.T) {
	ol := NewObjectLock()
	ol.EnableObjectLock("test-bucket", "GOVERNANCE", 30, 0)

	err := ol.SetLegalHold("test-bucket", "test-key", "ON", "user1")
	if err != nil {
		t.Fatalf("SetLegalHold failed: %v", err)
	}

	hold, ok := ol.GetLegalHold("test-bucket", "test-key")
	if !ok {
		t.Fatal("Should find legal hold")
	}
	if hold.Status != "ON" {
		t.Errorf("Status = %s, want ON", hold.Status)
	}
	if hold.CreatedBy != "user1" {
		t.Errorf("CreatedBy = %s, want user1", hold.CreatedBy)
	}
}

func TestSetLegalHoldLockNotEnabled(t *testing.T) {
	ol := NewObjectLock()

	err := ol.SetLegalHold("test-bucket", "test-key", "ON", "user1")
	if err == nil {
		t.Error("SetLegalHold should fail when lock not enabled")
	}
}

func TestGetLegalHold(t *testing.T) {
	ol := NewObjectLock()
	ol.EnableObjectLock("test-bucket", "GOVERNANCE", 30, 0)

	_, ok := ol.GetLegalHold("test-bucket", "nonexistent")
	if ok {
		t.Error("Should not find legal hold for nonexistent key")
	}

	ol.SetLegalHold("test-bucket", "test-key", "ON", "user1")

	hold, ok := ol.GetLegalHold("test-bucket", "test-key")
	if !ok {
		t.Fatal("Should find legal hold")
	}
	if hold.Key != "test-key" {
		t.Errorf("Key = %v, want test-key", hold.Key)
	}
}

func TestDeleteLegalHold(t *testing.T) {
	ol := NewObjectLock()
	ol.EnableObjectLock("test-bucket", "GOVERNANCE", 30, 0)

	ol.SetLegalHold("test-bucket", "test-key", "ON", "user1")

	err := ol.DeleteLegalHold("test-bucket", "test-key")
	if err != nil {
		t.Fatalf("DeleteLegalHold failed: %v", err)
	}

	_, ok := ol.GetLegalHold("test-bucket", "test-key")
	if ok {
		t.Error("Legal hold should be deleted")
	}
}

func TestDeleteLegalHoldNonexistent(t *testing.T) {
	ol := NewObjectLock()

	err := ol.DeleteLegalHold("test-bucket", "nonexistent")
	if err != nil {
		t.Fatalf("DeleteLegalHold should not fail for nonexistent hold")
	}
}

func TestCheckRetention(t *testing.T) {
	ol := NewObjectLock()
	ol.EnableObjectLock("test-bucket", "GOVERNANCE", 30, 0)

	// No retention or legal hold
	err := ol.CheckRetention("test-bucket", "test-key")
	if err != nil {
		t.Errorf("CheckRetention should pass for object without retention: %v", err)
	}
}

func TestCheckRetentionWithLegalHold(t *testing.T) {
	ol := NewObjectLock()
	ol.EnableObjectLock("test-bucket", "GOVERNANCE", 30, 0)

	ol.SetLegalHold("test-bucket", "test-key", "ON", "user1")

	err := ol.CheckRetention("test-bucket", "test-key")
	if err == nil {
		t.Error("CheckRetention should fail when legal hold is active")
	}
}

func TestCheckRetentionWithLegalHoldOff(t *testing.T) {
	ol := NewObjectLock()
	ol.EnableObjectLock("test-bucket", "GOVERNANCE", 30, 0)

	ol.SetLegalHold("test-bucket", "test-key", "OFF", "user1")

	err := ol.CheckRetention("test-bucket", "test-key")
	if err != nil {
		t.Errorf("CheckRetention should pass when legal hold is OFF: %v", err)
	}
}

func TestCheckRetentionUnderRetention(t *testing.T) {
	ol := NewObjectLock()
	ol.EnableObjectLock("test-bucket", "GOVERNANCE", 30, 0)

	retainUntil := time.Now().Add(24 * time.Hour)
	ol.SetRetention(context.Background(), "test-bucket", "test-key", "GOVERNANCE", retainUntil, "user1")

	err := ol.CheckRetention("test-bucket", "test-key")
	if err == nil {
		t.Error("CheckRetention should fail when retention is active")
	}
}

func TestCheckRetentionExpired(t *testing.T) {
	ol := NewObjectLock()
	ol.EnableObjectLock("test-bucket", "GOVERNANCE", 30, 0)

	retainUntil := time.Now().Add(-1 * time.Hour)
	ol.SetRetention(context.Background(), "test-bucket", "test-key", "GOVERNANCE", retainUntil, "user1")

	err := ol.CheckRetention("test-bucket", "test-key")
	if err != nil {
		t.Errorf("CheckRetention should pass for expired retention: %v", err)
	}
}

func TestListRetentions(t *testing.T) {
	ol := NewObjectLock()
	ol.EnableObjectLock("test-bucket", "GOVERNANCE", 30, 0)

	// Empty list
	retentions := ol.ListRetentions("test-bucket")
	if len(retentions) != 0 {
		t.Errorf("len(retentions) = %d, want 0", len(retentions))
	}

	// Add retentions
	retainUntil := time.Now().Add(24 * time.Hour)
	ol.SetRetention(context.Background(), "test-bucket", "key1", "GOVERNANCE", retainUntil, "user1")
	ol.SetRetention(context.Background(), "test-bucket", "key2", "GOVERNANCE", retainUntil, "user1")

	retentions = ol.ListRetentions("test-bucket")
	if len(retentions) != 2 {
		t.Errorf("len(retentions) = %d, want 2", len(retentions))
	}
}

func TestListRetentionsOtherBucket(t *testing.T) {
	ol := NewObjectLock()
	ol.EnableObjectLock("test-bucket", "GOVERNANCE", 30, 0)
	ol.EnableObjectLock("other-bucket", "GOVERNANCE", 30, 0)

	retainUntil := time.Now().Add(24 * time.Hour)
	ol.SetRetention(context.Background(), "test-bucket", "key1", "GOVERNANCE", retainUntil, "user1")
	ol.SetRetention(context.Background(), "other-bucket", "key2", "GOVERNANCE", retainUntil, "user1")

	retentions := ol.ListRetentions("test-bucket")
	if len(retentions) != 1 {
		t.Errorf("len(retentions) = %d, want 1", len(retentions))
	}
}

func TestListLegalHolds(t *testing.T) {
	ol := NewObjectLock()
	ol.EnableObjectLock("test-bucket", "GOVERNANCE", 30, 0)

	// Empty list
	holds := ol.ListLegalHolds("test-bucket")
	if len(holds) != 0 {
		t.Errorf("len(holds) = %d, want 0", len(holds))
	}

	// Add legal holds
	ol.SetLegalHold("test-bucket", "key1", "ON", "user1")
	ol.SetLegalHold("test-bucket", "key2", "ON", "user1")

	holds = ol.ListLegalHolds("test-bucket")
	if len(holds) != 2 {
		t.Errorf("len(holds) = %d, want 2", len(holds))
	}
}

func TestListLegalHoldsOtherBucket(t *testing.T) {
	ol := NewObjectLock()
	ol.EnableObjectLock("test-bucket", "GOVERNANCE", 30, 0)
	ol.EnableObjectLock("other-bucket", "GOVERNANCE", 30, 0)

	ol.SetLegalHold("test-bucket", "key1", "ON", "user1")
	ol.SetLegalHold("other-bucket", "key2", "ON", "user1")

	holds := ol.ListLegalHolds("test-bucket")
	if len(holds) != 1 {
		t.Errorf("len(holds) = %d, want 1", len(holds))
	}
}

func TestIsLockEnabled(t *testing.T) {
	ol := NewObjectLock()

	if ol.isLockEnabled("nonexistent") {
		t.Error("isLockEnabled should return false for nonexistent bucket")
	}

	ol.EnableObjectLock("test-bucket", "GOVERNANCE", 30, 0)
	if !ol.isLockEnabled("test-bucket") {
		t.Error("isLockEnabled should return true for enabled bucket")
	}
}

func TestConcurrentAccess(t *testing.T) {
	ol := NewObjectLock()
	ol.EnableObjectLock("test-bucket", "GOVERNANCE", 30, 0)

	done := make(chan bool)

	// Concurrent writes
	for i := 0; i < 10; i++ {
		go func(i int) {
			key := string(rune('a' + i))
			ol.SetLegalHold("test-bucket", key, "ON", "user1")
			done <- true
		}(i)
	}

	// Concurrent reads
	for i := 0; i < 10; i++ {
		go func(i int) {
			key := string(rune('a' + i))
			ol.GetLegalHold("test-bucket", key)
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 20; i++ {
		<-done
	}
}

func TestSetRetentionComplianceReduceRetention(t *testing.T) {
	ol := NewObjectLock()
	ol.EnableObjectLock("test-bucket", "COMPLIANCE", 30, 0)

	retainUntil := time.Now().Add(48 * time.Hour)
	err := ol.SetRetention(context.Background(), "test-bucket", "test-key", "COMPLIANCE", retainUntil, "user1")
	if err != nil {
		t.Fatalf("SetRetention failed: %v", err)
	}

	shorterRetainUntil := time.Now().Add(24 * time.Hour)
	err = ol.SetRetention(context.Background(), "test-bucket", "test-key", "COMPLIANCE", shorterRetainUntil, "user1")
	if err == nil {
		t.Error("SetRetention should fail when reducing COMPLIANCE retention period")
	}
}

func TestSetRetentionGovernanceCanReduce(t *testing.T) {
	ol := NewObjectLock()
	ol.EnableObjectLock("test-bucket", "GOVERNANCE", 30, 0)

	retainUntil := time.Now().Add(48 * time.Hour)
	err := ol.SetRetention(context.Background(), "test-bucket", "test-key", "GOVERNANCE", retainUntil, "user1")
	if err != nil {
		t.Fatalf("SetRetention failed: %v", err)
	}

	shorterRetainUntil := time.Now().Add(24 * time.Hour)
	err = ol.SetRetention(context.Background(), "test-bucket", "test-key", "GOVERNANCE", shorterRetainUntil, "user1")
	if err != nil {
		t.Errorf("GOVERNANCE mode should allow reducing retention: %v", err)
	}
}
