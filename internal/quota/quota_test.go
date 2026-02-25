package quota

import (
	"context"
	"testing"
	"time"
)

func TestQuotaTypes(t *testing.T) {
	if QuotaTypeStorage != "storage" {
		t.Errorf("QuotaTypeStorage = %v, want storage", QuotaTypeStorage)
	}
	if QuotaTypeObjects != "objects" {
		t.Errorf("QuotaTypeObjects = %v, want objects", QuotaTypeObjects)
	}
	if QuotaTypeBandwidth != "bandwidth" {
		t.Errorf("QuotaTypeBandwidth = %v, want bandwidth", QuotaTypeBandwidth)
	}
}

func TestSetAndGetQuota(t *testing.T) {
	mgr := NewQuotaManager()

	err := mgr.SetQuota("bucket1", QuotaTypeStorage, 1000, 0.8, true)
	if err != nil {
		t.Fatalf("SetQuota failed: %v", err)
	}

	quota, ok := mgr.GetQuota("bucket1")
	if !ok {
		t.Fatal("GetQuota should return ok=true")
	}
	if quota.Bucket != "bucket1" {
		t.Errorf("Bucket = %v, want bucket1", quota.Bucket)
	}
	if quota.Type != QuotaTypeStorage {
		t.Errorf("Type = %v, want storage", quota.Type)
	}
	if quota.Limit != 1000 {
		t.Errorf("Limit = %v, want 1000", quota.Limit)
	}
	if quota.WarningThreshold != 0.8 {
		t.Errorf("WarningThreshold = %v, want 0.8", quota.WarningThreshold)
	}
	if !quota.Enforce {
		t.Error("Enforce should be true")
	}
}

func TestGetQuotaNotFound(t *testing.T) {
	mgr := NewQuotaManager()

	_, ok := mgr.GetQuota("nonexistent")
	if ok {
		t.Error("GetQuota should return ok=false for nonexistent bucket")
	}
}

func TestDeleteQuota(t *testing.T) {
	mgr := NewQuotaManager()

	mgr.SetQuota("bucket1", QuotaTypeStorage, 1000, 0.8, true)
	mgr.DeleteQuota("bucket1")

	_, ok := mgr.GetQuota("bucket1")
	if ok {
		t.Error("GetQuota should return ok=false after DeleteQuota")
	}
}

func TestCheckQuotaNoQuotaSet(t *testing.T) {
	mgr := NewQuotaManager()

	ok, status, err := mgr.CheckQuota(context.Background(), "bucket1", QuotaTypeStorage, 100)
	if !ok {
		t.Error("CheckQuota should return true when no quota set")
	}
	if status != "" {
		t.Errorf("status = %v, want empty", status)
	}
	if err != nil {
		t.Errorf("err = %v, want nil", err)
	}
}

func TestCheckQuotaDifferentType(t *testing.T) {
	mgr := NewQuotaManager()
	mgr.SetQuota("bucket1", QuotaTypeStorage, 1000, 0.8, true)

	ok, status, _ := mgr.CheckQuota(context.Background(), "bucket1", QuotaTypeObjects, 100)
	if !ok {
		t.Error("CheckQuota should return true for different quota type")
	}
	if status != "" {
		t.Errorf("status = %v, want empty", status)
	}
}

func TestCheckQuotaWithinLimit(t *testing.T) {
	mgr := NewQuotaManager()
	mgr.SetQuota("bucket1", QuotaTypeStorage, 1000, 0.8, true)

	ok, status, err := mgr.CheckQuota(context.Background(), "bucket1", QuotaTypeStorage, 500)
	if !ok {
		t.Error("CheckQuota should return true when within limit")
	}
	if status != "" {
		t.Errorf("status = %v, want empty", status)
	}
	if err != nil {
		t.Errorf("err = %v, want nil", err)
	}
}

func TestCheckQuotaExceedsLimit(t *testing.T) {
	mgr := NewQuotaManager()
	mgr.SetQuota("bucket1", QuotaTypeStorage, 1000, 0.8, true)

	ok, status, err := mgr.CheckQuota(context.Background(), "bucket1", QuotaTypeStorage, 1500)
	if ok {
		t.Error("CheckQuota should return false when exceeds limit")
	}
	if status != "QuotaExceeded" {
		t.Errorf("status = %v, want QuotaExceeded", status)
	}
	if err == nil {
		t.Error("err should not be nil when quota exceeded")
	}
}

func TestCheckQuotaWarningThreshold(t *testing.T) {
	mgr := NewQuotaManager()
	mgr.SetQuota("bucket1", QuotaTypeStorage, 1000, 0.5, true)

	ok, status, err := mgr.CheckQuota(context.Background(), "bucket1", QuotaTypeStorage, 600)
	if !ok {
		t.Error("CheckQuota should return true when within limit but above threshold")
	}
	if status != "QuotaWarning" {
		t.Errorf("status = %v, want QuotaWarning", status)
	}
	if err != nil {
		t.Errorf("err = %v, want nil", err)
	}
}

func TestCheckQuotaNoEnforce(t *testing.T) {
	mgr := NewQuotaManager()
	mgr.SetQuota("bucket1", QuotaTypeStorage, 1000, 0.8, false)

	ok, status, _ := mgr.CheckQuota(context.Background(), "bucket1", QuotaTypeStorage, 1500)
	if !ok {
		t.Error("CheckQuota should return true when not enforced")
	}
	if status != "QuotaWarning" {
		t.Errorf("status = %v, want QuotaWarning", status)
	}
}

func TestCheckQuotaNoEnforceNoWarningThreshold(t *testing.T) {
	mgr := NewQuotaManager()
	mgr.SetQuota("bucket1", QuotaTypeStorage, 1000, 0, false)

	ok, status, _ := mgr.CheckQuota(context.Background(), "bucket1", QuotaTypeStorage, 1500)
	if !ok {
		t.Error("CheckQuota should return true when not enforced")
	}
	if status != "" {
		t.Errorf("status = %v, want empty", status)
	}
}

func TestUpdateUsage(t *testing.T) {
	mgr := NewQuotaManager()
	mgr.SetQuota("bucket1", QuotaTypeStorage, 1000, 0.8, true)

	err := mgr.UpdateUsage("bucket1", QuotaTypeStorage, 100)
	if err != nil {
		t.Fatalf("UpdateUsage failed: %v", err)
	}

	quota, _ := mgr.GetQuota("bucket1")
	if quota.Used != 100 {
		t.Errorf("Used = %v, want 100", quota.Used)
	}
}

func TestUpdateUsageNoQuota(t *testing.T) {
	mgr := NewQuotaManager()

	err := mgr.UpdateUsage("bucket1", QuotaTypeStorage, 100)
	if err != nil {
		t.Errorf("UpdateUsage should return nil when no quota set")
	}
}

func TestUpdateUsageDifferentType(t *testing.T) {
	mgr := NewQuotaManager()
	mgr.SetQuota("bucket1", QuotaTypeStorage, 1000, 0.8, true)

	err := mgr.UpdateUsage("bucket1", QuotaTypeObjects, 100)
	if err != nil {
		t.Fatalf("UpdateUsage failed: %v", err)
	}

	quota, _ := mgr.GetQuota("bucket1")
	if quota.Used != 0 {
		t.Errorf("Used = %v, want 0 (different type)", quota.Used)
	}
}

func TestResetUsage(t *testing.T) {
	mgr := NewQuotaManager()
	mgr.SetQuota("bucket1", QuotaTypeStorage, 1000, 0.8, true)
	mgr.UpdateUsage("bucket1", QuotaTypeStorage, 100)

	err := mgr.ResetUsage("bucket1")
	if err != nil {
		t.Fatalf("ResetUsage failed: %v", err)
	}

	quota, _ := mgr.GetQuota("bucket1")
	if quota.Used != 0 {
		t.Errorf("Used = %v, want 0 after reset", quota.Used)
	}
}

func TestResetUsageNoQuota(t *testing.T) {
	mgr := NewQuotaManager()

	err := mgr.ResetUsage("bucket1")
	if err != nil {
		t.Errorf("ResetUsage should return nil when no quota set")
	}
}

func TestSetBandwidthLimit(t *testing.T) {
	mgr := NewQuotaManager()
	mgr.SetBandwidthLimit("bucket1", 10000)

	if mgr.bandwidth["bucket1"] == nil {
		t.Error("bandwidth tracker should be set")
	}
	if mgr.bandwidth["bucket1"].limit != 10000 {
		t.Errorf("limit = %v, want 10000", mgr.bandwidth["bucket1"].limit)
	}
}

func TestCheckBandwidthNoLimit(t *testing.T) {
	mgr := NewQuotaManager()

	ok, err := mgr.CheckBandwidth("bucket1", 1000, 1000)
	if !ok {
		t.Error("CheckBandwidth should return true when no limit set")
	}
	if err != nil {
		t.Errorf("err = %v, want nil", err)
	}
}

func TestCheckBandwidthWithinLimit(t *testing.T) {
	mgr := NewQuotaManager()
	mgr.SetBandwidthLimit("bucket1", 10000)

	ok, err := mgr.CheckBandwidth("bucket1", 1000, 1000)
	if !ok {
		t.Error("CheckBandwidth should return true when within limit")
	}
	if err != nil {
		t.Errorf("err = %v, want nil", err)
	}
}

func TestCheckBandwidthExceedsLimit(t *testing.T) {
	mgr := NewQuotaManager()
	mgr.SetBandwidthLimit("bucket1", 1000)

	ok, err := mgr.CheckBandwidth("bucket1", 500, 1500)
	if ok {
		t.Error("CheckBandwidth should return false when write exceeds limit")
	}
	if err == nil {
		t.Error("err should not be nil when bandwidth exceeded")
	}

	ok, err = mgr.CheckBandwidth("bucket1", 1500, 500)
	if ok {
		t.Error("CheckBandwidth should return false when read exceeds limit")
	}
}

func TestCheckBandwidthReset(t *testing.T) {
	mgr := NewQuotaManager()
	mgr.SetBandwidthLimit("bucket1", 10000)

	mgr.CheckBandwidth("bucket1", 1000, 1000)

	tracker := mgr.bandwidth["bucket1"]
	tracker.lastReset = time.Now().Add(-2 * time.Second)

	ok, err := mgr.CheckBandwidth("bucket1", 1000, 1000)
	if !ok {
		t.Error("CheckBandwidth should return true after reset")
	}
	if err != nil {
		t.Errorf("err = %v, want nil", err)
	}
}

func TestGetUsage(t *testing.T) {
	mgr := NewQuotaManager()
	mgr.SetQuota("bucket1", QuotaTypeStorage, 1000, 0.8, true)
	mgr.UpdateUsage("bucket1", QuotaTypeStorage, 500)

	usage, err := mgr.GetUsage("bucket1")
	if err != nil {
		t.Fatalf("GetUsage failed: %v", err)
	}

	if usage["bucket"] != "bucket1" {
		t.Errorf("bucket = %v, want bucket1", usage["bucket"])
	}
	if usage["type"] != QuotaTypeStorage {
		t.Errorf("type = %v, want storage", usage["type"])
	}
	if usage["limit"] != int64(1000) {
		t.Errorf("limit = %v, want 1000", usage["limit"])
	}
	if usage["used"] != int64(500) {
		t.Errorf("used = %v, want 500", usage["used"])
	}
	if usage["available"] != int64(500) {
		t.Errorf("available = %v, want 500", usage["available"])
	}
	if usage["usage_percent"] != 50.0 {
		t.Errorf("usage_percent = %v, want 50.0", usage["usage_percent"])
	}
}

func TestGetUsageNoQuota(t *testing.T) {
	mgr := NewQuotaManager()

	_, err := mgr.GetUsage("bucket1")
	if err == nil {
		t.Error("GetUsage should return error when no quota set")
	}
}

func TestListQuotas(t *testing.T) {
	mgr := NewQuotaManager()
	mgr.SetQuota("bucket1", QuotaTypeStorage, 1000, 0.8, true)
	mgr.SetQuota("bucket2", QuotaTypeStorage, 2000, 0.9, false)

	quotas := mgr.ListQuotas()
	if len(quotas) != 2 {
		t.Errorf("len(quotas) = %v, want 2", len(quotas))
	}
}

func TestListQuotasEmpty(t *testing.T) {
	mgr := NewQuotaManager()

	quotas := mgr.ListQuotas()
	if len(quotas) != 0 {
		t.Errorf("len(quotas) = %v, want 0", len(quotas))
	}
}

func TestComplianceCheckerCheckCompliance(t *testing.T) {
	mgr := NewQuotaManager()
	checker := NewComplianceChecker(mgr)

	result, err := checker.CheckCompliance("bucket1")
	if err != nil {
		t.Fatalf("CheckCompliance failed: %v", err)
	}

	if result["has_quota"] {
		t.Error("has_quota should be false for bucket without quota")
	}
}

func TestComplianceCheckerWithQuota(t *testing.T) {
	mgr := NewQuotaManager()
	mgr.SetQuota("bucket1", QuotaTypeStorage, 1000, 0.8, true)

	checker := NewComplianceChecker(mgr)
	result, err := checker.CheckCompliance("bucket1")
	if err != nil {
		t.Fatalf("CheckCompliance failed: %v", err)
	}

	if !result["has_quota"] {
		t.Error("has_quota should be true")
	}
	if !result["within_quota"] {
		t.Error("within_quota should be true")
	}
	if !result["has_warning_threshold"] {
		t.Error("has_warning_threshold should be true")
	}
	if !result["enforced"] {
		t.Error("enforced should be true")
	}
}

func TestComplianceCheckerWithBandwidth(t *testing.T) {
	mgr := NewQuotaManager()
	mgr.SetBandwidthLimit("bucket1", 10000)

	checker := NewComplianceChecker(mgr)
	result, err := checker.CheckCompliance("bucket1")
	if err != nil {
		t.Fatalf("CheckCompliance failed: %v", err)
	}

	if !result["has_bandwidth_limit"] {
		t.Error("has_bandwidth_limit should be true")
	}
}

func TestQuotaLastUpdated(t *testing.T) {
	mgr := NewQuotaManager()
	before := time.Now()
	mgr.SetQuota("bucket1", QuotaTypeStorage, 1000, 0.8, true)
	after := time.Now()

	quota, _ := mgr.GetQuota("bucket1")
	if quota.LastUpdated.Before(before) || quota.LastUpdated.After(after) {
		t.Error("LastUpdated should be set to current time")
	}
}
