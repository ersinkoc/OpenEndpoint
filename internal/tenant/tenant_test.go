package tenant

import (
	"testing"

	"go.uber.org/zap"
)

func TestNewManager(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)
	if mgr == nil {
		t.Fatal("Manager should not be nil")
	}
	if mgr.tenants == nil {
		t.Error("tenants map should be initialized")
	}
	if mgr.quotas == nil {
		t.Error("quotas map should be initialized")
	}
}

func TestCreateTenant(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	quota := &Quota{
		StorageBytes:  1000000,
		ObjectCount:   1000,
		BucketCount:   10,
		BandwidthMbps: 100,
		APIRequests:   10000,
	}

	tenant, err := mgr.CreateTenant("test-tenant", "us-east-1", quota)
	if err != nil {
		t.Fatalf("CreateTenant failed: %v", err)
	}

	if tenant.ID == "" {
		t.Error("tenant ID should not be empty")
	}
	if tenant.Name != "test-tenant" {
		t.Errorf("Name = %v, want test-tenant", tenant.Name)
	}
	if tenant.Region != "us-east-1" {
		t.Errorf("Region = %v, want us-east-1", tenant.Region)
	}
	if tenant.Status != "active" {
		t.Errorf("Status = %v, want active", tenant.Status)
	}
	if tenant.Quota != quota {
		t.Error("Quota should be set")
	}
}

func TestCreateTenantDuplicateName(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	mgr.CreateTenant("test-tenant", "us-east-1", nil)
	_, err := mgr.CreateTenant("test-tenant", "us-west-1", nil)
	if err == nil {
		t.Error("CreateTenant should fail for duplicate name")
	}
}

func TestGetTenant(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	tenant, _ := mgr.CreateTenant("test-tenant", "us-east-1", nil)

	got, ok := mgr.GetTenant(tenant.ID)
	if !ok {
		t.Fatal("GetTenant should return ok=true")
	}
	if got.ID != tenant.ID {
		t.Errorf("ID = %v, want %v", got.ID, tenant.ID)
	}
}

func TestGetTenantNotFound(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	_, ok := mgr.GetTenant("nonexistent")
	if ok {
		t.Error("GetTenant should return ok=false for nonexistent tenant")
	}
}

func TestGetTenantByName(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	mgr.CreateTenant("test-tenant", "us-east-1", nil)

	got, ok := mgr.GetTenantByName("test-tenant")
	if !ok {
		t.Fatal("GetTenantByName should return ok=true")
	}
	if got.Name != "test-tenant" {
		t.Errorf("Name = %v, want test-tenant", got.Name)
	}
}

func TestGetTenantByNameNotFound(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	_, ok := mgr.GetTenantByName("nonexistent")
	if ok {
		t.Error("GetTenantByName should return ok=false for nonexistent name")
	}
}

func TestGetTenantByNameDeleted(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	tenant, _ := mgr.CreateTenant("test-tenant", "us-east-1", nil)
	mgr.DeleteTenant(tenant.ID)

	_, ok := mgr.GetTenantByName("test-tenant")
	if ok {
		t.Error("GetTenantByName should return ok=false for deleted tenant")
	}
}

func TestListTenants(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	mgr.CreateTenant("tenant1", "us-east-1", nil)
	mgr.CreateTenant("tenant2", "us-west-1", nil)

	tenants := mgr.ListTenants()
	if len(tenants) != 2 {
		t.Errorf("len(tenants) = %v, want 2", len(tenants))
	}
}

func TestListTenantsExcludesDeleted(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	tenant, _ := mgr.CreateTenant("tenant1", "us-east-1", nil)
	mgr.CreateTenant("tenant2", "us-west-1", nil)
	mgr.DeleteTenant(tenant.ID)

	tenants := mgr.ListTenants()
	if len(tenants) != 1 {
		t.Errorf("len(tenants) = %v, want 1 (excludes deleted)", len(tenants))
	}
}

func TestUpdateTenant(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	tenant, _ := mgr.CreateTenant("test-tenant", "us-east-1", nil)

	updates := &Tenant{
		Name:     "updated-tenant",
		Region:   "eu-west-1",
		Quota:    &Quota{StorageBytes: 5000000},
		Settings: TenantSettings{DefaultRegion: "eu-west-1"},
	}

	updated, err := mgr.UpdateTenant(tenant.ID, updates)
	if err != nil {
		t.Fatalf("UpdateTenant failed: %v", err)
	}

	if updated.Name != "updated-tenant" {
		t.Errorf("Name = %v, want updated-tenant", updated.Name)
	}
	if updated.Region != "eu-west-1" {
		t.Errorf("Region = %v, want eu-west-1", updated.Region)
	}
	if updated.Quota.StorageBytes != 5000000 {
		t.Errorf("Quota.StorageBytes = %v, want 5000000", updated.Quota.StorageBytes)
	}
}

func TestUpdateTenantNotFound(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	_, err := mgr.UpdateTenant("nonexistent", &Tenant{Name: "test"})
	if err == nil {
		t.Error("UpdateTenant should fail for nonexistent tenant")
	}
}

func TestSuspendTenant(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	tenant, _ := mgr.CreateTenant("test-tenant", "us-east-1", nil)

	err := mgr.SuspendTenant(tenant.ID)
	if err != nil {
		t.Fatalf("SuspendTenant failed: %v", err)
	}

	got, _ := mgr.GetTenant(tenant.ID)
	if got.Status != "suspended" {
		t.Errorf("Status = %v, want suspended", got.Status)
	}
}

func TestSuspendTenantNotFound(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	err := mgr.SuspendTenant("nonexistent")
	if err == nil {
		t.Error("SuspendTenant should fail for nonexistent tenant")
	}
}

func TestActivateTenant(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	tenant, _ := mgr.CreateTenant("test-tenant", "us-east-1", nil)
	mgr.SuspendTenant(tenant.ID)

	err := mgr.ActivateTenant(tenant.ID)
	if err != nil {
		t.Fatalf("ActivateTenant failed: %v", err)
	}

	got, _ := mgr.GetTenant(tenant.ID)
	if got.Status != "active" {
		t.Errorf("Status = %v, want active", got.Status)
	}
}

func TestActivateTenantNotFound(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	err := mgr.ActivateTenant("nonexistent")
	if err == nil {
		t.Error("ActivateTenant should fail for nonexistent tenant")
	}
}

func TestDeleteTenant(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	tenant, _ := mgr.CreateTenant("test-tenant", "us-east-1", nil)

	err := mgr.DeleteTenant(tenant.ID)
	if err != nil {
		t.Fatalf("DeleteTenant failed: %v", err)
	}

	got, _ := mgr.GetTenant(tenant.ID)
	if got.Status != "deleted" {
		t.Errorf("Status = %v, want deleted", got.Status)
	}
}

func TestDeleteTenantNotFound(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	err := mgr.DeleteTenant("nonexistent")
	if err == nil {
		t.Error("DeleteTenant should fail for nonexistent tenant")
	}
}

func TestGetUsage(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	tenant, _ := mgr.CreateTenant("test-tenant", "us-east-1", nil)

	usage, err := mgr.GetUsage(tenant.ID)
	if err != nil {
		t.Fatalf("GetUsage failed: %v", err)
	}
	if usage == nil {
		t.Error("usage should not be nil")
	}
}

func TestGetUsageNotFound(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	_, err := mgr.GetUsage("nonexistent")
	if err == nil {
		t.Error("GetUsage should fail for nonexistent tenant")
	}
}

func TestUpdateUsage(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	tenant, _ := mgr.CreateTenant("test-tenant", "us-east-1", nil)

	newUsage := &Usage{
		StorageBytes: 500000,
		ObjectCount:  100,
		BucketCount:  5,
		APIRequests:  1000,
	}

	err := mgr.UpdateUsage(tenant.ID, newUsage)
	if err != nil {
		t.Fatalf("UpdateUsage failed: %v", err)
	}

	usage, _ := mgr.GetUsage(tenant.ID)
	if usage.StorageBytes != 500000 {
		t.Errorf("StorageBytes = %v, want 500000", usage.StorageBytes)
	}
}

func TestUpdateUsageNotFound(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	err := mgr.UpdateUsage("nonexistent", &Usage{})
	if err == nil {
		t.Error("UpdateUsage should fail for nonexistent tenant")
	}
}

func TestCheckQuota(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	quota := &Quota{StorageBytes: 1000000}
	tenant, _ := mgr.CreateTenant("test-tenant", "us-east-1", quota)

	ok, err := mgr.CheckQuota(tenant.ID, 500000)
	if !ok || err != nil {
		t.Errorf("CheckQuota(500000) = %v, %v, want true, nil", ok, err)
	}
}

func TestCheckQuotaExceeded(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	quota := &Quota{StorageBytes: 1000000}
	tenant, _ := mgr.CreateTenant("test-tenant", "us-east-1", quota)
	mgr.AddStorageUsage(tenant.ID, 800000)

	ok, err := mgr.CheckQuota(tenant.ID, 500000)
	if ok {
		t.Error("CheckQuota should return false when quota exceeded")
	}
	if err == nil {
		t.Error("CheckQuota should return error when quota exceeded")
	}
}

func TestCheckQuotaNotFound(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	_, err := mgr.CheckQuota("nonexistent", 1000)
	if err == nil {
		t.Error("CheckQuota should fail for nonexistent tenant")
	}
}

func TestCheckQuotaSuspendedTenant(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	quota := &Quota{StorageBytes: 1000000}
	tenant, _ := mgr.CreateTenant("test-tenant", "us-east-1", quota)
	mgr.SuspendTenant(tenant.ID)

	ok, err := mgr.CheckQuota(tenant.ID, 500000)
	if ok {
		t.Error("CheckQuota should return false for suspended tenant")
	}
	if err == nil {
		t.Error("CheckQuota should return error for suspended tenant")
	}
}

func TestAddStorageUsage(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	tenant, _ := mgr.CreateTenant("test-tenant", "us-east-1", nil)

	err := mgr.AddStorageUsage(tenant.ID, 100000)
	if err != nil {
		t.Fatalf("AddStorageUsage failed: %v", err)
	}

	usage, _ := mgr.GetUsage(tenant.ID)
	if usage.StorageBytes != 100000 {
		t.Errorf("StorageBytes = %v, want 100000", usage.StorageBytes)
	}
}

func TestAddAPIRequest(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	tenant, _ := mgr.CreateTenant("test-tenant", "us-east-1", nil)

	mgr.AddAPIRequest(tenant.ID)
	mgr.AddAPIRequest(tenant.ID)

	usage, _ := mgr.GetUsage(tenant.ID)
	if usage.APIRequests != 2 {
		t.Errorf("APIRequests = %v, want 2", usage.APIRequests)
	}
}

func TestAddObjectCount(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	tenant, _ := mgr.CreateTenant("test-tenant", "us-east-1", nil)

	err := mgr.AddObjectCount(tenant.ID, 10)
	if err != nil {
		t.Fatalf("AddObjectCount failed: %v", err)
	}

	usage, _ := mgr.GetUsage(tenant.ID)
	if usage.ObjectCount != 10 {
		t.Errorf("ObjectCount = %v, want 10", usage.ObjectCount)
	}
}

func TestCheckQuotaNoUsageSet(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	quota := &Quota{StorageBytes: 1000000}
	tenant, _ := mgr.CreateTenant("test-tenant", "us-east-1", quota)

	delete(mgr.quotas, tenant.ID)

	ok, err := mgr.CheckQuota(tenant.ID, 500000)
	if !ok || err != nil {
		t.Errorf("CheckQuota with no usage set = %v, %v, want true, nil", ok, err)
	}
}

func TestCheckQuotaNilQuota(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	tenant, _ := mgr.CreateTenant("test-tenant", "us-east-1", nil)
	mgr.AddStorageUsage(tenant.ID, 800000)

	ok, err := mgr.CheckQuota(tenant.ID, 500000)
	if !ok || err != nil {
		t.Errorf("CheckQuota with nil quota = %v, %v, want true, nil", ok, err)
	}
}
