package tenant

import (
	"testing"
	"time"
)

func TestNewManager(t *testing.T) {
	mgr := NewManager()
	if mgr == nil {
		t.Fatal("Manager should not be nil")
	}
}

func TestManager_CreateTenant(t *testing.T) {
	mgr := NewManager()

	config := &TenantConfig{
		Name:   "test-tenant",
		Domain: "test.example.com",
	}

	tenant, err := mgr.CreateTenant(config)
	if err != nil {
		t.Fatalf("CreateTenant failed: %v", err)
	}

	if tenant == nil {
		t.Fatal("Tenant should not be nil")
	}

	if tenant.ID == "" {
		t.Error("Tenant ID should be generated")
	}
}

func TestManager_CreateTenant_Duplicate(t *testing.T) {
	mgr := NewManager()

	config := &TenantConfig{Name: "test-tenant"}
	mgr.CreateTenant(config)

	_, err := mgr.CreateTenant(config)
	if err == nil {
		t.Error("Should fail for duplicate tenant")
	}
}

func TestManager_GetTenant(t *testing.T) {
	mgr := NewManager()

	config := &TenantConfig{Name: "test-tenant"}
	created, _ := mgr.CreateTenant(config)

	tenant, err := mgr.GetTenant(created.ID)
	if err != nil {
		t.Fatalf("GetTenant failed: %v", err)
	}

	if tenant.Name != "test-tenant" {
		t.Errorf("Name = %s, want test-tenant", tenant.Name)
	}
}

func TestManager_GetTenant_NotFound(t *testing.T) {
	mgr := NewManager()

	_, err := mgr.GetTenant("non-existent")
	if err == nil {
		t.Error("Should fail for non-existent tenant")
	}
}

func TestManager_DeleteTenant(t *testing.T) {
	mgr := NewManager()

	config := &TenantConfig{Name: "test-tenant"}
	tenant, _ := mgr.CreateTenant(config)

	err := mgr.DeleteTenant(tenant.ID)
	if err != nil {
		t.Fatalf("DeleteTenant failed: %v", err)
	}

	_, err = mgr.GetTenant(tenant.ID)
	if err == nil {
		t.Error("Tenant should be deleted")
	}
}

func TestManager_ListTenants(t *testing.T) {
	mgr := NewManager()

	// Empty
	tenants := mgr.ListTenants()
	if len(tenants) != 0 {
		t.Errorf("Empty list = %d, want 0", len(tenants))
	}

	mgr.CreateTenant(&TenantConfig{Name: "tenant1"})
	mgr.CreateTenant(&TenantConfig{Name: "tenant2"})

	tenants = mgr.ListTenants()
	if len(tenants) != 2 {
		t.Errorf("Tenant count = %d, want 2", len(tenants))
	}
}

func TestManager_GetTenantByDomain(t *testing.T) {
	mgr := NewManager()

	config := &TenantConfig{
		Name:   "test-tenant",
		Domain: "test.example.com",
	}
	mgr.CreateTenant(config)

	tenant, err := mgr.GetTenantByDomain("test.example.com")
	if err != nil {
		t.Fatalf("GetTenantByDomain failed: %v", err)
	}

	if tenant.Name != "test-tenant" {
		t.Errorf("Name = %s, want test-tenant", tenant.Name)
	}
}

func TestManager_UpdateTenant(t *testing.T) {
	mgr := NewManager()

	config := &TenantConfig{Name: "test-tenant"}
	tenant, _ := mgr.CreateTenant(config)

	updates := &TenantConfig{Name: "updated-tenant"}
	err := mgr.UpdateTenant(tenant.ID, updates)
	if err != nil {
		t.Fatalf("UpdateTenant failed: %v", err)
	}

	updated, _ := mgr.GetTenant(tenant.ID)
	if updated.Name != "updated-tenant" {
		t.Errorf("Name = %s, want updated-tenant", updated.Name)
	}
}

func TestTenant(t *testing.T) {
	tenant := &Tenant{
		ID:        "tenant-123",
		Name:      "test-tenant",
		Domain:    "test.example.com",
		CreatedAt: time.Now(),
		Active:    true,
	}

	if tenant.ID != "tenant-123" {
		t.Errorf("ID = %s, want tenant-123", tenant.ID)
	}

	if !tenant.Active {
		t.Error("Tenant should be active")
	}
}

func TestTenantConfig(t *testing.T) {
	config := &TenantConfig{
		Name:         "test-tenant",
		Domain:       "test.example.com",
		AdminEmail:   "admin@example.com",
		StorageQuota: 1024 * 1024 * 1024,
	}

	if config.Name != "test-tenant" {
		t.Errorf("Name = %s, want test-tenant", config.Name)
	}

	if config.StorageQuota != 1024*1024*1024 {
		t.Error("StorageQuota should be 1GB")
	}
}

func TestManager_SetTenantQuota(t *testing.T) {
	mgr := NewManager()

	config := &TenantConfig{Name: "test-tenant"}
	tenant, _ := mgr.CreateTenant(config)

	err := mgr.SetTenantQuota(tenant.ID, 1024*1024*1024)
	if err != nil {
		t.Fatalf("SetTenantQuota failed: %v", err)
	}
}

func TestManager_GetTenantUsage(t *testing.T) {
	mgr := NewManager()

	config := &TenantConfig{Name: "test-tenant"}
	tenant, _ := mgr.CreateTenant(config)

	usage, err := mgr.GetTenantUsage(tenant.ID)
	if err != nil {
		t.Fatalf("GetTenantUsage failed: %v", err)
	}

	if usage == nil {
		t.Fatal("Usage should not be nil")
	}
}

func TestManager_Concurrent(t *testing.T) {
	mgr := NewManager()
	done := make(chan bool)

	for i := 0; i < 10; i++ {
		go func(id int) {
			name := string(rune('A' + id))
			mgr.CreateTenant(&TenantConfig{Name: name})
			mgr.ListTenants()
			done <- true
		}(i)
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}
