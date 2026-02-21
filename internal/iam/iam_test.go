package iam

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

func TestManager_CreateUser(t *testing.T) {
	mgr := NewManager()

	user, err := mgr.CreateUser("test-user")
	if err != nil {
		t.Fatalf("CreateUser failed: %v", err)
	}

	if user == nil {
		t.Fatal("User should not be nil")
	}

	if user.Name != "test-user" {
		t.Errorf("Name = %s, want test-user", user.Name)
	}
}

func TestManager_CreateUser_Duplicate(t *testing.T) {
	mgr := NewManager()

	mgr.CreateUser("test-user")
	_, err := mgr.CreateUser("test-user")

	if err == nil {
		t.Error("Should fail to create duplicate user")
	}
}

func TestManager_GetUser(t *testing.T) {
	mgr := NewManager()

	mgr.CreateUser("test-user")

	user, err := mgr.GetUser("test-user")
	if err != nil {
		t.Fatalf("GetUser failed: %v", err)
	}

	if user.Name != "test-user" {
		t.Errorf("Name = %s, want test-user", user.Name)
	}
}

func TestManager_GetUser_NotFound(t *testing.T) {
	mgr := NewManager()

	_, err := mgr.GetUser("non-existent")
	if err == nil {
		t.Error("Should fail to get non-existent user")
	}
}

func TestManager_DeleteUser(t *testing.T) {
	mgr := NewManager()

	mgr.CreateUser("test-user")
	err := mgr.DeleteUser("test-user")
	if err != nil {
		t.Fatalf("DeleteUser failed: %v", err)
	}

	_, err = mgr.GetUser("test-user")
	if err == nil {
		t.Error("User should be deleted")
	}
}

func TestManager_ListUsers(t *testing.T) {
	mgr := NewManager()

	// Empty list
	users := mgr.ListUsers()
	if len(users) != 0 {
		t.Errorf("Empty list should have 0 users")
	}

	mgr.CreateUser("user1")
	mgr.CreateUser("user2")

	users = mgr.ListUsers()
	if len(users) != 2 {
		t.Errorf("User count = %d, want 2", len(users))
	}
}

func TestManager_CreateAccessKey(t *testing.T) {
	mgr := NewManager()

	mgr.CreateUser("test-user")
	key, err := mgr.CreateAccessKey("test-user")

	if err != nil {
		t.Fatalf("CreateAccessKey failed: %v", err)
	}

	if key.AccessKey == "" {
		t.Error("AccessKey should not be empty")
	}

	if key.SecretKey == "" {
		t.Error("SecretKey should not be empty")
	}
}

func TestManager_GetAccessKey(t *testing.T) {
	mgr := NewManager()

	mgr.CreateUser("test-user")
	created, _ := mgr.CreateAccessKey("test-user")

	key, err := mgr.GetAccessKey(created.AccessKey)
	if err != nil {
		t.Fatalf("GetAccessKey failed: %v", err)
	}

	if key.AccessKey != created.AccessKey {
		t.Error("AccessKey mismatch")
	}
}

func TestManager_DeleteAccessKey(t *testing.T) {
	mgr := NewManager()

	mgr.CreateUser("test-user")
	key, _ := mgr.CreateAccessKey("test-user")

	err := mgr.DeleteAccessKey(key.AccessKey)
	if err != nil {
		t.Fatalf("DeleteAccessKey failed: %v", err)
	}

	_, err = mgr.GetAccessKey(key.AccessKey)
	if err == nil {
		t.Error("AccessKey should be deleted")
	}
}

func TestManager_AttachPolicy(t *testing.T) {
	mgr := NewManager()

	mgr.CreateUser("test-user")
	err := mgr.AttachPolicy("test-user", "read-only")
	if err != nil {
		t.Fatalf("AttachPolicy failed: %v", err)
	}
}

func TestManager_DetachPolicy(t *testing.T) {
	mgr := NewManager()

	mgr.CreateUser("test-user")
	mgr.AttachPolicy("test-user", "read-only")

	err := mgr.DetachPolicy("test-user", "read-only")
	if err != nil {
		t.Fatalf("DetachPolicy failed: %v", err)
	}
}

func TestManager_ListPolicies(t *testing.T) {
	mgr := NewManager()

	mgr.CreateUser("test-user")
	mgr.AttachPolicy("test-user", "policy1")
	mgr.AttachPolicy("test-user", "policy2")

	policies := mgr.ListPolicies("test-user")
	if len(policies) != 2 {
		t.Errorf("Policy count = %d, want 2", len(policies))
	}
}

func TestUser(t *testing.T) {
	user := &User{
		Name:      "test-user",
		CreatedAt: time.Now(),
	}

	if user.Name != "test-user" {
		t.Errorf("Name = %s, want test-user", user.Name)
	}
}

func TestAccessKey(t *testing.T) {
	key := &AccessKey{
		AccessKey: "AKIAIOSFODNN7EXAMPLE",
		SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		Status:    "Active",
	}

	if key.AccessKey != "AKIAIOSFODNN7EXAMPLE" {
		t.Error("AccessKey mismatch")
	}

	if key.Status != "Active" {
		t.Error("Status should be Active")
	}
}

func TestManager_Concurrent(t *testing.T) {
	mgr := NewManager()
	done := make(chan bool)

	for i := 0; i < 10; i++ {
		go func(id int) {
			name := string(rune('A' + id))
			mgr.CreateUser(name)
			mgr.CreateAccessKey(name)
			mgr.GetUser(name)
			done <- true
		}(i)
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}
