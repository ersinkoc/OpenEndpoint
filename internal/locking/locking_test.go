package locking

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

func TestManager_AcquireLock(t *testing.T) {
	mgr := NewManager()

	acquired, err := mgr.Acquire("test-lock", 10*time.Second)
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}

	if !acquired {
		t.Error("Lock should be acquired")
	}
}

func TestManager_ReleaseLock(t *testing.T) {
	mgr := NewManager()

	mgr.Acquire("test-lock", 10*time.Second)
	err := mgr.Release("test-lock")
	if err != nil {
		t.Fatalf("Release failed: %v", err)
	}
}

func TestManager_TryAcquire(t *testing.T) {
	mgr := NewManager()

	acquired := mgr.TryAcquire("test-lock", 10*time.Second)
	if !acquired {
		t.Error("Lock should be acquired")
	}

	// Second try should fail
	acquired2 := mgr.TryAcquire("test-lock", 10*time.Second)
	if acquired2 {
		t.Error("Second acquire should fail")
	}
}

func TestManager_IsLocked(t *testing.T) {
	mgr := NewManager()

	if mgr.IsLocked("test-lock") {
		t.Error("Lock should not exist")
	}

	mgr.Acquire("test-lock", 10*time.Second)
	if !mgr.IsLocked("test-lock") {
		t.Error("Lock should exist")
	}
}

func TestManager_ExtendLock(t *testing.T) {
	mgr := NewManager()

	mgr.Acquire("test-lock", 5*time.Second)
	err := mgr.Extend("test-lock", 10*time.Second)
	if err != nil {
		t.Fatalf("Extend failed: %v", err)
	}
}

func TestManager_GetLockInfo(t *testing.T) {
	mgr := NewManager()

	mgr.Acquire("test-lock", 10*time.Second)
	info, err := mgr.GetLockInfo("test-lock")
	if err != nil {
		t.Fatalf("GetLockInfo failed: %v", err)
	}

	if info == nil {
		t.Fatal("Info should not be nil")
	}
}

func TestManager_ListLocks(t *testing.T) {
	mgr := NewManager()

	locks := mgr.ListLocks()
	if len(locks) != 0 {
		t.Errorf("New manager should have 0 locks")
	}

	mgr.Acquire("lock1", 10*time.Second)
	mgr.Acquire("lock2", 10*time.Second)

	locks = mgr.ListLocks()
	if len(locks) != 2 {
		t.Errorf("Lock count = %d, want 2", len(locks))
	}
}

func TestLock_Expiration(t *testing.T) {
	mgr := NewManager()

	mgr.Acquire("test-lock", 100*time.Millisecond)

	time.Sleep(150 * time.Millisecond)

	// Lock should be expired
	if mgr.IsLocked("test-lock") {
		t.Error("Lock should be expired")
	}
}

func TestLockInfo(t *testing.T) {
	info := &LockInfo{
		Name:      "test-lock",
		AcquiredAt: time.Now(),
		ExpiresAt:  time.Now().Add(10 * time.Second),
		Holder:     "test-holder",
	}

	if info.Name != "test-lock" {
		t.Errorf("Name = %s, want test-lock", info.Name)
	}
}

func TestManager_Concurrent(t *testing.T) {
	mgr := NewManager()
	done := make(chan bool)

	for i := 0; i < 10; i++ {
		go func(id int) {
			name := string(rune('A' + id))
			mgr.TryAcquire(name, 10*time.Second)
			mgr.IsLocked(name)
			mgr.Release(name)
			done <- true
		}(i)
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}
