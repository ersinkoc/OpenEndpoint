package health

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestNewChecker(t *testing.T) {
	checker := NewChecker(30 * time.Second)
	if checker == nil {
		t.Fatal("Checker should not be nil")
	}
}

func TestChecker_Liveness(t *testing.T) {
	checker := NewChecker(30 * time.Second)

	req := httptest.NewRequest("GET", "/healthz", nil)
	w := httptest.NewRecorder()

	checker.Liveness()(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status = %d, want %d", w.Code, http.StatusOK)
	}
}

func TestChecker_Readiness(t *testing.T) {
	checker := NewChecker(30 * time.Second)

	req := httptest.NewRequest("GET", "/readyz", nil)
	w := httptest.NewRecorder()

	checker.Readiness()(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status = %d, want %d", w.Code, http.StatusOK)
	}
}

func TestChecker_ReadinessWithChecks(t *testing.T) {
	checker := NewChecker(30 * time.Second)

	// Add a failing check
	checker.AddCheck("database", func() error {
		return nil // Pass
	})

	req := httptest.NewRequest("GET", "/readyz", nil)
	w := httptest.NewRecorder()

	checker.Readiness()(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status = %d, want %d", w.Code, http.StatusOK)
	}
}

func TestChecker_ReadinessWithFailingCheck(t *testing.T) {
	checker := NewChecker(30 * time.Second)

	// Add a failing check
	checker.AddCheck("failing-check", func() error {
		return fmt.Errorf("check failed")
	})

	req := httptest.NewRequest("GET", "/readyz", nil)
	w := httptest.NewRecorder()

	checker.Readiness()(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("Status = %d, want %d", w.Code, http.StatusServiceUnavailable)
	}
}

func TestChecker_AddCheck(t *testing.T) {
	checker := NewChecker(30 * time.Second)

	checkCalled := false
	checker.AddCheck("test-check", func() error {
		checkCalled = true
		return nil
	})

	// Trigger readiness check
	req := httptest.NewRequest("GET", "/readyz", nil)
	w := httptest.NewRecorder()
	checker.Readiness()(w, req)

	if !checkCalled {
		t.Error("Check should have been called")
	}
}

func TestChecker_RemoveCheck(t *testing.T) {
	checker := NewChecker(30 * time.Second)

	checker.AddCheck("test-check", func() error {
		return fmt.Errorf("should fail")
	})

	checker.RemoveCheck("test-check")

	// Should pass after removal
	req := httptest.NewRequest("GET", "/readyz", nil)
	w := httptest.NewRecorder()
	checker.Readiness()(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status = %d, want %d", w.Code, http.StatusOK)
	}
}

func TestChecker_Status(t *testing.T) {
	checker := NewChecker(30 * time.Second)

	checker.AddCheck("passing", func() error { return nil })
	checker.AddCheck("failing", func() error { return fmt.Errorf("failed") })

	req := httptest.NewRequest("GET", "/status", nil)
	w := httptest.NewRecorder()

	checker.Status()(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status = %d, want %d", w.Code, http.StatusOK)
	}

	// Should return JSON
	var result map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &result)
	if err != nil {
		t.Fatalf("Response should be valid JSON: %v", err)
	}
}

func TestChecker_Start(t *testing.T) {
	checker := NewChecker(100 * time.Millisecond)

	checkCount := 0
	checker.AddCheck("counter", func() error {
		checkCount++
		return nil
	})

	go checker.Start()
	defer checker.Stop()

	// Wait for a few check cycles
	time.Sleep(300 * time.Millisecond)

	if checkCount < 2 {
		t.Errorf("Check count = %d, want at least 2", checkCount)
	}
}

func TestChecker_Stop(t *testing.T) {
	checker := NewChecker(50 * time.Millisecond)

	// Should not panic
	checker.Start()
	time.Sleep(100 * time.Millisecond)
	checker.Stop()
}

func TestChecker_ConcurrentChecks(t *testing.T) {
	checker := NewChecker(30 * time.Second)

	callCount := 0
	checker.AddCheck("check1", func() error {
		callCount++
		return nil
	})

	// Run multiple concurrent readiness checks
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func() {
			req := httptest.NewRequest("GET", "/readyz", nil)
			w := httptest.NewRecorder()
			checker.Readiness()(w, req)
			done <- true
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestChecker_MultipleChecks(t *testing.T) {
	checker := NewChecker(30 * time.Second)

	checker.AddCheck("check1", func() error { return nil })
	checker.AddCheck("check2", func() error { return nil })
	checker.AddCheck("check3", func() error { return nil })

	req := httptest.NewRequest("GET", "/readyz", nil)
	w := httptest.NewRecorder()

	checker.Readiness()(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status = %d, want %d", w.Code, http.StatusOK)
	}
}

func TestChecker_PartialFailure(t *testing.T) {
	checker := NewChecker(30 * time.Second)

	checker.AddCheck("passing1", func() error { return nil })
	checker.AddCheck("failing", func() error { return fmt.Errorf("failed") })
	checker.AddCheck("passing2", func() error { return nil })

	req := httptest.NewRequest("GET", "/readyz", nil)
	w := httptest.NewRecorder()

	checker.Readiness()(w, req)

	// Should fail because one check fails
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("Status = %d, want %d", w.Code, http.StatusServiceUnavailable)
	}
}

func TestHealthResponse(t *testing.T) {
	checker := NewChecker(30 * time.Second)

	req := httptest.NewRequest("GET", "/healthz", nil)
	w := httptest.NewRecorder()

	checker.Liveness()(w, req)

	var response map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &response)

	if response["status"] != "healthy" {
		t.Errorf("Status = %v, want healthy", response["status"])
	}
}

func TestReadyResponse(t *testing.T) {
	checker := NewChecker(30 * time.Second)

	req := httptest.NewRequest("GET", "/readyz", nil)
	w := httptest.NewRecorder()

	checker.Readiness()(w, req)

	var response map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &response)

	if response["status"] != "ready" {
		t.Errorf("Status = %v, want ready", response["status"])
	}
}
