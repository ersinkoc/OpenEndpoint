package health

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestCheckStatusConstants(t *testing.T) {
	tests := []struct {
		status CheckStatus
		want   string
	}{
		{StatusHealthy, "healthy"},
		{StatusUnhealthy, "unhealthy"},
		{StatusDegraded, "degraded"},
	}
	for _, tt := range tests {
		if string(tt.status) != tt.want {
			t.Errorf("CheckStatus %s = %s, want %s", tt.status, tt.status, tt.want)
		}
	}
}

func TestCheckStruct(t *testing.T) {
	now := time.Now()
	c := Check{
		Name:      "test",
		Status:    StatusHealthy,
		Message:   "OK",
		LastCheck: now,
		Duration:  100 * time.Millisecond,
	}
	if c.Name != "test" {
		t.Error("Check.Name mismatch")
	}
	if c.Status != StatusHealthy {
		t.Error("Check.Status mismatch")
	}
}

func TestNewChecker(t *testing.T) {
	c := NewChecker(30 * time.Second)
	if c == nil {
		t.Fatal("NewChecker returned nil")
	}
	if c.interval != 30*time.Second {
		t.Error("Interval mismatch")
	}
	if c.checks == nil {
		t.Error("checks map should be initialized")
	}
}

func TestCheckerRegisterCheck(t *testing.T) {
	c := NewChecker(time.Minute)
	c.RegisterCheck("test", func() error { return nil })

	c.mu.RLock()
	_, exists := c.checks["test"]
	c.mu.RUnlock()

	if !exists {
		t.Error("Check should be registered")
	}
}

func TestCheckerRunChecks(t *testing.T) {
	c := NewChecker(time.Minute)
	c.RegisterCheck("check1", func() error { return nil })
	c.RegisterCheck("check2", func() error { return nil })

	results := c.RunChecks(context.Background())

	if len(results) != 2 {
		t.Errorf("Expected 2 checks, got %d", len(results))
	}

	for name, check := range results {
		if check.Status != StatusHealthy {
			t.Errorf("Check %s should be healthy, got %s", name, check.Status)
		}
		if check.Message != "OK" {
			t.Errorf("Check %s message should be OK, got %s", name, check.Message)
		}
	}
}

func TestCheckerRunChecksWithEmptyChecks(t *testing.T) {
	c := NewChecker(time.Minute)

	results := c.RunChecks(context.Background())

	if len(results) != 0 {
		t.Errorf("Expected 0 checks, got %d", len(results))
	}
}

func TestGetOverallStatusHealthy(t *testing.T) {
	c := NewChecker(time.Minute)
	c.RegisterCheck("check1", func() error { return nil })

	status, checks := c.GetOverallStatus(context.Background())

	if status != StatusHealthy {
		t.Errorf("Expected healthy status, got %s", status)
	}
	if len(checks) != 1 {
		t.Errorf("Expected 1 check, got %d", len(checks))
	}
}

func TestGetOverallStatusEmpty(t *testing.T) {
	c := NewChecker(time.Minute)

	status, checks := c.GetOverallStatus(context.Background())

	if status != StatusHealthy {
		t.Errorf("Empty checks should be healthy, got %s", status)
	}
	if len(checks) != 0 {
		t.Errorf("Expected 0 checks, got %d", len(checks))
	}
}

func TestHTTPHandlerHealthy(t *testing.T) {
	c := NewChecker(time.Minute)
	c.RegisterCheck("check1", func() error { return nil })

	req := httptest.NewRequest("GET", "/health", nil)
	rec := httptest.NewRecorder()

	c.HTTPHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d", http.StatusOK, rec.Code)
	}

	var response map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	if response["status"] != string(StatusHealthy) {
		t.Errorf("Expected healthy status, got %v", response["status"])
	}
}

func TestHTTPHandlerContentType(t *testing.T) {
	c := NewChecker(time.Minute)

	req := httptest.NewRequest("GET", "/health", nil)
	rec := httptest.NewRecorder()

	c.HTTPHandler().ServeHTTP(rec, req)

	if rec.Header().Get("Content-Type") != "application/json" {
		t.Errorf("Expected application/json content type, got %s", rec.Header().Get("Content-Type"))
	}
}

func TestNewReadyChecker(t *testing.T) {
	r := NewReadyChecker()
	if r == nil {
		t.Fatal("NewReadyChecker returned nil")
	}
	if r.checks == nil {
		t.Error("checks slice should be initialized")
	}
}

func TestReadyCheckerRegisterCheck(t *testing.T) {
	r := NewReadyChecker()
	r.RegisterCheck(func(ctx context.Context) error { return nil })

	if len(r.checks) != 1 {
		t.Errorf("Expected 1 check, got %d", len(r.checks))
	}
}

func TestReadyCheckerCheck(t *testing.T) {
	r := NewReadyChecker()
	r.RegisterCheck(func(ctx context.Context) error { return nil })
	r.RegisterCheck(func(ctx context.Context) error { return nil })

	if err := r.Check(context.Background()); err != nil {
		t.Errorf("Check should succeed, got error: %v", err)
	}
}

func TestReadyCheckerCheckFails(t *testing.T) {
	r := NewReadyChecker()
	r.RegisterCheck(func(ctx context.Context) error { return nil })
	r.RegisterCheck(func(ctx context.Context) error { return context.Canceled })

	if err := r.Check(context.Background()); err == nil {
		t.Error("Check should fail")
	}
}

func TestReadyCheckerCheckEmpty(t *testing.T) {
	r := NewReadyChecker()

	if err := r.Check(context.Background()); err != nil {
		t.Errorf("Check with no checks should succeed, got: %v", err)
	}
}

func TestReadyCheckerHTTPHandlerReady(t *testing.T) {
	r := NewReadyChecker()
	r.RegisterCheck(func(ctx context.Context) error { return nil })

	req := httptest.NewRequest("GET", "/ready", nil)
	rec := httptest.NewRecorder()

	r.HTTPHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d", http.StatusOK, rec.Code)
	}

	var response map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	if response["status"] != "ready" {
		t.Errorf("Expected ready status, got %s", response["status"])
	}
}

func TestReadyCheckerHTTPHandlerNotReady(t *testing.T) {
	r := NewReadyChecker()
	r.RegisterCheck(func(ctx context.Context) error { return context.Canceled })

	req := httptest.NewRequest("GET", "/ready", nil)
	rec := httptest.NewRecorder()

	r.HTTPHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("Expected status %d, got %d", http.StatusServiceUnavailable, rec.Code)
	}

	var response map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	if response["status"] != "not ready" {
		t.Errorf("Expected not ready status, got %s", response["status"])
	}
}

func TestTCPConnectionCheckSuccess(t *testing.T) {
	server, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer server.Close()

	addr := server.Addr().(*net.TCPAddr)
	check := TCPConnectionCheck("127.0.0.1", addr.Port)

	if err := check(); err != nil {
		t.Errorf("TCP check should succeed: %v", err)
	}
}

func TestTCPConnectionCheckFail(t *testing.T) {
	check := TCPConnectionCheck("127.0.0.1", 59999)

	if err := check(); err == nil {
		t.Error("TCP check should fail for unreachable port")
	}
}

func TestHTTPHealthCheckSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	check := HTTPHealthCheck(server.URL)
	if err := check(); err != nil {
		t.Errorf("HTTP health check should succeed: %v", err)
	}
}

func TestHTTPHealthCheckFailStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	check := HTTPHealthCheck(server.URL)
	if err := check(); err == nil {
		t.Error("HTTP health check should fail for 500 status")
	}
}

func TestHTTPHealthCheckFailUnreachable(t *testing.T) {
	check := HTTPHealthCheck("http://127.0.0.1:59999/health")
	if err := check(); err == nil {
		t.Error("HTTP health check should fail for unreachable URL")
	}
}

func TestDiskSpaceCheck(t *testing.T) {
	check := DiskSpaceCheck("/", 1024)
	if err := check(); err != nil {
		t.Errorf("DiskSpaceCheck should return nil: %v", err)
	}
}

func TestMemoryCheck(t *testing.T) {
	check := MemoryCheck(1024)
	if err := check(); err != nil {
		t.Errorf("MemoryCheck should return nil: %v", err)
	}
}

func TestDefaultChecker(t *testing.T) {
	c := DefaultChecker()
	if c == nil {
		t.Fatal("DefaultChecker returned nil")
	}

	if len(c.checks) != 2 {
		t.Errorf("Expected 2 default checks, got %d", len(c.checks))
	}
}

func TestDefaultReadyChecker(t *testing.T) {
	r := DefaultReadyChecker()
	if r == nil {
		t.Fatal("DefaultReadyChecker returned nil")
	}

	if len(r.checks) != 1 {
		t.Errorf("Expected 1 default check, got %d", len(r.checks))
	}
}

func TestCheckDurationAndLastCheck(t *testing.T) {
	c := NewChecker(time.Minute)
	c.RegisterCheck("test", func() error { return nil })

	before := time.Now()
	results := c.RunChecks(context.Background())
	after := time.Now()

	check, ok := results["test"]
	if !ok {
		t.Fatal("Check not found")
	}

	if check.Duration < 0 {
		t.Error("Duration should not be negative")
	}

	if check.LastCheck.Before(before) || check.LastCheck.After(after) {
		t.Error("LastCheck should be between start and end of RunChecks")
	}
}

func TestCheckerRunChecksUnhealthy(t *testing.T) {
	c := NewChecker(time.Minute)
	c.RegisterCheck("failing", func() error { return fmt.Errorf("check failed") })

	results := c.RunChecks(context.Background())

	if results["failing"].Status != StatusUnhealthy {
		t.Errorf("Expected unhealthy status, got %s", results["failing"].Status)
	}
	if results["failing"].Message != "check failed" {
		t.Errorf("Expected error message, got %s", results["failing"].Message)
	}
}

func TestGetOverallStatusUnhealthy(t *testing.T) {
	c := NewChecker(time.Minute)
	c.RegisterCheck("failing", func() error { return fmt.Errorf("failed") })

	status, _ := c.GetOverallStatus(context.Background())

	if status != StatusUnhealthy {
		t.Errorf("Expected unhealthy status, got %s", status)
	}
}

func TestHTTPHandlerUnhealthy(t *testing.T) {
	c := NewChecker(time.Minute)
	c.RegisterCheck("failing", func() error { return fmt.Errorf("failed") })

	req := httptest.NewRequest("GET", "/health", nil)
	rec := httptest.NewRecorder()

	c.HTTPHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("Expected status %d, got %d", http.StatusServiceUnavailable, rec.Code)
	}
}

func TestHTTPHandlerDegraded(t *testing.T) {
	c := NewChecker(time.Minute)
	c.RegisterCheck("degraded", func() error { return &DegradedError{Message: "partially available"} })

	req := httptest.NewRequest("GET", "/health", nil)
	rec := httptest.NewRecorder()

	c.HTTPHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status %d for degraded, got %d", http.StatusOK, rec.Code)
	}

	var response map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	if response["status"] != string(StatusDegraded) {
		t.Errorf("Expected degraded status, got %v", response["status"])
	}
}

func TestGetOverallStatusDegraded(t *testing.T) {
	c := NewChecker(time.Minute)
	c.RegisterCheck("degraded", func() error { return &DegradedError{Message: "partially available"} })

	status, _ := c.GetOverallStatus(context.Background())

	if status != StatusDegraded {
		t.Errorf("Expected degraded status, got %s", status)
	}
}

func TestDefaultCheckerRunsChecks(t *testing.T) {
	c := DefaultChecker()
	results := c.RunChecks(context.Background())

	if len(results) != 2 {
		t.Errorf("Expected 2 checks, got %d", len(results))
	}

	for name, check := range results {
		if check.Status != StatusHealthy {
			t.Errorf("Check %s should be healthy, got %s", name, check.Status)
		}
	}
}

func TestDefaultReadyCheckerRunsChecks(t *testing.T) {
	r := DefaultReadyChecker()

	if err := r.Check(context.Background()); err != nil {
		t.Errorf("Default ready check should succeed, got: %v", err)
	}
}

func TestCheckerRegisterCheckWithNilFunction(t *testing.T) {
	c := NewChecker(time.Minute)
	c.RegisterCheck("nilcheck", nil)

	c.mu.Lock()
	c.checkFns["nilcheck"] = nil
	c.mu.Unlock()

	results := c.RunChecks(context.Background())

	if results["nilcheck"].Status != StatusHealthy {
		t.Errorf("Check with nil function should be healthy, got %s", results["nilcheck"].Status)
	}
}

func TestCheckerExecuteCheckMissingFunction(t *testing.T) {
	c := NewChecker(time.Minute)
	c.mu.Lock()
	c.checks["missing"] = Check{Name: "missing"}
	c.mu.Unlock()

	results := c.RunChecks(context.Background())

	if results["missing"].Status != StatusHealthy {
		t.Errorf("Check with missing function should be healthy, got %s", results["missing"].Status)
	}
}
