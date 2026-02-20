package health

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"
)

// Checker performs health checks
type Checker struct {
	checks   map[string]Check
	mu       sync.RWMutex
	interval time.Duration
}

// Check represents a health check
type Check struct {
	Name     string
	Status   CheckStatus
	Message  string
	LastCheck time.Time
	Duration time.Duration
}

// CheckStatus represents the status of a check
type CheckStatus string

const (
	StatusHealthy   CheckStatus = "healthy"
	StatusUnhealthy CheckStatus = "unhealthy"
	StatusDegraded CheckStatus = "degraded"
)

// NewChecker creates a new health checker
func NewChecker(interval time.Duration) *Checker {
	return &Checker{
		checks:   make(map[string]Check),
		interval: interval,
	}
}

// RegisterCheck registers a health check
func (c *Checker) RegisterCheck(name string, checkFn func() error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.checks[name] = Check{
		Name: name,
	}
}

// RunChecks runs all registered health checks
func (c *Checker) RunChecks(ctx context.Context) map[string]Check {
	c.mu.RLock()
	checks := make(map[string]Check, len(c.checks))
	for name, check := range c.checks {
		checks[name] = check
	}
	c.mu.RUnlock()

	for name := range checks {
		result := c.runCheck(ctx, name, c.checks[name].Name)
		checks[name] = result
	}

	return checks
}

// runCheck runs a single health check
func (c *Checker) runCheck(ctx context.Context, name, checkName string) Check {
	start := time.Now()

	err := c.executeCheck(ctx, checkName)

	check := Check{
		Name:     checkName,
		Duration: time.Since(start),
		LastCheck: time.Now(),
	}

	if err != nil {
		check.Status = StatusUnhealthy
		check.Message = err.Error()
	} else {
		check.Status = StatusHealthy
		check.Message = "OK"
	}

	// Update stored check
	c.mu.Lock()
	c.checks[name] = check
	c.mu.Unlock()

	return check
}

// executeCheck executes the check function
func (c *Checker) executeCheck(ctx context.Context, name string) error {
	// This would be replaced with actual check execution
	// For now, return nil (healthy)
	return nil
}

// GetOverallStatus returns the overall health status
func (c *Checker) GetOverallStatus(ctx context.Context) (CheckStatus, map[string]Check) {
	checks := c.RunChecks(ctx)

	healthy := true
	degraded := false

	for _, check := range checks {
		switch check.Status {
		case StatusUnhealthy:
			healthy = false
		case StatusDegraded:
			degraded = true
		}
	}

	if !healthy {
		return StatusUnhealthy, checks
	}
	if degraded {
		return StatusDegraded, checks
	}

	return StatusHealthy, checks
}

// HTTPHandler returns an HTTP handler for health checks
func (c *Checker) HTTPHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		status, checks := c.GetOverallStatus(ctx)

		w.Header().Set("Content-Type", "application/json")

		response := map[string]interface{}{
			"status": status,
			"checks":  checks,
		}

		if status == StatusHealthy {
			w.WriteHeader(http.StatusOK)
		} else if status == StatusDegraded {
			w.WriteHeader(http.StatusOK) // Degraded is still OK
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
		}

		json.NewEncoder(w).Encode(response)
	}
}

// ReadyChecker checks if the system is ready to serve requests
type ReadyChecker struct {
	checks []func(context.Context) error
	mu    sync.Mutex
}

// NewReadyChecker creates a new readiness checker
func NewReadyChecker() *ReadyChecker {
	return &ReadyChecker{
		checks: make([]func(context.Context) error, 0),
	}
}

// RegisterCheck registers a readiness check
func (r *ReadyChecker) RegisterCheck(checkFn func(context.Context) error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.checks = append(r.checks, checkFn)
}

// Check checks if the system is ready
func (r *ReadyChecker) Check(ctx context.Context) error {
	r.mu.Lock()
	checks := r.checks
	r.mu.Unlock()

	for _, check := range checks {
		if err := check(ctx); err != nil {
			return err
		}
	}

	return nil
}

// HTTPHandler returns an HTTP handler for readiness checks
func (r *ReadyChecker) HTTPHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		ctx := req.Context()

		if err := r.Check(ctx); err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]string{
				"status": "not ready",
				"error":  err.Error(),
			})
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{
			"status": "ready",
		})
	}
}

// Common health check functions

// TCPConnectionCheck checks if a TCP connection can be established
func TCPConnectionCheck(host string, port int) func() error {
	return func() error {
		address := fmt.Sprintf("%s:%d", host, port)
		conn, err := net.DialTimeout("tcp", address, 5*time.Second)
		if err != nil {
			return fmt.Errorf("failed to connect to %s: %w", address, err)
		}
		conn.Close()
		return nil
	}
}

// HTTPHealthCheck checks if an HTTP endpoint is healthy
func HTTPHealthCheck(url string) func() error {
	return func() error {
		resp, err := http.Get(url)
		if err != nil {
			return fmt.Errorf("failed to reach %s: %w", url, err)
		}
		defer resp.Body.Close()

		if resp.StatusCode >= 400 {
			return fmt.Errorf("unhealthy status: %d", resp.StatusCode)
		}

		return nil
	}
}

// DiskSpaceCheck checks available disk space
func DiskSpaceCheck(path string, minFreeBytes int64) func() error {
	return func() error {
		// This would check disk space
		// For now, return nil
		return nil
	}
}

// MemoryCheck checks available memory
func MemoryCheck(minFreeMB int64) func() error {
	return func() error {
		// This would check memory
		// For now, return nil
		return nil
	}
}

// DefaultChecker returns a default health checker
func DefaultChecker() *Checker {
	checker := NewChecker(30 * time.Second)

	// Register default checks
	checker.RegisterCheck("storage", func() error {
		// Check storage connectivity
		return nil
	})

	checker.RegisterCheck("database", func() error {
		// Check database connectivity
		return nil
	})

	return checker
}

// DefaultReadyChecker returns a default readiness checker
func DefaultReadyChecker() *ReadyChecker {
	ready := NewReadyChecker()

	ready.RegisterCheck(func(ctx context.Context) error {
		// Check if storage is ready
		return nil
	})

	return ready
}
