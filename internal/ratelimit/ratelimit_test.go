package ratelimit

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestNewLimiter(t *testing.T) {
	limiter := NewLimiter(100, 10)
	if limiter == nil {
		t.Fatal("Limiter should not be nil")
	}

	if limiter.tokens != 100 {
		t.Errorf("Initial tokens = %f, want 100", limiter.tokens)
	}

	if limiter.maxTokens != 100 {
		t.Errorf("Max tokens = %f, want 100", limiter.maxTokens)
	}

	if limiter.refillRate != 10 {
		t.Errorf("Refill rate = %f, want 10", limiter.refillRate)
	}
}

func TestLimiter_Allow(t *testing.T) {
	limiter := NewLimiter(5, 1)

	// Should allow 5 requests
	for i := 0; i < 5; i++ {
		if !limiter.Allow() {
			t.Errorf("Request %d should be allowed", i+1)
		}
	}

	// 6th request should be denied
	if limiter.Allow() {
		t.Error("6th request should be denied")
	}
}

func TestLimiter_AllowN(t *testing.T) {
	limiter := NewLimiter(10, 1)

	// Should allow 5 tokens at once
	if !limiter.AllowN(5) {
		t.Error("Should allow 5 tokens")
	}

	// Should have 5 tokens left
	if !limiter.AllowN(5) {
		t.Error("Should allow another 5 tokens")
	}

	// Should be empty now
	if limiter.AllowN(1) {
		t.Error("Should not allow any tokens")
	}
}

func TestLimiter_Refill(t *testing.T) {
	limiter := NewLimiter(100, 100) // 100 tokens per second

	// Use all tokens
	for limiter.Allow() {
	}

	// Wait for refill
	time.Sleep(100 * time.Millisecond)

	// Should have refilled some tokens
	if !limiter.Allow() {
		t.Error("Should have refilled tokens after waiting")
	}
}

func TestLimiter_Reset(t *testing.T) {
	limiter := NewLimiter(10, 1)

	// Use all tokens
	for limiter.Allow() {
	}

	// Reset
	limiter.Reset()

	// Should have full tokens again
	for i := 0; i < 10; i++ {
		if !limiter.Allow() {
			t.Errorf("Request %d should be allowed after reset", i+1)
		}
	}
}

func TestNewBucketLimiter(t *testing.T) {
	bl := NewBucketLimiter(100, 10)
	if bl == nil {
		t.Fatal("BucketLimiter should not be nil")
	}

	if bl.limiter == nil {
		t.Error("Limiter should not be nil")
	}

	if bl.ipLimits == nil {
		t.Error("IP limits map should not be nil")
	}

	// Clean up
	bl.Stop()
}

func TestBucketLimiter_Stop(t *testing.T) {
	bl := NewBucketLimiter(100, 10)

	// Should not panic
	bl.Stop()

	// Give time for goroutine to stop
	time.Sleep(100 * time.Millisecond)
}

func TestBucketLimiter_Middleware(t *testing.T) {
	bl := NewBucketLimiter(100, 10)
	defer bl.Stop()

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	middleware := bl.Middleware(handler)

	req := httptest.NewRequest("GET", "/test", nil)
	req.RemoteAddr = "192.168.1.1:1234"
	w := httptest.NewRecorder()

	middleware.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status = %d, want %d", w.Code, http.StatusOK)
	}
}

func TestBucketLimiter_Middleware_RateLimited(t *testing.T) {
	bl := NewBucketLimiter(2, 0) // 2 tokens, 0 refill
	defer bl.Stop()

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	middleware := bl.Middleware(handler)

	req := httptest.NewRequest("GET", "/test", nil)
	req.RemoteAddr = "192.168.1.1:1234"

	// First request should pass
	w1 := httptest.NewRecorder()
	middleware.ServeHTTP(w1, req)
	if w1.Code != http.StatusOK {
		t.Errorf("First request: Status = %d, want %d", w1.Code, http.StatusOK)
	}

	// Second request should pass
	w2 := httptest.NewRecorder()
	middleware.ServeHTTP(w2, req)
	if w2.Code != http.StatusOK {
		t.Errorf("Second request: Status = %d, want %d", w2.Code, http.StatusOK)
	}

	// Third request should be rate limited
	w3 := httptest.NewRecorder()
	middleware.ServeHTTP(w3, req)
	if w3.Code != http.StatusTooManyRequests {
		t.Errorf("Third request: Status = %d, want %d", w3.Code, http.StatusTooManyRequests)
	}
}

func TestBucketLimiter_GetLimiter(t *testing.T) {
	bl := NewBucketLimiter(100, 10)
	defer bl.Stop()

	// Same IP should get same limiter
	limiter1 := bl.getLimiter("192.168.1.1")
	limiter2 := bl.getLimiter("192.168.1.1")

	if limiter1 != limiter2 {
		t.Error("Same IP should get same limiter")
	}

	// Different IP should get different limiter
	limiter3 := bl.getLimiter("192.168.1.2")
	if limiter1 == limiter3 {
		t.Error("Different IP should get different limiter")
	}
}

func TestBucketLimiter_ConcurrentGetLimiter(t *testing.T) {
	bl := NewBucketLimiter(100, 10)
	defer bl.Stop()

	var wg sync.WaitGroup
	limiters := make([]*Limiter, 100)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			limiters[idx] = bl.getLimiter("192.168.1.1")
		}(i)
	}

	wg.Wait()

	// All limiters should be the same
	for i := 1; i < 100; i++ {
		if limiters[i] != limiters[0] {
			t.Errorf("Limiter %d != limiter 0", i)
		}
	}
}

func TestBucketLimiter_Cleanup(t *testing.T) {
	bl := NewBucketLimiter(100, 10)
	bl.cleanupPeriod = 100 * time.Millisecond
	bl.maxAge = 200 * time.Millisecond
	defer bl.Stop()

	// Add some IPs
	bl.getLimiter("192.168.1.1")
	bl.getLimiter("192.168.1.2")
	bl.getLimiter("192.168.1.3")

	if len(bl.ipLimits) != 3 {
		t.Errorf("IP limits = %d, want 3", len(bl.ipLimits))
	}

	// Wait for cleanup
	time.Sleep(300 * time.Millisecond)

	// Old entries should be cleaned up
	bl.mu.RLock()
	count := len(bl.ipLimits)
	bl.mu.RUnlock()

	if count > 0 {
		t.Errorf("IP limits after cleanup = %d, want 0", count)
	}
}

func TestGetClientIP_RemoteAddr(t *testing.T) {
	req := httptest.NewRequest("GET", "/test", nil)
	req.RemoteAddr = "192.168.1.1:1234"

	ip := getClientIP(req)

	if ip != "192.168.1.1:1234" {
		t.Errorf("IP = %s, want 192.168.1.1:1234", ip)
	}
}

func TestGetClientIP_XForwardedFor(t *testing.T) {
	req := httptest.NewRequest("GET", "/test", nil)
	req.RemoteAddr = "192.168.1.1:1234"
	req.Header.Set("X-Forwarded-For", "10.0.0.1, 10.0.0.2, 10.0.0.3")

	ip := getClientIP(req)

	if ip != "10.0.0.1" {
		t.Errorf("IP = %s, want 10.0.0.1", ip)
	}
}

func TestGetClientIP_XRealIP(t *testing.T) {
	req := httptest.NewRequest("GET", "/test", nil)
	req.RemoteAddr = "192.168.1.1:1234"
	req.Header.Set("X-Real-IP", "10.0.0.5")

	ip := getClientIP(req)

	if ip != "10.0.0.5" {
		t.Errorf("IP = %s, want 10.0.0.5", ip)
	}
}

func TestSplitIPs(t *testing.T) {
	tests := []struct {
		input    string
		expected int
	}{
		{"10.0.0.1", 1},
		{"10.0.0.1, 10.0.0.2", 2},
		{"10.0.0.1,10.0.0.2,10.0.0.3", 3},
		{"", 1},
	}

	for _, test := range tests {
		result := splitIPs(test.input)
		if len(result) != test.expected {
			t.Errorf("splitIPs(%s) returned %d IPs, want %d", test.input, len(result), test.expected)
		}
	}
}

func TestRateLimitMiddleware(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	middleware := RateLimitMiddleware(handler)

	req := httptest.NewRequest("GET", "/test", nil)
	req.RemoteAddr = "192.168.1.1:1234"

	w := httptest.NewRecorder()
	middleware.ServeHTTP(w, req)

	// Should work
	if w.Code != http.StatusOK {
		t.Errorf("Status = %d, want %d", w.Code, http.StatusOK)
	}
}

func TestBucketLimiter_DifferentIPs(t *testing.T) {
	bl := NewBucketLimiter(2, 0) // 2 tokens per IP
	defer bl.Stop()

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	middleware := bl.Middleware(handler)

	// IP 1 should get 2 requests
	req1 := httptest.NewRequest("GET", "/test", nil)
	req1.RemoteAddr = "192.168.1.1:1234"

	for i := 0; i < 2; i++ {
		w := httptest.NewRecorder()
		middleware.ServeHTTP(w, req1)
		if w.Code != http.StatusOK {
			t.Errorf("IP1 request %d: Status = %d, want %d", i+1, w.Code, http.StatusOK)
		}
	}

	// IP 1 should be rate limited
	w := httptest.NewRecorder()
	middleware.ServeHTTP(w, req1)
	if w.Code != http.StatusTooManyRequests {
		t.Errorf("IP1 rate limited: Status = %d, want %d", w.Code, http.StatusTooManyRequests)
	}

	// IP 2 should still be able to make requests
	req2 := httptest.NewRequest("GET", "/test", nil)
	req2.RemoteAddr = "192.168.1.2:1234"

	w2 := httptest.NewRecorder()
	middleware.ServeHTTP(w2, req2)
	if w2.Code != http.StatusOK {
		t.Errorf("IP2 request: Status = %d, want %d", w2.Code, http.StatusOK)
	}
}

func TestLimiter_ConcurrentAccess(t *testing.T) {
	limiter := NewLimiter(1000, 100)

	var wg sync.WaitGroup
	successCount := 0
	var mu sync.Mutex

	for i := 0; i < 2000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if limiter.Allow() {
				mu.Lock()
				successCount++
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	// Should have allowed approximately 1000 requests
	if successCount < 900 || successCount > 1100 {
		t.Errorf("Success count = %d, expected around 1000", successCount)
	}
}

func TestLimiterEntry_LastAccess(t *testing.T) {
	bl := NewBucketLimiter(100, 10)
	defer bl.Stop()

	// Get limiter for IP
	bl.getLimiter("192.168.1.1")

	// Check entry exists
	bl.mu.RLock()
	entry, ok := bl.ipLimits["192.168.1.1"]
	bl.mu.RUnlock()

	if !ok {
		t.Fatal("Entry should exist")
	}

	initialTime := entry.lastAccess

	// Wait a bit
	time.Sleep(10 * time.Millisecond)

	// Access again
	bl.getLimiter("192.168.1.1")

	// Check lastAccess was updated
	bl.mu.RLock()
	newTime := bl.ipLimits["192.168.1.1"].lastAccess
	bl.mu.RUnlock()

	if !newTime.After(initialTime) {
		t.Error("lastAccess should have been updated")
	}
}
