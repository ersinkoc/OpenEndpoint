package ratelimit

import (
	"net/http"
	"sync"
	"time"
)

// Limiter implements token bucket rate limiting
type Limiter struct {
	tokens    float64
	maxTokens float64
	refillRate float64 // tokens per second
	lastRefill time.Time
	mu        sync.Mutex
}

// NewLimiter creates a new rate limiter
func NewLimiter(maxTokens, refillRate float64) *Limiter {
	return &Limiter{
		tokens:     maxTokens,
		maxTokens:  maxTokens,
		refillRate: refillRate,
		lastRefill: time.Now(),
	}
}

// Allow checks if a request is allowed
func (l *Limiter) Allow() bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.refill()

	if l.tokens >= 1 {
		l.tokens--
		return true
	}

	return false
}

// AllowN checks if n requests are allowed
func (l *Limiter) AllowN(n int) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.refill()

	if l.tokens >= float64(n) {
		l.tokens -= float64(n)
		return true
	}

	return false
}

// refill adds tokens based on time elapsed
func (l *Limiter) refill() {
	now := time.Now()
	elapsed := now.Sub(l.lastRefill).Seconds()

	// Add tokens based on elapsed time
	newTokens := elapsed * l.refillRate
	l.tokens += newTokens
	if l.tokens > l.maxTokens {
		l.tokens = l.maxTokens
	}

	l.lastRefill = now
}

// Reset resets the limiter
func (l *Limiter) Reset() {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.tokens = l.maxTokens
	l.lastRefill = time.Now()
}

// BucketLimiter wraps a Limiter for HTTP handling
type BucketLimiter struct {
	limiter       *Limiter
	ipLimits      map[string]*limiterEntry
	defaultLimit  *Limiter
	mu            sync.RWMutex
	cleanupPeriod time.Duration
	maxAge        time.Duration
	stopCh        chan struct{}
}

// limiterEntry wraps a limiter with last access time
type limiterEntry struct {
	limiter    *Limiter
	lastAccess time.Time
}

// NewBucketLimiter creates a new bucket-based limiter
func NewBucketLimiter(maxTokens, refillRate float64) *BucketLimiter {
	bl := &BucketLimiter{
		limiter:       NewLimiter(maxTokens, refillRate),
		ipLimits:      make(map[string]*limiterEntry),
		defaultLimit:  NewLimiter(maxTokens, refillRate),
		cleanupPeriod: 5 * time.Minute,
		maxAge:        30 * time.Minute,
		stopCh:        make(chan struct{}),
	}
	// Start cleanup goroutine
	go bl.cleanup()
	return bl
}

// Stop stops the cleanup goroutine
func (bl *BucketLimiter) Stop() {
	close(bl.stopCh)
}

// cleanup periodically removes stale limiters
func (bl *BucketLimiter) cleanup() {
	ticker := time.NewTicker(bl.cleanupPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-bl.stopCh:
			return
		case <-ticker.C:
			bl.mu.Lock()
			now := time.Now()
			for ip, entry := range bl.ipLimits {
				if now.Sub(entry.lastAccess) > bl.maxAge {
					delete(bl.ipLimits, ip)
				}
			}
			bl.mu.Unlock()
		}
	}
}

// Middleware returns a middleware that rate limits requests
func (bl *BucketLimiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Get client IP
		clientIP := getClientIP(r)

		// Get or create limiter for this IP
		limiter := bl.getLimiter(clientIP)

		if !limiter.Allow() {
			http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// getLimiter gets or creates a limiter for an IP
func (bl *BucketLimiter) getLimiter(clientIP string) *Limiter {
	bl.mu.RLock()
	entry, ok := bl.ipLimits[clientIP]
	bl.mu.RUnlock()

	if ok {
		entry.lastAccess = time.Now()
		return entry.limiter
	}

	// Create new limiter for this IP
	bl.mu.Lock()
	defer bl.mu.Unlock()

	// Double-check after acquiring write lock
	entry, ok = bl.ipLimits[clientIP]
	if ok {
		entry.lastAccess = time.Now()
		return entry.limiter
	}

	entry = &limiterEntry{
		limiter:    NewLimiter(100, 50), // 100 requests, 50 per second
		lastAccess: time.Now(),
	}
	bl.ipLimits[clientIP] = entry

	return entry.limiter
}

// getClientIP extracts client IP from request
func getClientIP(r *http.Request) string {
	// Check X-Forwarded-For header
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		// Take the first IP
		for _, ip := range splitIPs(xff) {
			if ip != "" {
				return ip
			}
		}
	}

	// Check X-Real-IP header
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return xri
	}

	// Fall back to RemoteAddr
	return r.RemoteAddr
}

// splitIPs splits comma-separated IPs
func splitIPs(s string) []string {
	var ips []string
	start := 0
	for i, c := range s {
		if c == ',' {
			ips = append(ips, s[start:i])
			start = i + 1
		}
	}
	ips = append(ips, s[start:])
	return ips
}

// GlobalLimiter is a global rate limiter
var GlobalLimiter = NewBucketLimiter(1000, 500) // 1000 req/s default

// RateLimitMiddleware returns a global rate limiting middleware
func RateLimitMiddleware(next http.Handler) http.Handler {
	return GlobalLimiter.Middleware(next)
}
