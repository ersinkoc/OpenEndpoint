package middleware

import (
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestLogging(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("test"))
	})

	loggingMiddleware := Logging(handler)

	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()

	loggingMiddleware.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status = %d, want %d", w.Code, http.StatusOK)
	}
}

func TestRecovery(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("test panic")
	})

	recoveryMiddleware := Recovery(handler)

	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()

	// Should not panic
	recoveryMiddleware.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("Status = %d, want %d", w.Code, http.StatusInternalServerError)
	}
}

func TestRecovery_NoPanic(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	recoveryMiddleware := Recovery(handler)

	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()

	recoveryMiddleware.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status = %d, want %d", w.Code, http.StatusOK)
	}
}

func TestCORS(t *testing.T) {
	allowedOrigins := []string{"https://example.com", "https://test.com"}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	corsMiddleware := CORS(allowedOrigins)(handler)

	tests := []struct {
		name           string
		origin         string
		expectHeader   bool
		expectedOrigin string
	}{
		{"Allowed origin", "https://example.com", true, "https://example.com"},
		{"Another allowed", "https://test.com", true, "https://test.com"},
		{"Disallowed origin", "https://evil.com", false, ""},
		{"No origin", "", false, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/test", nil)
			if tt.origin != "" {
				req.Header.Set("Origin", tt.origin)
			}
			w := httptest.NewRecorder()

			corsMiddleware.ServeHTTP(w, req)

			allowOrigin := w.Header().Get("Access-Control-Allow-Origin")
			if tt.expectHeader {
				if allowOrigin != tt.expectedOrigin {
					t.Errorf("Access-Control-Allow-Origin = %s, want %s", allowOrigin, tt.expectedOrigin)
				}
			} else {
				if allowOrigin != "" {
					t.Errorf("Access-Control-Allow-Origin should be empty, got %s", allowOrigin)
				}
			}
		})
	}
}

func TestCORS_Wildcard(t *testing.T) {
	allowedOrigins := []string{"*"}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	corsMiddleware := CORS(allowedOrigins)(handler)

	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("Origin", "https://any-origin.com")
	w := httptest.NewRecorder()

	corsMiddleware.ServeHTTP(w, req)

	allowOrigin := w.Header().Get("Access-Control-Allow-Origin")
	if allowOrigin != "*" {
		t.Errorf("Access-Control-Allow-Origin = %s, want *", allowOrigin)
	}
}

func TestCORS_Preflight(t *testing.T) {
	allowedOrigins := []string{"*"}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	corsMiddleware := CORS(allowedOrigins)(handler)

	req := httptest.NewRequest("OPTIONS", "/test", nil)
	req.Header.Set("Origin", "https://example.com")
	w := httptest.NewRecorder()

	corsMiddleware.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status = %d, want %d", w.Code, http.StatusOK)
	}
}

func TestCompression(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		// Write enough data to trigger compression
		w.Write([]byte(strings.Repeat("Hello World! ", 100)))
	})

	compressionMiddleware := Compression(handler)

	tests := []struct {
		name            string
		acceptEncoding  string
		expectCompressed bool
	}{
		{"Gzip accepted", "gzip", true},
		{"No encoding", "", false},
		{"Other encoding", "deflate", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/test", nil)
			if tt.acceptEncoding != "" {
				req.Header.Set("Accept-Encoding", tt.acceptEncoding)
			}
			w := httptest.NewRecorder()

			compressionMiddleware.ServeHTTP(w, req)

			encoding := w.Header().Get("Content-Encoding")
			if tt.expectCompressed {
				if encoding != "gzip" {
					t.Errorf("Content-Encoding = %s, want gzip", encoding)
				}
			} else {
				if encoding == "gzip" {
					t.Error("Should not compress without Accept-Encoding: gzip")
				}
			}
		})
	}
}

func TestCompression_SmallResponse(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("small")) // Too small to compress
	})

	compressionMiddleware := Compression(handler)

	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	w := httptest.NewRecorder()

	compressionMiddleware.ServeHTTP(w, req)

	// Small responses should not be compressed
	encoding := w.Header().Get("Content-Encoding")
	if encoding == "gzip" {
		t.Error("Small response should not be compressed")
	}
}

func TestTimeout(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	})

	timeoutMiddleware := Timeout(50 * time.Millisecond)(handler)

	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()

	timeoutMiddleware.ServeHTTP(w, req)

	// Should timeout
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("Status = %d, want %d", w.Code, http.StatusServiceUnavailable)
	}
}

func TestTimeout_NoTimeout(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	timeoutMiddleware := Timeout(1 * time.Second)(handler)

	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()

	timeoutMiddleware.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status = %d, want %d", w.Code, http.StatusOK)
	}
}

func TestRequestID(t *testing.T.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check if request ID is in context
		w.WriteHeader(http.StatusOK)
	})

	requestIDMiddleware := RequestID(handler)

	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()

	requestIDMiddleware.ServeHTTP(w, req)

	requestID := w.Header().Get("X-Request-ID")
	if requestID == "" {
		t.Error("X-Request-ID header should be set")
	}
}

func TestRequestID_Existing(t *testing.T) {
	existingID := "existing-request-id"
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	requestIDMiddleware := RequestID(handler)

	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("X-Request-ID", existingID)
	w := httptest.NewRecorder()

	requestIDMiddleware.ServeHTTP(w, req)

	requestID := w.Header().Get("X-Request-ID")
	if requestID != existingID {
		t.Errorf("X-Request-ID = %s, want %s", requestID, existingID)
	}
}

func TestChain(t *testing.T) {
	order := []string{}

	middleware1 := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			order = append(order, "m1-before")
			next.ServeHTTP(w, r)
			order = append(order, "m1-after")
		})
	}

	middleware2 := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			order = append(order, "m2-before")
			next.ServeHTTP(w, r)
			order = append(order, "m2-after")
		})
	}

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		order = append(order, "handler")
		w.WriteHeader(http.StatusOK)
	})

	chain := Chain(middleware1, middleware2)(handler)

	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()

	chain.ServeHTTP(w, req)

	expected := []string{"m1-before", "m2-before", "handler", "m2-after", "m1-after"}
	if len(order) != len(expected) {
		t.Errorf("Order length = %d, want %d", len(order), len(expected))
	}

	for i, v := range expected {
		if order[i] != v {
			t.Errorf("order[%d] = %s, want %s", i, order[i], v)
		}
	}
}

func TestAuthenticate(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	authMiddleware := Authenticate("test-key")(handler)

	tests := []struct {
		name         string
		apiKey       string
		expectStatus int
	}{
		{"Valid key", "test-key", http.StatusOK},
		{"Invalid key", "wrong-key", http.StatusUnauthorized},
		{"No key", "", http.StatusUnauthorized},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/test", nil)
			if tt.apiKey != "" {
				req.Header.Set("X-API-Key", tt.apiKey)
			}
			w := httptest.NewRecorder()

			authMiddleware.ServeHTTP(w, req)

			if w.Code != tt.expectStatus {
				t.Errorf("Status = %d, want %d", w.Code, tt.expectStatus)
			}
		})
	}
}

func TestRateLimit(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// Very restrictive rate limit for testing
	rateLimitMiddleware := RateLimit(2, 0)(handler) // 2 requests, 0 refill

	// First request
	req1 := httptest.NewRequest("GET", "/test", nil)
	req1.RemoteAddr = "192.168.1.1:1234"
	w1 := httptest.NewRecorder()
	rateLimitMiddleware.ServeHTTP(w1, req1)

	if w1.Code != http.StatusOK {
		t.Errorf("First request: Status = %d, want %d", w1.Code, http.StatusOK)
	}

	// Second request
	req2 := httptest.NewRequest("GET", "/test", nil)
	req2.RemoteAddr = "192.168.1.1:1234"
	w2 := httptest.NewRecorder()
	rateLimitMiddleware.ServeHTTP(w2, req2)

	if w2.Code != http.StatusOK {
		t.Errorf("Second request: Status = %d, want %d", w2.Code, http.StatusOK)
	}

	// Third request should be rate limited
	req3 := httptest.NewRequest("GET", "/test", nil)
	req3.RemoteAddr = "192.168.1.1:1234"
	w3 := httptest.NewRecorder()
	rateLimitMiddleware.ServeHTTP(w3, req3)

	if w3.Code != http.StatusTooManyRequests {
		t.Errorf("Third request: Status = %d, want %d", w3.Code, http.StatusTooManyRequests)
	}
}

// Helper function to read gzipped response
func readGzip(r io.Reader) (string, error) {
	gr, err := gzip.NewReader(r)
	if err != nil {
		return "", err
	}
	defer gr.Close()

	data, err := io.ReadAll(gr)
	if err != nil {
		return "", err
	}
	return string(data), nil
}
