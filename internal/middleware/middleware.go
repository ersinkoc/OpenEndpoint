package middleware

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"time"
)

// gzipResponseWriter wraps an http.ResponseWriter with gzip compression
type gzipResponseWriter struct {
	http.ResponseWriter
	writer *gzip.Writer
}

// Write implements http.ResponseWriter
func (g *gzipResponseWriter) Write(b []byte) (int, error) {
	if g.Header().Get("Content-Type") == "" || strings.HasPrefix(g.Header().Get("Content-Type"), "text/") || strings.HasPrefix(g.Header().Get("Content-Type"), "application/json") {
		return g.writer.Write(b)
	}
	return g.ResponseWriter.Write(b)
}

// Flush implements http.Flusher
func (g *gzipResponseWriter) Flush() {
	g.writer.Flush()
	if f, ok := g.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Close closes the gzip writer
func (g *gzipResponseWriter) Close() error {
	return g.writer.Close()
}

// RequestID adds a unique request ID to each request
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check for existing request ID
		id := r.Header.Get("X-Request-ID")
		if id == "" {
			id = generateRequestID()
		}

		w.Header().Set("X-Request-ID", id)
		r = r.WithContext(withRequestID(r.Context(), id))

		next.ServeHTTP(w, r)
	})
}

// Logger logs HTTP requests
func Logger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		wrapped := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		next.ServeHTTP(wrapped, r)

		duration := time.Since(start)

		log.Printf(
			"%s %s %d %s %s",
			r.Method,
			r.URL.Path,
			wrapped.statusCode,
			duration,
			r.RemoteAddr,
		)
	})
}

// Recoverer recovers from panics
func Recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				log.Printf("Panic recovered: %v", err)
				http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			}
		}()

		next.ServeHTTP(w, r)
	})
}

// Compress applies gzip compression
func Compress(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check if client accepts compression
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			next.ServeHTTP(w, r)
			return
		}

		// Create gzip writer
		gw := gzip.NewWriter(w)
		defer gw.Close()

		// Set content encoding header
		w.Header().Set("Content-Encoding", "gzip")

		// Use wrapper to make gzip.Writer work as http.ResponseWriter
		gzw := &gzipResponseWriter{
			ResponseWriter: w,
			writer:         gw,
		}

		next.ServeHTTP(gzw, r)
	})
}

// Decompress decompresses gzip requests
func Decompress(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Content-Encoding") == "gzip" {
			gr, err := gzip.NewReader(r.Body)
			if err != nil {
				http.Error(w, "Invalid gzip", http.StatusBadRequest)
				return
			}
			defer gr.Close()
			r.Body = io.NopCloser(gr)
		}

		next.ServeHTTP(w, r)
	})
}

// Headers adds common headers
func Headers(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("X-XSS-Protection", "1; mode=block")
		w.Header().Set("Server", "OpenEndpoint/0.1.0")

		next.ServeHTTP(w, r)
	})
}

// CORS adds CORS headers
func CORS(allowedOrigins []string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")

			// Check if origin is allowed
			for _, allowed := range allowedOrigins {
				if allowed == "*" || allowed == origin {
					w.Header().Set("Access-Control-Allow-Origin", allowed)
					w.Header().Set("Access-Control-Allow-Methods", "GET, PUT, DELETE, HEAD, POST, OPTIONS")
					w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Amz-Date, X-Amz-Content-Sha256, X-Requested-With")
					w.Header().Set("Access-Control-Max-Age", "3600")
					break
				}
			}

			if r.Method == "OPTIONS" {
				w.WriteHeader(http.StatusOK)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// Timeout adds a timeout to requests
func Timeout(timeout time.Duration) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			done := make(chan struct{})

			go func() {
				next.ServeHTTP(w, r)
				close(done)
			}()

			select {
			case <-done:
				return
			case <-time.After(timeout):
				log.Printf("Request timed out: %s %s", r.Method, r.URL.Path)
				http.Error(w, "Request Timeout", http.StatusRequestTimeout)
			}
		})
	}
}

// MaxBodySize limits request body size
func MaxBodySize(maxSize int64) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.ContentLength > maxSize {
				http.Error(w, "Request Entity Too Large", http.StatusRequestEntityTooLarge)
				return
			}

			r.Body = http.MaxBytesReader(w, r.Body, maxSize)
			next.ServeHTTP(w, r)
		})
	}
}

// Chain chains multiple middleware together
func Chain(middlewares ...func(http.Handler) http.Handler) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		// Apply in reverse order (last middleware is applied first)
		for i := len(middlewares) - 1; i >= 0; i-- {
			next = middlewares[i](next)
		}
		return next
	}
}

// Common middleware chain
var Common = Chain(
	RequestID,
	Logger,
	Recoverer,
	Headers,
)

// responseWriter wraps http.ResponseWriter to capture status code
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

// requestIDContextKey is the context key for request ID
type requestIDContextKey string

const reqIDContextKey requestIDContextKey = "requestID"

// withRequestID adds request ID to context
func withRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, reqIDContextKey, id)
}

// GetRequestID gets request ID from context
func GetRequestID(ctx context.Context) string {
	if id := ctx.Value(reqIDContextKey); id != nil {
		return id.(string)
	}
	return ""
}

func generateRequestID() string {
	return fmt.Sprintf("%d-%d", time.Now().UnixNano(), rand.Intn(1000000))
}
