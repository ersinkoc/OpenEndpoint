package mgmt

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNewHandler(t *testing.T) {
	handler := NewHandler()
	if handler == nil {
		t.Fatal("Handler should not be nil")
	}
}

func TestHandler_HandleStatus(t *testing.T) {
	handler := NewHandler()

	req := httptest.NewRequest("GET", "/_mgmt/status", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusOK)
	}
}

func TestHandler_HandleHealth(t *testing.T) {
	handler := NewHandler()

	req := httptest.NewRequest("GET", "/_mgmt/health", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusOK)
	}
}

func TestHandler_HandleReady(t *testing.T) {
	handler := NewHandler()

	req := httptest.NewRequest("GET", "/_mgmt/ready", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusOK)
	}
}

func TestHandler_HandleMetrics(t *testing.T) {
	handler := NewHandler()

	req := httptest.NewRequest("GET", "/_mgmt/metrics", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	// Metrics endpoint should work
	if w.Code != http.StatusOK {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusOK)
	}
}

func TestHandler_HandleBuckets(t *testing.T) {
	handler := NewHandler()

	req := httptest.NewRequest("GET", "/_mgmt/buckets", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusOK)
	}
}

func TestHandler_HandleSettings(t *testing.T) {
	handler := NewHandler()

	req := httptest.NewRequest("GET", "/_mgmt/settings", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusOK)
	}
}

func TestHandler_HandleStats(t *testing.T) {
	handler := NewHandler()

	req := httptest.NewRequest("GET", "/_mgmt/stats", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusOK)
	}
}
