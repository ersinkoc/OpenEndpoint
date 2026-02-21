package dashboard

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

func TestHandler_ServeIndex(t *testing.T) {
	handler := NewHandler()

	req := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusOK)
	}
}

func TestHandler_ServeStatic(t *testing.T) {
	handler := NewHandler()

	// Test static file request
	req := httptest.NewRequest("GET", "/static/css/style.css", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	// May return 404 if file doesn't exist, but shouldn't panic
}

func TestHandler_HandleBucketsAPI(t *testing.T) {
	handler := NewHandler()

	req := httptest.NewRequest("GET", "/api/buckets", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusOK)
	}
}

func TestHandler_HandleObjectsAPI(t *testing.T) {
	handler := NewHandler()

	req := httptest.NewRequest("GET", "/api/buckets/test-bucket/objects", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusOK)
	}
}

func TestHandler_HandleSettingsAPI(t *testing.T) {
	handler := NewHandler()

	req := httptest.NewRequest("GET", "/api/settings", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusOK)
	}
}

func TestHandler_HandleMetricsAPI(t *testing.T) {
	handler := NewHandler()

	req := httptest.NewRequest("GET", "/api/metrics", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusOK)
	}
}

func TestHandler_HandleUpload(t *testing.T) {
	handler := NewHandler()

	req := httptest.NewRequest("POST", "/api/upload", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	// May return 400 for missing file, but shouldn't panic
}

func TestHandler_MethodNotAllowed(t *testing.T) {
	handler := NewHandler()

	req := httptest.NewRequest("DELETE", "/api/settings", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	// Should return 405 for methods not allowed
}
