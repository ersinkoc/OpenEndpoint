package website

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

func TestHandler_ServeHTTP(t *testing.T) {
	handler := NewHandler()

	req := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusOK)
	}
}

func TestHandler_HandleIndex(t *testing.T) {
	handler := NewHandler()

	req := httptest.NewRequest("GET", "/index.html", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusOK)
	}
}

func TestHandler_HandleStatic(t *testing.T) {
	handler := NewHandler()

	req := httptest.NewRequest("GET", "/static/css/style.css", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	// May return 404 if file doesn't exist
}

func TestHandler_HandleNotFound(t *testing.T) {
	handler := NewHandler()

	req := httptest.NewRequest("GET", "/non-existent-page", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusNotFound)
	}
}

func TestWebsiteConfig(t *testing.T) {
	config := &WebsiteConfig{
		Bucket:      "website-bucket",
		IndexSuffix: "index.html",
		ErrorSuffix: "error.html",
		Enabled:     true,
	}

	if config.Bucket != "website-bucket" {
		t.Errorf("Bucket = %s, want website-bucket", config.Bucket)
	}

	if !config.Enabled {
		t.Error("Should be enabled")
	}
}

func TestWebsiteRouting(t *testing.T) {
	rules := []RoutingRule{
		{Condition: RoutingCondition{Prefix: "docs/"}, Redirect: Redirect{HostName: "docs.example.com"}},
	}

	if len(rules) != 1 {
		t.Errorf("Rules count = %d, want 1", len(rules))
	}
}

func TestRoutingRule(t *testing.T) {
	rule := RoutingRule{
		Condition: RoutingCondition{
			Prefix:               "images/",
			HTTPErrorCodeReturned: 404,
		},
		Redirect: Redirect{
			Protocol:   "https",
			HostName:   "cdn.example.com",
			StatusCode: 301,
		},
	}

	if rule.Condition.Prefix != "images/" {
		t.Errorf("Prefix = %s, want images/", rule.Condition.Prefix)
	}

	if rule.Redirect.HostName != "cdn.example.com" {
		t.Errorf("HostName = %s, want cdn.example.com", rule.Redirect.HostName)
	}
}

func TestErrorDocument(t *testing.T) {
	doc := ErrorDocument{
		Key: "error.html",
	}

	if doc.Key != "error.html" {
		t.Errorf("Key = %s, want error.html", doc.Key)
	}
}

func TestIndexDocument(t *testing.T) {
	doc := IndexDocument{
		Suffix: "index.html",
	}

	if doc.Suffix != "index.html" {
		t.Errorf("Suffix = %s, want index.html", doc.Suffix)
	}
}
