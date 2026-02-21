package analytics

import (
	"testing"
	"time"
)

func TestNewService(t *testing.T) {
	svc := NewService()
	if svc == nil {
		t.Fatal("Service should not be nil")
	}
}

func TestService_RecordEvent(t *testing.T) {
	svc := NewService()

	event := &AnalyticsEvent{
		Type:      "api_request",
		Timestamp: time.Now(),
		Properties: map[string]interface{}{
			"method": "GET",
			"path":   "/api/buckets",
		},
	}

	err := svc.RecordEvent(event)
	if err != nil {
		t.Fatalf("RecordEvent failed: %v", err)
	}
}

func TestService_GetEvents(t *testing.T) {
	svc := NewService()

	// Empty
	events := svc.GetEvents(time.Hour)
	if len(events) != 0 {
		t.Errorf("Empty events = %d, want 0", len(events))
	}

	// Add events
	svc.RecordEvent(&AnalyticsEvent{Type: "test1"})
	svc.RecordEvent(&AnalyticsEvent{Type: "test2"})

	events = svc.GetEvents(time.Hour)
	if len(events) != 2 {
		t.Errorf("Events count = %d, want 2", len(events))
	}
}

func TestService_GetEventsByType(t *testing.T) {
	svc := NewService()

	svc.RecordEvent(&AnalyticsEvent{Type: "api_request"})
	svc.RecordEvent(&AnalyticsEvent{Type: "api_request"})
	svc.RecordEvent(&AnalyticsEvent{Type: "error"})

	events := svc.GetEventsByType("api_request", time.Hour)
	if len(events) != 2 {
		t.Errorf("API request count = %d, want 2", len(events))
	}
}

func TestService_GetStats(t *testing.T) {
	svc := NewService()

	svc.RecordEvent(&AnalyticsEvent{Type: "api_request"})
	svc.RecordEvent(&AnalyticsEvent{Type: "api_request"})
	svc.RecordEvent(&AnalyticsEvent{Type: "error"})

	stats := svc.GetStats(time.Hour)

	if stats.TotalEvents != 3 {
		t.Errorf("Total events = %d, want 3", stats.TotalEvents)
	}
}

func TestService_ClearEvents(t *testing.T) {
	svc := NewService()

	svc.RecordEvent(&AnalyticsEvent{Type: "test"})
	svc.ClearEvents()

	events := svc.GetEvents(time.Hour)
	if len(events) != 0 {
		t.Errorf("Events after clear = %d, want 0", len(events))
	}
}

func TestAnalyticsEvent(t *testing.T) {
	event := &AnalyticsEvent{
		ID:        "event-123",
		Type:      "api_request",
		Timestamp: time.Now(),
		UserID:    "user-1",
		Properties: map[string]interface{}{
			"key": "value",
		},
	}

	if event.ID != "event-123" {
		t.Errorf("ID = %s, want event-123", event.ID)
	}

	if event.Type != "api_request" {
		t.Errorf("Type = %s, want api_request", event.Type)
	}
}

func TestAnalyticsStats(t *testing.T) {
	stats := &AnalyticsStats{
		TotalEvents:    100,
		UniqueUsers:    25,
		TopEventTypes:  map[string]int{"api_request": 80, "error": 20},
		EventsPerHour:  []int{10, 15, 12, 8},
	}

	if stats.TotalEvents != 100 {
		t.Errorf("Total events = %d, want 100", stats.TotalEvents)
	}

	if stats.UniqueUsers != 25 {
		t.Errorf("Unique users = %d, want 25", stats.UniqueUsers)
	}
}

func TestService_Concurrent(t *testing.T) {
	svc := NewService()
	done := make(chan bool)

	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 10; j++ {
				svc.RecordEvent(&AnalyticsEvent{
					Type: "concurrent_test",
				})
			}
			done <- true
		}(i)
	}

	for i := 0; i < 10; i++ {
		<-done
	}

	events := svc.GetEvents(time.Hour)
	if len(events) != 100 {
		t.Errorf("Total events = %d, want 100", len(events))
	}
}
