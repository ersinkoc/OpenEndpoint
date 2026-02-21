package events

import (
	"testing"
	"time"
)

func TestNewManager(t *testing.T) {
	mgr := NewManager()
	if mgr == nil {
		t.Fatal("Manager should not be nil")
	}
}

func TestManager_Publish(t *testing.T) {
	mgr := NewManager()

	event := &Event{
		Type:      "test-event",
		Timestamp: time.Now(),
		Data:      map[string]interface{}{"key": "value"},
	}

	err := mgr.Publish(event)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
}

func TestManager_Subscribe(t *testing.T) {
	mgr := NewManager()

	received := make(chan *Event, 1)
	handler := func(e *Event) {
		received <- e
	}

	mgr.Subscribe("test.*", handler)

	event := &Event{Type: "test.event"}
	mgr.Publish(event)

	select {
	case e := <-received:
		if e.Type != "test.event" {
			t.Errorf("Event type = %s, want test.event", e.Type)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Should have received event")
	}
}

func TestManager_Unsubscribe(t *testing.T) {
	mgr := NewManager()

	handler := func(e *Event) {}
	subID := mgr.Subscribe("test.*", handler)

	err := mgr.Unsubscribe(subID)
	if err != nil {
		t.Fatalf("Unsubscribe failed: %v", err)
	}
}

func TestManager_GetHistory(t *testing.T) {
	mgr := NewManager()

	event1 := &Event{Type: "event1"}
	event2 := &Event{Type: "event2"}

	mgr.Publish(event1)
	mgr.Publish(event2)

	history := mgr.GetHistory(10)
	if len(history) != 2 {
		t.Errorf("History count = %d, want 2", len(history))
	}
}

func TestManager_ClearHistory(t *testing.T) {
	mgr := NewManager()

	mgr.Publish(&Event{Type: "test"})
	mgr.ClearHistory()

	history := mgr.GetHistory(10)
	if len(history) != 0 {
		t.Errorf("History count after clear = %d, want 0", len(history))
	}
}

func TestEvent(t *testing.T) {
	event := &Event{
		ID:        "event-123",
		Type:      "test.event",
		Timestamp: time.Now(),
		Source:    "test-source",
		Data:      map[string]interface{}{"key": "value"},
	}

	if event.ID != "event-123" {
		t.Errorf("ID = %s, want event-123", event.ID)
	}

	if event.Type != "test.event" {
		t.Errorf("Type = %s, want test.event", event.Type)
	}
}

func TestEventType_Matching(t *testing.T) {
	tests := []struct {
		pattern string
		event   string
		matches bool
	}{
		{"s3.ObjectCreated.*", "s3.ObjectCreated.Put", true},
		{"s3.ObjectCreated.*", "s3.ObjectRemoved.Delete", false},
		{"s3.*", "s3.ObjectCreated.Put", true},
		{"*", "any.event", true},
	}

	for _, tt := range tests {
		t.Run(tt.pattern+"_"+tt.event, func(t *testing.T) {
			result := matchesPattern(tt.pattern, tt.event)
			if result != tt.matches {
				t.Errorf("matchesPattern(%s, %s) = %v, want %v", tt.pattern, tt.event, result, tt.matches)
			}
		})
	}
}

func TestManager_Concurrent(t *testing.T) {
	mgr := NewManager()
	done := make(chan bool)

	// Subscribe
	mgr.Subscribe("*", func(e *Event) {})

	// Concurrent publish
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 10; j++ {
				mgr.Publish(&Event{Type: "test"})
			}
			done <- true
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}
