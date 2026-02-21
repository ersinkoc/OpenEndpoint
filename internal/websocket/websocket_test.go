package websocket

import (
	"testing"
	"time"
)

func TestNewHub(t *testing.T) {
	hub := NewHub()
	if hub == nil {
		t.Fatal("Hub should not be nil")
	}
}

func TestHub_Register(t *testing.T) {
	hub := NewHub()

	client := &Client{ID: "client-1"}
	hub.Register(client)

	if hub.ClientCount() != 1 {
		t.Errorf("Client count = %d, want 1", hub.ClientCount())
	}
}

func TestHub_Unregister(t *testing.T) {
	hub := NewHub()

	client := &Client{ID: "client-1"}
	hub.Register(client)
	hub.Unregister(client)

	if hub.ClientCount() != 0 {
		t.Errorf("Client count = %d, want 0", hub.ClientCount())
	}
}

func TestHub_Broadcast(t *testing.T) {
	hub := NewHub()

	client1 := &Client{ID: "client-1", Send: make(chan []byte, 10)}
	client2 := &Client{ID: "client-2", Send: make(chan []byte, 10)}

	hub.Register(client1)
	hub.Register(client2)

	message := []byte("test message")
	hub.Broadcast(message)

	// Both clients should receive the message
	select {
	case msg := <-client1.Send:
		if string(msg) != "test message" {
			t.Errorf("Message = %s, want test message", string(msg))
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Client 1 should receive message")
	}

	select {
	case msg := <-client2.Send:
		if string(msg) != "test message" {
			t.Errorf("Message = %s, want test message", string(msg))
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Client 2 should receive message")
	}
}

func TestHub_SendToClient(t *testing.T) {
	hub := NewHub()

	client := &Client{ID: "client-1", Send: make(chan []byte, 10)}
	hub.Register(client)

	message := []byte("direct message")
	err := hub.SendToClient("client-1", message)
	if err != nil {
		t.Fatalf("SendToClient failed: %v", err)
	}

	select {
	case msg := <-client.Send:
		if string(msg) != "direct message" {
			t.Errorf("Message = %s, want direct message", string(msg))
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Client should receive message")
	}
}

func TestHub_SendToClient_NotFound(t *testing.T) {
	hub := NewHub()

	err := hub.SendToClient("non-existent", []byte("test"))
	if err == nil {
		t.Error("Should fail for non-existent client")
	}
}

func TestClient(t *testing.T) {
	client := &Client{
		ID:        "client-1",
		UserID:    "user-1",
		Connected: time.Now(),
		Send:      make(chan []byte, 10),
	}

	if client.ID != "client-1" {
		t.Errorf("ID = %s, want client-1", client.ID)
	}
}

func TestMessage(t *testing.T) {
	msg := &Message{
		Type:      "notification",
		Data:      []byte(`{"text": "hello"}`),
		Timestamp: time.Now(),
	}

	if msg.Type != "notification" {
		t.Errorf("Type = %s, want notification", msg.Type)
	}
}

func TestHub_ListClients(t *testing.T) {
	hub := NewHub()

	hub.Register(&Client{ID: "client-1"})
	hub.Register(&Client{ID: "client-2"})

	clients := hub.ListClients()
	if len(clients) != 2 {
		t.Errorf("Client count = %d, want 2", len(clients))
	}
}

func TestHub_GetClient(t *testing.T) {
	hub := NewHub()

	hub.Register(&Client{ID: "client-1"})

	client, ok := hub.GetClient("client-1")
	if !ok {
		t.Fatal("Client should exist")
	}

	if client.ID != "client-1" {
		t.Errorf("ID = %s, want client-1", client.ID)
	}
}

func TestHub_GetClient_NotFound(t *testing.T) {
	hub := NewHub()

	_, ok := hub.GetClient("non-existent")
	if ok {
		t.Error("Client should not exist")
	}
}

func TestHub_Concurrent(t *testing.T) {
	hub := NewHub()
	done := make(chan bool)

	for i := 0; i < 10; i++ {
		go func(id int) {
			client := &Client{
				ID:   string(rune('A' + id)),
				Send: make(chan []byte, 10),
			}
			hub.Register(client)
			hub.GetClient(client.ID)
			done <- true
		}(i)
	}

	for i := 0; i < 10; i++ {
		<-done
	}

	if hub.ClientCount() != 10 {
		t.Errorf("Client count = %d, want 10", hub.ClientCount())
	}
}
