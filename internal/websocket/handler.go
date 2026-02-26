package websocket

import (
	"encoding/json"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

var logger, _ = zap.NewProduction()

// Message represents a WebSocket message
type Message struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// Client represents a WebSocket client
type Client struct {
	ID      string
	conn    *websocket.Conn
	send    chan []byte
	hub     *Hub
}

// Hub maintains the set of active clients
type Hub struct {
	clients    map[*Client]bool
	broadcast  chan []byte
	register   chan *Client
	unregister chan *Client
	mu         sync.RWMutex
}

// NewHub creates a new WebSocket hub
func NewHub() *Hub {
	return &Hub{
		clients:    make(map[*Client]bool),
		broadcast:  make(chan []byte, 256),
		register:   make(chan *Client),
		unregister: make(chan *Client),
	}
}

// Run starts the hub
func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = true
			h.mu.Unlock()
			logger.Info("client connected", zap.String("client_id", client.ID))

		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
			}
			h.mu.Unlock()
			logger.Info("client disconnected", zap.String("client_id", client.ID))

		case message := <-h.broadcast:
			h.mu.RLock()
			for client := range h.clients {
				select {
				case client.send <- message:
				default:
					// Send buffer full, close connection
					close(client.send)
					delete(h.clients, client)
				}
			}
			h.mu.RUnlock()
		}
	}
}

// Broadcast broadcasts a message to all clients
func (h *Hub) Broadcast(msgType string, payload interface{}) {
	data, err := json.Marshal(Message{
		Type:    msgType,
		Payload: json.RawMessage(payload.([]byte)),
	})
	if err != nil {
		logger.Error("failed to marshal message", zap.Error(err))
		return
	}
	h.broadcast <- data
}

// BroadcastJSON broadcasts a JSON message to all clients
func (h *Hub) BroadcastJSON(msgType string, payload interface{}) {
	data, err := json.Marshal(map[string]interface{}{
		"type":    msgType,
		"payload": payload,
	})
	if err != nil {
		logger.Error("failed to marshal message", zap.Error(err))
		return
	}
	h.broadcast <- data
}

// ServeWS handles WebSocket connections
func (h *Hub) ServeWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		logger.Error("websocket upgrade error", zap.Error(err))
		return
	}

	client := &Client{
		ID:   generateClientID(),
		conn: conn,
		send: make(chan []byte, 256),
		hub:  h,
	}

	h.register <- client

	// Start writer goroutine
	go client.writePump()

	// Start reader goroutine
	go client.readPump()
}

// readPump reads messages from the WebSocket
func (c *Client) readPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()

	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				logger.Error("websocket error", zap.Error(err))
			}
			break
		}

		// Handle incoming message
		c.handleMessage(message)
	}
}

// writePump writes messages to the WebSocket
func (c *Client) writePump() {
	defer c.conn.Close()

	for {
		message, ok := <-c.send
		if !ok {
			c.conn.WriteMessage(websocket.CloseMessage, []byte{})
			return
		}

		if err := c.conn.WriteMessage(websocket.TextMessage, message); err != nil {
			return
		}
	}
}

// handleMessage handles incoming WebSocket messages
func (c *Client) handleMessage(data []byte) {
	var msg Message
	if err := json.Unmarshal(data, &msg); err != nil {
		logger.Error("failed to unmarshal message", zap.Error(err))
		return
	}

	switch msg.Type {
	case "ping":
		c.send <- []byte(`{"type":"pong"}`)
	case "subscribe":
		// Handle bucket/object subscription
		logger.Info("client subscribed",
			zap.String("client_id", c.ID),
			zap.ByteString("payload", msg.Payload))
	case "unsubscribe":
		// Handle unsubscription
		logger.Info("client unsubscribed",
			zap.String("client_id", c.ID),
			zap.ByteString("payload", msg.Payload))
	}
}

// upgrader upgrades HTTP connections to WebSocket
var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins in development
	},
}

// GlobalHub is the global WebSocket hub
var GlobalHub = NewHub()

// StartHub starts the global WebSocket hub
func StartHub() {
	go GlobalHub.Run()
}

// generateClientID generates a unique client ID
func generateClientID() string {
	// Simple ID generation
	return randomString(8)
}

func randomString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[i % len(letters)]
	}
	return string(b)
}

// Event types for real-time notifications
const (
	EventObjectCreated = "object:created"
	EventObjectDeleted = "object:deleted"
	EventObjectUpdated = "object:updated"
	EventBucketCreated = "bucket:created"
	EventBucketDeleted = "bucket:deleted"
)

// NotifyObjectCreated notifies clients about object creation
func NotifyObjectCreated(bucket, key string) {
	GlobalHub.BroadcastJSON(EventObjectCreated, map[string]string{
		"bucket": bucket,
		"key":    key,
	})
}

// NotifyObjectDeleted notifies clients about object deletion
func NotifyObjectDeleted(bucket, key string) {
	GlobalHub.BroadcastJSON(EventObjectDeleted, map[string]string{
		"bucket": bucket,
		"key":    key,
	})
}

// NotifyBucketCreated notifies clients about bucket creation
func NotifyBucketCreated(bucket string) {
	GlobalHub.BroadcastJSON(EventBucketCreated, map[string]string{
		"bucket": bucket,
	})
}

// NotifyBucketDeleted notifies clients about bucket deletion
func NotifyBucketDeleted(bucket string) {
	GlobalHub.BroadcastJSON(EventBucketDeleted, map[string]string{
		"bucket": bucket,
	})
}
