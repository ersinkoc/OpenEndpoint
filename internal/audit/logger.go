package audit

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

// EventType represents the type of audit event
type EventType string

const (
	EventBucketCreated      EventType = "s3:BucketCreated"
	EventBucketDeleted      EventType = "s3:BucketDeleted"
	EventObjectPut          EventType = "s3:ObjectPut"
	EventObjectGet          EventType = "s3:ObjectGet"
	EventObjectDeleted      EventType = "s3:ObjectDeleted"
	EventObjectCopy        EventType = "s3:ObjectCopy"
	EventAccessKeyCreated   EventType = "iam:AccessKeyCreated"
	EventUserCreated       EventType = "iam:UserCreated"
	EventPolicyChanged     EventType = "iam:PolicyChanged"
	EventLogin             EventType = "iam:Login"
	EventLoginFailed       EventType = "iam:LoginFailed"
	EventConfigChanged     EventType = "config:Changed"
)

// Event represents an audit event
type Event struct {
	ID              string                 `json:"id"`
	Timestamp       time.Time              `json:"timestamp"`
	EventType       EventType              `json:"event_type"`
	TenantID        string                 `json:"tenant_id"`
	UserID          string                 `json:"user_id"`
	UserAgent       string                 `json:"user_agent"`
	IPAddress       string                 `json:"ip_address"`
	RequestID       string                 `json:"request_id"`
	Resource        string                 `json:"resource"`
	Action          string                 `json:"action"`
	Status          string                 `json:"status"` // success, failure
	ErrorMessage    string                 `json:"error_message,omitempty"`
	Details         map[string]interface{} `json:"details,omitempty"`
	RequestParams   map[string]interface{} `json:"request_params,omitempty"`
	ResponseParams  map[string]interface{} `json:"response_params,omitempty"`
}

// Logger logs audit events
type Logger struct {
	config  LoggerConfig
	logger  *zap.Logger
	mu      sync.RWMutex
	file    *os.File
	writer  io.WriteCloser
	stopCh  chan struct{}
	closed  bool
}

// LoggerConfig contains logger configuration
type LoggerConfig struct {
	OutputPath   string        // Path to log file
	MaxSizeMB    int           // Max file size before rotation
	MaxBackups   int           // Number of backup files to keep
	Compress     bool          // Compress rotated logs
	Format       string        // json, text
	RedactFields []string      // Fields to redact
}

// DefaultLoggerConfig returns default configuration
func DefaultLoggerConfig() LoggerConfig {
	return LoggerConfig{
		OutputPath:   "/var/log/openendpoint/audit.log",
		MaxSizeMB:    100,
		MaxBackups:   30,
		Compress:     true,
		Format:       "json",
		RedactFields: []string{"secret", "password", "token", "key"},
	}
}

// NewLogger creates a new audit logger
func NewLogger(config LoggerConfig, logger *zap.Logger) (*Logger, error) {
	l := &Logger{
		config: config,
		logger: logger,
		stopCh: make(chan struct{}),
	}

	// Ensure directory exists
	dir := filepath.Dir(config.OutputPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create log directory: %w", err)
	}

	// Open log file
	f, err := os.OpenFile(config.OutputPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}

	l.file = f
	l.writer = f

	return l, nil
}

// Start starts the audit logger
func (l *Logger) Start(ctx context.Context) {
	l.logger.Info("Starting audit logger",
		zap.String("output", l.config.OutputPath))

	// Start rotation goroutine
	go l.rotationLoop(ctx)
}

// Stop stops the audit logger
func (l *Logger) Stop() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return nil
	}

	close(l.stopCh)

	if l.writer != nil {
		l.writer.Close()
	}

	l.closed = true
	l.logger.Info("Audit logger stopped")
	return nil
}

// Log logs an audit event
func (l *Logger) Log(event *Event) {
	// Set defaults
	if event.ID == "" {
		event.ID = uuid.New().String()
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}

	// Redact sensitive fields
	if len(l.config.RedactFields) > 0 {
		l.redact(event)
	}

	// Marshal to JSON
	var data []byte
	var err error

	if l.config.Format == "json" {
		data, err = json.Marshal(event)
	} else {
		data, err = json.MarshalIndent(event, "", "  ")
	}

	if err != nil {
		l.logger.Error("Failed to marshal audit event",
			zap.Error(err))
		return
	}

	// Write to file
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.writer != nil {
		l.writer.Write(data)
		l.writer.Write([]byte("\n"))
	}

	l.logger.Debug("Audit event logged",
		zap.String("id", event.ID),
		zap.String("type", string(event.EventType)),
		zap.String("user", event.UserID),
		zap.String("status", event.Status))
}

// redact redacts sensitive fields
func (l *Logger) redact(event *Event) {
	redact := func(m map[string]interface{}) {
		for _, field := range l.config.RedactFields {
			if val, ok := m[field]; ok {
				m[field] = "***REDACTED***"
				_ = val // Use val to avoid unused warning
			}
		}
	}

	if event.Details != nil {
		redact(event.Details)
	}
	if event.RequestParams != nil {
		redact(event.RequestParams)
	}
	if event.ResponseParams != nil {
		redact(event.ResponseParams)
	}
}

// rotationLoop handles log rotation
func (l *Logger) rotationLoop(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-l.stopCh:
			return
		case <-ticker.C:
			l.checkRotation()
		}
	}
}

// checkRotation checks if rotation is needed
func (l *Logger) checkRotation() {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.file == nil {
		return
	}

	stat, err := l.file.Stat()
	if err != nil {
		return
	}

	sizeMB := int(stat.Size() / (1024 * 1024))
	if sizeMB >= l.config.MaxSizeMB {
		l.rotate()
	}
}

// rotate rotates the log file
func (l *Logger) rotate() {
	now := time.Now()
	backupName := fmt.Sprintf("%s.%s",
		l.config.OutputPath,
		now.Format("2006-01-02-15-04-05"))

	// Close current file
	if l.writer != nil {
		l.writer.Close()
	}

	// Rename current file
	if err := os.Rename(l.config.OutputPath, backupName); err != nil {
		l.logger.Error("Failed to rotate log file",
			zap.Error(err))
		return
	}

	// Open new file
	f, err := os.OpenFile(l.config.OutputPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		l.logger.Error("Failed to open new log file",
			zap.Error(err))
		return
	}

	l.file = f
	l.writer = f

	// Clean up old backups
	l.cleanupBackups()

	l.logger.Info("Log file rotated",
		zap.String("backup", backupName))
}

// cleanupBackups removes old backup files
func (l *Logger) cleanupBackups() {
	dir := filepath.Dir(l.config.OutputPath)
	pattern := filepath.Base(l.config.OutputPath) + ".*"

	matches, err := filepath.Glob(filepath.Join(dir, pattern))
	if err != nil {
		return
	}

	if len(matches) <= l.config.MaxBackups {
		return
	}

	// Sort by modification time
	sortByModTime(matches)

	// Remove oldest files
	toRemove := len(matches) - l.config.MaxBackups
	for i := 0; i < toRemove; i++ {
		if err := os.Remove(matches[i]); err != nil {
			l.logger.Warn("failed to remove old audit log", zap.String("file", matches[i]), zap.Error(err))
		}
	}
}

// sortByModTime sorts files by modification time (oldest first)
func sortByModTime(files []string) {
	// Simple bubble sort for small number of files
	for i := 0; i < len(files); i++ {
		for j := i + 1; j < len(files); j++ {
			ti, erri := os.Stat(files[i])
			tj, errj := os.Stat(files[j])
			// Skip if either file can't be stat'ed
			if erri != nil || errj != nil {
				continue
			}
			if ti.ModTime().After(tj.ModTime()) {
				files[i], files[j] = files[j], files[i]
			}
		}
	}
}

// Query queries audit events
func (l *Logger) Query(ctx context.Context, query Query) ([]*Event, error) {
	// This is a simple implementation - in production, use a proper query engine
	var results []*Event

	// Open log file
	f, err := os.Open(l.config.OutputPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}
	defer f.Close()

	decoder := json.NewDecoder(f)
	for {
		var event Event
		if err := decoder.Decode(&event); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("failed to decode event: %w", err)
		}

		// Apply filters
		if query.TenantID != "" && event.TenantID != query.TenantID {
			continue
		}
		if query.UserID != "" && event.UserID != query.UserID {
			continue
		}
		if query.EventType != "" && event.EventType != query.EventType {
			continue
		}
		if query.StartTime.Unix() > 0 && event.Timestamp.Before(query.StartTime) {
			continue
		}
		if query.EndTime.Unix() > 0 && event.Timestamp.After(query.EndTime) {
			continue
		}

		results = append(results, &event)
	}

	return results, nil
}

// Query contains query parameters
type Query struct {
	TenantID  string
	UserID    string
	EventType EventType
	StartTime time.Time
	EndTime   time.Time
	Limit     int
}

// LogEvent is a convenience function to create and log an event
func (l *Logger) LogEvent(ctx context.Context, eventType EventType, tenantID, userID, action, resource, status string) {
	event := &Event{
		EventType: eventType,
		TenantID:  tenantID,
		UserID:    userID,
		Action:    action,
		Resource:  resource,
		Status:    status,
	}

	// Add request context if available
	if reqID := ctx.Value("request_id"); reqID != nil {
		event.RequestID = reqID.(string)
	}
	if ua := ctx.Value("user_agent"); ua != nil {
		event.UserAgent = ua.(string)
	}
	if ip := ctx.Value("ip_address"); ip != nil {
		event.IPAddress = ip.(string)
	}

	l.Log(event)
}
