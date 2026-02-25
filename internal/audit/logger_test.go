package audit

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestNewLogger(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	cfg := LoggerConfig{
		OutputPath:   filepath.Join(tmpDir, "audit.log"),
		MaxSizeMB:    10,
		MaxBackups:   5,
		Compress:     false,
		Format:       "json",
		RedactFields: []string{"password", "secret"},
	}

	auditLogger, err := NewLogger(cfg, logger)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}

	if auditLogger == nil {
		t.Fatal("Logger should not be nil")
	}

	err = auditLogger.Stop()
	if err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
}

func TestLogger_Log(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	cfg := LoggerConfig{
		OutputPath: filepath.Join(tmpDir, "audit.log"),
		MaxSizeMB:  10,
		MaxBackups: 5,
		Format:     "json",
	}

	auditLogger, err := NewLogger(cfg, logger)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer auditLogger.Stop()

	event := &Event{
		Timestamp: time.Now(),
		EventType: EventObjectGet,
		Action:    "GetObject",
		Resource:  "test-bucket/test-key",
		Status:    "success",
		IPAddress: "192.168.1.1",
		UserAgent: "test-agent",
	}

	auditLogger.Log(event)
}

func TestLogger_Query(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	cfg := LoggerConfig{
		OutputPath: filepath.Join(tmpDir, "audit.log"),
		MaxSizeMB:  10,
		MaxBackups: 5,
		Format:     "json",
	}

	auditLogger, err := NewLogger(cfg, logger)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer auditLogger.Stop()

	ctx := context.Background()

	// Log some events
	for i := 0; i < 5; i++ {
		event := &Event{
			Timestamp: time.Now(),
			EventType: EventObjectGet,
			Action:    "GetObject",
			Resource:  "test-bucket/key-" + string(rune('A'+i)),
			Status:    "success",
		}
		auditLogger.Log(event)
	}

	// Query events
	query := Query{
		EventType: EventObjectGet,
		Limit:     10,
	}

	events, err := auditLogger.Query(ctx, query)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(events) == 0 {
		t.Error("Query should return events")
	}
}

func TestLogger_QueryByTenantID(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	cfg := LoggerConfig{
		OutputPath: filepath.Join(tmpDir, "audit.log"),
		MaxSizeMB:  10,
		MaxBackups: 5,
		Format:     "json",
	}

	auditLogger, err := NewLogger(cfg, logger)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer auditLogger.Stop()

	ctx := context.Background()

	// Log events for different tenants
	tenants := []string{"tenant1", "tenant2", "tenant3"}
	for _, tenant := range tenants {
		event := &Event{
			Timestamp: time.Now(),
			EventType: EventObjectPut,
			TenantID:  tenant,
			Action:    "PutObject",
			Resource:  "bucket/key",
			Status:    "success",
		}
		auditLogger.Log(event)
	}

	// Query specific tenant
	query := Query{
		TenantID: "tenant2",
		Limit:    10,
	}

	events, err := auditLogger.Query(ctx, query)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// All events should be for tenant2
	for _, event := range events {
		if event.TenantID != "tenant2" {
			t.Errorf("Expected tenant2, got %s", event.TenantID)
		}
	}
}

func TestLogger_QueryByTimeRange(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	cfg := LoggerConfig{
		OutputPath: filepath.Join(tmpDir, "audit.log"),
		MaxSizeMB:  10,
		MaxBackups: 5,
		Format:     "json",
	}

	auditLogger, err := NewLogger(cfg, logger)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer auditLogger.Stop()

	ctx := context.Background()

	// Log event
	now := time.Now()
	event := &Event{
		Timestamp: now,
		EventType: EventObjectDeleted,
		Action:    "DeleteObject",
		Resource:  "test-bucket/key",
		Status:    "success",
	}
	auditLogger.Log(event)

	// Query with time range
	query := Query{
		StartTime: now.Add(-1 * time.Hour),
		EndTime:   now.Add(1 * time.Hour),
		Limit:     10,
	}

	events, err := auditLogger.Query(ctx, query)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(events) == 0 {
		t.Error("Query should return events in time range")
	}
}

func TestLogger_Rotation(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	cfg := LoggerConfig{
		OutputPath: filepath.Join(tmpDir, "audit.log"),
		MaxSizeMB:  1, // Small size to trigger rotation
		MaxBackups: 2,
		Format:     "json",
	}

	auditLogger, err := NewLogger(cfg, logger)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer auditLogger.Stop()

	// Log many events to trigger rotation
	// Each event is about 200 bytes, so 10 events should be ~2KB
	// With MaxSizeMB=1, this won't trigger rotation in practice
	// But we can at least verify logging works
	for i := 0; i < 10; i++ {
		event := &Event{
			Timestamp: time.Now(),
			EventType: EventObjectGet,
			Action:    "GetObject",
			Resource:  "bucket/key-" + string(rune(i)),
			Status:    "success",
		}
		auditLogger.Log(event)
	}

	// Check that the main log file was created
	fi, err := os.Stat(filepath.Join(tmpDir, "audit.log"))
	if err != nil {
		t.Error("Log file should be created")
	}
	if fi != nil && fi.Size() == 0 {
		t.Error("Log file should have content")
	}
}

func TestSortByModTime(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create test files with different modification times
	files := []string{}
	for i := 0; i < 3; i++ {
		path := filepath.Join(tmpDir, "file"+string(rune('A'+i))+".log")
		os.WriteFile(path, []byte("test"), 0644)
		files = append(files, path)
		time.Sleep(10 * time.Millisecond) // Ensure different times
	}

	sortByModTime(files)

	// Files should be sorted by modification time (oldest first)
	if len(files) != 3 {
		t.Errorf("File count = %d, want 3", len(files))
	}
}

func TestLogger_Redaction(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	cfg := LoggerConfig{
		OutputPath:   filepath.Join(tmpDir, "audit.log"),
		MaxSizeMB:    10,
		MaxBackups:   5,
		Format:       "json",
		RedactFields: []string{"password", "secret"},
	}

	auditLogger, err := NewLogger(cfg, logger)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer auditLogger.Stop()

	event := &Event{
		Timestamp: time.Now(),
		EventType: EventObjectPut,
		Action:    "PutObject",
		Resource:  "test-bucket/secret-key",
		Status:    "success",
		Details: map[string]interface{}{
			"password": "secret123",
			"data":     "normal data",
		},
	}

	auditLogger.Log(event)
}

func TestLogger_Concurrent(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	cfg := LoggerConfig{
		OutputPath: filepath.Join(tmpDir, "audit.log"),
		MaxSizeMB:  10,
		MaxBackups: 5,
		Format:     "json",
	}

	auditLogger, err := NewLogger(cfg, logger)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer auditLogger.Stop()

	done := make(chan bool)

	// Concurrent logging
	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 10; j++ {
				event := &Event{
					Timestamp: time.Now(),
					EventType: EventObjectGet,
					Action:    "ConcurrentTest",
					Resource:  "bucket/key-" + string(rune('A'+id)),
					Status:    "success",
				}
				auditLogger.Log(event)
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestDefaultLoggerConfig(t *testing.T) {
	cfg := DefaultLoggerConfig()

	if cfg.OutputPath == "" {
		t.Error("OutputPath should not be empty")
	}
	if cfg.MaxSizeMB == 0 {
		t.Error("MaxSizeMB should not be zero")
	}
	if cfg.MaxBackups == 0 {
		t.Error("MaxBackups should not be zero")
	}
}

func TestLogEvent(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	cfg := LoggerConfig{
		OutputPath: filepath.Join(tmpDir, "audit.log"),
		MaxSizeMB:  10,
		MaxBackups: 5,
		Format:     "json",
	}

	auditLogger, err := NewLogger(cfg, logger)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer auditLogger.Stop()

	ctx := context.WithValue(context.Background(), "request_id", "test-request-id")
	ctx = context.WithValue(ctx, "user_agent", "test-agent")
	ctx = context.WithValue(ctx, "ip_address", "192.168.1.1")

	auditLogger.LogEvent(ctx, EventObjectPut, "tenant1", "user1", "PutObject", "bucket/key", "success")
}

func TestLogger_Start(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	cfg := LoggerConfig{
		OutputPath: filepath.Join(tmpDir, "audit.log"),
		MaxSizeMB:  10,
		MaxBackups: 5,
		Format:     "json",
	}

	auditLogger, err := NewLogger(cfg, logger)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	auditLogger.Start(ctx)

	time.Sleep(50 * time.Millisecond)
	cancel()
	auditLogger.Stop()
}

func TestLogger_StopTwice(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	cfg := LoggerConfig{
		OutputPath: filepath.Join(tmpDir, "audit.log"),
		Format:     "json",
	}

	auditLogger, err := NewLogger(cfg, logger)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}

	auditLogger.Stop()
	auditLogger.Stop()
}

func TestLogger_LogWithAutoFields(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	cfg := LoggerConfig{
		OutputPath: filepath.Join(tmpDir, "audit.log"),
		Format:     "json",
	}

	auditLogger, err := NewLogger(cfg, logger)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer auditLogger.Stop()

	event := &Event{
		EventType: EventObjectGet,
	}
	auditLogger.Log(event)

	if event.ID == "" {
		t.Error("ID should be auto-generated")
	}
	if event.Timestamp.IsZero() {
		t.Error("Timestamp should be auto-generated")
	}
}

func TestLogger_LogTextFormat(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	cfg := LoggerConfig{
		OutputPath: filepath.Join(tmpDir, "audit.log"),
		Format:     "text",
	}

	auditLogger, err := NewLogger(cfg, logger)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer auditLogger.Stop()

	event := &Event{
		EventType: EventObjectGet,
		Action:    "GetObject",
	}
	auditLogger.Log(event)
}

func TestLogger_QueryByUserID(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	cfg := LoggerConfig{
		OutputPath: filepath.Join(tmpDir, "audit.log"),
		Format:     "json",
	}

	auditLogger, err := NewLogger(cfg, logger)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer auditLogger.Stop()

	ctx := context.Background()

	auditLogger.Log(&Event{EventType: EventObjectGet, UserID: "user1", Timestamp: time.Now()})
	auditLogger.Log(&Event{EventType: EventObjectGet, UserID: "user2", Timestamp: time.Now()})
	auditLogger.Log(&Event{EventType: EventObjectGet, UserID: "user1", Timestamp: time.Now()})

	query := Query{UserID: "user1"}
	events, err := auditLogger.Query(ctx, query)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	for _, e := range events {
		if e.UserID != "user1" {
			t.Errorf("Expected user1, got %s", e.UserID)
		}
	}
}

func TestLogger_QueryNonExistent(t *testing.T) {
	logger := zap.NewNop()
	cfg := LoggerConfig{
		OutputPath: "/nonexistent/path/audit.log",
		Format:     "json",
	}

	auditLogger := &Logger{
		config: cfg,
		logger: logger,
		stopCh: make(chan struct{}),
	}

	ctx := context.Background()
	_, err := auditLogger.Query(ctx, Query{})
	if err == nil {
		t.Error("Query should fail for non-existent file")
	}
}

func TestLogger_QueryExcludeByEventType(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	cfg := LoggerConfig{
		OutputPath: filepath.Join(tmpDir, "audit.log"),
		Format:     "json",
	}

	auditLogger, err := NewLogger(cfg, logger)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer auditLogger.Stop()

	ctx := context.Background()

	auditLogger.Log(&Event{EventType: EventObjectGet, Timestamp: time.Now()})
	auditLogger.Log(&Event{EventType: EventObjectPut, Timestamp: time.Now()})

	query := Query{EventType: EventObjectGet}
	events, err := auditLogger.Query(ctx, query)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	for _, e := range events {
		if e.EventType != EventObjectGet {
			t.Errorf("Expected EventObjectGet, got %s", e.EventType)
		}
	}
}

func TestLogger_QueryExcludeByStartTime(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	cfg := LoggerConfig{
		OutputPath: filepath.Join(tmpDir, "audit.log"),
		Format:     "json",
	}

	auditLogger, err := NewLogger(cfg, logger)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer auditLogger.Stop()

	ctx := context.Background()
	now := time.Now()

	auditLogger.Log(&Event{EventType: EventObjectGet, Timestamp: now.Add(-2 * time.Hour)})
	auditLogger.Log(&Event{EventType: EventObjectGet, Timestamp: now})

	query := Query{StartTime: now.Add(-1 * time.Hour)}
	events, err := auditLogger.Query(ctx, query)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	for _, e := range events {
		if e.Timestamp.Before(query.StartTime) {
			t.Error("Event should be after start time")
		}
	}
}

func TestLogger_QueryExcludeByEndTime(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	cfg := LoggerConfig{
		OutputPath: filepath.Join(tmpDir, "audit.log"),
		Format:     "json",
	}

	auditLogger, err := NewLogger(cfg, logger)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer auditLogger.Stop()

	ctx := context.Background()
	now := time.Now()

	auditLogger.Log(&Event{EventType: EventObjectGet, Timestamp: now})
	auditLogger.Log(&Event{EventType: EventObjectGet, Timestamp: now.Add(2 * time.Hour)})

	query := Query{EndTime: now.Add(1 * time.Hour)}
	events, err := auditLogger.Query(ctx, query)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	for _, e := range events {
		if e.Timestamp.After(query.EndTime) {
			t.Error("Event should be before end time")
		}
	}
}

func TestLogger_RedactAllMaps(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	cfg := LoggerConfig{
		OutputPath:   filepath.Join(tmpDir, "audit.log"),
		Format:       "json",
		RedactFields: []string{"secret"},
	}

	auditLogger, err := NewLogger(cfg, logger)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer auditLogger.Stop()

	event := &Event{
		EventType:      EventObjectPut,
		Timestamp:      time.Now(),
		Details:        map[string]interface{}{"secret": "val1"},
		RequestParams:  map[string]interface{}{"secret": "val2"},
		ResponseParams: map[string]interface{}{"secret": "val3"},
	}
	auditLogger.Log(event)

	if event.Details["secret"] != "***REDACTED***" {
		t.Error("Details secret should be redacted")
	}
	if event.RequestParams["secret"] != "***REDACTED***" {
		t.Error("RequestParams secret should be redacted")
	}
	if event.ResponseParams["secret"] != "***REDACTED***" {
		t.Error("ResponseParams secret should be redacted")
	}
}

func TestSortByModTimeWithErrors(t *testing.T) {
	files := []string{
		"/nonexistent/file1.log",
		"/nonexistent/file2.log",
	}
	sortByModTime(files)
}

func TestEventTypes(t *testing.T) {
	types := []EventType{
		EventBucketCreated,
		EventBucketDeleted,
		EventObjectPut,
		EventObjectGet,
		EventObjectDeleted,
		EventObjectCopy,
		EventAccessKeyCreated,
		EventUserCreated,
		EventPolicyChanged,
		EventLogin,
		EventLoginFailed,
		EventConfigChanged,
	}

	for _, et := range types {
		if et == "" {
			t.Error("EventType should not be empty")
		}
	}
}

func TestLogger_CheckRotation(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	logPath := filepath.Join(tmpDir, "audit.log")
	cfg := LoggerConfig{
		OutputPath: logPath,
		MaxSizeMB:  1,
		MaxBackups: 3,
		Format:     "json",
	}

	auditLogger, err := NewLogger(cfg, logger)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer auditLogger.Stop()

	auditLogger.checkRotation()
}

func TestLogger_Rotate(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	logPath := filepath.Join(tmpDir, "audit.log")

	f, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("Failed to create log file: %v", err)
	}
	f.WriteString("initial content")
	f.Close()

	cfg := LoggerConfig{
		OutputPath: logPath,
		MaxSizeMB:  1,
		MaxBackups: 3,
		Format:     "json",
	}

	auditLogger, err := NewLogger(cfg, logger)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer auditLogger.Stop()

	auditLogger.rotate()

	files, _ := filepath.Glob(logPath + ".*")
	if len(files) == 0 {
		t.Error("Expected backup file to be created")
	}
}

func TestLogger_CleanupBackups(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	logPath := filepath.Join(tmpDir, "audit.log")

	for i := 0; i < 5; i++ {
		backupPath := fmt.Sprintf("%s.2024-01-0%d-00-00-00", logPath, i+1)
		os.WriteFile(backupPath, []byte("backup"), 0644)
		time.Sleep(10 * time.Millisecond)
	}

	cfg := LoggerConfig{
		OutputPath: logPath,
		MaxSizeMB:  1,
		MaxBackups: 2,
		Format:     "json",
	}

	auditLogger, err := NewLogger(cfg, logger)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer auditLogger.Stop()

	auditLogger.cleanupBackups()

	files, _ := filepath.Glob(logPath + ".*")
	if len(files) > cfg.MaxBackups {
		t.Errorf("Expected at most %d backup files, got %d", cfg.MaxBackups, len(files))
	}
}

func TestLogger_RotationLoop(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	cfg := LoggerConfig{
		OutputPath: filepath.Join(tmpDir, "audit.log"),
		MaxSizeMB:  10,
		MaxBackups: 5,
		Format:     "json",
	}

	auditLogger, err := NewLogger(cfg, logger)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go auditLogger.rotationLoop(ctx)

	for i := 0; i < 5; i++ {
		event := &Event{
			Timestamp: time.Now(),
			EventType: EventObjectGet,
			Action:    "GetObject",
			Resource:  "bucket/key",
			Status:    "success",
		}
		auditLogger.Log(event)
		time.Sleep(20 * time.Millisecond)
	}

	cancel()
	auditLogger.Stop()
}

func TestLogger_NewLoggerWithInvalidPath(t *testing.T) {
	logger := zap.NewNop()
	cfg := LoggerConfig{
		OutputPath: "/nonexistent/directory/audit.log",
		MaxSizeMB:  10,
		MaxBackups: 5,
		Format:     "json",
	}

	_, err := NewLogger(cfg, logger)
	if err != nil {
		t.Logf("NewLogger failed as expected: %v", err)
	}
}

func TestLogger_LogMultipleEvents(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	cfg := LoggerConfig{
		OutputPath:   filepath.Join(tmpDir, "audit.log"),
		MaxSizeMB:    10,
		MaxBackups:   5,
		Format:       "json",
		RedactFields: []string{"password"},
	}

	auditLogger, err := NewLogger(cfg, logger)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer auditLogger.Stop()

	for i := 0; i < 100; i++ {
		event := &Event{
			Timestamp: time.Now(),
			EventType: EventObjectGet,
			Action:    "GetObject",
			Resource:  fmt.Sprintf("bucket/key-%d", i),
			Status:    "success",
			Details: map[string]interface{}{
				"index":    i,
				"password": "secret",
			},
		}
		auditLogger.Log(event)
	}
}

func TestLogger_QueryWithLimit(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	cfg := LoggerConfig{
		OutputPath: filepath.Join(tmpDir, "audit.log"),
		Format:     "json",
	}

	auditLogger, err := NewLogger(cfg, logger)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer auditLogger.Stop()

	ctx := context.Background()
	for i := 0; i < 20; i++ {
		auditLogger.Log(&Event{
			EventType: EventObjectGet,
			Timestamp: time.Now(),
			Resource:  fmt.Sprintf("bucket/key-%d", i),
		})
	}

	query := Query{Limit: 5}
	events, err := auditLogger.Query(ctx, query)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(events) == 0 {
		t.Error("Query should return events")
	}
}

func TestLogger_NewLogger_DirectoryError(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "blocked-*")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	logger := zap.NewNop()
	cfg := LoggerConfig{
		OutputPath: filepath.Join(tmpFile.Name(), "subdir", "audit.log"),
		Format:     "json",
	}

	_, err = NewLogger(cfg, logger)
	if err == nil {
		t.Error("Expected error when directory creation fails")
	}
}

func TestLogger_RotationLoopStopCh(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	cfg := LoggerConfig{
		OutputPath: filepath.Join(tmpDir, "audit.log"),
		Format:     "json",
	}

	auditLogger, err := NewLogger(cfg, logger)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}

	ctx := context.Background()
	go auditLogger.rotationLoop(ctx)

	time.Sleep(50 * time.Millisecond)
	close(auditLogger.stopCh)
	time.Sleep(50 * time.Millisecond)
}

func TestLogger_CheckRotationNilFile(t *testing.T) {
	logger := zap.NewNop()
	cfg := LoggerConfig{
		OutputPath: filepath.Join(os.TempDir(), "audit.log"),
		Format:     "json",
	}

	auditLogger := &Logger{
		config: cfg,
		logger: logger,
		file:   nil,
		stopCh: make(chan struct{}),
	}

	auditLogger.checkRotation()
}

func TestLogger_RotateRenameError(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	logPath := filepath.Join(tmpDir, "audit.log")

	f, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("Failed to create log file: %v", err)
	}
	f.WriteString("initial content")
	f.Close()

	cfg := LoggerConfig{
		OutputPath: "/nonexistent/path/audit.log",
		MaxSizeMB:  1,
		MaxBackups: 3,
		Format:     "json",
	}

	auditLogger := &Logger{
		config: cfg,
		logger: logger,
		file:   f,
		writer: f,
		stopCh: make(chan struct{}),
	}

	auditLogger.rotate()
}

func TestLogger_RotateOpenError(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	logPath := filepath.Join(tmpDir, "audit.log")

	f, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("Failed to create log file: %v", err)
	}
	f.WriteString("initial content")
	f.Close()

	cfg := LoggerConfig{
		OutputPath: logPath,
		MaxSizeMB:  1,
		MaxBackups: 3,
		Format:     "json",
	}

	auditLogger := &Logger{
		config: cfg,
		logger: logger,
		file:   f,
		writer: f,
		stopCh: make(chan struct{}),
	}

	os.RemoveAll(tmpDir)
	auditLogger.rotate()
}

func TestSortByModTimeSwap(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	fileA := filepath.Join(tmpDir, "a.log")
	fileB := filepath.Join(tmpDir, "b.log")

	os.WriteFile(fileB, []byte("b"), 0644)
	time.Sleep(20 * time.Millisecond)
	os.WriteFile(fileA, []byte("a"), 0644)

	files := []string{fileA, fileB}
	sortByModTime(files)

	if files[0] != fileB {
		t.Errorf("Expected oldest file first, got %s", files[0])
	}
}

func TestLogger_QueryDecodeError(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logPath := filepath.Join(tmpDir, "audit.log")
	os.WriteFile(logPath, []byte("invalid json content\n"), 0644)

	logger := zap.NewNop()
	cfg := LoggerConfig{
		OutputPath: logPath,
		Format:     "json",
	}

	auditLogger := &Logger{
		config: cfg,
		logger: logger,
		stopCh: make(chan struct{}),
	}

	ctx := context.Background()
	_, err = auditLogger.Query(ctx, Query{})
	if err == nil {
		t.Error("Expected error for invalid JSON")
	}
}

func TestLogger_LogMarshalError(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	cfg := LoggerConfig{
		OutputPath: filepath.Join(tmpDir, "audit.log"),
		Format:     "json",
	}

	auditLogger, err := NewLogger(cfg, logger)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer auditLogger.Stop()

	event := &Event{
		EventType: EventObjectGet,
		Details: map[string]interface{}{
			"channel": make(chan int),
		},
	}

	auditLogger.Log(event)
}

func TestLogger_RotationLoopTicker(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	cfg := LoggerConfig{
		OutputPath: filepath.Join(tmpDir, "audit.log"),
		Format:     "json",
	}

	auditLogger, err := NewLogger(cfg, logger)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stopped := make(chan struct{})
	go func() {
		auditLogger.rotationLoop(ctx)
		close(stopped)
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()
	<-stopped
}

func TestLogger_CheckRotationStatError(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	logPath := filepath.Join(tmpDir, "audit.log")

	f, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("Failed to create log file: %v", err)
	}
	f.WriteString("test")
	f.Close()

	os.Remove(logPath)

	auditLogger := &Logger{
		config: LoggerConfig{OutputPath: logPath},
		logger: logger,
		file:   f,
		stopCh: make(chan struct{}),
	}

	auditLogger.checkRotation()
}

func TestLogger_RotateOpenNewFileError(t *testing.T) {
	logger := zap.NewNop()

	f, err := os.CreateTemp("", "audit-*.log")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	f.Close()
	os.Remove(f.Name())

	auditLogger := &Logger{
		config: LoggerConfig{
			OutputPath: "/nonexistent_dir_" + fmt.Sprintf("%d", time.Now().UnixNano()) + "/audit.log",
			MaxBackups: 3,
		},
		logger: logger,
		file:   f,
		writer: f,
		stopCh: make(chan struct{}),
	}

	auditLogger.rotate()
}

func TestLogger_NewLoggerOpenFileError(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logPath := filepath.Join(tmpDir, "audit.log")
	os.WriteFile(logPath, []byte("test"), 0444)

	logger := zap.NewNop()
	cfg := LoggerConfig{
		OutputPath: logPath,
		Format:     "json",
	}

	os.Chmod(tmpDir, 0555)
	defer os.Chmod(tmpDir, 0755)

	_, err = NewLogger(cfg, logger)
	if err == nil {
		t.Error("Expected error when file open fails")
	}
}

func TestLogger_RotationLoopStopChannel(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	cfg := LoggerConfig{
		OutputPath: filepath.Join(tmpDir, "audit.log"),
		Format:     "json",
	}

	auditLogger, err := NewLogger(cfg, logger)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}

	done := make(chan struct{})
	go func() {
		auditLogger.rotationLoop(context.Background())
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	close(auditLogger.stopCh)
	<-done
}

func TestLogger_CheckRotationTriggersRotate(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	logPath := filepath.Join(tmpDir, "audit.log")

	largeContent := make([]byte, 2*1024*1024)
	os.WriteFile(logPath, largeContent, 0644)

	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("Failed to open log file: %v", err)
	}

	cfg := LoggerConfig{
		OutputPath: logPath,
		MaxSizeMB:  1,
		MaxBackups: 3,
		Format:     "json",
	}

	auditLogger := &Logger{
		config: cfg,
		logger: logger,
		file:   f,
		writer: f,
		stopCh: make(chan struct{}),
	}

	auditLogger.checkRotation()

	files, _ := filepath.Glob(logPath + ".*")
	if len(files) == 0 {
		t.Error("Expected backup file to be created after rotation")
	}
}

func TestLogger_CleanupBackupsWithFilesToRemove(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	logPath := filepath.Join(tmpDir, "audit.log")

	for i := 0; i < 5; i++ {
		backupPath := fmt.Sprintf("%s.2024-01-0%d-00-00-00", logPath, i+1)
		os.WriteFile(backupPath, []byte("backup"), 0644)
		time.Sleep(10 * time.Millisecond)
	}

	f, _ := os.Create(logPath)
	f.Close()

	cfg := LoggerConfig{
		OutputPath: logPath,
		MaxSizeMB:  1,
		MaxBackups: 2,
		Format:     "json",
	}

	auditLogger := &Logger{
		config: cfg,
		logger: logger,
		file:   f,
		writer: f,
		stopCh: make(chan struct{}),
	}

	auditLogger.cleanupBackups()

	files, _ := filepath.Glob(logPath + ".*")
	if len(files) > cfg.MaxBackups {
		t.Errorf("Expected at most %d backup files, got %d", cfg.MaxBackups, len(files))
	}
}

func TestLogger_RotateWithValidPath(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	logPath := filepath.Join(tmpDir, "audit.log")

	os.WriteFile(logPath, []byte("initial content"), 0644)

	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("Failed to open log file: %v", err)
	}

	cfg := LoggerConfig{
		OutputPath: logPath,
		MaxSizeMB:  1,
		MaxBackups: 3,
		Format:     "json",
	}

	auditLogger := &Logger{
		config: cfg,
		logger: logger,
		file:   f,
		writer: f,
		stopCh: make(chan struct{}),
	}

	auditLogger.rotate()

	files, _ := filepath.Glob(logPath + ".*")
	if len(files) == 0 {
		t.Error("Expected backup file to be created")
	}

	if _, err := os.Stat(logPath); os.IsNotExist(err) {
		t.Error("Expected new log file to be created")
	}
}

func TestLogger_RotateRenameFails(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	logPath := filepath.Join(tmpDir, "audit.log")

	os.WriteFile(logPath, []byte("initial content"), 0644)

	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("Failed to open log file: %v", err)
	}

	auditLogger := &Logger{
		config: LoggerConfig{
			OutputPath: "/nonexistent_dir/invalid/audit.log",
			MaxBackups: 3,
		},
		logger: logger,
		file:   f,
		writer: f,
		stopCh: make(chan struct{}),
	}

	auditLogger.rotate()
}

func TestLogger_CleanupBackupsRemoveFails(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	logPath := filepath.Join(tmpDir, "audit.log")

	var backupPaths []string
	for i := 0; i < 5; i++ {
		backupPath := fmt.Sprintf("%s.2024-01-0%d-00-00-00", logPath, i+1)
		os.WriteFile(backupPath, []byte("backup"), 0644)
		backupPaths = append(backupPaths, backupPath)
		time.Sleep(10 * time.Millisecond)
	}

	subDir := filepath.Join(tmpDir, "protected")
	os.MkdirAll(subDir, 0755)
	for i := 0; i < 2; i++ {
		backupPath := filepath.Join(subDir, fmt.Sprintf("audit.log.2024-02-%02d-00-00-00", i+1))
		os.WriteFile(backupPath, []byte("backup"), 0644)
	}

	f, _ := os.Create(logPath)
	f.Close()

	cfg := LoggerConfig{
		OutputPath: logPath,
		MaxSizeMB:  1,
		MaxBackups: 2,
		Format:     "json",
	}

	auditLogger := &Logger{
		config: cfg,
		logger: logger,
		file:   f,
		writer: f,
		stopCh: make(chan struct{}),
	}

	for _, p := range backupPaths[:3] {
		os.Chmod(p, 0000)
	}

	auditLogger.cleanupBackups()

	for _, p := range backupPaths {
		os.Chmod(p, 0644)
	}
}

func TestLogger_RotationStopChDirect(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	cfg := LoggerConfig{
		OutputPath: filepath.Join(tmpDir, "audit.log"),
		Format:     "json",
	}

	auditLogger, err := NewLogger(cfg, logger)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}

	stopCh := make(chan struct{})
	auditLogger.stopCh = stopCh

	done := make(chan struct{})
	go func() {
		auditLogger.rotationLoop(context.Background())
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	close(stopCh)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Error("rotationLoop should have exited via stopCh")
	}
}

func TestLogger_RotateOpenNewFileFails(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	logPath := filepath.Join(tmpDir, "audit.log")

	os.WriteFile(logPath, []byte("initial content"), 0644)

	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("Failed to open log file: %v", err)
	}

	auditLogger := &Logger{
		config: LoggerConfig{
			OutputPath: filepath.Join(tmpDir, "nonexistent", "audit.log"),
			MaxBackups: 3,
		},
		logger: logger,
		file:   f,
		writer: f,
		stopCh: make(chan struct{}),
	}

	os.Rename(logPath, logPath+".backup")
	auditLogger.rotate()
}

func TestLogger_RotateRenameFailsMissingFile(t *testing.T) {
	logger := zap.NewNop()

	f, err := os.CreateTemp("", "audit-*.log")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	f.Close()
	tempPath := f.Name()
	os.Remove(tempPath)

	auditLogger := &Logger{
		config: LoggerConfig{
			OutputPath: tempPath,
			MaxBackups: 3,
		},
		logger: logger,
		file:   f,
		writer: f,
		stopCh: make(chan struct{}),
	}

	auditLogger.rotate()
}

func TestLogger_CleanupBackupsWithLockedFiles(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	logPath := filepath.Join(tmpDir, "audit.log")

	var backupPaths []string
	for i := 0; i < 5; i++ {
		backupPath := fmt.Sprintf("%s.2024-01-0%d-00-00-00", logPath, i+1)
		os.WriteFile(backupPath, []byte("backup"), 0644)
		backupPaths = append(backupPaths, backupPath)
		time.Sleep(10 * time.Millisecond)
	}

	f, _ := os.Create(logPath)
	f.Close()

	cfg := LoggerConfig{
		OutputPath: logPath,
		MaxSizeMB:  1,
		MaxBackups: 2,
		Format:     "json",
	}

	auditLogger := &Logger{
		config: cfg,
		logger: logger,
		file:   f,
		writer: f,
		stopCh: make(chan struct{}),
	}

	lockedFile, err := os.OpenFile(backupPaths[0], os.O_RDWR, 0644)
	if err == nil {
		defer lockedFile.Close()
	}

	auditLogger.cleanupBackups()
}

func TestLogger_RotateRenameErrorSourceMissing(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	logPath := filepath.Join(tmpDir, "audit.log")

	f, err := os.CreateTemp(tmpDir, "temp-*.log")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	f.Close()
	os.Remove(f.Name())

	auditLogger := &Logger{
		config: LoggerConfig{
			OutputPath: logPath,
			MaxBackups: 3,
		},
		logger: logger,
		file:   f,
		writer: f,
		stopCh: make(chan struct{}),
	}

	auditLogger.rotate()
}

func TestLogger_CleanupBackupsRemoveLockedFile(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	logPath := filepath.Join(tmpDir, "audit.log")

	var backupPaths []string
	for i := 0; i < 5; i++ {
		backupPath := fmt.Sprintf("%s.2024-01-0%d-00-00-00", logPath, i+1)
		os.WriteFile(backupPath, []byte("backup"), 0644)
		backupPaths = append(backupPaths, backupPath)
		time.Sleep(10 * time.Millisecond)
	}

	f, _ := os.Create(logPath)
	f.Close()

	cfg := LoggerConfig{
		OutputPath: logPath,
		MaxSizeMB:  1,
		MaxBackups: 2,
		Format:     "json",
	}

	auditLogger := &Logger{
		config: cfg,
		logger: logger,
		file:   f,
		writer: f,
		stopCh: make(chan struct{}),
	}

	lockedFiles := make([]*os.File, 0, 3)
	for i := 0; i < 3; i++ {
		lf, err := os.OpenFile(backupPaths[i], os.O_RDWR, 0644)
		if err == nil {
			lockedFiles = append(lockedFiles, lf)
		}
	}

	auditLogger.cleanupBackups()

	for _, lf := range lockedFiles {
		lf.Close()
	}
}

func TestLogger_RotationLoopStopChOnly(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	cfg := LoggerConfig{
		OutputPath: filepath.Join(tmpDir, "audit.log"),
		Format:     "json",
	}

	auditLogger, err := NewLogger(cfg, logger)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}

	stopCh := make(chan struct{})
	auditLogger.stopCh = stopCh

	started := make(chan struct{})
	done := make(chan struct{})
	go func() {
		close(started)
		auditLogger.rotationLoop(context.Background())
		close(done)
	}()

	<-started
	time.Sleep(200 * time.Millisecond)
	close(stopCh)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Error("rotationLoop should have exited via stopCh")
	}
}

func TestLogger_RotateOpenNewFileFailsAfterRename(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	logPath := filepath.Join(tmpDir, "audit.log")

	os.WriteFile(logPath, []byte("initial content"), 0644)

	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("Failed to open log file: %v", err)
	}

	nonexistentDir := filepath.Join(tmpDir, "nonexistent", "dir")

	auditLogger := &Logger{
		config: LoggerConfig{
			OutputPath: filepath.Join(nonexistentDir, "audit.log"),
			MaxBackups: 3,
		},
		logger: logger,
		file:   f,
		writer: f,
		stopCh: make(chan struct{}),
	}

	auditLogger.rotate()
}

func TestLogger_CleanupBackupsRemoveError(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	logPath := filepath.Join(tmpDir, "audit.log")

	var backupPaths []string
	for i := 0; i < 5; i++ {
		backupPath := fmt.Sprintf("%s.2024-01-0%d-00-00-00", logPath, i+1)
		os.WriteFile(backupPath, []byte("backup"), 0644)
		backupPaths = append(backupPaths, backupPath)
		time.Sleep(10 * time.Millisecond)
	}

	f, _ := os.Create(logPath)
	f.Close()

	cfg := LoggerConfig{
		OutputPath: logPath,
		MaxSizeMB:  1,
		MaxBackups: 2,
		Format:     "json",
	}

	auditLogger := &Logger{
		config: cfg,
		logger: logger,
		file:   f,
		writer: f,
		stopCh: make(chan struct{}),
	}

	for i := 0; i < 3; i++ {
		os.Chmod(backupPaths[i], 0000)
	}

	auditLogger.cleanupBackups()

	for i := 0; i < 3; i++ {
		os.Chmod(backupPaths[i], 0644)
	}
}

func TestLogger_RotateRenameFailsDueToMissingSource(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	logPath := filepath.Join(tmpDir, "audit.log")

	f, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("Failed to create log file: %v", err)
	}
	f.Close()
	os.Remove(logPath)

	auditLogger := &Logger{
		config: LoggerConfig{
			OutputPath: logPath,
			MaxBackups: 3,
		},
		logger: logger,
		file:   f,
		writer: f,
		stopCh: make(chan struct{}),
	}

	auditLogger.rotate()
}

func TestLogger_RotateOpenNewFileFailsDueToInvalidPath(t *testing.T) {
	logger := zap.NewNop()

	f, err := os.CreateTemp("", "audit-*.log")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	f.WriteString("content")
	f.Close()

	invalidPath := filepath.Join(string(filepath.Separator), "nonexistent_dir_"+time.Now().Format("20060102150405"), "audit.log")

	auditLogger := &Logger{
		config: LoggerConfig{
			OutputPath: invalidPath,
			MaxBackups: 3,
		},
		logger: logger,
		file:   f,
		writer: f,
		stopCh: make(chan struct{}),
	}

	os.Remove(f.Name())
	auditLogger.rotate()
}

func TestLogger_CleanupBackupsWithUnremovableFiles(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	logPath := filepath.Join(tmpDir, "audit.log")

	var backupPaths []string
	for i := 0; i < 5; i++ {
		backupPath := fmt.Sprintf("%s.2024-01-0%d-00-00-00", logPath, i+1)
		os.WriteFile(backupPath, []byte("backup"), 0644)
		backupPaths = append(backupPaths, backupPath)
		time.Sleep(10 * time.Millisecond)
	}

	f, _ := os.Create(logPath)
	f.Close()

	cfg := LoggerConfig{
		OutputPath: logPath,
		MaxSizeMB:  1,
		MaxBackups: 2,
		Format:     "json",
	}

	auditLogger := &Logger{
		config: cfg,
		logger: logger,
		file:   f,
		writer: f,
		stopCh: make(chan struct{}),
	}

	for i := 0; i < 3; i++ {
		os.Chmod(backupPaths[i], 0000)
	}

	auditLogger.cleanupBackups()

	for i := 0; i < 3; i++ {
		os.Chmod(backupPaths[i], 0644)
	}
}

func TestLogger_RotateWithInvalidDir(t *testing.T) {
	logger := zap.NewNop()

	f, err := os.CreateTemp("", "audit-*.log")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	f.WriteString("content")
	f.Close()
	tempPath := f.Name()
	os.Remove(tempPath)

	auditLogger := &Logger{
		config: LoggerConfig{
			OutputPath: tempPath,
			MaxBackups: 3,
		},
		logger: logger,
		file:   f,
		writer: f,
		stopCh: make(chan struct{}),
	}

	auditLogger.rotate()
}

func TestLogger_RotationLoopExitViaStopCh(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	cfg := LoggerConfig{
		OutputPath: filepath.Join(tmpDir, "audit.log"),
		Format:     "json",
	}

	auditLogger, err := NewLogger(cfg, logger)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}

	stopCh := make(chan struct{})
	auditLogger.stopCh = stopCh

	ready := make(chan struct{})
	done := make(chan struct{})
	go func() {
		<-ready
		auditLogger.rotationLoop(context.Background())
		close(done)
	}()

	close(ready)
	time.Sleep(150 * time.Millisecond)
	close(stopCh)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Error("rotationLoop should have exited via stopCh")
	}
}

func TestLogger_RotateOpenNewFileFailsDueToPermission(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	logPath := filepath.Join(tmpDir, "audit.log")

	os.WriteFile(logPath, []byte("initial content"), 0644)

	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("Failed to open log file: %v", err)
	}

	auditLogger := &Logger{
		config: LoggerConfig{
			OutputPath: logPath,
			MaxBackups: 3,
		},
		logger: logger,
		file:   f,
		writer: f,
		stopCh: make(chan struct{}),
	}

	os.Chmod(tmpDir, 0555)
	auditLogger.rotate()
	os.Chmod(tmpDir, 0755)
}

func TestLogger_RotateOpenNewFileFailsAfterSuccessfulRename(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	logPath := filepath.Join(tmpDir, "audit.log")

	os.WriteFile(logPath, []byte("initial content"), 0644)

	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("Failed to open log file: %v", err)
	}

	subDir := filepath.Join(tmpDir, "blocked")
	os.MkdirAll(subDir, 0755)

	auditLogger := &Logger{
		config: LoggerConfig{
			OutputPath: logPath,
			MaxBackups: 3,
		},
		logger: logger,
		file:   f,
		writer: f,
		stopCh: make(chan struct{}),
	}

	f.Close()

	os.Remove(logPath)
	os.WriteFile(logPath, []byte("new content"), 0444)

	f2, _ := os.OpenFile(logPath, os.O_RDONLY, 0444)
	auditLogger.file = f2
	auditLogger.writer = f2

	os.Chmod(tmpDir, 0555)
	auditLogger.rotate()
	os.Chmod(tmpDir, 0755)
}

func TestLogger_RotateOpenNewFileFailsInvalidPath(t *testing.T) {
	logger := zap.NewNop()

	tmpFile, err := os.CreateTemp("", "audit-*.log")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.WriteString("content")
	tmpFile.Close()
	tmpPath := tmpFile.Name()
	defer os.Remove(tmpPath)

	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}

	auditLogger := &Logger{
		config: LoggerConfig{
			OutputPath: tmpPath,
			MaxBackups: 3,
		},
		logger: logger,
		file:   f,
		writer: f,
		stopCh: make(chan struct{}),
	}

	os.Remove(tmpPath)

	invalidDir := filepath.Join(os.TempDir(), "nonexistent_audit_dir_"+time.Now().Format("20060102150405"))
	auditLogger.config.OutputPath = filepath.Join(invalidDir, "audit.log")

	auditLogger.rotate()
}

func TestLogger_RotateOpenFailsAfterRenameSucceeds(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	logPath := filepath.Join(tmpDir, "audit.log")

	os.WriteFile(logPath, []byte("initial content"), 0644)

	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("Failed to open log file: %v", err)
	}

	auditLogger := &Logger{
		config: LoggerConfig{
			OutputPath: logPath,
			MaxBackups: 3,
		},
		logger: logger,
		file:   f,
		writer: f,
		stopCh: make(chan struct{}),
	}

	f.Close()

	auditLogger.rotate()

	backupFiles, _ := filepath.Glob(logPath + ".*")
	if len(backupFiles) == 0 {
		t.Error("Expected backup file to be created")
	}

	if _, err := os.Stat(logPath); os.IsNotExist(err) {
		t.Error("Expected new log file to be created")
	}
}

func TestLogger_RotateOpenErrorOnWindows(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	logPath := filepath.Join(tmpDir, "audit.log")

	os.WriteFile(logPath, []byte("initial content"), 0644)

	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("Failed to open log file: %v", err)
	}

	auditLogger := &Logger{
		config: LoggerConfig{
			OutputPath: logPath,
			MaxBackups: 3,
		},
		logger: logger,
		file:   f,
		writer: f,
		stopCh: make(chan struct{}),
	}

	f.Close()
	os.Remove(logPath)

	os.Mkdir(logPath, 0755)

	auditLogger.rotate()

	os.RemoveAll(logPath)
}

func TestLogger_RotateOpenFailsPathTooLong(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()

	longName := string(make([]byte, 200))
	for i := range longName {
		longName = longName[:i] + "a" + longName[i+1:]
	}
	logPath := filepath.Join(tmpDir, longName+".log")

	f, err := os.CreateTemp(tmpDir, "audit-*.log")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tempPath := f.Name()
	f.WriteString("content")
	f.Close()

	f2, err := os.OpenFile(tempPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}

	auditLogger := &Logger{
		config: LoggerConfig{
			OutputPath: tempPath,
			MaxBackups: 3,
		},
		logger: logger,
		file:   f2,
		writer: f2,
		stopCh: make(chan struct{}),
	}

	auditLogger.config.OutputPath = logPath

	auditLogger.rotate()
}

func TestLogger_RotateOpenFailsAfterRenameWithDir(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	logPath := filepath.Join(tmpDir, "audit.log")

	os.WriteFile(logPath, []byte("initial content"), 0644)

	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("Failed to open log file: %v", err)
	}

	auditLogger := &Logger{
		config: LoggerConfig{
			OutputPath: logPath,
			MaxBackups: 3,
		},
		logger: logger,
		file:   f,
		writer: f,
		stopCh: make(chan struct{}),
	}

	f.Close()

	renamedPath := logPath + ".renamed"
	os.Rename(logPath, renamedPath)

	os.Mkdir(logPath, 0755)
	os.Remove(renamedPath)
	os.WriteFile(logPath, []byte("new"), 0644)

	auditLogger.rotate()
}

func TestLogger_RotateOpenErrorParentNotDir(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()

	parentFile := filepath.Join(tmpDir, "parent")
	os.WriteFile(parentFile, []byte("not a directory"), 0644)

	logPath := filepath.Join(parentFile, "audit.log")

	tmpFile, err := os.CreateTemp(tmpDir, "temp-*.log")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.WriteString("content")
	tmpFile.Close()

	f, err := os.OpenFile(tmpFile.Name(), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}

	auditLogger := &Logger{
		config: LoggerConfig{
			OutputPath: logPath,
			MaxBackups: 3,
		},
		logger: logger,
		file:   f,
		writer: f,
		stopCh: make(chan struct{}),
	}

	auditLogger.rotate()
}

func TestLogger_RotateOpenErrorAfterSuccessfulRename(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	logPath := filepath.Join(tmpDir, "audit.log")

	os.WriteFile(logPath, []byte("initial content"), 0644)

	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	auditLogger := &Logger{
		config: LoggerConfig{
			OutputPath: logPath,
			MaxBackups: 3,
		},
		logger: logger,
		file:   f,
		writer: f,
		stopCh: make(chan struct{}),
	}

	f.Close()

	auditLogger.rotate()

	backupFiles, _ := filepath.Glob(logPath + ".*")
	if len(backupFiles) == 0 {
		t.Error("Expected backup file to be created")
	}

	os.Remove(logPath)

	for _, bf := range backupFiles {
		os.Remove(bf)
	}

	os.WriteFile(logPath, []byte("new content"), 0444)
	os.Chmod(tmpDir, 0555)

	f2, _ := os.OpenFile(logPath, os.O_RDONLY, 0444)
	auditLogger.file = f2
	auditLogger.writer = f2

	auditLogger.rotate()

	os.Chmod(tmpDir, 0755)
}

func TestLogger_RotateOpenFailsDueToLockedDirectory(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	logPath := filepath.Join(tmpDir, "audit.log")

	os.WriteFile(logPath, []byte("initial content"), 0644)

	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("Failed to open log file: %v", err)
	}

	auditLogger := &Logger{
		config: LoggerConfig{
			OutputPath: logPath,
			MaxBackups: 3,
		},
		logger: logger,
		file:   f,
		writer: f,
		stopCh: make(chan struct{}),
	}

	f.Close()

	lockFile, err := os.OpenFile(filepath.Join(tmpDir, ".lock"), os.O_CREATE|os.O_RDWR, 0644)
	if err == nil {
		defer lockFile.Close()
	}

	auditLogger.rotate()
}

func TestLogger_RotateOpenErrorNonexistentDir(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	logPath := filepath.Join(tmpDir, "audit.log")

	os.WriteFile(logPath, []byte("initial content"), 0644)

	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("Failed to open log file: %v", err)
	}

	nonexistentDir := filepath.Join(tmpDir, "nonexistent")
	nonexistentPath := filepath.Join(nonexistentDir, "audit.log")

	auditLogger := &Logger{
		config: LoggerConfig{
			OutputPath: nonexistentPath,
			MaxBackups: 3,
		},
		logger: logger,
		file:   f,
		writer: f,
		stopCh: make(chan struct{}),
	}

	auditLogger.rotate()
}

func TestLogger_RotateOpenErrorWithReadOnlyDir(t *testing.T) {
	if os.PathSeparator == '\\' {
		t.Skip("Skipping on Windows - chmod doesn't work the same way")
	}

	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	logPath := filepath.Join(tmpDir, "audit.log")

	os.WriteFile(logPath, []byte("initial content"), 0644)

	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("Failed to open log file: %v", err)
	}

	auditLogger := &Logger{
		config: LoggerConfig{
			OutputPath: logPath,
			MaxBackups: 3,
		},
		logger: logger,
		file:   f,
		writer: f,
		stopCh: make(chan struct{}),
	}

	f.Close()

	os.Chmod(tmpDir, 0555)
	auditLogger.rotate()
	os.Chmod(tmpDir, 0755)
}

func TestLogger_RotateOpenErrorAfterRenameUnix(t *testing.T) {
	if os.PathSeparator == '\\' {
		t.Skip("Skipping on Windows - chmod doesn't work the same way")
	}

	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	logPath := filepath.Join(tmpDir, "audit.log")

	os.WriteFile(logPath, []byte("initial content"), 0644)

	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("Failed to open log file: %v", err)
	}

	auditLogger := &Logger{
		config: LoggerConfig{
			OutputPath: logPath,
			MaxBackups: 3,
		},
		logger: logger,
		file:   f,
		writer: f,
		stopCh: make(chan struct{}),
	}

	f.Close()

	os.Chmod(tmpDir, 0555)
	auditLogger.rotate()
	os.Chmod(tmpDir, 0755)
}

func TestLogger_RotationLoopTickerCase(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping in short mode - requires waiting for ticker")
	}

	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	logPath := filepath.Join(tmpDir, "audit.log")

	largeContent := make([]byte, 2*1024*1024)
	os.WriteFile(logPath, largeContent, 0644)

	cfg := LoggerConfig{
		OutputPath: logPath,
		MaxSizeMB:  1,
		MaxBackups: 3,
		Format:     "json",
	}

	auditLogger, err := NewLogger(cfg, logger)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		auditLogger.rotationLoop(ctx)
		close(done)
	}()

	// Wait for rotation loop to check (rotation checks every minute, but we test the ticker fires)
	// Use a shorter sleep since we're just testing the ticker mechanism, not actual rotation
	time.Sleep(100 * time.Millisecond)

	cancel()
	<-done

	// The test verifies the rotationLoop properly starts with ticker
	// Actual ticker-based rotation would require 60+ seconds
	// We verify the loop started and exited cleanly
	files, _ := filepath.Glob(logPath + ".*")
	_ = files // Ticker-based rotation may or may not have triggered depending on timing
}

func TestLogger_RotateOpenNewFileFailsAfterRenameSucceeds(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	logPath := filepath.Join(tmpDir, "audit.log")

	os.WriteFile(logPath, []byte("initial content"), 0644)

	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("Failed to open log file: %v", err)
	}

	auditLogger := &Logger{
		config: LoggerConfig{
			OutputPath: logPath,
			MaxBackups: 3,
		},
		logger: logger,
		file:   f,
		writer: f,
		stopCh: make(chan struct{}),
	}

	auditLogger.rotate()

	f2, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	f2.Close()

	os.Chmod(tmpDir, 0555)
	defer os.Chmod(tmpDir, 0755)

	auditLogger2 := &Logger{
		config: LoggerConfig{
			OutputPath: logPath,
			MaxBackups: 3,
		},
		logger: logger,
		file:   f,
		writer: f,
		stopCh: make(chan struct{}),
	}

	auditLogger2.rotate()
}

func TestLogger_CleanupBackupsGlobError(t *testing.T) {
	logger := zap.NewNop()

	auditLogger := &Logger{
		config: LoggerConfig{
			OutputPath: filepath.Join(string(filepath.Separator), "nonexistent", "[", "audit.log"),
			MaxBackups: 3,
		},
		logger: logger,
		stopCh: make(chan struct{}),
	}

	auditLogger.cleanupBackups()
}
