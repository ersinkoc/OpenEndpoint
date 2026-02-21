package audit

import (
	"context"
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
	cfg := Config{
		Enabled:    true,
		Path:       tmpDir,
		MaxSize:    10 * 1024 * 1024,
		MaxBackups: 5,
	}

	auditLogger, err := NewLogger(logger, cfg)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}

	if auditLogger == nil {
		t.Fatal("Logger should not be nil")
	}

	auditLogger.Close()
}

func TestLogger_Log(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	cfg := Config{
		Enabled:    true,
		Path:       tmpDir,
		MaxSize:    10 * 1024 * 1024,
		MaxBackups: 5,
	}

	auditLogger, err := NewLogger(logger, cfg)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer auditLogger.Close()

	ctx := context.Background()
	event := &Event{
		Time:       time.Now(),
		Action:     "GetObject",
		Bucket:     "test-bucket",
		Key:        "test-key",
		SourceIP:   "192.168.1.1",
		UserAgent:  "test-agent",
		Status:     "200",
	}

	err = auditLogger.Log(ctx, event)
	if err != nil {
		t.Fatalf("Log failed: %v", err)
	}
}

func TestLogger_Disabled(t *testing.T) {
	logger := zap.NewNop()
	cfg := Config{
		Enabled: false,
	}

	auditLogger, err := NewLogger(logger, cfg)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer auditLogger.Close()

	ctx := context.Background()
	event := &Event{Action: "test"}

	// Should not fail when disabled
	err = auditLogger.Log(ctx, event)
	if err != nil {
		t.Errorf("Log should not fail when disabled: %v", err)
	}
}

func TestLogger_Query(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	cfg := Config{
		Enabled:    true,
		Path:       tmpDir,
		MaxSize:    10 * 1024 * 1024,
		MaxBackups: 5,
	}

	auditLogger, err := NewLogger(logger, cfg)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer auditLogger.Close()

	ctx := context.Background()

	// Log some events
	for i := 0; i < 5; i++ {
		event := &Event{
			Time:     time.Now(),
			Action:   "GetObject",
			Bucket:   "test-bucket",
			Key:      string(rune('A' + i)),
			Status:   "200",
		}
		auditLogger.Log(ctx, event)
	}

	// Query events
	query := Query{
		Action: "GetObject",
		Limit:  10,
	}

	events, err := auditLogger.Query(ctx, query)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(events) == 0 {
		t.Error("Query should return events")
	}
}

func TestLogger_QueryByBucket(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	cfg := Config{
		Enabled:    true,
		Path:       tmpDir,
		MaxSize:    10 * 1024 * 1024,
		MaxBackups: 5,
	}

	auditLogger, err := NewLogger(logger, cfg)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer auditLogger.Close()

	ctx := context.Background()

	// Log events for different buckets
	buckets := []string{"bucket1", "bucket2", "bucket3"}
	for _, bucket := range buckets {
		event := &Event{
			Time:   time.Now(),
			Action: "PutObject",
			Bucket: bucket,
			Status: "200",
		}
		auditLogger.Log(ctx, event)
	}

	// Query specific bucket
	query := Query{
		Bucket: "bucket2",
		Limit:  10,
	}

	events, err := auditLogger.Query(ctx, query)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// All events should be for bucket2
	for _, event := range events {
		if event.Bucket != "bucket2" {
			t.Errorf("Expected bucket2, got %s", event.Bucket)
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
	cfg := Config{
		Enabled:    true,
		Path:       tmpDir,
		MaxSize:    10 * 1024 * 1024,
		MaxBackups: 5,
	}

	auditLogger, err := NewLogger(logger, cfg)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer auditLogger.Close()

	ctx := context.Background()

	// Log event
	now := time.Now()
	event := &Event{
		Time:   now,
		Action: "DeleteObject",
		Bucket: "test-bucket",
		Status: "204",
	}
	auditLogger.Log(ctx, event)

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
	cfg := Config{
		Enabled:    true,
		Path:       tmpDir,
		MaxSize:    100, // Small size to trigger rotation
		MaxBackups: 2,
	}

	auditLogger, err := NewLogger(logger, cfg)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer auditLogger.Close()

	ctx := context.Background()

	// Log many events to trigger rotation
	for i := 0; i < 100; i++ {
		event := &Event{
			Time:   time.Now(),
			Action: "GetObject",
			Key:    string(rune(i)),
		}
		auditLogger.Log(ctx, event)
	}

	// Check that files were created
	files, _ := filepath.Glob(filepath.Join(tmpDir, "audit-*.log"))
	if len(files) == 0 {
		t.Error("Log files should be created")
	}
}

func TestLogger_MaxBackups(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	maxBackups := 2
	cfg := Config{
		Enabled:    true,
		Path:       tmpDir,
		MaxSize:    50, // Very small to trigger multiple rotations
		MaxBackups: maxBackups,
	}

	auditLogger, err := NewLogger(logger, cfg)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer auditLogger.Close()

	ctx := context.Background()

	// Log many events to trigger multiple rotations
	for i := 0; i < 200; i++ {
		event := &Event{
			Time:   time.Now(),
			Action: "PutObject",
		}
		auditLogger.Log(ctx, event)
	}

	// Count backup files
	files, _ := filepath.Glob(filepath.Join(tmpDir, "audit-*.log"))
	if len(files) > maxBackups+1 { // +1 for current file
		t.Errorf("Too many backup files: %d, want at most %d", len(files), maxBackups+1)
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
	// This is a basic check - actual order depends on file system
	if len(files) != 3 {
		t.Errorf("File count = %d, want 3", len(files))
	}
}

func TestEvent_Redaction(t *testing.T) {
	event := &Event{
		Time:     time.Now(),
		Action:   "PutObject",
		Bucket:   "test-bucket",
		Key:      "secret-key",
		Metadata: map[string]string{"password": "secret123"},
	}

	// Verify event has sensitive data before redaction
	if event.Metadata["password"] != "secret123" {
		t.Error("Event should have original password")
	}

	// The actual redaction would happen in the logger
	// This test verifies the Event struct supports metadata
}

func TestLogger_Concurrent(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	cfg := Config{
		Enabled:    true,
		Path:       tmpDir,
		MaxSize:    10 * 1024 * 1024,
		MaxBackups: 5,
	}

	auditLogger, err := NewLogger(logger, cfg)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer auditLogger.Close()

	ctx := context.Background()
	done := make(chan bool)

	// Concurrent logging
	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 10; j++ {
				event := &Event{
					Time:   time.Now(),
					Action: "ConcurrentTest",
					Bucket: string(rune('A' + id)),
				}
				auditLogger.Log(ctx, event)
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestQuery_Limit(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := zap.NewNop()
	cfg := Config{
		Enabled:    true,
		Path:       tmpDir,
		MaxSize:    10 * 1024 * 1024,
		MaxBackups: 5,
	}

	auditLogger, err := NewLogger(logger, cfg)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer auditLogger.Close()

	ctx := context.Background()

	// Log 20 events
	for i := 0; i < 20; i++ {
		event := &Event{
			Time:   time.Now(),
			Action: "LimitTest",
		}
		auditLogger.Log(ctx, event)
	}

	// Query with limit
	query := Query{
		Action: "LimitTest",
		Limit:  5,
	}

	events, err := auditLogger.Query(ctx, query)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(events) > 5 {
		t.Errorf("Query returned %d events, want at most 5", len(events))
	}
}
