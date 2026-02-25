package logging

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoggerLevels(t *testing.T) {
	tests := []struct {
		level   Level
		wantStr string
	}{
		{DebugLevel, "DEBUG"},
		{InfoLevel, "INFO"},
		{WarnLevel, "WARN"},
		{ErrorLevel, "ERROR"},
		{PanicLevel, "PANIC"},
		{FatalLevel, "FATAL"},
	}

	for _, tt := range tests {
		t.Run(tt.wantStr, func(t *testing.T) {
			if tt.level.String() != tt.wantStr {
				t.Errorf("Level.String() = %v, want %v", tt.level.String(), tt.wantStr)
			}
		})
	}
}

func TestLoggerDefaultLevel(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := New(buf, InfoLevel)

	// Debug should not be logged
	logger.Debug("debug message")
	if buf.Len() != 0 {
		t.Error("Debug should not be logged when level is InfoLevel")
	}

	// Info should be logged
	logger.Info("info message")
	if !strings.Contains(buf.String(), "info message") {
		t.Error("Info should be logged")
	}
}

func TestLoggerLevelFiltering(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := New(buf, ErrorLevel)

	logger.Debug("debug")
	logger.Info("info")
	logger.Warn("warn")
	logger.Error("error")

	output := buf.String()

	if strings.Contains(output, "debug") {
		t.Error("Debug should be filtered")
	}
	if strings.Contains(output, "info") {
		t.Error("Info should be filtered")
	}
	if strings.Contains(output, "warn") {
		t.Error("Warn should be filtered")
	}
	if !strings.Contains(output, "error") {
		t.Error("Error should be logged")
	}
}

func TestLoggerSetLevel(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := New(buf, ErrorLevel)

	// Initially, Info should be filtered
	logger.Info("info message")
	if buf.Len() != 0 {
		t.Error("Info should be filtered at ErrorLevel")
	}

	// Change level to Info
	logger.SetLevel(InfoLevel)
	logger.Info("info message")

	if !strings.Contains(buf.String(), "info message") {
		t.Error("Info should be logged after level change")
	}
}

func TestLoggerFormat(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := New(buf, DebugLevel)
	logger.format = "[%s] %s: %s\n"

	logger.Info("test message")

	output := buf.String()
	if !strings.Contains(output, "INFO") {
		t.Errorf("Output should contain INFO, got: %s", output)
	}
	if !strings.Contains(output, "test message") {
		t.Errorf("Output should contain test message, got: %s", output)
	}
}

func TestLoggerMethods(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := New(buf, DebugLevel)

	logger.Debug("debug")
	logger.Debugf("debug %s", "formatted")
	logger.Info("info")
	logger.Infof("info %s", "formatted")
	logger.Warn("warn")
	logger.Warnf("warn %s", "formatted")
	logger.Error("error")
	logger.Errorf("error %s", "formatted")

	output := buf.String()

	if !strings.Contains(output, "DEBUG") {
		t.Error("Debug should be logged")
	}
	if !strings.Contains(output, "INFO") {
		t.Error("Info should be logged")
	}
	if !strings.Contains(output, "WARN") {
		t.Error("Warn should be logged")
	}
	if !strings.Contains(output, "ERROR") {
		t.Error("Error should be logged")
	}
	if !strings.Contains(output, "formatted") {
		t.Error("Formatted messages should be logged")
	}
}

func TestLoggerConcurrent(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := New(buf, DebugLevel)

	done := make(chan bool)

	go func() {
		for i := 0; i < 100; i++ {
			logger.Info("message from goroutine 1")
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 100; i++ {
			logger.Info("message from goroutine 2")
		}
		done <- true
	}()

	<-done
	<-done

	output := buf.String()
	count := strings.Count(output, "message from goroutine")
	if count != 200 {
		t.Errorf("Expected 200 messages, got %d", count)
	}
}

func TestLoggerWithTimeLocation(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := New(buf, DebugLevel)
	logger.WithTimeLocation(nil) // Should not panic

	_ = buf.String() // Use the buffer
}

func TestLoggerWithFormat(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := New(buf, DebugLevel)
	logger.WithFormat("%s - %s\n")

	logger.Info("test")

	_ = buf.String() // Use the buffer
}

func TestGlobalLogger(t *testing.T) {
	// Save original global logger
	orig := Global

	// Replace with test logger
	buf := &bytes.Buffer{}
	Global = New(buf, DebugLevel)

	Info("test info message")
	if !strings.Contains(buf.String(), "test info message") {
		t.Error("Global Info should log message")
	}

	// Restore
	Global = orig
}

func TestNewFile(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := tmpDir + "/test.log"

	logger, err := NewFile(logFile, DebugLevel)
	if err != nil {
		t.Fatalf("NewFile() error = %v", err)
	}
	defer logger.Close()

	logger.Info("test message")

	// Read the file to verify
	content, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}

	if !strings.Contains(string(content), "test message") {
		t.Error("Log file should contain the message")
	}
}

func TestNewStd(t *testing.T) {
	logger := NewStd(InfoLevel)
	if logger == nil {
		t.Error("NewStd should return a logger")
	}
}

func TestLoggerClose(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := New(buf, DebugLevel)

	// Closing a non-file logger should not panic
	err := logger.Close()
	if err != nil {
		t.Errorf("Close() error = %v", err)
	}
}

func TestLevelUnknown(t *testing.T) {
	level := Level(999)
	if level.String() != "UNKNOWN" {
		t.Errorf("Unknown level should return UNKNOWN, got %s", level.String())
	}
}

func TestLoggerPanic(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := New(buf, DebugLevel)

	defer func() {
		if r := recover(); r == nil {
			t.Error("Panic should have panicked")
		}
		if !strings.Contains(buf.String(), "panic message") {
			t.Error("Panic message should be logged")
		}
	}()

	logger.Panic("panic message")
}

func TestLoggerPanicf(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := New(buf, DebugLevel)

	defer func() {
		if r := recover(); r == nil {
			t.Error("Panicf should have panicked")
		}
		if !strings.Contains(buf.String(), "panic formatted") {
			t.Error("Panicf message should be logged")
		}
	}()

	logger.Panicf("panic %s", "formatted")
}

func TestLoggerFatal(t *testing.T) {
	if os.Getenv("TEST_FATAL") == "1" {
		buf := &bytes.Buffer{}
		logger := New(buf, DebugLevel)
		logger.Fatal("fatal message")
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestLoggerFatal")
	cmd.Env = append(os.Environ(), "TEST_FATAL=1")
	err := cmd.Run()
	if e, ok := err.(*exec.ExitError); ok && !e.Success() {
		return
	}
	t.Fatalf("Fatal should have exited with status 1, got %v", err)
}

func TestLoggerFatalf(t *testing.T) {
	if os.Getenv("TEST_FATALF") == "1" {
		buf := &bytes.Buffer{}
		logger := New(buf, DebugLevel)
		logger.Fatalf("fatal %s", "formatted")
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestLoggerFatalf")
	cmd.Env = append(os.Environ(), "TEST_FATALF=1")
	err := cmd.Run()
	if e, ok := err.(*exec.ExitError); ok && !e.Success() {
		return
	}
	t.Fatalf("Fatalf should have exited with status 1, got %v", err)
}

func TestLoggerRotateNonFile(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := New(buf, DebugLevel)

	err := logger.Rotate()
	if err != nil {
		t.Errorf("Rotate on non-file should return nil, got %v", err)
	}
}

func TestLoggerRotateFile(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := tmpDir + "/rotate.log"

	logger, err := NewFile(logFile, DebugLevel)
	if err != nil {
		t.Fatalf("NewFile() error = %v", err)
	}

	logger.Info("before rotate")
	logger.Close()

	filesBefore, _ := os.ReadDir(tmpDir)
	if len(filesBefore) != 1 {
		t.Fatalf("Expected 1 file before test, got %d", len(filesBefore))
	}
}

func TestLoggerRotateClosedFile(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := tmpDir + "/closed.log"

	logger, err := NewFile(logFile, DebugLevel)
	if err != nil {
		t.Fatalf("NewFile() error = %v", err)
	}

	logger.Close()

	err = logger.Rotate()
	if err == nil {
		t.Error("Rotate on closed file should fail")
	}
}

func TestGlobalDebugFunctions(t *testing.T) {
	orig := Global
	buf := &bytes.Buffer{}
	Global = New(buf, DebugLevel)
	defer func() { Global = orig }()

	Debug("global debug")
	Debugf("global debug %s", "formatted")

	if !strings.Contains(buf.String(), "global debug") {
		t.Error("Global Debug should log")
	}
}

func TestGlobalWarnFunctions(t *testing.T) {
	orig := Global
	buf := &bytes.Buffer{}
	Global = New(buf, DebugLevel)
	defer func() { Global = orig }()

	Warn("global warn")
	Warnf("global warn %s", "formatted")

	if !strings.Contains(buf.String(), "global warn") {
		t.Error("Global Warn should log")
	}
}

func TestGlobalErrorFunctions(t *testing.T) {
	orig := Global
	buf := &bytes.Buffer{}
	Global = New(buf, DebugLevel)
	defer func() { Global = orig }()

	Error("global error")
	Errorf("global error %s", "formatted")

	if !strings.Contains(buf.String(), "global error") {
		t.Error("Global Error should log")
	}
}

func TestGlobalSetLevel(t *testing.T) {
	orig := Global
	buf := &bytes.Buffer{}
	Global = New(buf, DebugLevel)
	defer func() { Global = orig }()

	SetLevel(ErrorLevel)
	Debug("should not log")

	if strings.Contains(buf.String(), "should not log") {
		t.Error("Debug should not log after SetLevel(ErrorLevel)")
	}
}

func TestNewFileDirectoryCreation(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := tmpDir + "/subdir/deep/nested/test.log"

	logger, err := NewFile(logFile, DebugLevel)
	if err != nil {
		t.Fatalf("NewFile() should create directories, got error: %v", err)
	}
	defer logger.Close()

	if _, err := os.Stat(logFile); os.IsNotExist(err) {
		t.Error("Log file should be created")
	}
}

func TestNewFileInvalidPath(t *testing.T) {
	logger, err := NewFile("/nonexistent\x00path/test.log", DebugLevel)
	if err == nil {
		logger.Close()
		t.Error("NewFile should fail for invalid path with null byte")
	}
}

func TestLoggerRotateSuccessDI(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "success-di.log")
	mockFile := filepath.Join(tmpDir, "mock.log")

	logger, err := NewFile(logFile, DebugLevel)
	if err != nil {
		t.Fatalf("NewFile() error = %v", err)
	}

	logger.Info("before")

	origFile := logger.output.(*os.File)
	mockOutFile, _ := os.OpenFile(mockFile, os.O_CREATE|os.O_WRONLY, 0644)

	origRename := osRename
	origOpenFile := osOpenFile
	osRename = func(oldpath, newpath string) error {
		return nil
	}
	osOpenFile = func(name string, flag int, perm os.FileMode) (*os.File, error) {
		return mockOutFile, nil
	}
	defer func() {
		osRename = origRename
		osOpenFile = origOpenFile
	}()

	err = logger.Rotate()
	if err != nil {
		t.Errorf("Rotate() error = %v", err)
	}

	logger.Info("after")

	origFile.Close()
	mockOutFile.Close()
}

func TestGlobalInfofFunction(t *testing.T) {
	orig := Global
	buf := &bytes.Buffer{}
	Global = New(buf, DebugLevel)
	defer func() { Global = orig }()

	Infof("info %s", "formatted")

	if !strings.Contains(buf.String(), "info formatted") {
		t.Error("Global Infof should log")
	}
}

func TestLoggerWithTimeLocationSet(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := New(buf, DebugLevel)
	logger.WithTimeLocation(time.UTC)

	logger.Info("test")

	if !strings.Contains(buf.String(), "test") {
		t.Error("Should log with time location")
	}
}

func TestLoggerWriteAfterRotate(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := tmpDir + "/write-rotate.log"

	logger, err := NewFile(logFile, DebugLevel)
	if err != nil {
		t.Fatalf("NewFile() error = %v", err)
	}

	logger.Info("message 1")
	logger.Rotate()
	logger.Info("message 2")
	logger.Close()

	content, _ := os.ReadFile(logFile)
	if !strings.Contains(string(content), "message 2") {
		t.Error("Should contain message 2 after rotate")
	}
}

func TestLoggerFilterByLevel(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := New(buf, WarnLevel)

	logger.Debug("debug")
	logger.Info("info")
	logger.Warn("warn")
	logger.Error("error")

	output := buf.String()

	if strings.Contains(output, "debug") {
		t.Error("Debug should be filtered at WarnLevel")
	}
	if strings.Contains(output, "info") {
		t.Error("Info should be filtered at WarnLevel")
	}
	if !strings.Contains(output, "warn") {
		t.Error("Warn should be logged")
	}
	if !strings.Contains(output, "error") {
		t.Error("Error should be logged")
	}
}

func TestGlobalPanicFunctions(t *testing.T) {
	orig := Global
	buf := &bytes.Buffer{}
	Global = New(buf, DebugLevel)
	defer func() {
		Global = orig
		if r := recover(); r == nil {
			t.Error("Global Panic should panic")
		}
	}()

	Panic("global panic")
}

func TestGlobalPanicfFunctions(t *testing.T) {
	orig := Global
	buf := &bytes.Buffer{}
	Global = New(buf, DebugLevel)
	defer func() {
		Global = orig
		if r := recover(); r == nil {
			t.Error("Global Panicf should panic")
		}
	}()

	Panicf("global panic %s", "formatted")
}

func TestLoggerMultipleClose(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := New(buf, DebugLevel)

	logger.Close()
	logger.Close()
}

func TestNewFileOpenFileError(t *testing.T) {
	tmpDir := t.TempDir()
	existingFile := tmpDir + "/blocked.log"
	os.WriteFile(existingFile, []byte("test"), 0644)
	os.Chmod(existingFile, 0000)
	defer os.Chmod(existingFile, 0644)

	logger, err := NewFile(existingFile, DebugLevel)
	if err == nil {
		logger.Close()
		t.Error("NewFile should fail when file has no permissions")
	}
}

func TestLoggerRotateRenameError(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := tmpDir + "/rename.log"

	logger, err := NewFile(logFile, DebugLevel)
	if err != nil {
		t.Fatalf("NewFile() error = %v", err)
	}

	logger.Info("test")
	logger.Close()

	anotherLogger, _ := NewFile(logFile, DebugLevel)
	defer anotherLogger.Close()

	logger.output = anotherLogger.output

	err = logger.Rotate()
	if err == nil {
		t.Error("Rotate should fail when file stat fails on different file handle")
	}
}

func TestLoggerRotateOpenFileError(t *testing.T) {
	if os.Getenv("GOOS") == "windows" {
		t.Skip("Skipping on Windows - permission handling differs")
	}
	tmpDir := t.TempDir()
	logFile := tmpDir + "/openfail.log"

	logger, err := NewFile(logFile, DebugLevel)
	if err != nil {
		t.Fatalf("NewFile() error = %v", err)
	}
	defer logger.Close()

	logger.Info("before")

	os.Chmod(tmpDir, 0000)
	defer os.Chmod(tmpDir, 0755)

	err = logger.Rotate()
	if err == nil {
		t.Error("Rotate should fail when cannot open new file")
	}
}

func TestLoggerRotateRenameErrorDI(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := tmpDir + "/rename-di.log"

	logger, err := NewFile(logFile, DebugLevel)
	if err != nil {
		t.Fatalf("NewFile() error = %v", err)
	}
	defer logger.Close()

	logger.Info("before")

	origRename := osRename
	osRename = func(oldpath, newpath string) error {
		return fmt.Errorf("mock rename error")
	}
	defer func() { osRename = origRename }()

	err = logger.Rotate()
	if err == nil {
		t.Error("Rotate should fail when rename fails")
	}
	if !strings.Contains(err.Error(), "mock rename error") {
		t.Errorf("Expected mock rename error, got: %v", err)
	}
}

func TestLoggerRotateOpenFileErrorDI(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := tmpDir + "/openfail-di.log"

	logger, err := NewFile(logFile, DebugLevel)
	if err != nil {
		t.Fatalf("NewFile() error = %v", err)
	}
	defer logger.Close()

	logger.Info("before")

	origRename := osRename
	origOpenFile := osOpenFile

	osRename = func(oldpath, newpath string) error {
		return nil
	}
	osOpenFile = func(name string, flag int, perm os.FileMode) (*os.File, error) {
		return nil, fmt.Errorf("mock openfile error")
	}
	defer func() {
		osRename = origRename
		osOpenFile = origOpenFile
	}()

	err = logger.Rotate()
	if err == nil {
		t.Error("Rotate should fail when openfile fails")
	}
	if !strings.Contains(err.Error(), "mock openfile error") {
		t.Errorf("Expected mock openfile error, got: %v", err)
	}
}

func TestLoggerRotateStatErrorDI(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := tmpDir + "/stat-error.log"

	logger, err := NewFile(logFile, DebugLevel)
	if err != nil {
		t.Fatalf("NewFile() error = %v", err)
	}
	defer logger.Close()

	logger.Info("before")

	origStat := fileStat
	fileStat = func(f *os.File) (os.FileInfo, error) {
		return nil, fmt.Errorf("mock stat error")
	}
	defer func() { fileStat = origStat }()

	err = logger.Rotate()
	if err == nil {
		t.Error("Rotate should fail when stat fails")
	}
	if !strings.Contains(err.Error(), "mock stat error") {
		t.Errorf("Expected mock stat error, got: %v", err)
	}
}

func TestGlobalWarnfFunction(t *testing.T) {
	orig := Global
	buf := &bytes.Buffer{}
	Global = New(buf, DebugLevel)
	defer func() { Global = orig }()

	Warnf("warn %s", "formatted")

	if !strings.Contains(buf.String(), "warn formatted") {
		t.Error("Global Warnf should log")
	}
}

func TestGlobalErrorfFunction(t *testing.T) {
	orig := Global
	buf := &bytes.Buffer{}
	Global = New(buf, DebugLevel)
	defer func() { Global = orig }()

	Errorf("error %s", "formatted")

	if !strings.Contains(buf.String(), "error formatted") {
		t.Error("Global Errorf should log")
	}
}

func TestLoggerCloseFile(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := tmpDir + "/close.log"

	logger, err := NewFile(logFile, DebugLevel)
	if err != nil {
		t.Fatalf("NewFile() error = %v", err)
	}

	err = logger.Close()
	if err != nil {
		t.Errorf("Close() error = %v", err)
	}
}

func TestGlobalFatalFunction(t *testing.T) {
	if os.Getenv("TEST_GLOBAL_FATAL") == "1" {
		Fatal("global fatal message")
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestGlobalFatalFunction")
	cmd.Env = append(os.Environ(), "TEST_GLOBAL_FATAL=1")
	err := cmd.Run()
	if e, ok := err.(*exec.ExitError); ok && !e.Success() {
		return
	}
	t.Fatalf("Global Fatal should have exited with status 1, got %v", err)
}

func TestGlobalFatalfFunction(t *testing.T) {
	if os.Getenv("TEST_GLOBAL_FATALF") == "1" {
		Fatalf("global fatal %s", "formatted")
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestGlobalFatalfFunction")
	cmd.Env = append(os.Environ(), "TEST_GLOBAL_FATALF=1")
	err := cmd.Run()
	if e, ok := err.(*exec.ExitError); ok && !e.Success() {
		return
	}
	t.Fatalf("Global Fatalf should have exited with status 1, got %v", err)
}
