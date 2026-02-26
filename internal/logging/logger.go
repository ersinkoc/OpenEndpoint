package logging

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var (
	osRename   = os.Rename
	osOpenFile = os.OpenFile
	fileStat   = defaultFileStat
)

func defaultFileStat(f *os.File) (os.FileInfo, error) { return f.Stat() }

// Level represents log level
type Level int

const (
	DebugLevel Level = iota
	InfoLevel
	WarnLevel
	ErrorLevel
	PanicLevel
	FatalLevel
)

// String returns the level as string
func (l Level) String() string {
	switch l {
	case DebugLevel:
		return "DEBUG"
	case InfoLevel:
		return "INFO"
	case WarnLevel:
		return "WARN"
	case ErrorLevel:
		return "ERROR"
	case PanicLevel:
		return "PANIC"
	case FatalLevel:
		return "FATAL"
	default:
		return "UNKNOWN"
	}
}

// Logger represents a logger
type Logger struct {
	mu      sync.Mutex
	level   Level
	output  io.Writer
	format  string
	timeLoc *time.Location
}

// New creates a new logger
func New(output io.Writer, level Level) *Logger {
	return &Logger{
		output:  output,
		level:   level,
		format:  "[%s] %s %s\n",
		timeLoc: time.UTC,
	}
}

// NewFile creates a logger that writes to a file
func NewFile(filename string, level Level) (*Logger, error) {
	// Create directory if needed
	dir := filepath.Dir(filename)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create log directory: %w", err)
	}

	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}

	return &Logger{
		output:  file,
		level:   level,
		format:  "[%s] %s %s\n",
		timeLoc: time.UTC,
	}, nil
}

// NewStd creates a logger that writes to stdout/stderr
func NewStd(level Level) *Logger {
	return New(os.Stdout, level)
}

// SetLevel sets the log level
func (l *Logger) SetLevel(level Level) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.level = level
}

// Debug logs a debug message
func (l *Logger) Debug(v ...interface{}) {
	l.log(DebugLevel, fmt.Sprint(v...))
}

// Debugf logs a formatted debug message
func (l *Logger) Debugf(format string, v ...interface{}) {
	l.log(DebugLevel, fmt.Sprintf(format, v...))
}

// Info logs an info message
func (l *Logger) Info(v ...interface{}) {
	l.log(InfoLevel, fmt.Sprint(v...))
}

// Infof logs a formatted info message
func (l *Logger) Infof(format string, v ...interface{}) {
	l.log(InfoLevel, fmt.Sprintf(format, v...))
}

// Warn logs a warning message
func (l *Logger) Warn(v ...interface{}) {
	l.log(WarnLevel, fmt.Sprint(v...))
}

// Warnf logs a formatted warning message
func (l *Logger) Warnf(format string, v ...interface{}) {
	l.log(WarnLevel, fmt.Sprintf(format, v...))
}

// Error logs an error message
func (l *Logger) Error(v ...interface{}) {
	l.log(ErrorLevel, fmt.Sprint(v...))
}

// Errorf logs a formatted error message
func (l *Logger) Errorf(format string, v ...interface{}) {
	l.log(ErrorLevel, fmt.Sprintf(format, v...))
}

// Panic logs a panic message and panics
func (l *Logger) Panic(v ...interface{}) {
	l.log(PanicLevel, fmt.Sprint(v...))
	panic(fmt.Sprint(v...))
}

// Panicf logs a formatted panic message and panics
func (l *Logger) Panicf(format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	l.log(PanicLevel, msg)
	panic(msg)
}

// Fatal logs a fatal message and exits
func (l *Logger) Fatal(v ...interface{}) {
	l.log(FatalLevel, fmt.Sprint(v...))
	// Use runtime.Goexit() for a more graceful exit in goroutines
	// but still terminate the program
	os.Exit(1)
}

// Fatalf logs a formatted fatal message and exits
func (l *Logger) Fatalf(format string, v ...interface{}) {
	l.log(FatalLevel, fmt.Sprintf(format, v...))
	os.Exit(1)
}

// log logs a message at the specified level
func (l *Logger) log(level Level, message string) {
	if level < l.level {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	timestamp := time.Now().In(l.timeLoc).Format("2006-01-02 15:04:05")
	fmt.Fprintf(l.output, l.format, timestamp, level, message)
}

// Rotate rotates the log file
func (l *Logger) Rotate() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Check if output is a file
	file, ok := l.output.(*os.File)
	if !ok {
		return nil
	}

	// Get current file info
	info, err := fileStat(file)
	if err != nil {
		return err
	}

	// Rename current file with timestamp
	dir := filepath.Dir(info.Name())
	base := filepath.Base(info.Name())
	ext := filepath.Ext(base)
	name := base[:len(base)-len(ext)]

	newName := filepath.Join(dir, fmt.Sprintf("%s-%s%s", name, time.Now().Format("20060102-150405"), ext))

	if err := osRename(info.Name(), newName); err != nil {
		return err
	}

	newFile, err := osOpenFile(info.Name(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	l.output = newFile
	return nil
}

// Close closes the logger
func (l *Logger) Close() error {
	if file, ok := l.output.(*os.File); ok {
		return file.Close()
	}
	return nil
}

// WithTimeLocation sets the time location
func (l *Logger) WithTimeLocation(loc *time.Location) *Logger {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.timeLoc = loc
	return l
}

// WithFormat sets the log format
func (l *Logger) WithFormat(format string) *Logger {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.format = format
	return l
}

// Global is the global logger
var Global = NewStd(InfoLevel)

// Package-level convenience functions

// Debug logs a debug message to the global logger
func Debug(v ...interface{}) {
	Global.Debug(v...)
}

// Debugf logs a formatted debug message
func Debugf(format string, v ...interface{}) {
	Global.Debugf(format, v...)
}

// Info logs an info message
func Info(v ...interface{}) {
	Global.Info(v...)
}

// Infof logs a formatted info message
func Infof(format string, v ...interface{}) {
	Global.Infof(format, v...)
}

// Warn logs a warning message
func Warn(v ...interface{}) {
	Global.Warn(v...)
}

// Warnf logs a formatted warning message
func Warnf(format string, v ...interface{}) {
	Global.Warnf(format, v...)
}

// Error logs an error message
func Error(v ...interface{}) {
	Global.Error(v...)
}

// Errorf logs a formatted error message
func Errorf(format string, v ...interface{}) {
	Global.Errorf(format, v...)
}

// Panic logs a panic message and panics
func Panic(v ...interface{}) {
	Global.Panic(v...)
}

// Panicf logs a formatted panic message and panics
func Panicf(format string, v ...interface{}) {
	Global.Panicf(format, v...)
}

// Fatal logs a fatal message and exits
func Fatal(v ...interface{}) {
	Global.Fatal(v...)
}

// Fatalf logs a formatted fatal message and exits
func Fatalf(format string, v ...interface{}) {
	Global.Fatalf(format, v...)
}

// SetLevel sets the global log level
func SetLevel(level Level) {
	Global.SetLevel(level)
}
