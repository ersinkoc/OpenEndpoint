package telemetry

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestNewLoggerErrorPath(t *testing.T) {
	config := zap.Config{
		Level:            zap.NewAtomicLevelAt(zapcore.InfoLevel),
		Encoding:         "json",
		OutputPaths:      []string{"/nonexistent/path/that/does/not/exist.log"},
		ErrorOutputPaths: []string{"/nonexistent/path/that/does/not/exist.log"},
		EncoderConfig: zapcore.EncoderConfig{
			TimeKey:    "time",
			LevelKey:   "level",
			MessageKey: "msg",
		},
	}

	logger, err := buildLogger(config)
	if err == nil {
		t.Error("Expected error for invalid config, got nil")
	}
	if logger != nil {
		t.Error("Expected nil logger for invalid config")
	}
}

func TestNewLogger(t *testing.T) {
	levels := []string{"debug", "info", "warn", "error", "unknown"}

	for _, level := range levels {
		logger, err := NewLogger(level)
		if err != nil {
			t.Fatalf("NewLogger(%s) failed: %v", level, err)
		}
		if logger == nil {
			t.Errorf("Logger for level %s should not be nil", level)
		}
	}
}

func TestNewLoggerDebug(t *testing.T) {
	logger, err := NewLogger("debug")
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	if logger == nil {
		t.Fatal("Logger should not be nil")
	}
}

func TestNewLoggerInfo(t *testing.T) {
	logger, err := NewLogger("info")
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	if logger == nil {
		t.Fatal("Logger should not be nil")
	}
}

func TestNewLoggerWarn(t *testing.T) {
	logger, err := NewLogger("warn")
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	if logger == nil {
		t.Fatal("Logger should not be nil")
	}
}

func TestNewLoggerError(t *testing.T) {
	logger, err := NewLogger("error")
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	if logger == nil {
		t.Fatal("Logger should not be nil")
	}
}

func TestIncStorageBytes(t *testing.T) {
	IncStorageBytes(1000)
	val := GetStorageBytes()
	if val < 1000 {
		t.Errorf("GetStorageBytes = %v, want >= 1000", val)
	}
}

func TestDecStorageBytes(t *testing.T) {
	IncStorageBytes(2000)
	DecStorageBytes(1000)
	val := GetStorageBytes()
	if val < 0 {
		t.Errorf("GetStorageBytes = %v, want >= 0", val)
	}
}

func TestSetStorageBytes(t *testing.T) {
	SetStorageBytes(5000)
	val := GetStorageBytes()
	if val != 5000 {
		t.Errorf("GetStorageBytes = %v, want 5000", val)
	}
}

func TestSetStorageObjects(t *testing.T) {
	SetStorageObjects(100)
	val := GetStorageObjects()
	if val != 100 {
		t.Errorf("GetStorageObjects = %v, want 100", val)
	}
}

func TestSetStorageBuckets(t *testing.T) {
	SetStorageBuckets(10)
	val := GetStorageBuckets()
	if val != 10 {
		t.Errorf("GetStorageBuckets = %v, want 10", val)
	}
}

func TestSetStorageObjectsTotal(t *testing.T) {
	SetStorageObjectsTotal(200)
	val := GetStorageObjects()
	if val != 200 {
		t.Errorf("GetStorageObjects = %v, want 200", val)
	}
}

func TestIncTotalObjects(t *testing.T) {
	SetStorageObjectsTotal(0)
	IncTotalObjects()
	IncTotalObjects()
	val := GetStorageObjects()
	if val != 2 {
		t.Errorf("GetStorageObjects = %v, want 2", val)
	}
}

func TestDecTotalObjects(t *testing.T) {
	SetStorageObjectsTotal(5)
	DecTotalObjects()
	val := GetStorageObjects()
	if val != 4 {
		t.Errorf("GetStorageObjects = %v, want 4", val)
	}
}

func TestSetStorageDiskUsage(t *testing.T) {
	SetStorageDiskUsage(75.5)
	val := GetDiskUsage()
	if val != 75.5 {
		t.Errorf("GetDiskUsage = %v, want 75.5", val)
	}
}

func TestUpdateDashboardMetrics(t *testing.T) {
	uploaded := GetBytesUploaded()
	downloaded := GetBytesDownloaded()

	UpdateDashboardMetrics(1000, 2000)

	if GetBytesUploaded() <= uploaded {
		t.Error("BytesUploaded should increase")
	}
	if GetBytesDownloaded() <= downloaded {
		t.Error("BytesDownloaded should increase")
	}
}

func TestUpdateLatency(t *testing.T) {
	UpdateLatency("GetObject", 0.1)

	p50 := GetLatencyP50()
	p95 := GetLatencyP95()
	p99 := GetLatencyP99()

	if p50 <= 0 {
		t.Error("P50 latency should be positive")
	}
	if p95 <= 0 {
		t.Error("P95 latency should be positive")
	}
	if p99 <= 0 {
		t.Error("P99 latency should be positive")
	}
}

func TestIncOperation(t *testing.T) {
	ops := []string{"GetObject", "PutObject", "DeleteObject", "ListObjects"}

	for _, op := range ops {
		before := GetOperationsTotal(op)
		IncOperation(op)
		after := GetOperationsTotal(op)
		if after <= before {
			t.Errorf("Operation %s count should increase", op)
		}
	}
}

func TestIncOperationUnknown(t *testing.T) {
	IncOperation("UnknownOperation")
}

func TestGetOperationsTotalUnknown(t *testing.T) {
	val := GetOperationsTotal("Unknown")
	if val != 0 {
		t.Errorf("Unknown operation should return 0, got %v", val)
	}
}

func TestIncFailed(t *testing.T) {
	before := GetFailedRequests("GetObject")
	IncFailed("GetObject")
	after := GetFailedRequests("GetObject")
	if after <= before {
		t.Error("Failed count should increase")
	}
}

func TestGetActiveRequests(t *testing.T) {
	val := GetActiveRequests()
	_ = val
}

func TestIncBucketObjects(t *testing.T) {
	IncBucketObjects("test-bucket")
}

func TestDecBucketObjects(t *testing.T) {
	DecBucketObjects("test-bucket")
}

func TestSetBucketBytes(t *testing.T) {
	SetBucketBytes("test-bucket", 10000)
}

func TestDeleteBucketMetrics(t *testing.T) {
	IncBucketObjects("delete-test-bucket")
	DeleteBucketMetrics("delete-test-bucket")
}

func TestLoggingMiddleware(t *testing.T) {
	logger, _ := NewLogger("info")

	middleware := LoggingMiddleware(logger)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("test"))
	})

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rec := httptest.NewRecorder()

	middleware(handler).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("Status = %d, want %d", rec.Code, http.StatusOK)
	}
}

func TestResponseWriter(t *testing.T) {
	rec := httptest.NewRecorder()
	rw := &responseWriter{ResponseWriter: rec, statusCode: http.StatusOK}

	rw.WriteHeader(http.StatusCreated)

	if rw.statusCode != http.StatusCreated {
		t.Errorf("statusCode = %d, want %d", rw.statusCode, http.StatusCreated)
	}
}

func TestLoggingMiddlewareDifferentStatus(t *testing.T) {
	logger, _ := NewLogger("info")

	middleware := LoggingMiddleware(logger)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})

	req := httptest.NewRequest(http.MethodGet, "/notfound", nil)
	rec := httptest.NewRecorder()

	middleware(handler).ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("Status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestLoggingMiddlewarePost(t *testing.T) {
	logger, _ := NewLogger("info")

	middleware := LoggingMiddleware(logger)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
	})

	req := httptest.NewRequest(http.MethodPost, "/upload", nil)
	rec := httptest.NewRecorder()

	middleware(handler).ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Errorf("Status = %d, want %d", rec.Code, http.StatusCreated)
	}
}

func TestPrometheusMetrics(t *testing.T) {
	if StorageBytesStored == nil {
		t.Error("StorageBytesStored should be initialized")
	}
	if StorageObjectsTotal == nil {
		t.Error("StorageObjectsTotal should be initialized")
	}
	if StorageBucketsTotal == nil {
		t.Error("StorageBucketsTotal should be initialized")
	}
	if RequestsActive == nil {
		t.Error("RequestsActive should be initialized")
	}
	if OperationDuration == nil {
		t.Error("OperationDuration should be initialized")
	}
	if OperationsTotal == nil {
		t.Error("OperationsTotal should be initialized")
	}
	if BucketObjects == nil {
		t.Error("BucketObjects should be initialized")
	}
	if BucketBytes == nil {
		t.Error("BucketBytes should be initialized")
	}
	if ClusterNodesTotal == nil {
		t.Error("ClusterNodesTotal should be initialized")
	}
	if ClusterReplicationLag == nil {
		t.Error("ClusterReplicationLag should be initialized")
	}
	if LifecycleObjectsExpired == nil {
		t.Error("LifecycleObjectsExpired should be initialized")
	}
	if LifecycleObjectsTransitioned == nil {
		t.Error("LifecycleObjectsTransitioned should be initialized")
	}
}

func TestHTTPMetrics(t *testing.T) {
	if RequestsTotal == nil {
		t.Error("RequestsTotal should be initialized")
	}
	if RequestDuration == nil {
		t.Error("RequestDuration should be initialized")
	}
	if BytesUploaded == nil {
		t.Error("BytesUploaded should be initialized")
	}
	if BytesDownloaded == nil {
		t.Error("BytesDownloaded should be initialized")
	}
}

func TestLoggerWithSugarMethods(t *testing.T) {
	logger, _ := NewLogger("debug")

	logger.Info("test info")
	logger.Debug("test debug")
	logger.Warn("test warn")
	logger.Error("test error")

	logger.Infow("test infow", "key", "value")
	logger.Debugw("test debugw", "key", "value")
	logger.Warnw("test warnw", "key", "value")
	logger.Errorw("test errorw", "key", "value")
}

func TestZapLogger(t *testing.T) {
	logger, _ := NewLogger("info")

	sugar := logger
	if sugar == nil {
		t.Fatal("SugaredLogger should not be nil")
	}

	named := sugar.Named("test")
	if named == nil {
		t.Error("Named logger should not be nil")
	}

	with := sugar.With("key", "value")
	if with == nil {
		t.Error("With logger should not be nil")
	}
}

func TestResponseWriterDefaultStatus(t *testing.T) {
	rec := httptest.NewRecorder()
	rw := &responseWriter{ResponseWriter: rec, statusCode: http.StatusOK}

	if rw.statusCode != http.StatusOK {
		t.Errorf("Default statusCode = %d, want %d", rw.statusCode, http.StatusOK)
	}
}

func TestMultipleMetricsUpdates(t *testing.T) {
	SetStorageBytes(0)
	SetStorageObjects(0)
	SetStorageBuckets(0)

	for i := 0; i < 10; i++ {
		IncStorageBytes(100)
		IncTotalObjects()
	}
	SetStorageBuckets(5)

	bytes := GetStorageBytes()
	objects := GetStorageObjects()
	buckets := GetStorageBuckets()

	if bytes != 1000 {
		t.Errorf("Bytes = %v, want 1000", bytes)
	}
	if objects != 10 {
		t.Errorf("Objects = %v, want 10", objects)
	}
	if buckets != 5 {
		t.Errorf("Buckets = %v, want 5", buckets)
	}
}

func TestMetricsConcurrent(t *testing.T) {
	done := make(chan bool)

	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				IncStorageBytes(1)
				IncTotalObjects()
				IncOperation("GetObject")
			}
			done <- true
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}
