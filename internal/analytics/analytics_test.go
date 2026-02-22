package analytics

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestNewMetricsCollector(t *testing.T) {
	logger := zap.NewNop()
	collector := NewMetricsCollector(logger)
	if collector == nil {
		t.Fatal("MetricsCollector should not be nil")
	}
}

func TestMetricsCollector_RecordObject(t *testing.T) {
	logger := zap.NewNop()
	collector := NewMetricsCollector(logger)

	collector.RecordObject("test-bucket", "test-key", 1024)

	metrics := collector.GetStorageMetrics()
	if metrics.TotalBytes != 1024 {
		t.Errorf("TotalBytes = %d, want 1024", metrics.TotalBytes)
	}

	if metrics.TotalObjects != 1 {
		t.Errorf("TotalObjects = %d, want 1", metrics.TotalObjects)
	}
}

func TestMetricsCollector_DeleteObject(t *testing.T) {
	logger := zap.NewNop()
	collector := NewMetricsCollector(logger)

	collector.RecordObject("test-bucket", "test-key", 1024)
	collector.DeleteObject("test-bucket", "test-key", 1024)

	metrics := collector.GetStorageMetrics()
	if metrics.TotalBytes != 0 {
		t.Errorf("TotalBytes after delete = %d, want 0", metrics.TotalBytes)
	}
}

func TestMetricsCollector_RecordRequest(t *testing.T) {
	logger := zap.NewNop()
	collector := NewMetricsCollector(logger)

	collector.RecordRequest("PutObject", true, 1024, 50.0)
	collector.RecordRequest("GetObject", true, 512, 25.0)
	collector.RecordRequest("GetObject", false, 0, 10.0)

	metrics := collector.GetRequestMetrics()
	if metrics.TotalRequests != 3 {
		t.Errorf("TotalRequests = %d, want 3", metrics.TotalRequests)
	}

	if metrics.TotalErrors != 1 {
		t.Errorf("TotalErrors = %d, want 1", metrics.TotalErrors)
	}
}

func TestMetricsCollector_RecordAccess(t *testing.T) {
	logger := zap.NewNop()
	collector := NewMetricsCollector(logger)

	collector.RecordAccess("test-bucket", "test-key")
	collector.RecordAccess("test-bucket", "test-key")

	// Should not panic
}

func TestMetricsCollector_GetStorageMetrics(t *testing.T) {
	logger := zap.NewNop()
	collector := NewMetricsCollector(logger)

	collector.RecordObject("bucket1", "key1", 100)
	collector.RecordObject("bucket1", "key2", 200)
	collector.RecordObject("bucket2", "key1", 300)

	metrics := collector.GetStorageMetrics()

	if metrics.TotalBytes != 600 {
		t.Errorf("TotalBytes = %d, want 600", metrics.TotalBytes)
	}

	if metrics.TotalObjects != 3 {
		t.Errorf("TotalObjects = %d, want 3", metrics.TotalObjects)
	}

	if metrics.TotalBuckets != 2 {
		t.Errorf("TotalBuckets = %d, want 2", metrics.TotalBuckets)
	}
}

func TestMetricsCollector_GetRequestMetrics(t *testing.T) {
	logger := zap.NewNop()
	collector := NewMetricsCollector(logger)

	collector.RecordRequest("PutObject", true, 1000, 100.0)
	collector.RecordRequest("GetObject", true, 0, 50.0)

	metrics := collector.GetRequestMetrics()

	if metrics.TotalRequests != 2 {
		t.Errorf("TotalRequests = %d, want 2", metrics.TotalRequests)
	}

	if metrics.BytesUploaded != 1000 {
		t.Errorf("BytesUploaded = %d, want 1000", metrics.BytesUploaded)
	}
}

func TestMetricsCollector_GenerateReport(t *testing.T) {
	logger := zap.NewNop()
	collector := NewMetricsCollector(logger)

	collector.RecordObject("bucket1", "key1", 1024)
	collector.RecordRequest("PutObject", true, 1024, 50.0)

	ctx := context.Background()
	report := collector.GenerateReport(ctx, time.Now().Add(-1*time.Hour), time.Now())

	if report == nil {
		t.Fatal("Report should not be nil")
	}

	if report.Storage.TotalBytes != 1024 {
		t.Errorf("Storage.TotalBytes = %d, want 1024", report.Storage.TotalBytes)
	}
}

func TestReporter_NewReporter(t *testing.T) {
	logger := zap.NewNop()
	collector := NewMetricsCollector(logger)
	reporter := NewReporter(collector, logger)

	if reporter == nil {
		t.Fatal("Reporter should not be nil")
	}
}

func TestReporter_GenerateHourlyReport(t *testing.T) {
	logger := zap.NewNop()
	collector := NewMetricsCollector(logger)
	reporter := NewReporter(collector, logger)

	collector.RecordObject("bucket1", "key1", 1024)
	collector.RecordRequest("PutObject", true, 1024, 50.0)

	ctx := context.Background()
	report, err := reporter.GenerateHourlyReport(ctx)
	if err != nil {
		t.Fatalf("GenerateHourlyReport failed: %v", err)
	}

	if report == nil {
		t.Fatal("Report should not be nil")
	}
}

func TestReporter_GenerateDailyReport(t *testing.T) {
	logger := zap.NewNop()
	collector := NewMetricsCollector(logger)
	reporter := NewReporter(collector, logger)

	ctx := context.Background()
	report, err := reporter.GenerateDailyReport(ctx)
	if err != nil {
		t.Fatalf("GenerateDailyReport failed: %v", err)
	}

	if report == nil {
		t.Fatal("Report should not be nil")
	}
}

func TestReporter_GetInsights(t *testing.T) {
	logger := zap.NewNop()
	collector := NewMetricsCollector(logger)
	reporter := NewReporter(collector, logger)

	ctx := context.Background()
	insights := reporter.GetInsights(ctx)

	// Should return slice (may be empty)
	if insights == nil {
		t.Error("Insights should not be nil")
	}
}

func TestStorageMetrics(t *testing.T) {
	metrics := StorageMetrics{
		TotalBytes:   1024,
		TotalObjects: 10,
		TotalBuckets: 2,
	}

	if metrics.TotalBytes != 1024 {
		t.Errorf("TotalBytes = %d, want 1024", metrics.TotalBytes)
	}
}

func TestRequestMetrics(t *testing.T) {
	metrics := RequestMetrics{
		TotalRequests:   100,
		TotalErrors:     5,
		BytesUploaded:   1024,
		BytesDownloaded: 2048,
		AvgLatencyMs:    50.5,
	}

	if metrics.TotalRequests != 100 {
		t.Errorf("TotalRequests = %d, want 100", metrics.TotalRequests)
	}
}

func TestCostEstimate(t *testing.T) {
	cost := CostEstimate{
		StorageCost:    10.50,
		RequestsCost:   2.25,
		BandwidthCost:  5.75,
		TotalCost:      18.50,
	}

	if cost.TotalCost != 18.50 {
		t.Errorf("TotalCost = %.2f, want 18.50", cost.TotalCost)
	}
}

func TestInsight(t *testing.T) {
	insight := Insight{
		Type:    "error_rate",
		Level:   "warning",
		Message: "Error rate is 10%",
	}

	if insight.Type != "error_rate" {
		t.Errorf("Type = %s, want error_rate", insight.Type)
	}
}

func TestMetricsCollector_Concurrent(t *testing.T) {
	logger := zap.NewNop()
	collector := NewMetricsCollector(logger)
	done := make(chan bool)

	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				collector.RecordObject("bucket", "key", 100)
				collector.RecordRequest("PutObject", true, 100, 10.0)
			}
			done <- true
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}

	metrics := collector.GetStorageMetrics()
	if metrics.TotalObjects != 1000 {
		t.Errorf("TotalObjects = %d, want 1000", metrics.TotalObjects)
	}
}
