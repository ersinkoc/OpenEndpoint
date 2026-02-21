package telemetry

import (
	"testing"
	"time"
)

func TestNewMetrics(t *testing.T) {
	metrics := NewMetrics()
	if metrics == nil {
		t.Fatal("Metrics should not be nil")
	}
}

func TestMetrics_RecordRequest(t *testing.T) {
	metrics := NewMetrics()

	metrics.RecordRequest("GetObject", 200, 100*time.Millisecond)
	metrics.RecordRequest("PutObject", 200, 150*time.Millisecond)
	metrics.RecordRequest("GetObject", 404, 10*time.Millisecond)

	// Should not panic
}

func TestMetrics_RecordBytes(t *testing.T) {
	metrics := NewMetrics()

	metrics.RecordBytesIn(1024)
	metrics.RecordBytesOut(2048)

	// Should not panic
}

func TestMetrics_IncObject(t *testing.T) {
	metrics := NewMetrics()

	metrics.IncObject()
	metrics.IncObject()
	metrics.DecObject()

	// Should not panic
}

func TestMetrics_SetBucketCount(t *testing.T) {
	metrics := NewMetrics()

	metrics.SetBucketCount(5)

	// Should not panic
}

func TestMetrics_GetSnapshot(t *testing.T) {
	metrics := NewMetrics()

	metrics.RecordRequest("GetObject", 200, 100*time.Millisecond)
	metrics.RecordBytesIn(1024)
	metrics.IncObject()

	snapshot := metrics.GetSnapshot()

	if snapshot == nil {
		t.Fatal("Snapshot should not be nil")
	}
}

func TestMetrics_Reset(t *testing.T) {
	metrics := NewMetrics()

	metrics.RecordRequest("GetObject", 200, 100*time.Millisecond)
	metrics.Reset()

	// Should not panic
}

func TestCollector_Start(t *testing.T) {
	collector := NewCollector()

	go collector.Start(100 * time.Millisecond)
	time.Sleep(150 * time.Millisecond)
	collector.Stop()

	// Should not panic
}

func TestCollector_RecordLatency(t *testing.T) {
	collector := NewCollector()

	collector.RecordLatency("test_operation", 50*time.Millisecond)
	collector.RecordLatency("test_operation", 100*time.Millisecond)
	collector.RecordLatency("test_operation", 150*time.Millisecond)

	stats := collector.GetLatencyStats("test_operation")
	if stats == nil {
		t.Fatal("Stats should not be nil")
	}

	if stats.Count != 3 {
		t.Errorf("Count = %d, want 3", stats.Count)
	}
}

func TestCollector_GetLatencyStats_NotFound(t *testing.T) {
	collector := NewCollector()

	stats := collector.GetLatencyStats("non_existent")
	if stats != nil {
		t.Error("Stats should be nil for non-existent operation")
	}
}

func TestLatencyStats(t *testing.T) {
	stats := &LatencyStats{
		Count: 10,
		Min:   10 * time.Millisecond,
		Max:   100 * time.Millisecond,
		Avg:   50 * time.Millisecond,
		P50:   45 * time.Millisecond,
		P95:   90 * time.Millisecond,
		P99:   95 * time.Millisecond,
	}

	if stats.Count != 10 {
		t.Errorf("Count = %d, want 10", stats.Count)
	}
}

func TestPrometheusExporter(t *testing.T) {
	metrics := NewMetrics()
	exporter := NewPrometheusExporter(metrics)

	if exporter == nil {
		t.Fatal("Exporter should not be nil")
	}

	// Export should not panic
	exporter.Export()
}

func TestMetrics_Concurrent(t *testing.T) {
	metrics := NewMetrics()
	done := make(chan bool)

	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				metrics.RecordRequest("GetObject", 200, time.Millisecond)
				metrics.RecordBytesIn(1024)
				metrics.IncObject()
			}
			done <- true
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestHistogram(t *testing.T) {
	h := NewHistogram()

	for i := 0; i < 100; i++ {
		h.Record(time.Duration(i) * time.Millisecond)
	}

	p50 := h.Percentile(50)
	p95 := h.Percentile(95)
	p99 := h.Percentile(99)

	if p50 == 0 {
		t.Error("P50 should not be 0")
	}

	if p95 <= p50 {
		t.Error("P95 should be greater than P50")
	}

	if p99 <= p95 {
		t.Error("P99 should be greater than P95")
	}
}

func TestCounter(t *testing.T) {
	counter := NewCounter()

	counter.Inc()
	counter.Inc()
	counter.Add(5)

	if counter.Value() != 7 {
		t.Errorf("Counter value = %d, want 7", counter.Value())
	}

	counter.Reset()
	if counter.Value() != 0 {
		t.Errorf("Counter after reset = %d, want 0", counter.Value())
	}
}

func TestGauge(t *testing.T) {
	gauge := NewGauge()

	gauge.Set(10)
	if gauge.Value() != 10 {
		t.Errorf("Gauge value = %d, want 10", gauge.Value())
	}

	gauge.Inc()
	if gauge.Value() != 11 {
		t.Errorf("Gauge after inc = %d, want 11", gauge.Value())
	}

	gauge.Dec()
	if gauge.Value() != 10 {
		t.Errorf("Gauge after dec = %d, want 10", gauge.Value())
	}
}
