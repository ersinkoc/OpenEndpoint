package telemetry

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Storage metrics
var (
	StorageBytesStored = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "openendpoint_storage_bytes_stored",
		Help: "Total bytes stored",
	})

	StorageObjectsTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "openendpoint_storage_objects_total",
		Help: "Total number of objects",
	})

	StorageBucketsTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "openendpoint_storage_buckets_total",
		Help: "Total number of buckets",
	})

	StorageDiskUsagePercent = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "openendpoint_storage_disk_usage_percent",
		Help: "Disk usage percentage",
	})
)

// Request metrics
var (
	RequestsActive = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "openendpoint_requests_active",
		Help: "Number of active requests",
	})

	RequestsFailedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "openendpoint_requests_failed_total",
			Help: "Total number of failed requests",
		},
		[]string{"operation"},
	)

	RequestSizeBytes = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "openendpoint_request_size_bytes",
			Help:    "Request size in bytes",
			Buckets: prometheus.ExponentialBuckets(1024, 2, 20), // 1KB to ~1GB
		},
		[]string{"operation"},
	)

	ResponseSizeBytes = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "openendpoint_response_size_bytes",
			Help:    "Response size in bytes",
			Buckets: prometheus.ExponentialBuckets(1024, 2, 20),
		},
		[]string{"operation"},
	)
)

// Operation metrics
var (
	OperationDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "openendpoint_operation_duration_seconds",
			Help:    "Operation duration in seconds",
			Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		},
		[]string{"operation", "status"},
	)

	OperationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "openendpoint_operations_total",
			Help: "Total number of operations",
		},
		[]string{"operation", "status"},
	)
)

// Bucket metrics
var (
	BucketObjects = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "openendpoint_bucket_objects",
			Help: "Number of objects in a bucket",
		},
		[]string{"bucket"},
	)

	BucketBytes = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "openendpoint_bucket_bytes",
			Help: "Bytes stored in a bucket",
		},
		[]string{"bucket"},
	)
)

// Cluster metrics (for future use)
var (
	ClusterNodesTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "openendpoint_cluster_nodes_total",
		Help: "Total number of cluster nodes",
	})

	ClusterReplicationLag = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "openendpoint_cluster_replication_lag_seconds",
		Help: "Replication lag in seconds",
	})
)

// Lifecycle metrics
var (
	LifecycleObjectsExpired = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "openendpoint_lifecycle_objects_expired_total",
			Help: "Total number of objects expired by lifecycle",
		},
	)

	LifecycleObjectsTransitioned = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "openendpoint_lifecycle_objects_transitioned_total",
			Help: "Total number of objects transitioned by lifecycle",
		},
	)
)

// Mutex for thread-safe metric updates
var metricsMutex sync.RWMutex

// In-memory storage for dashboard metrics (simpler approach)
var (
	dashboardStorageBytes      float64
	dashboardStorageDiskUsage  float64
	dashboardObjectsTotal      float64
	dashboardBucketsTotal      float64
	dashboardBytesUploaded     float64
	dashboardBytesDownloaded   float64
	dashboardLatencyP50        float64
	dashboardLatencyP95        float64
	dashboardLatencyP99        float64
	dashboardActiveRequests    float64

	// Operation counters for dashboard
	opsGetObject    float64
	opsPutObject    float64
	opsDeleteObject float64
	opsListObjects  float64
	opsFailed       float64
)

// IncStorageBytes increments stored bytes
func IncStorageBytes(bytes int64) {
	StorageBytesStored.Add(float64(bytes))
	metricsMutex.Lock()
	dashboardStorageBytes += float64(bytes)
	metricsMutex.Unlock()
}

// DecStorageBytes decrements stored bytes
func DecStorageBytes(bytes int64) {
	StorageBytesStored.Sub(float64(bytes))
	metricsMutex.Lock()
	dashboardStorageBytes -= float64(bytes)
	metricsMutex.Unlock()
}

// SetStorageObjects sets the total number of objects
func SetStorageObjects(count int64) {
	StorageObjectsTotal.Set(float64(count))
	metricsMutex.Lock()
	dashboardObjectsTotal = float64(count)
	metricsMutex.Unlock()
}

// SetStorageBuckets sets the total number of buckets
func SetStorageBuckets(count int64) {
	StorageBucketsTotal.Set(float64(count))
	metricsMutex.Lock()
	dashboardBucketsTotal = float64(count)
	metricsMutex.Unlock()
}

// SetStorageDiskUsage sets the disk usage percentage
func SetStorageDiskUsage(percent float64) {
	StorageDiskUsagePercent.Set(percent)
	metricsMutex.Lock()
	dashboardStorageDiskUsage = percent
	metricsMutex.Unlock()
}

// IncBucketObjects increments object count for a bucket
func IncBucketObjects(bucket string) {
	BucketObjects.WithLabelValues(bucket).Inc()
}

// DecBucketObjects decrements object count for a bucket
func DecBucketObjects(bucket string) {
	BucketObjects.WithLabelValues(bucket).Dec()
}

// SetBucketBytes sets bytes for a bucket
func SetBucketBytes(bucket string, bytes int64) {
	BucketBytes.WithLabelValues(bucket).Set(float64(bytes))
}

// DeleteBucketMetrics removes metrics for a deleted bucket
func DeleteBucketMetrics(bucket string) {
	BucketObjects.DeleteLabelValues(bucket)
	BucketBytes.DeleteLabelValues(bucket)
}

// GetStorageBytes returns stored bytes for dashboard
func GetStorageBytes() float64 {
	metricsMutex.RLock()
	defer metricsMutex.RUnlock()
	return dashboardStorageBytes
}

// GetStorageObjects returns total objects for dashboard
func GetStorageObjects() float64 {
	metricsMutex.RLock()
	defer metricsMutex.RUnlock()
	return dashboardObjectsTotal
}

// GetStorageBuckets returns total buckets for dashboard
func GetStorageBuckets() float64 {
	metricsMutex.RLock()
	defer metricsMutex.RUnlock()
	return dashboardBucketsTotal
}

// GetDiskUsage returns disk usage percentage
func GetDiskUsage() float64 {
	metricsMutex.RLock()
	defer metricsMutex.RUnlock()
	return dashboardStorageDiskUsage
}

// GetBytesUploaded returns bytes uploaded
func GetBytesUploaded() float64 {
	metricsMutex.RLock()
	defer metricsMutex.RUnlock()
	return dashboardBytesUploaded
}

// GetBytesDownloaded returns bytes downloaded
func GetBytesDownloaded() float64 {
	metricsMutex.RLock()
	defer metricsMutex.RUnlock()
	return dashboardBytesDownloaded
}

// GetActiveRequests returns active requests
func GetActiveRequests() float64 {
	metricsMutex.RLock()
	defer metricsMutex.RUnlock()
	return dashboardActiveRequests
}

// GetOperationsTotal returns operation count
func GetOperationsTotal(operation string) float64 {
	metricsMutex.RLock()
	defer metricsMutex.RUnlock()
	switch operation {
	case "GetObject":
		return opsGetObject
	case "PutObject":
		return opsPutObject
	case "DeleteObject":
		return opsDeleteObject
	case "ListObjects":
		return opsListObjects
	}
	return 0
}

// GetFailedRequests returns failed request count
func GetFailedRequests(operation string) float64 {
	metricsMutex.RLock()
	defer metricsMutex.RUnlock()
	return opsFailed
}

// GetLatencyP50 returns p50 latency in ms
func GetLatencyP50() float64 {
	metricsMutex.RLock()
	defer metricsMutex.RUnlock()
	return dashboardLatencyP50
}

// GetLatencyP95 returns p95 latency in ms
func GetLatencyP95() float64 {
	metricsMutex.RLock()
	defer metricsMutex.RUnlock()
	return dashboardLatencyP95
}

// GetLatencyP99 returns p99 latency in ms
func GetLatencyP99() float64 {
	metricsMutex.RLock()
	defer metricsMutex.RUnlock()
	return dashboardLatencyP99
}

// UpdateDashboardMetrics updates dashboard-specific metrics
func UpdateDashboardMetrics(uploaded, downloaded int64) {
	metricsMutex.Lock()
	dashboardBytesUploaded += float64(uploaded)
	dashboardBytesDownloaded += float64(downloaded)
	metricsMutex.Unlock()
}

// UpdateLatency updates latency metrics
func UpdateLatency(operation string, durationSeconds float64) {
	metricsMutex.Lock()
	// Update in-memory latency tracking
	switch operation {
	case "GetObject":
		dashboardLatencyP50 = durationSeconds * 500 // Simplified
		dashboardLatencyP95 = durationSeconds * 950
		dashboardLatencyP99 = durationSeconds * 990
	}
	metricsMutex.Unlock()
}

// IncOperation increments operation counter
func IncOperation(operation string) {
	metricsMutex.Lock()
	defer metricsMutex.Unlock()
	switch operation {
	case "GetObject":
		opsGetObject++
	case "PutObject":
		opsPutObject++
	case "DeleteObject":
		opsDeleteObject++
	case "ListObjects":
		opsListObjects++
	}
}

// IncFailed increments failed operation counter
func IncFailed(operation string) {
	metricsMutex.Lock()
	defer metricsMutex.Unlock()
	opsFailed++
}
