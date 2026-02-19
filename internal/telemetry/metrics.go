package telemetry

import (
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

// IncStorageBytes increments stored bytes
func IncStorageBytes(bytes int64) {
	StorageBytesStored.Add(float64(bytes))
}

// DecStorageBytes decrements stored bytes
func DecStorageBytes(bytes int64) {
	StorageBytesStored.Sub(float64(bytes))
}

// SetStorageObjects sets the total number of objects
func SetStorageObjects(count int64) {
	StorageObjectsTotal.Set(float64(count))
}

// SetStorageBuckets sets the total number of buckets
func SetStorageBuckets(count int64) {
	StorageBucketsTotal.Set(float64(count))
}

// SetStorageDiskUsage sets the disk usage percentage
func SetStorageDiskUsage(percent float64) {
	StorageDiskUsagePercent.Set(percent)
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
