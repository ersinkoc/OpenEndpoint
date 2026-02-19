package mgmt

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/openendpoint/openendpoint/internal/cluster"
	"github.com/openendpoint/openendpoint/internal/engine"
	"go.uber.org/zap"
)

// Router handles management API requests
type Router struct {
	engine        *engine.ObjectService
	logger        *zap.SugaredLogger
	clusterService *cluster.Cluster
}

// NewRouter creates a new management API router
func NewRouter(engine *engine.ObjectService, logger *zap.SugaredLogger, config interface{}, clusterSvc *cluster.Cluster) *Router {
	return &Router{
		engine:        engine,
		logger:        logger,
		clusterService: clusterSvc,
	}
}

// ServeHTTP handles management API requests
func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// Strip /_mgmt/ prefix
	path := req.URL.Path
	if len(path) > 6 && path[:6] == "/_mgmt" {
		path = path[6:]
	}
	if path == "" {
		path = "/"
	}

	// Route request
	r.route(w, req, path)
}

func (r *Router) route(w http.ResponseWriter, req *http.Request, path string) {
	switch {
	case req.Method == http.MethodGet && path == "/":
		r.handleStatus(w, req)
	case req.Method == http.MethodGet && path == "/health":
		r.handleHealth(w, req)
	case req.Method == http.MethodGet && path == "/ready":
		r.handleReady(w, req)
	case req.Method == http.MethodGet && path == "/buckets":
		r.handleListBuckets(w, req)
	case req.Method == http.MethodPost && path == "/buckets":
		r.handleCreateBucket(w, req)
	case req.Method == http.MethodDelete && len(path) > 9 && path[:9] == "/buckets/":
		r.handleDeleteBucket(w, req, path[9:])
	case req.Method == http.MethodGet && len(path) > 9 && path[:9] == "/buckets/":
		bucket := path[9:]
		r.handleGetBucket(w, req, bucket)
	case req.Method == http.MethodGet && path == "/metrics":
		r.handleMetrics(w, req)
	case req.Method == http.MethodGet && path == "/version":
		r.handleVersion(w, req)
	case req.Method == http.MethodGet && path == "/cluster":
		r.handleCluster(w, req)

	default:
		r.writeError(w, http.StatusNotFound, "Not Found")
	}
}

// handleStatus returns system status
func (r *Router) handleStatus(w http.ResponseWriter, req *http.Request) {
	status := map[string]interface{}{
		"status":    "running",
		"timestamp": time.Now().Unix(),
		"uptime":    "0", // Would need to track start time
	}

	r.writeJSON(w, http.StatusOK, status)
}

// handleHealth returns health check
func (r *Router) handleHealth(w http.ResponseWriter, req *http.Request) {
	r.writeJSON(w, http.StatusOK, map[string]string{
		"status": "healthy",
	})
}

// handleReady returns readiness check
func (r *Router) handleReady(w http.ResponseWriter, req *http.Request) {
	r.writeJSON(w, http.StatusOK, map[string]string{
		"status": "ready",
	})
}

// handleListBuckets lists all buckets
func (r *Router) handleListBuckets(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()

	buckets, err := r.engine.ListBuckets(ctx)
	if err != nil {
		r.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	r.writeJSON(w, http.StatusOK, map[string]interface{}{
		"buckets": buckets,
	})
}

// handleCreateBucket creates a new bucket
func (r *Router) handleCreateBucket(w http.ResponseWriter, req *http.Request) {
	var body struct {
		Name string `json:"name"`
	}

	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		r.writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	if body.Name == "" {
		r.writeError(w, http.StatusBadRequest, "Bucket name is required")
		return
	}

	ctx := req.Context()
	if err := r.engine.CreateBucket(ctx, body.Name); err != nil {
		r.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	r.writeJSON(w, http.StatusCreated, map[string]string{
		"name": body.Name,
	})
}

// handleDeleteBucket deletes a bucket
func (r *Router) handleDeleteBucket(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	if err := r.engine.DeleteBucket(ctx, bucket); err != nil {
		r.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	r.writeJSON(w, http.StatusOK, map[string]string{
		"name": bucket,
	})
}

// handleGetBucket returns bucket details
func (r *Router) handleGetBucket(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	buckets, err := r.engine.ListBuckets(ctx)
	if err != nil {
		r.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	for _, b := range buckets {
		if b.Name == bucket {
			r.writeJSON(w, http.StatusOK, b)
			return
		}
	}

	r.writeError(w, http.StatusNotFound, fmt.Sprintf("Bucket not found: %s", bucket))
}

// handleMetrics returns Prometheus metrics
func (r *Router) handleMetrics(w http.ResponseWriter, req *http.Request) {
	// This is handled by the Prometheus middleware
	// Just return empty response
	w.WriteHeader(http.StatusOK)
}

// handleVersion returns version info
func (r *Router) handleVersion(w http.ResponseWriter, req *http.Request) {
	r.writeJSON(w, http.StatusOK, map[string]string{
		"version": "1.0.0",
		"build":   "release",
	})
}

// handleCluster returns cluster status
func (r *Router) handleCluster(w http.ResponseWriter, req *http.Request) {
	if r.clusterService == nil {
		r.writeJSON(w, http.StatusOK, map[string]interface{}{
			"enabled": false,
			"nodes":   []interface{}{},
		})
		return
	}

	nodes := r.clusterService.GetNodes()
	info := r.clusterService.GetClusterInfo()

	// Convert nodes to dashboard format
	nodeList := make([]map[string]interface{}, len(nodes))
	for i, node := range nodes {
		nodeList[i] = map[string]interface{}{
			"id":               node.ID,
			"name":             node.Name,
			"address":          node.Address,
			"port":             node.Port,
			"status":           node.Status(),
			"version":          node.Version,
			"region":           node.Metadata.Region,
			"zone":             node.Metadata.Zone,
			"storageUsed":      node.Metadata.StorageUsed,
			"storageCapacity":  node.Metadata.StorageCapacity,
			"uptime":           time.Since(node.JoinTime).String(),
		}
	}

	r.writeJSON(w, http.StatusOK, map[string]interface{}{
		"enabled":            true,
		"nodeID":             info.NodeID,
		"replicationFactor":  info.ReplicationFactor,
		"totalNodes":         len(nodes),
		"nodes":              nodeList,
		"ringDistribution":   r.clusterService.GetRingDistribution(),
	})
}

// writeJSON writes a JSON response
func (r *Router) writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

// writeError writes an error response
func (r *Router) writeError(w http.ResponseWriter, status int, message string) {
	r.logger.Warnw("Management API error",
		"status", status,
		"message", message,
	)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{
		"error": message,
	})
}
