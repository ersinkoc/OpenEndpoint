package mgmt

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/openendpoint/openendpoint/internal/bucketconfig"
	"github.com/openendpoint/openendpoint/internal/cluster"
	"github.com/openendpoint/openendpoint/internal/engine"
	"github.com/openendpoint/openendpoint/internal/iam"
	"github.com/openendpoint/openendpoint/internal/lifecycle"
	"github.com/openendpoint/openendpoint/internal/replication"
	"github.com/openendpoint/openendpoint/internal/telemetry"
	"go.uber.org/zap"
)

// Router handles management API requests
type Router struct {
	engine          *engine.ObjectService
	logger          *zap.SugaredLogger
	clusterService  *cluster.Cluster
	iamManager     *iam.Manager
	lifecycleSvc   *lifecycle.Lifecycle
	replicationSvc *replication.Replication
	bucketConfig   *bucketconfig.Config
}

// NewRouter creates a new management API router
func NewRouter(engine *engine.ObjectService, logger *zap.SugaredLogger, config interface{}, clusterSvc *cluster.Cluster) *Router {
	return &Router{
		engine:          engine,
		logger:          logger,
		clusterService:  clusterSvc,
		iamManager:     iam.NewManager(logger.Desugar()),
		lifecycleSvc:   lifecycle.New(),
		replicationSvc: replication.New(),
		bucketConfig:   bucketconfig.New(),
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
	case req.Method == http.MethodGet && path == "/metrics/json":
		r.handleMetricsJSON(w, req)
	case req.Method == http.MethodGet && path == "/version":
		r.handleVersion(w, req)
	case req.Method == http.MethodGet && path == "/settings":
		r.handleSettings(w, req)
	case req.Method == http.MethodPost && path == "/settings":
		r.handleSettings(w, req)
	case req.Method == http.MethodGet && path == "/cluster":
		r.handleCluster(w, req)
	// NOTE: Specific routes must come BEFORE general /buckets/{bucket} routes
	case req.Method == http.MethodGet && len(path) > 9 && path[:9] == "/buckets/" && strings.Contains(path[9:], "/objects"):
		// /buckets/{bucket}/objects or /buckets/{bucket}/objects/{prefix}
		parts := strings.SplitN(path[9:], "/objects", 2)
		bucket := parts[0]
		prefix := ""
		if len(parts) > 1 && parts[1] != "" {
			prefix = parts[1][1:] // Remove leading /
		}
		r.handleListObjects(w, req, bucket, prefix)
	case req.Method == http.MethodDelete && len(path) > 9 && path[:9] == "/buckets/":
		// /buckets/{bucket}/objects/{key} - must check before general bucket delete
		rest := path[9:]
		parts := strings.SplitN(rest, "/objects/", 2)
		if len(parts) == 2 {
			bucket := parts[0]
			key := parts[1]
			r.handleDeleteObject(w, req, bucket, key)
			return
		}
		r.handleDeleteBucket(w, req, rest)
	case req.Method == http.MethodPost && len(path) > 9 && path[:9] == "/buckets/" && strings.Contains(path[9:], "/objects"):
		// Upload object - /buckets/{bucket}/objects
		parts := strings.SplitN(path[9:], "/objects", 2)
		bucket := parts[0]
		r.handleUploadObject(w, req, bucket)
		return
	case req.Method == http.MethodGet && len(path) > 9 && path[:9] == "/buckets/":
		// /buckets/{bucket} - general bucket info (must be after specific routes)
		bucket := path[9:]
		r.handleGetBucket(w, req, bucket)

	// IAM Routes
	case req.Method == http.MethodGet && path == "/iam/users":
		r.handleListIAMUsers(w, req)
	case req.Method == http.MethodPost && path == "/iam/users":
		r.handleCreateIAMUser(w, req)
	case req.Method == http.MethodDelete && len(path) > 10 && path[:10] == "/iam/users/":
		r.handleDeleteIAMUser(w, req, path[10:])
	case req.Method == http.MethodGet && len(path) > 10 && path[:10] == "/iam/users/" && strings.Contains(path[10:], "/keys"):
		// /iam/users/{name}/keys
		parts := strings.SplitN(path[10:], "/keys", 2)
		userName := parts[0]
		r.handleListIAMKeys(w, req, userName)
	case req.Method == http.MethodPost && len(path) > 10 && path[:10] == "/iam/users/" && strings.Contains(path[10:], "/keys"):
		parts := strings.SplitN(path[10:], "/keys", 2)
		userName := parts[0]
		r.handleCreateIAMKey(w, req, userName)
	case req.Method == http.MethodDelete && len(path) > 14 && path[:14] == "/iam/users/keys/":
		r.handleDeleteIAMKey(w, req, path[14:])
	case req.Method == http.MethodGet && path == "/iam/groups":
		r.handleListIAMGroups(w, req)
	case req.Method == http.MethodPost && path == "/iam/groups":
		r.handleCreateIAMGroup(w, req)
	case req.Method == http.MethodDelete && len(path) > 12 && path[:12] == "/iam/groups/":
		r.handleDeleteIAMGroup(w, req, path[12:])
	case req.Method == http.MethodGet && path == "/iam/policies":
		r.handleListIAMPolicies(w, req)
	case req.Method == http.MethodPost && path == "/iam/policies":
		r.handleCreateIAMPolicy(w, req)
	case req.Method == http.MethodDelete && len(path) > 13 && path[:13] == "/iam/policies/":
		r.handleDeleteIAMPolicy(w, req, path[13:])

	// Lifecycle Routes
	case req.Method == http.MethodGet && len(path) > 11 && path[:11] == "/lifecycle/":
		bucket := path[11:]
		r.handleGetLifecycleRules(w, req, bucket)
	case req.Method == http.MethodPut && len(path) > 11 && path[:11] == "/lifecycle/":
		bucket := path[11:]
		r.handleSetLifecycleRules(w, req, bucket)
	case req.Method == http.MethodDelete && len(path) > 11 && path[:11] == "/lifecycle/":
		bucket := path[11:]
		r.handleDeleteLifecycleRules(w, req, bucket)

	// Replication Routes
	case req.Method == http.MethodGet && len(path) > 12 && path[:12] == "/replication/":
		bucket := path[12:]
		r.handleGetReplicationRules(w, req, bucket)
	case req.Method == http.MethodPut && len(path) > 12 && path[:12] == "/replication/":
		bucket := path[12:]
		r.handleSetReplicationRules(w, req, bucket)
	case req.Method == http.MethodDelete && len(path) > 12 && path[:12] == "/replication/":
		bucket := path[12:]
		r.handleDeleteReplicationRules(w, req, bucket)

	// Bucket Config Routes (Versioning, CORS, Policy)
	case req.Method == http.MethodGet && len(path) > 10 && path[:10] == "/buckets/" && strings.Contains(path[10:], "/versioning"):
		bucket := strings.SplitN(path[10:], "/versioning", 2)[0]
		r.handleGetVersioning(w, req, bucket)
	case req.Method == http.MethodPut && len(path) > 10 && path[:10] == "/buckets/" && strings.Contains(path[10:], "/versioning"):
		bucket := strings.SplitN(path[10:], "/versioning", 2)[0]
		r.handleSetVersioning(w, req, bucket)
	case req.Method == http.MethodGet && len(path) > 10 && path[:10] == "/buckets/" && strings.Contains(path[10:], "/cors"):
		bucket := strings.SplitN(path[10:], "/cors", 2)[0]
		r.handleGetCORS(w, req, bucket)
	case req.Method == http.MethodPut && len(path) > 10 && path[:10] == "/buckets/" && strings.Contains(path[10:], "/cors"):
		bucket := strings.SplitN(path[10:], "/cors", 2)[0]
		r.handleSetCORS(w, req, bucket)
	case req.Method == http.MethodDelete && len(path) > 10 && path[:10] == "/buckets/" && strings.Contains(path[10:], "/cors"):
		bucket := strings.SplitN(path[10:], "/cors", 2)[0]
		r.handleDeleteCORS(w, req, bucket)
	case req.Method == http.MethodGet && len(path) > 10 && path[:10] == "/buckets/" && strings.Contains(path[10:], "/policy"):
		bucket := strings.SplitN(path[10:], "/policy", 2)[0]
		r.handleGetBucketPolicy(w, req, bucket)
	case req.Method == http.MethodPut && len(path) > 10 && path[:10] == "/buckets/" && strings.Contains(path[10:], "/policy"):
		bucket := strings.SplitN(path[10:], "/policy", 2)[0]
		r.handleSetBucketPolicy(w, req, bucket)
	case req.Method == http.MethodDelete && len(path) > 10 && path[:10] == "/buckets/" && strings.Contains(path[10:], "/policy"):
		bucket := strings.SplitN(path[10:], "/policy", 2)[0]
		r.handleDeleteBucketPolicy(w, req, bucket)

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

// handleListObjects lists objects in a bucket
func (r *Router) handleListObjects(w http.ResponseWriter, req *http.Request, bucket, prefix string) {
	ctx := req.Context()

	// Get query parameters
	delimiter := req.URL.Query().Get("delimiter")
	maxKeys := 1000
	if maxKeysStr := req.URL.Query().Get("maxKeys"); maxKeysStr != "" {
		fmt.Sscanf(maxKeysStr, "%d", &maxKeys)
	}

	result, err := r.engine.ListObjects(ctx, bucket, engine.ListObjectsOptions{
		Prefix:    prefix,
		Delimiter: delimiter,
		MaxKeys:   maxKeys,
	})
	if err != nil {
		r.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Convert to S3-style response for UI compatibility
	response := map[string]interface{}{
		"Contents":       result.Objects,
		"CommonPrefixes": result.CommonPrefixes,
		"Prefix":         result.Prefix,
		"Delimiter":      result.Delimiter,
		"MaxKeys":        result.MaxKeys,
		"NextMarker":     result.NextMarker,
		"IsTruncated":    result.IsTruncated,
	}

	r.writeJSON(w, http.StatusOK, response)
}

// handleDeleteObject deletes an object
func (r *Router) handleDeleteObject(w http.ResponseWriter, req *http.Request, bucket, key string) {
	ctx := req.Context()

	// URL decode the key
	key, err := url.PathUnescape(key)
	if err != nil {
		r.writeError(w, http.StatusBadRequest, "Invalid object key")
		return
	}

	if err := r.engine.DeleteObject(ctx, bucket, key, engine.DeleteObjectOptions{}); err != nil {
		r.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	r.writeJSON(w, http.StatusOK, map[string]string{
		"bucket": bucket,
		"key":    key,
	})
}

// handleUploadObject uploads an object to a bucket
func (r *Router) handleUploadObject(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	// Parse multipart form
	if err := req.ParseMultipartForm(10 << 20); err != nil { // 10MB max
		r.writeError(w, http.StatusBadRequest, "Invalid request: "+err.Error())
		return
	}

	file, header, err := req.FormFile("file")
	if err != nil {
		r.writeError(w, http.StatusBadRequest, "No file uploaded")
		return
	}
	defer file.Close()

	// Get the key from the form, or use the filename
	key := req.FormValue("key")
	if key == "" {
		key = header.Filename
	}

	// Read file content
	content := make([]byte, header.Size)
	_, err = file.Read(content)
	if err != nil {
		r.writeError(w, http.StatusInternalServerError, "Failed to read file")
		return
	}

	// Upload to engine
	_, err = r.engine.PutObject(ctx, bucket, key, bytes.NewReader(content), engine.PutObjectOptions{
		ContentType: header.Header.Get("Content-Type"),
	})
	if err != nil {
		r.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	r.writeJSON(w, http.StatusCreated, map[string]interface{}{
		"bucket": bucket,
		"key":    key,
		"size":   header.Size,
	})
}

// handleMetrics returns Prometheus metrics
func (r *Router) handleMetrics(w http.ResponseWriter, req *http.Request) {
	// This is handled by the Prometheus middleware
	// Just return empty response
	w.WriteHeader(http.StatusOK)
}

// handleMetricsJSON returns metrics in JSON format for dashboard
func (r *Router) handleMetricsJSON(w http.ResponseWriter, req *http.Request) {
	// Get operation totals using the telemetry helper functions
	totalGet := telemetry.GetOperationsTotal("GetObject")
	totalPut := telemetry.GetOperationsTotal("PutObject")
	totalDelete := telemetry.GetOperationsTotal("DeleteObject")
	totalList := telemetry.GetOperationsTotal("ListObjects")

	// Get failed requests
	failedGet := telemetry.GetFailedRequests("GetObject")
	failedPut := telemetry.GetFailedRequests("PutObject")
	failedDelete := telemetry.GetFailedRequests("DeleteObject")
	failedList := telemetry.GetFailedRequests("ListObjects")
	failedTotal := failedGet + failedPut + failedDelete + failedList

	r.writeJSON(w, http.StatusOK, map[string]interface{}{
		"storage": map[string]interface{}{
			"bytesStored":      telemetry.GetStorageBytes(),
			"objectsTotal":     telemetry.GetStorageObjects(),
			"bucketsTotal":     telemetry.GetStorageBuckets(),
			"diskUsagePercent": telemetry.GetDiskUsage(),
		},
		"requests": map[string]interface{}{
			"bytesUploaded":   telemetry.GetBytesUploaded(),
			"bytesDownloaded": telemetry.GetBytesDownloaded(),
			"activeRequests":  telemetry.GetActiveRequests(),
			"failedRequests":  failedTotal,
		},
		"operations": map[string]interface{}{
			"getObject":    totalGet,
			"putObject":    totalPut,
			"deleteObject": totalDelete,
			"listObjects":  totalList,
		},
		"latency": map[string]float64{
			"p50": telemetry.GetLatencyP50(),
			"p95": telemetry.GetLatencyP95(),
			"p99": telemetry.GetLatencyP99(),
		},
		"timestamp": time.Now().Unix(),
	})
}

// handleVersion returns version info
func (r *Router) handleVersion(w http.ResponseWriter, req *http.Request) {
	r.writeJSON(w, http.StatusOK, map[string]string{
		"version": "1.0.0",
		"build":   "release",
	})
}

// handleSettings returns or updates settings
func (r *Router) handleSettings(w http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodGet {
		// Return current settings
		r.writeJSON(w, http.StatusOK, map[string]interface{}{
			"region":                "us-east-1",
			"storageClass":          "STANDARD",
			"objectLock":            false,
			"publicAccessBlock":     true,
			"serverEncryption":      true,
			"auditLogging":          true,
		})
		return
	}

	// POST - Update settings
	var settings map[string]interface{}
	if err := json.NewDecoder(req.Body).Decode(&settings); err != nil {
		r.writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	// In a real implementation, these would be persisted
	r.logger.Infow("Settings updated", "settings", settings)

	r.writeJSON(w, http.StatusOK, map[string]string{
		"status": "success",
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

// ==================== IAM Handlers ====================

// handleListIAMUsers lists all IAM users
func (r *Router) handleListIAMUsers(w http.ResponseWriter, req *http.Request) {
	users := r.iamManager.ListUsers("default")
	r.writeJSON(w, http.StatusOK, map[string]interface{}{
		"users": users,
	})
}

// handleCreateIAMUser creates a new IAM user
func (r *Router) handleCreateIAMUser(w http.ResponseWriter, req *http.Request) {
	var body struct {
		Username string `json:"username"`
		Email    string `json:"email"`
	}

	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		r.writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	if body.Username == "" {
		r.writeError(w, http.StatusBadRequest, "Username is required")
		return
	}

	user, err := r.iamManager.CreateUser("default", body.Username, body.Email)
	if err != nil {
		r.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	r.writeJSON(w, http.StatusCreated, user)
}

// handleDeleteIAMUser deletes an IAM user
func (r *Router) handleDeleteIAMUser(w http.ResponseWriter, req *http.Request, userID string) {
	if err := r.iamManager.DeleteUser(userID); err != nil {
		r.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	r.writeJSON(w, http.StatusOK, map[string]string{"id": userID})
}

// handleListIAMKeys lists access keys for a user
func (r *Router) handleListIAMKeys(w http.ResponseWriter, req *http.Request, userID string) {
	user, ok := r.iamManager.GetUser(userID)
	if !ok {
		r.writeError(w, http.StatusNotFound, "User not found")
		return
	}
	r.writeJSON(w, http.StatusOK, map[string]interface{}{
		"accessKeys": user.AccessKeys,
	})
}

// handleCreateIAMKey creates an access key for a user
func (r *Router) handleCreateIAMKey(w http.ResponseWriter, req *http.Request, userID string) {
	key, err := r.iamManager.CreateAccessKey(userID)
	if err != nil {
		r.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	r.writeJSON(w, http.StatusCreated, key)
}

// handleDeleteIAMKey deletes an access key - simplified
func (r *Router) handleDeleteIAMKey(w http.ResponseWriter, req *http.Request, keyID string) {
	r.writeJSON(w, http.StatusOK, map[string]string{"id": keyID})
}

// handleListIAMGroups lists all IAM groups
func (r *Router) handleListIAMGroups(w http.ResponseWriter, req *http.Request) {
	// ListGroups returns all groups - simplified for now
	r.writeJSON(w, http.StatusOK, map[string]interface{}{
		"groups": []interface{}{},
	})
}

// handleCreateIAMGroup creates a new IAM group
func (r *Router) handleCreateIAMGroup(w http.ResponseWriter, req *http.Request) {
	var body struct {
		Name string `json:"name"`
	}

	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		r.writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	if body.Name == "" {
		r.writeError(w, http.StatusBadRequest, "Group name is required")
		return
	}

	group, err := r.iamManager.CreateGroup("default", body.Name)
	if err != nil {
		r.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	r.writeJSON(w, http.StatusCreated, group)
}

// handleDeleteIAMGroup deletes an IAM group - simplified
func (r *Router) handleDeleteIAMGroup(w http.ResponseWriter, req *http.Request, groupID string) {
	r.writeJSON(w, http.StatusOK, map[string]string{"id": groupID})
}

// handleListIAMPolicies lists all IAM policies
func (r *Router) handleListIAMPolicies(w http.ResponseWriter, req *http.Request) {
	// Simplified - list all policies
	r.writeJSON(w, http.StatusOK, map[string]interface{}{
		"policies": []interface{}{},
	})
}

// handleCreateIAMPolicy creates a new IAM policy
func (r *Router) handleCreateIAMPolicy(w http.ResponseWriter, req *http.Request) {
	var body struct {
		Name   string          `json:"name"`
		Policy iam.PolicyDoc   `json:"policy"`
	}

	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		r.writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	if body.Name == "" {
		r.writeError(w, http.StatusBadRequest, "Policy name is required")
		return
	}

	policy, err := r.iamManager.CreatePolicy("default", body.Name, body.Policy)
	if err != nil {
		r.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	r.writeJSON(w, http.StatusCreated, policy)
}

// handleDeleteIAMPolicy deletes an IAM policy - simplified
func (r *Router) handleDeleteIAMPolicy(w http.ResponseWriter, req *http.Request, policyID string) {
	r.writeJSON(w, http.StatusOK, map[string]string{"id": policyID})
}

// ==================== Lifecycle Handlers ====================

// handleGetLifecycleRules gets lifecycle rules for a bucket
func (r *Router) handleGetLifecycleRules(w http.ResponseWriter, req *http.Request, bucket string) {
	rules := r.lifecycleSvc.ListRules(bucket)
	r.writeJSON(w, http.StatusOK, map[string]interface{}{
		"bucket": bucket,
		"rules":  rules,
	})
}

// handleSetLifecycleRules sets lifecycle rules for a bucket
func (r *Router) handleSetLifecycleRules(w http.ResponseWriter, req *http.Request, bucket string) {
	var body struct {
		Rules []*lifecycle.Rule `json:"rules"`
	}

	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		r.writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	// Delete existing rules and add new ones
	r.lifecycleSvc.DeleteBucketRules(bucket)

	for _, rule := range body.Rules {
		if err := r.lifecycleSvc.AddRule(bucket, rule); err != nil {
			r.writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
	}

	r.writeJSON(w, http.StatusOK, map[string]string{"bucket": bucket})
}

// handleDeleteLifecycleRules deletes lifecycle rules for a bucket
func (r *Router) handleDeleteLifecycleRules(w http.ResponseWriter, req *http.Request, bucket string) {
	if err := r.lifecycleSvc.DeleteBucketRules(bucket); err != nil {
		r.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	r.writeJSON(w, http.StatusOK, map[string]string{"bucket": bucket})
}

// ==================== Replication Handlers ====================

// handleGetReplicationRules gets replication rules for a bucket
func (r *Router) handleGetReplicationRules(w http.ResponseWriter, req *http.Request, bucket string) {
	rules := r.replicationSvc.ListRules(bucket)
	stats, _ := r.replicationSvc.GetStats(bucket)
	r.writeJSON(w, http.StatusOK, map[string]interface{}{
		"bucket": bucket,
		"rules":  rules,
		"stats":  stats,
	})
}

// handleSetReplicationRules sets replication rules for a bucket
func (r *Router) handleSetReplicationRules(w http.ResponseWriter, req *http.Request, bucket string) {
	var body struct {
		Rules []*replication.Rule `json:"rules"`
	}

	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		r.writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	// Delete existing rules and add new ones
	r.replicationSvc.DeleteBucketRules(bucket)

	for _, rule := range body.Rules {
		if err := r.replicationSvc.AddRule(bucket, rule); err != nil {
			r.writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
	}

	r.writeJSON(w, http.StatusOK, map[string]string{"bucket": bucket})
}

// handleDeleteReplicationRules deletes replication rules for a bucket
func (r *Router) handleDeleteReplicationRules(w http.ResponseWriter, req *http.Request, bucket string) {
	if err := r.replicationSvc.DeleteBucketRules(bucket); err != nil {
		r.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	r.writeJSON(w, http.StatusOK, map[string]string{"bucket": bucket})
}

// ==================== Bucket Config Handlers ====================

// handleGetVersioning gets versioning configuration
func (r *Router) handleGetVersioning(w http.ResponseWriter, req *http.Request, bucket string) {
	config, ok := r.bucketConfig.GetVersioningConfig(bucket)
	if !ok {
		r.writeJSON(w, http.StatusOK, map[string]interface{}{
			"status":    "Disabled",
			"mfaDelete": "Disabled",
		})
		return
	}
	r.writeJSON(w, http.StatusOK, config)
}

// handleSetVersioning sets versioning configuration
func (r *Router) handleSetVersioning(w http.ResponseWriter, req *http.Request, bucket string) {
	var body struct {
		Status    string `json:"status"`
		MFADelete string `json:"mfaDelete"`
	}

	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		r.writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	config := &bucketconfig.VersioningConfig{
		Status:    body.Status,
		MFADelete: body.MFADelete,
	}

	if err := r.bucketConfig.SetVersioningConfig(bucket, config); err != nil {
		r.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	r.writeJSON(w, http.StatusOK, config)
}

// handleGetCORS gets CORS configuration
func (r *Router) handleGetCORS(w http.ResponseWriter, req *http.Request, bucket string) {
	config, ok := r.bucketConfig.GetCORSConfig(bucket)
	if !ok {
		r.writeJSON(w, http.StatusOK, map[string]interface{}{
			"corsRules": []interface{}{},
		})
		return
	}
	r.writeJSON(w, http.StatusOK, config)
}

// handleSetCORS sets CORS configuration
func (r *Router) handleSetCORS(w http.ResponseWriter, req *http.Request, bucket string) {
	var body struct {
		CORSRules []*bucketconfig.CORSRule `json:"corsRules"`
	}

	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		r.writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	config := &bucketconfig.CORSConfig{
		CORSRules: body.CORSRules,
	}

	if err := r.bucketConfig.SetCORSConfig(bucket, config); err != nil {
		r.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	r.writeJSON(w, http.StatusOK, config)
}

// handleDeleteCORS deletes CORS configuration
func (r *Router) handleDeleteCORS(w http.ResponseWriter, req *http.Request, bucket string) {
	if err := r.bucketConfig.DeleteCORSConfig(bucket); err != nil {
		r.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	r.writeJSON(w, http.StatusOK, map[string]string{"bucket": bucket})
}

// handleGetBucketPolicy gets bucket policy
func (r *Router) handleGetBucketPolicy(w http.ResponseWriter, req *http.Request, bucket string) {
	policy, ok := r.bucketConfig.GetBucketPolicy(bucket)
	if !ok {
		r.writeJSON(w, http.StatusOK, map[string]interface{}{
			"policy": nil,
		})
		return
	}
	r.writeJSON(w, http.StatusOK, policy)
}

// handleSetBucketPolicy sets bucket policy
func (r *Router) handleSetBucketPolicy(w http.ResponseWriter, req *http.Request, bucket string) {
	var body bucketconfig.BucketPolicy

	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		r.writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	if err := r.bucketConfig.SetBucketPolicy(bucket, &body); err != nil {
		r.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	r.writeJSON(w, http.StatusOK, &body)
}

// handleDeleteBucketPolicy deletes bucket policy
func (r *Router) handleDeleteBucketPolicy(w http.ResponseWriter, req *http.Request, bucket string) {
	if err := r.bucketConfig.DeleteBucketPolicy(bucket); err != nil {
		r.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	r.writeJSON(w, http.StatusOK, map[string]string{"bucket": bucket})
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
