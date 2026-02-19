package api

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/openendpoint/openendpoint/internal/auth"
	"github.com/openendpoint/openendpoint/internal/config"
	"github.com/openendpoint/openendpoint/internal/engine"
	s3types "github.com/openendpoint/openendpoint/pkg/s3types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
)

// Router handles S3 API requests
type Router struct {
	engine *engine.ObjectService
	auth   *auth.Auth
	logger *zap.SugaredLogger
	config *config.Config
}

// s3RequestsTotal is a metric for tracking S3 API requests
var s3RequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "openendpoint_s3_requests_total",
	Help: "Total number of S3 API requests",
}, []string{"operation", "status"})

// NewRouter creates a new S3 API router
func NewRouter(engine *engine.ObjectService, auth *auth.Auth, logger *zap.SugaredLogger, cfg *config.Config) *Router {
	return &Router{
		engine: engine,
		auth:   auth,
		logger: logger,
		config: cfg,
	}
}

// ServeHTTP handles S3 API requests
func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// Route request
	r.route(w, req)
}

// route routes the request to the appropriate handler
func (r *Router) route(w http.ResponseWriter, req *http.Request) {
	// Get bucket and key from path
	bucket, key, err := parseBucketKey(req, req.URL.Path)
	if err != nil {
		r.writeError(w, ErrInvalidURI)
		return
	}

	// Check for multipart upload operations
	if bucket != "" && key != "" {
		// Check if uploads parameter exists (S3 uses ?uploads or ?uploads=)
		_, hasUploads := req.URL.Query()["uploads"]
		if hasUploads && req.Method == http.MethodPost {
			r.handleCreateMultipartUpload(w, req, bucket, key)
			return
		}
		if req.URL.Query().Get("uploadId") != "" {
			switch req.Method {
			case http.MethodPut:
				if req.URL.Query().Get("partNumber") != "" {
					r.handleUploadPart(w, req, bucket, key)
					return
				}
			case http.MethodPost:
				r.handleCompleteMultipartUpload(w, req, bucket, key)
				return
			case http.MethodDelete:
				r.handleAbortMultipartUpload(w, req, bucket, key)
				return
			case http.MethodGet:
				r.handleListParts(w, req, bucket, key)
				return
			}
		}
	}

	// Route based on HTTP method and path
	switch req.Method {
	case http.MethodGet:
		if bucket == "" {
			r.handleListBuckets(w, req)
		} else if key == "" {
			r.handleListObjects(w, req, bucket)
		} else {
			r.handleGetObject(w, req, bucket, key)
		}
	case http.MethodPut:
		if bucket == "" {
			r.writeError(w, ErrInvalidBucketName)
		} else if key == "" {
			r.handleCreateBucket(w, req, bucket)
		} else {
			r.handlePutObject(w, req, bucket, key)
		}
	case http.MethodDelete:
		if key == "" {
			r.handleDeleteBucket(w, req, bucket)
		} else {
			r.handleDeleteObject(w, req, bucket, key)
		}
	case http.MethodHead:
		if bucket != "" && key != "" {
			r.handleHeadObject(w, req, bucket, key)
		} else {
			r.writeError(w, ErrNotImplemented)
		}
	case http.MethodPost:
		// Handle post to bucket for list multipart uploads
		if bucket != "" && key == "" {
			r.handleListMultipartUploads(w, req, bucket)
			return
		}
		r.writeError(w, ErrNotImplemented)
	default:
		r.writeError(w, ErrMethodNotAllowed)
	}
}

// parseBucketKey parses the bucket and key from the request path
func parseBucketKey(req *http.Request, path string) (bucket, key string, err error) {
	// Handle S3 API path format
	// /s3/ -> root (list buckets)
	// /s3/bucket -> bucket root (list objects)
	// /s3/bucket/key -> bucket + key

	// Remove /s3/ prefix (note: includes trailing slash)
	if len(path) >= 4 && (path[:4] == "/s3" || path[:4] == "/s3/") {
		path = path[4:]
		// If there's still a leading slash after removing /s3, remove it too
		if len(path) > 0 && path[0] == '/' {
			path = path[1:]
		}
	}

	// If path is empty now, it's root (list buckets)
	if path == "" {
		return "", "", nil
	}

	// Split into bucket and key
	if idx := findByteIndex(path, '/'); idx >= 0 {
		bucket = path[:idx]
		key = path[idx+1:]
	} else {
		bucket = path
	}

	return bucket, key, nil
}

// findByteIndex finds the index of a byte in a string
func findByteIndex(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}

// writeError writes an error response
func (r *Router) writeError(w http.ResponseWriter, err S3Error) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(err.StatusCode())

	resp := s3types.Error{
		Code:      err.Code(),
		Message:   err.Message(),
		RequestID: "openendpoint-" + fmt.Sprintf("%d", time.Now().UnixNano()),
	}
	xmlBytes, _ := xml.Marshal(resp)
	w.Write(xmlBytes)
}

// writeXML writes an XML response
func (r *Router) writeXML(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	xmlBytes, _ := xml.Marshal(v)
	w.Write(xmlBytes)
}

// handleListBuckets handles ListBuckets
func (r *Router) handleListBuckets(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	buckets, err := r.engine.ListBuckets(ctx)
	if err != nil {
		r.logger.Warnw("failed to list buckets", "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	// Convert to S3 format
	xmlBuckets := make([]s3types.Bucket, len(buckets))
	for i, b := range buckets {
		xmlBuckets[i] = s3types.Bucket{
			Name:         b.Name,
			CreationDate: time.Unix(b.CreationDate, 0).Format(time.RFC3339),
		}
	}

	result := s3types.ListAllMyBucketsResult{
		Owner: &s3types.Owner{
			ID:          "root",
			DisplayName: "root",
		},
		Buckets: &s3types.Buckets{
			Bucket: xmlBuckets,
		},
	}

	r.writeXML(w, http.StatusOK, result)
	s3RequestsTotal.WithLabelValues("ListBuckets", "200").Inc()
}

// handleListObjects handles ListObjects
func (r *Router) handleListObjects(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	prefix := req.URL.Query().Get("prefix")
	delimiter := req.URL.Query().Get("delimiter")
	maxKeys := parseInt(req.URL.Query().Get("max-keys"), 1000)

	result, err := r.engine.ListObjects(ctx, bucket, engine.ListObjectsOptions{
		Prefix:    prefix,
		Delimiter: delimiter,
		MaxKeys:   maxKeys,
	})
	if err != nil {
		r.logger.Warnw("failed to list objects", "bucket", bucket, "error", err)
		r.writeError(w, ErrNoSuchBucket)
		return
	}

	// Convert engine objects to S3 objects
	contents := make([]s3types.Object, len(result.Objects))
	for i, obj := range result.Objects {
		contents[i] = s3types.Object{
			Key:          obj.Key,
			LastModified: time.Unix(obj.LastModified, 0).Format(time.RFC3339),
			ETag:         obj.ETag,
			Size:         fmt.Sprintf("%d", obj.Size),
			StorageClass: obj.StorageClass,
			Owner: &s3types.Owner{
				ID:          "root",
				DisplayName: "root",
			},
		}
	}

	xmlResult := s3types.ListObjectsV2Output{
		Name:                  bucket,
		Prefix:                prefix,
		Delimiter:             delimiter,
		MaxKeys:               fmt.Sprintf("%d", maxKeys),
		KeyCount:              fmt.Sprintf("%d", len(result.Objects)),
		IsTruncated:           result.IsTruncated,
		Contents:              contents,
		CommonPrefixes:        result.CommonPrefixes,
		NextContinuationToken: result.NextMarker,
	}

	r.writeXML(w, http.StatusOK, xmlResult)
	s3RequestsTotal.WithLabelValues("ListObjects", "200").Inc()
}

// handleGetObject handles GetObject
func (r *Router) handleGetObject(w http.ResponseWriter, req *http.Request, bucket, key string) {
	ctx := req.Context()

	obj, err := r.engine.GetObject(ctx, bucket, key, engine.GetObjectOptions{})
	if err != nil {
		r.logger.Warnw("failed to get object", "bucket", bucket, "key", key, "error", err)
		r.writeError(w, ErrNoSuchKey)
		return
	}
	defer obj.Body.Close()

	// Set headers
	w.Header().Set("Content-Type", obj.ContentType)
	w.Header().Set("Content-Length", fmt.Sprintf("%d", obj.Size))
	w.Header().Set("ETag", obj.ETag)

	// Use a buffer to ensure data is properly sent
	data, err := io.ReadAll(obj.Body)
	if err != nil {
		r.logger.Warnw("failed to read object data", "bucket", bucket, "key", key, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	// Write data directly
	w.Write(data)

	s3RequestsTotal.WithLabelValues("GetObject", "200").Inc()
}

// handleHeadObject handles HeadObject
func (r *Router) handleHeadObject(w http.ResponseWriter, req *http.Request, bucket, key string) {
	ctx := req.Context()

	meta, err := r.engine.HeadObject(ctx, bucket, key)
	if err != nil {
		r.logger.Warnw("failed to head object", "bucket", bucket, "key", key, "error", err)
		r.writeError(w, ErrNoSuchKey)
		return
	}

	w.Header().Set("Content-Type", meta.ContentType)
	w.Header().Set("Content-Length", fmt.Sprintf("%d", meta.Size))
	w.Header().Set("ETag", meta.ETag)
	w.WriteHeader(http.StatusOK)

	s3RequestsTotal.WithLabelValues("HeadObject", "200").Inc()
}

// handlePutObject handles PutObject
func (r *Router) handlePutObject(w http.ResponseWriter, req *http.Request, bucket, key string) {
	ctx := req.Context()

	// Read content
	data := req.Body
	contentLength := req.ContentLength
	contentType := req.Header.Get("Content-Type")

	result, err := r.engine.PutObject(ctx, bucket, key, data, engine.PutObjectOptions{
		ContentType: contentType,
	})
	_ = contentLength // Reserved for future use

	if err != nil {
		r.logger.Warnw("failed to put object", "bucket", bucket, "key", key, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	// Set response headers
	w.Header().Set("ETag", result.ETag)
	w.WriteHeader(http.StatusOK)

	s3RequestsTotal.WithLabelValues("PutObject", "200").Inc()
}

// handleCreateBucket handles CreateBucket
func (r *Router) handleCreateBucket(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	err := r.engine.CreateBucket(ctx, bucket)
	if err != nil {
		r.logger.Warnw("failed to create bucket", "bucket", bucket, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	w.WriteHeader(http.StatusOK)
	s3RequestsTotal.WithLabelValues("CreateBucket", "200").Inc()
}

// handleDeleteBucket handles DeleteBucket
func (r *Router) handleDeleteBucket(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	err := r.engine.DeleteBucket(ctx, bucket)
	if err != nil {
		r.logger.Warnw("failed to delete bucket", "bucket", bucket, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	w.WriteHeader(http.StatusNoContent)
	s3RequestsTotal.WithLabelValues("DeleteBucket", "200").Inc()
}

// handleDeleteObject handles DeleteObject
func (r *Router) handleDeleteObject(w http.ResponseWriter, req *http.Request, bucket, key string) {
	ctx := req.Context()

	err := r.engine.DeleteObject(ctx, bucket, key, engine.DeleteObjectOptions{})
	if err != nil {
		r.logger.Warnw("failed to delete object", "bucket", bucket, "key", key, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	w.WriteHeader(http.StatusNoContent)
	s3RequestsTotal.WithLabelValues("DeleteObject", "200").Inc()
}

// parseInt parses an integer with default
func parseInt(s string, defaultVal int) int {
	if s == "" {
		return defaultVal
	}
	var i int
	fmt.Sscanf(s, "%d", &i)
	return i
}

// handleCreateMultipartUpload handles CreateMultipartUpload
func (r *Router) handleCreateMultipartUpload(w http.ResponseWriter, req *http.Request, bucket, key string) {
	ctx := req.Context()

	result, err := r.engine.CreateMultipartUpload(ctx, bucket, key, engine.PutObjectOptions{
		ContentType: req.Header.Get("Content-Type"),
	})
	if err != nil {
		r.logger.Warnw("failed to create multipart upload", "bucket", bucket, "key", key, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)

	resp := s3types.InitiateMultipartUploadResult{
		Bucket:   bucket,
		Key:      key,
		UploadID: result.UploadID,
	}
	xmlBytes, _ := xml.Marshal(resp)
	w.Write(xmlBytes)

	s3RequestsTotal.WithLabelValues("CreateMultipartUpload", "200").Inc()
}

// handleUploadPart handles UploadPart
func (r *Router) handleUploadPart(w http.ResponseWriter, req *http.Request, bucket, key string) {
	ctx := req.Context()

	uploadID := req.URL.Query().Get("uploadId")
	partNumber := parseInt(req.URL.Query().Get("partNumber"), 0)

	// Read part data
	data, err := io.ReadAll(req.Body)
	if err != nil {
		r.logger.Warnw("failed to read part data", "bucket", bucket, "key", key, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	// Create a reader from the data
	result, err := r.engine.UploadPart(ctx, bucket, key, uploadID, partNumber, bytes.NewReader(data))
	if err != nil {
		r.logger.Warnw("failed to upload part", "bucket", bucket, "key", key, "part", partNumber, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	w.Header().Set("ETag", result.ETag)
	w.WriteHeader(http.StatusOK)

	s3RequestsTotal.WithLabelValues("UploadPart", "200").Inc()
}

// handleCompleteMultipartUpload handles CompleteMultipartUpload
func (r *Router) handleCompleteMultipartUpload(w http.ResponseWriter, req *http.Request, bucket, key string) {
	ctx := req.Context()

	uploadID := req.URL.Query().Get("uploadId")

	// Parse the completion XML
	var completeBody struct {
		Parts []struct {
			ETag         string `xml:"ETag"`
			PartNumber   int    `xml:"PartNumber"`
		} `xml:"Part"`
	}

	decoder := xml.NewDecoder(req.Body)
	if err := decoder.Decode(&completeBody); err != nil {
		r.logger.Warnw("failed to parse complete body", "bucket", bucket, "key", key, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	// Convert to engine parts
	parts := make([]engine.PartInfo, len(completeBody.Parts))
	for i, p := range completeBody.Parts {
		parts[i] = engine.PartInfo{
			ETag:       p.ETag,
			PartNumber: p.PartNumber,
		}
	}

	result, err := r.engine.CompleteMultipartUpload(ctx, bucket, key, uploadID, parts)
	if err != nil {
		r.logger.Warnw("failed to complete multipart upload", "bucket", bucket, "key", key, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("ETag", result.ETag)
	w.WriteHeader(http.StatusOK)

	resp := s3types.CompleteMultipartUploadResult{
		Bucket:       bucket,
		Key:          key,
		ETag:         result.ETag,
		Location:     "",
	}
	xmlBytes, _ := xml.Marshal(resp)
	w.Write(xmlBytes)

	s3RequestsTotal.WithLabelValues("CompleteMultipartUpload", "200").Inc()
}

// handleAbortMultipartUpload handles AbortMultipartUpload
func (r *Router) handleAbortMultipartUpload(w http.ResponseWriter, req *http.Request, bucket, key string) {
	ctx := req.Context()

	uploadID := req.URL.Query().Get("uploadId")

	err := r.engine.AbortMultipartUpload(ctx, bucket, key, uploadID)
	if err != nil {
		r.logger.Warnw("failed to abort multipart upload", "bucket", bucket, "key", key, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	w.WriteHeader(http.StatusNoContent)
	s3RequestsTotal.WithLabelValues("AbortMultipartUpload", "200").Inc()
}

// handleListParts handles ListParts
func (r *Router) handleListParts(w http.ResponseWriter, req *http.Request, bucket, key string) {
	ctx := req.Context()

	uploadID := req.URL.Query().Get("uploadId")

	parts, err := r.engine.ListParts(ctx, bucket, key, uploadID)
	if err != nil {
		r.logger.Warnw("failed to list parts", "bucket", bucket, "key", key, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)

	// Convert to S3 parts
	s3parts := make([]s3types.Part, len(parts))
	for i, p := range parts {
		s3parts[i] = s3types.Part{
			PartNumber: p.PartNumber,
			ETag:       p.ETag,
		}
	}

	resp := s3types.ListPartsOutput{
		Bucket:    bucket,
		Key:       key,
		UploadID: uploadID,
		Parts:    s3parts,
	}
	xmlBytes, _ := xml.Marshal(resp)
	w.Write(xmlBytes)

	s3RequestsTotal.WithLabelValues("ListParts", "200").Inc()
}

// handleListMultipartUploads handles ListMultipartUploads
func (r *Router) handleListMultipartUploads(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	result, err := r.engine.ListMultipartUpload(ctx, bucket, req.URL.Query().Get("prefix"))
	if err != nil {
		r.logger.Warnw("failed to list multipart uploads", "bucket", bucket, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)

	// Build response
	uploads := make([]s3types.Upload, len(result.Uploads))
	for i, u := range result.Uploads {
		uploads[i] = s3types.Upload{
			Key:       u.Key,
			UploadID:  u.UploadID,
			Initiated: time.Unix(u.Initiated, 0).Format(time.RFC3339),
		}
	}

	resp := s3types.ListMultipartUploadsOutput{
		Bucket:    bucket,
		Upload:    uploads,
	}
	xmlBytes, _ := xml.Marshal(resp)
	w.Write(xmlBytes)

	s3RequestsTotal.WithLabelValues("ListMultipartUploads", "200").Inc()
}
