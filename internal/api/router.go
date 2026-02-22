package api

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/openendpoint/openendpoint/internal/auth"
	"github.com/openendpoint/openendpoint/internal/config"
	"github.com/openendpoint/openendpoint/internal/engine"
	"github.com/openendpoint/openendpoint/internal/metadata"
	s3select "github.com/openendpoint/openendpoint/internal/s3select"
	"github.com/openendpoint/openendpoint/internal/tags"
	s3types "github.com/openendpoint/openendpoint/pkg/s3types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
)

// maxRequestBodySize limits request body size for XML/JSON parsing (10MB)
const maxRequestBodySize = 10 * 1024 * 1024

// Router handles S3 API requests
type Router struct {
	engine        *engine.ObjectService
	auth          *auth.Auth
	logger        *zap.SugaredLogger
	config        *config.Config
	selectService *s3select.SelectService
}

// s3RequestsTotal is a metric for tracking S3 API requests
var s3RequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "openendpoint_s3_requests_total",
	Help: "Total number of S3 API requests",
}, []string{"operation", "status"})

// NewRouter creates a new S3 API router
func NewRouter(engine *engine.ObjectService, auth *auth.Auth, logger *zap.SugaredLogger, cfg *config.Config) *Router {
	selectLogger, _ := zap.NewProduction()
	return &Router{
		engine:        engine,
		auth:          auth,
		logger:        logger,
		config:        cfg,
		selectService: s3select.NewSelectService(selectLogger),
	}
}

// readLimitedBody reads request body with size limit to prevent memory exhaustion
func readLimitedBody(body io.Reader) ([]byte, error) {
	return io.ReadAll(io.LimitReader(body, maxRequestBodySize+1))
}

// isBodyTooLarge checks if the body exceeded the size limit
func isBodyTooLarge(data []byte) bool {
	return len(data) > maxRequestBodySize
}

// sanitizeHeaderValue removes potentially dangerous characters from header values
// to prevent HTTP header injection attacks
func sanitizeHeaderValue(value string) string {
	// Remove newlines and carriage returns that could enable header injection
	result := strings.ReplaceAll(value, "\r", "")
	result = strings.ReplaceAll(result, "\n", "")
	return result
}

// ServeHTTP handles S3 API requests
func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// Check for presigned URL query parameters
	if req.URL.Query().Get("X-Amz-Signature") != "" {
		// Verify presigned URL
		bucket, key, err := r.auth.VerifyPresignedURL(req)
		if err != nil {
			r.logger.Warnw("invalid presigned URL", "error", err)
			r.writeError(w, ErrSignatureDoesNotMatch)
			return
		}

		// For presigned URLs, we need to set the bucket and key properly
		// The URL path should be adjusted to include the bucket for path-style
		if bucket != "" && key != "" {
			// Update the path to include bucket/key for path-style URLs
			req.URL.Path = "/s3/" + bucket + "/" + key
		}
	}

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
			// Check for query string operations on bucket
			if req.URL.Query().Get("versioning") != "" {
				r.handleGetBucketVersioning(w, req, bucket)
			} else if req.URL.Query().Get("lifecycle") != "" {
				r.handleGetBucketLifecycle(w, req, bucket)
			} else if req.URL.Query().Get("cors") != "" {
				r.handleGetBucketCors(w, req, bucket)
			} else if req.URL.Query().Get("policy") != "" {
				r.handleGetBucketPolicy(w, req, bucket)
			} else if req.URL.Query().Get("encryption") != "" {
				r.handleGetBucketEncryption(w, req, bucket)
			} else if req.URL.Query().Get("replication") != "" {
				r.handleGetBucketReplication(w, req, bucket)
			} else if req.URL.Query().Get("tagging") != "" {
				r.handleGetBucketTags(w, req, bucket)
			} else if req.URL.Query().Get("object-lock") != "" {
				r.handleGetObjectLock(w, req, bucket)
			} else if req.URL.Query().Get("public-access-block") != "" {
				r.handleGetPublicAccessBlock(w, req, bucket)
			} else if req.URL.Query().Get("accelerate") != "" {
				r.handleGetBucketAccelerate(w, req, bucket)
			} else if req.URL.Query().Get("inventory") != "" {
				r.handleGetBucketInventory(w, req, bucket)
			} else if req.URL.Query().Get("analytics") != "" {
				r.handleGetBucketAnalytics(w, req, bucket)
			} else if req.URL.Query().Get("website") != "" {
				r.handleGetBucketWebsite(w, req, bucket)
			} else if req.URL.Query().Get("notification") != "" {
				r.handleGetBucketNotification(w, req, bucket)
			} else if req.URL.Query().Get("logging") != "" {
				r.handleGetBucketLogging(w, req, bucket)
			} else if req.URL.Query().Get("location") != "" {
				r.handleGetBucketLocation(w, req, bucket)
			} else if req.URL.Query().Get("ownership-controls") != "" {
				r.handleGetBucketOwnershipControls(w, req, bucket)
			} else if req.URL.Query().Get("metrics") != "" {
				r.handleGetBucketMetrics(w, req, bucket)
			} else if req.URL.Query().Get("acl") != "" {
				r.handleGetBucketAcl(w, req, bucket)
			} else if req.URL.Query().Get("versions") != "" {
				r.handleListObjectVersions(w, req, bucket)
			} else {
				r.handleListObjects(w, req, bucket)
			}
		} else {
			// Check for query string operations on object
			if req.URL.Query().Get("presignedurl") != "" {
				r.handleGetPresignedURL(w, req, bucket, key)
			} else if req.URL.Query().Get("acl") != "" {
				r.handleGetObjectAcl(w, req, bucket, key)
			} else if req.URL.Query().Get("tagging") != "" {
				r.handleGetObjectTags(w, req, bucket, key)
			} else if req.URL.Query().Get("retention") != "" {
				r.handleGetObjectRetention(w, req, bucket, key)
			} else if req.URL.Query().Get("legal-hold") != "" {
				r.handleGetObjectLegalHold(w, req, bucket, key)
			} else {
				r.handleGetObject(w, req, bucket, key)
			}
		}
	case http.MethodPut:
		if bucket == "" {
			r.writeError(w, ErrInvalidBucketName)
		} else if key == "" {
			// Check for query string operations on bucket
			if req.URL.Query().Get("versioning") != "" {
				r.handlePutBucketVersioning(w, req, bucket)
			} else if req.URL.Query().Get("lifecycle") != "" {
				r.handlePutBucketLifecycle(w, req, bucket)
			} else if req.URL.Query().Get("cors") != "" {
				r.handlePutBucketCors(w, req, bucket)
			} else if req.URL.Query().Get("policy") != "" {
				r.handlePutBucketPolicy(w, req, bucket)
			} else if req.URL.Query().Get("encryption") != "" {
				r.handlePutBucketEncryption(w, req, bucket)
			} else if req.URL.Query().Get("replication") != "" {
				r.handlePutBucketReplication(w, req, bucket)
			} else if req.URL.Query().Get("tagging") != "" {
				r.handlePutBucketTags(w, req, bucket)
			} else if req.URL.Query().Get("object-lock") != "" {
				r.handlePutObjectLock(w, req, bucket)
			} else if req.URL.Query().Get("public-access-block") != "" {
				r.handlePutPublicAccessBlock(w, req, bucket)
			} else if req.URL.Query().Get("accelerate") != "" {
				r.handlePutBucketAccelerate(w, req, bucket)
			} else if req.URL.Query().Get("inventory") != "" {
				r.handlePutBucketInventory(w, req, bucket)
			} else if req.URL.Query().Get("analytics") != "" {
				r.handlePutBucketAnalytics(w, req, bucket)
			} else if req.URL.Query().Get("website") != "" {
				r.handlePutBucketWebsite(w, req, bucket)
			} else if req.URL.Query().Get("notification") != "" {
				r.handlePutBucketNotification(w, req, bucket)
			} else if req.URL.Query().Get("logging") != "" {
				r.handlePutBucketLogging(w, req, bucket)
			} else if req.URL.Query().Get("location") != "" {
				r.handlePutBucketLocation(w, req, bucket)
			} else if req.URL.Query().Get("ownership-controls") != "" {
				r.handlePutBucketOwnershipControls(w, req, bucket)
			} else if req.URL.Query().Get("metrics") != "" {
				r.handlePutBucketMetrics(w, req, bucket)
			} else if req.URL.Query().Get("acl") != "" {
				r.handlePutBucketAcl(w, req, bucket)
			} else {
				r.handleCreateBucket(w, req, bucket)
			}
		} else {
			// Check for query string operations on object
			if req.URL.Query().Get("presignedurl") != "" {
				r.handlePutPresignedURL(w, req, bucket, key)
			} else if req.URL.Query().Get("acl") != "" {
				r.handlePutObjectAcl(w, req, bucket, key)
			} else if req.URL.Query().Get("tagging") != "" {
				r.handlePutObjectTags(w, req, bucket, key)
			} else if req.URL.Query().Get("retention") != "" {
				r.handlePutObjectRetention(w, req, bucket, key)
			} else if req.URL.Query().Get("legal-hold") != "" {
				r.handlePutObjectLegalHold(w, req, bucket, key)
			} else if req.Header.Get("x-amz-copy-source") != "" {
				r.handleCopyObject(w, req, bucket, key)
			} else {
				r.handlePutObject(w, req, bucket, key)
			}
		}
	case http.MethodDelete:
		if key == "" {
			if req.URL.Query().Get("inventory") != "" {
				r.handleDeleteBucketInventory(w, req, bucket)
			} else if req.URL.Query().Get("analytics") != "" {
				r.handleDeleteBucketAnalytics(w, req, bucket)
			} else if req.URL.Query().Get("website") != "" {
				r.handleDeleteBucketWebsite(w, req, bucket)
			} else if req.URL.Query().Get("policy") != "" {
				r.handleDeleteBucketPolicy(w, req, bucket)
			} else if req.URL.Query().Get("lifecycle") != "" {
				r.handleDeleteBucketLifecycle(w, req, bucket)
			} else if req.URL.Query().Get("cors") != "" {
				r.handleDeleteBucketCors(w, req, bucket)
			} else if req.URL.Query().Get("encryption") != "" {
				r.handleDeleteBucketEncryption(w, req, bucket)
			} else if req.URL.Query().Get("replication") != "" {
				r.handleDeleteBucketReplication(w, req, bucket)
			} else if req.URL.Query().Get("tagging") != "" {
				r.handleDeleteBucketTags(w, req, bucket)
			} else if req.URL.Query().Get("object-lock") != "" {
				r.handleDeleteObjectLock(w, req, bucket)
			} else if req.URL.Query().Get("public-access-block") != "" {
				r.handleDeletePublicAccessBlock(w, req, bucket)
			} else if req.URL.Query().Get("accelerate") != "" {
				r.handleDeleteBucketAccelerate(w, req, bucket)
			} else if req.URL.Query().Get("notification") != "" {
				r.handleDeleteBucketNotification(w, req, bucket)
			} else if req.URL.Query().Get("logging") != "" {
				r.handleDeleteBucketLogging(w, req, bucket)
			} else if req.URL.Query().Get("ownership-controls") != "" {
				r.handleDeleteBucketOwnershipControls(w, req, bucket)
			} else if req.URL.Query().Get("metrics") != "" {
				r.handleDeleteBucketMetrics(w, req, bucket)
			} else if req.URL.Query().Get("acl") != "" {
				r.handleDeleteBucketAcl(w, req, bucket)
			} else {
				r.handleDeleteBucket(w, req, bucket)
			}
		} else {
			// Check for query string operations on object
			if req.URL.Query().Get("tagging") != "" {
				r.handleDeleteObjectTags(w, req, bucket, key)
			} else {
				r.handleDeleteObject(w, req, bucket, key)
			}
		}
	case http.MethodHead:
		if bucket != "" && key != "" {
			r.handleHeadObject(w, req, bucket, key)
		} else if bucket != "" {
			r.handleHeadBucket(w, req, bucket)
		} else {
			r.writeError(w, ErrNotImplemented)
		}
	case http.MethodPost:
		// Handle post to bucket/key (S3 Select)
		if bucket != "" && key != "" && req.URL.Query().Get("select") != "" {
			r.handleSelectObjectContent(w, req, bucket, key)
			return
		}
		// Handle post to bucket/key (Restore Object)
		if bucket != "" && key != "" && req.URL.Query().Get("restore") != "" {
			r.handleRestoreObject(w, req, bucket, key)
			return
		}
		// Handle post to bucket
		if bucket != "" && key == "" {
			// Check for query string operations
			if req.URL.Query().Get("delete") != "" {
				r.handleDeleteObjects(w, req, bucket)
				return
			}
			// Handle list multipart uploads
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

// handleListObjectVersions handles ListObjectVersions (GET /bucket?versions)
func (r *Router) handleListObjectVersions(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	prefix := req.URL.Query().Get("prefix")
	delimiter := req.URL.Query().Get("delimiter")
	maxKeys := parseInt(req.URL.Query().Get("max-keys"), 1000)

	objects, err := r.engine.ListObjects(ctx, bucket, engine.ListObjectsOptions{
		Prefix:    prefix,
		Delimiter: delimiter,
		MaxKeys:   maxKeys,
	})
	if err != nil {
		r.logger.Warnw("failed to list object versions", "bucket", bucket, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	// Build XML response - same as ListObjects for now
	// In a full implementation, this would include version IDs
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)

	var xmlResult string
	if len(objects.Objects) == 0 {
		xmlResult = fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>%s</Name>
  <Prefix>%s</Prefix>
  <KeyMarker></KeyMarker>
  <VersionIdMarker></VersionIdMarker>
  <MaxKeys>%d</MaxKeys>
  <IsTruncated>false</IsTruncated>
</ListVersionsResult>`, bucket, prefix, maxKeys)
	} else {
		var contents string
		for _, obj := range objects.Objects {
			contents += fmt.Sprintf(`<Version>
    <Key>%s</Key>
    <VersionId>%s</VersionId>
    <IsLatest>%v</IsLatest>
    <LastModified>%s</LastModified>
    <ETag>%s</ETag>
    <Size>%d</Size>
  </Version>`,
				tags.EscapeXML(obj.Key),
				obj.VersionID,
				obj.IsLatest,
				time.Unix(obj.LastModified, 0).Format(time.RFC3339),
				obj.ETag,
				obj.Size)
		}
		xmlResult = fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>%s</Name>
  <Prefix>%s</Prefix>
  <KeyMarker></KeyMarker>
  <VersionIdMarker></VersionIdMarker>
  <MaxKeys>%d</MaxKeys>
  <IsTruncated>false</IsTruncated>
  %s
</ListVersionsResult>`, bucket, prefix, maxKeys, contents)
	}

	w.Write([]byte(xmlResult))
	s3RequestsTotal.WithLabelValues("ListObjectVersions", "200").Inc()
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

	// Set headers (sanitize user-controlled values to prevent header injection)
	w.Header().Set("Content-Type", sanitizeHeaderValue(obj.ContentType))
	w.Header().Set("Content-Length", fmt.Sprintf("%d", obj.Size))
	w.Header().Set("ETag", sanitizeHeaderValue(obj.ETag))

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

	w.Header().Set("Content-Type", sanitizeHeaderValue(meta.ContentType))
	w.Header().Set("Content-Length", fmt.Sprintf("%d", meta.Size))
	w.Header().Set("ETag", sanitizeHeaderValue(meta.ETag))
	w.WriteHeader(http.StatusOK)

	s3RequestsTotal.WithLabelValues("HeadObject", "200").Inc()
}

// handleHeadBucket handles HeadBucket - checks if bucket exists
func (r *Router) handleHeadBucket(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	// Check if bucket exists
	err := r.engine.HeadBucket(ctx, bucket)
	if err != nil {
		r.logger.Warnw("failed to head bucket", "bucket", bucket, "error", err)
		r.writeError(w, ErrNoSuchBucket)
		return
	}

	// Set common headers
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("x-amz-bucket-region", "us-east-1")
	w.WriteHeader(http.StatusOK)

	s3RequestsTotal.WithLabelValues("HeadBucket", "200").Inc()
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
	w.Header().Set("ETag", sanitizeHeaderValue(result.ETag))
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

// handleCopyObject handles CopyObject (PUT with x-amz-copy-source)
func (r *Router) handleCopyObject(w http.ResponseWriter, req *http.Request, bucket, key string) {
	ctx := req.Context()

	// Parse the copy source header: /bucket/key
	copySource := req.Header.Get("x-amz-copy-source")
	if copySource == "" {
		r.writeError(w, ErrInvalidArgument)
		return
	}

	// Remove leading slash if present
	copySource = strings.TrimPrefix(copySource, "/")

	// Parse source bucket and key
	parts := strings.SplitN(copySource, "/", 2)
	if len(parts) != 2 {
		r.writeError(w, ErrInvalidArgument)
		return
	}

	srcBucket := parts[0]
	srcKey := parts[1]

	// Perform the copy
	result, err := r.engine.CopyObject(ctx, srcBucket, srcKey, bucket, key)
	if err != nil {
		r.logger.Warnw("failed to copy object", "srcBucket", srcBucket, "srcKey", srcKey, "dstBucket", bucket, "dstKey", key, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	// Return S3 CopyObject result
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)

	response := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<CopyObjectResult>
  <LastModified>%s</LastModified>
  <ETag>%s</ETag>
</CopyObjectResult>`,
		time.Unix(result.LastModified, 0).Format(time.RFC3339),
		result.ETag)

	w.Write([]byte(response))
	s3RequestsTotal.WithLabelValues("CopyObject", "200").Inc()
}

// handleGetObjectAcl handles GET /bucket/key?acl
func (r *Router) handleGetObjectAcl(w http.ResponseWriter, req *http.Request, bucket, key string) {
	ctx := req.Context()

	// Check if object exists
	_, err := r.engine.GetObject(ctx, bucket, key, engine.GetObjectOptions{})
	if err != nil {
		r.logger.Warnw("object not found for ACL", "bucket", bucket, "key", key, "error", err)
		r.writeError(w, ErrNoSuchKey)
		return
	}

	// Return default ACL (owner full control)
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)

	aclResponse := `<?xml version="1.0" encoding"?>
<AccessControlPolicy="UTF-8 xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Owner>
    <ID>owner</ID>
    <DisplayName>Owner</DisplayName>
  </Owner>
  <AccessControlList>
    <Grant>
      <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
        <ID>owner</ID>
        <DisplayName>Owner</DisplayName>
      </Grantee>
      <Permission>FULL_CONTROL</Permission>
    </Grant>
  </AccessControlList>
</AccessControlPolicy>`

	w.Write([]byte(aclResponse))
	s3RequestsTotal.WithLabelValues("GetObjectAcl", "200").Inc()
}

// handlePutObjectAcl handles PUT /bucket/key?acl
func (r *Router) handlePutObjectAcl(w http.ResponseWriter, req *http.Request, bucket, key string) {
	ctx := req.Context()

	// Check if object exists
	_, err := r.engine.GetObject(ctx, bucket, key, engine.GetObjectOptions{})
	if err != nil {
		r.logger.Warnw("object not found for ACL", "bucket", bucket, "key", key, "error", err)
		r.writeError(w, ErrNoSuchKey)
		return
	}

	// For now, just acknowledge the ACL was set
	// In a full implementation, we'd parse and store the ACL
	w.WriteHeader(http.StatusOK)
	s3RequestsTotal.WithLabelValues("PutObjectAcl", "200").Inc()
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

	w.Header().Set("ETag", sanitizeHeaderValue(result.ETag))
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
	w.Header().Set("ETag", sanitizeHeaderValue(result.ETag))
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

// handleGetBucketVersioning handles GET /bucket?versioning
func (r *Router) handleGetBucketVersioning(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	versioning, err := r.engine.GetBucketVersioning(ctx, bucket)
	if err != nil {
		r.logger.Warnw("failed to get bucket versioning", "bucket", bucket, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)

	status := ""
	if versioning != nil && versioning.Status == "Enabled" {
		status = "Enabled"
	}

	resp := s3types.GetBucketVersioningOutput{
		Status: status,
	}
	xmlBytes, _ := xml.Marshal(resp)
	w.Write(xmlBytes)

	s3RequestsTotal.WithLabelValues("GetBucketVersioning", "200").Inc()
}

// handlePutBucketVersioning handles PUT /bucket?versioning
func (r *Router) handlePutBucketVersioning(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	// Read body with size limit
	body, err := readLimitedBody(req.Body)
	if err != nil {
		r.logger.Warnw("failed to read request body", "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	var input s3types.PutBucketVersioningInput
	if err := xml.Unmarshal(body, &input); err != nil {
		r.logger.Warnw("failed to parse versioning input", "error", err)
		r.writeError(w, ErrMalformedXML)
		return
	}

	// Set versioning
	versioning := &metadata.BucketVersioning{
		Status: input.Status,
	}

	if err := r.engine.PutBucketVersioning(ctx, bucket, versioning); err != nil {
		r.logger.Warnw("failed to set bucket versioning", "bucket", bucket, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	w.WriteHeader(http.StatusOK)
	s3RequestsTotal.WithLabelValues("PutBucketVersioning", "200").Inc()
}

// handleGetBucketLifecycle handles GET /bucket?lifecycle
func (r *Router) handleGetBucketLifecycle(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	rules, err := r.engine.GetBucketLifecycle(ctx, bucket)
	if err != nil {
		r.logger.Warnw("failed to get bucket lifecycle", "bucket", bucket, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)

	// Convert metadata rules to s3types rules
	s3Rules := make([]s3types.LifecycleRule, len(rules))
	for i, rule := range rules {
		s3Rules[i] = s3types.LifecycleRule{
			ID:     rule.ID,
			Status: rule.Status,
		}
		if rule.Expiration != nil && rule.Expiration.Days > 0 {
			s3Rules[i].Expiration = &s3types.Expiration{
				Days: rule.Expiration.Days,
			}
		}
		if rule.NoncurrentVersionExpiration != nil && rule.NoncurrentVersionExpiration.NoncurrentDays > 0 {
			s3Rules[i].NoncurrentVersionExpiration = &s3types.NoncurrentVersionExpiration{
				NoncurrentDays: rule.NoncurrentVersionExpiration.NoncurrentDays,
			}
		}
	}

	resp := s3types.GetBucketLifecycleOutput{
		Rules: s3Rules,
	}
	xmlBytes, _ := xml.Marshal(resp)
	w.Write(xmlBytes)

	s3RequestsTotal.WithLabelValues("GetBucketLifecycle", "200").Inc()
}

// handlePutBucketLifecycle handles PUT /bucket?lifecycle
func (r *Router) handlePutBucketLifecycle(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	// Read body with size limit
	body, err := readLimitedBody(req.Body)
	if err != nil {
		r.logger.Warnw("failed to read request body", "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	var input s3types.PutBucketLifecycleInput
	if err := xml.Unmarshal(body, &input); err != nil {
		r.logger.Warnw("failed to parse lifecycle input", "error", err)
		r.writeError(w, ErrMalformedXML)
		return
	}

	// Convert s3types rules to metadata rules
	rules := make([]metadata.LifecycleRule, len(input.Rules))
	for i, rule := range input.Rules {
		rules[i] = metadata.LifecycleRule{
			ID:     rule.ID,
			Status: rule.Status,
		}
		if rule.Expiration != nil {
			rules[i].Expiration = &metadata.Expiration{
				Days: int(rule.Expiration.Days),
			}
		}
		if rule.NoncurrentVersionExpiration != nil {
			rules[i].NoncurrentVersionExpiration = &metadata.NoncurrentVersionExpiration{
				NoncurrentDays: int(rule.NoncurrentVersionExpiration.NoncurrentDays),
			}
		}
	}

	if err := r.engine.PutBucketLifecycle(ctx, bucket, rules); err != nil {
		r.logger.Warnw("failed to set bucket lifecycle", "bucket", bucket, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	w.WriteHeader(http.StatusOK)
	s3RequestsTotal.WithLabelValues("PutBucketLifecycle", "200").Inc()
}

// handleGetBucketCors handles GET /bucket?cors
func (r *Router) handleGetBucketCors(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	cors, err := r.engine.GetBucketCors(ctx, bucket)
	if err != nil {
		r.logger.Warnw("failed to get bucket cors", "bucket", bucket, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	// Return CORS configuration or empty
	if cors == nil {
		cors = &metadata.CORSConfiguration{}
	}

	r.writeXML(w, http.StatusOK, cors)
	s3RequestsTotal.WithLabelValues("GetBucketCors", "200").Inc()
}

// handlePutBucketCors handles PUT /bucket?cors
func (r *Router) handlePutBucketCors(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	// Read body with size limit
	body, err := readLimitedBody(req.Body)
	if err != nil {
		r.logger.Warnw("failed to read request body", "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	// Parse CORS configuration
	var cors metadata.CORSConfiguration
	if err := xml.Unmarshal(body, &cors); err != nil {
		r.logger.Warnw("failed to parse CORS configuration", "error", err)
		r.writeError(w, ErrInvalidRequest)
		return
	}

	// Store CORS configuration
	if err := r.engine.PutBucketCors(ctx, bucket, &cors); err != nil {
		r.logger.Warnw("failed to set bucket cors", "bucket", bucket, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	w.WriteHeader(http.StatusOK)
	s3RequestsTotal.WithLabelValues("PutBucketCors", "200").Inc()
}

// handleGetBucketPolicy handles GET /bucket?policy
func (r *Router) handleGetBucketPolicy(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	policy, err := r.engine.GetBucketPolicy(ctx, bucket)
	if err != nil {
		r.logger.Warnw("failed to get bucket policy", "bucket", bucket, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	// Return policy or empty JSON object
	if policy == nil || *policy == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("{}"))
		s3RequestsTotal.WithLabelValues("GetBucketPolicy", "200").Inc()
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(*policy))
	s3RequestsTotal.WithLabelValues("GetBucketPolicy", "200").Inc()
}

// handlePutBucketPolicy handles PUT /bucket?policy
func (r *Router) handlePutBucketPolicy(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	// Read body with size limit
	body, err := readLimitedBody(req.Body)
	if err != nil {
		r.logger.Warnw("failed to read request body", "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	// Validate JSON policy
	var policyJSON map[string]interface{}
	if err := json.Unmarshal(body, &policyJSON); err != nil {
		r.logger.Warnw("failed to parse bucket policy", "error", err)
		r.writeError(w, ErrInvalidRequest)
		return
	}

	// Store policy
	policyStr := string(body)
	if err := r.engine.PutBucketPolicy(ctx, bucket, &policyStr); err != nil {
		r.logger.Warnw("failed to set bucket policy", "bucket", bucket, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	w.WriteHeader(http.StatusOK)
	s3RequestsTotal.WithLabelValues("PutBucketPolicy", "200").Inc()
}

// handleGetBucketEncryption handles GET /bucket?encryption
func (r *Router) handleGetBucketEncryption(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	encryption, err := r.engine.GetBucketEncryption(ctx, bucket)
	if err != nil {
		r.logger.Warnw("failed to get bucket encryption", "bucket", bucket, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	// Return encryption configuration or empty
	if encryption == nil {
		encryption = &metadata.BucketEncryption{}
	}

	r.writeXML(w, http.StatusOK, encryption)
	s3RequestsTotal.WithLabelValues("GetBucketEncryption", "200").Inc()
}

// handlePutBucketEncryption handles PUT /bucket?encryption
func (r *Router) handlePutBucketEncryption(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	// Read body with size limit
	body, err := readLimitedBody(req.Body)
	if err != nil {
		r.logger.Warnw("failed to read request body", "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	// Parse encryption configuration
	var encryption metadata.BucketEncryption
	if err := xml.Unmarshal(body, &encryption); err != nil {
		r.logger.Warnw("failed to parse encryption configuration", "error", err)
		r.writeError(w, ErrMalformedXML)
		return
	}

	// Store encryption configuration
	if err := r.engine.PutBucketEncryption(ctx, bucket, &encryption); err != nil {
		r.logger.Warnw("failed to set bucket encryption", "bucket", bucket, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	w.WriteHeader(http.StatusOK)
	s3RequestsTotal.WithLabelValues("PutBucketEncryption", "200").Inc()
}

// handleGetBucketTags handles GET /bucket?tagging
func (r *Router) handleGetBucketTags(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	tags, err := r.engine.GetBucketTags(ctx, bucket)
	if err != nil {
		r.logger.Warnw("failed to get bucket tags", "bucket", bucket, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	// Return tags or empty map
	if tags == nil {
		tags = make(map[string]string)
	}

	// Format as XML response
	type Tag struct {
		Key   string `xml:"Key"`
		Value string `xml:"Value"`
	}
	type TagSet struct {
		Tag []Tag `xml:"Tag"`
	}
	type TaggingResponse struct {
		XMLName xml.Name `xml:"Tagging"`
		TagSet  TagSet  `xml:"TagSet"`
	}

	response := TaggingResponse{
		TagSet: TagSet{
			Tag: make([]Tag, 0),
		},
	}
	for k, v := range tags {
		response.TagSet.Tag = append(response.TagSet.Tag, Tag{Key: k, Value: v})
	}

	r.writeXML(w, http.StatusOK, response)
	s3RequestsTotal.WithLabelValues("GetBucketTags", "200").Inc()
}

// handlePutBucketTags handles PUT /bucket?tagging
func (r *Router) handlePutBucketTags(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	// Read body with size limit
	body, err := readLimitedBody(req.Body)
	if err != nil {
		r.logger.Warnw("failed to read request body", "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	// Parse tagging XML
	type Tag struct {
		Key   string `xml:"Key"`
		Value string `xml:"Value"`
	}
	type TagSet struct {
		Tag []Tag `xml:"Tag"`
	}
	type TaggingInput struct {
		TagSet TagSet `xml:"TagSet"`
	}

	var input TaggingInput
	if err := xml.Unmarshal(body, &input); err != nil {
		r.logger.Warnw("failed to parse tagging input", "error", err)
		r.writeError(w, ErrMalformedXML)
		return
	}

	// Convert to map
	tags := make(map[string]string)
	for _, t := range input.TagSet.Tag {
		tags[t.Key] = t.Value
	}

	// Store tags
	if err := r.engine.PutBucketTags(ctx, bucket, tags); err != nil {
		r.logger.Warnw("failed to set bucket tags", "bucket", bucket, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	w.WriteHeader(http.StatusOK)
	s3RequestsTotal.WithLabelValues("PutBucketTags", "200").Inc()
}

// handleGetObjectLock handles GET /bucket?object-lock
func (r *Router) handleGetObjectLock(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	config, err := r.engine.GetObjectLock(ctx, bucket)
	if err != nil {
		r.logger.Warnw("failed to get object lock", "bucket", bucket, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	// Return configuration or empty
	if config == nil {
		config = &metadata.ObjectLockConfig{}
	}

	r.writeXML(w, http.StatusOK, config)
	s3RequestsTotal.WithLabelValues("GetObjectLock", "200").Inc()
}

// handlePutObjectLock handles PUT /bucket?object-lock
func (r *Router) handlePutObjectLock(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	// Read body with size limit
	body, err := readLimitedBody(req.Body)
	if err != nil {
		r.logger.Warnw("failed to read request body", "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	// Parse object lock configuration
	var config metadata.ObjectLockConfig
	if err := xml.Unmarshal(body, &config); err != nil {
		r.logger.Warnw("failed to parse object lock configuration", "error", err)
		r.writeError(w, ErrMalformedXML)
		return
	}

	// Store configuration
	if err := r.engine.PutObjectLock(ctx, bucket, &config); err != nil {
		r.logger.Warnw("failed to set object lock", "bucket", bucket, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	w.WriteHeader(http.StatusOK)
	s3RequestsTotal.WithLabelValues("PutObjectLock", "200").Inc()
}

// handleGetPublicAccessBlock handles GET /bucket?public-access-block
func (r *Router) handleGetPublicAccessBlock(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	config, err := r.engine.GetPublicAccessBlock(ctx, bucket)
	if err != nil {
		r.logger.Warnw("failed to get public access block", "bucket", bucket, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	// Return configuration or empty
	if config == nil {
		config = &metadata.PublicAccessBlockConfiguration{}
	}

	// Wrap in XML response
	type PublicAccessBlockConfiguration struct {
		XMLName                      xml.Name `xml:"PublicAccessBlockConfiguration"`
		BlockPublicAcls       bool   `xml:"BlockPublicAcls"`
		BlockPublicPolicy     bool   `xml:"BlockPublicPolicy"`
		IgnorePublicAcls      bool   `xml:"IgnorePublicAcls"`
		RestrictPublicBuckets bool   `xml:"RestrictPublicBuckets"`
	}

	response := PublicAccessBlockConfiguration{
		BlockPublicAcls:       config.BlockPublicAcls,
		BlockPublicPolicy:     config.BlockPublicPolicy,
		IgnorePublicAcls:      config.IgnorePublicAcls,
		RestrictPublicBuckets: config.RestrictPublicBuckets,
	}

	r.writeXML(w, http.StatusOK, response)
	s3RequestsTotal.WithLabelValues("GetPublicAccessBlock", "200").Inc()
}

// handlePutPublicAccessBlock handles PUT /bucket?public-access-block
func (r *Router) handlePutPublicAccessBlock(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	// Read body with size limit
	body, err := readLimitedBody(req.Body)
	if err != nil {
		r.logger.Warnw("failed to read request body", "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	// Parse public access block configuration
	var config metadata.PublicAccessBlockConfiguration
	if err := xml.Unmarshal(body, &config); err != nil {
		r.logger.Warnw("failed to parse public access block configuration", "error", err)
		r.writeError(w, ErrMalformedXML)
		return
	}

	// Store configuration
	if err := r.engine.PutPublicAccessBlock(ctx, bucket, &config); err != nil {
		r.logger.Warnw("failed to set public access block", "bucket", bucket, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	w.WriteHeader(http.StatusOK)
	s3RequestsTotal.WithLabelValues("PutPublicAccessBlock", "200").Inc()
}

// handleGetBucketAccelerate handles GET /bucket?accelerate
func (r *Router) handleGetBucketAccelerate(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	config, err := r.engine.GetBucketAccelerate(ctx, bucket)
	if err != nil {
		r.logger.Warnw("failed to get bucket accelerate", "bucket", bucket, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	// Return configuration or empty
	if config == nil {
		config = &metadata.BucketAccelerateConfiguration{}
	}

	r.writeXML(w, http.StatusOK, config)
	s3RequestsTotal.WithLabelValues("GetBucketAccelerate", "200").Inc()
}

// handlePutBucketAccelerate handles PUT /bucket?accelerate
func (r *Router) handlePutBucketAccelerate(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	// Read body with size limit
	body, err := readLimitedBody(req.Body)
	if err != nil {
		r.logger.Warnw("failed to read request body", "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	// Parse accelerate configuration
	var config metadata.BucketAccelerateConfiguration
	if err := xml.Unmarshal(body, &config); err != nil {
		r.logger.Warnw("failed to parse accelerate configuration", "error", err)
		r.writeError(w, ErrMalformedXML)
		return
	}

	// Validate status
	if config.Status != "Enabled" && config.Status != "Suspended" && config.Status != "" {
		r.logger.Warnw("invalid accelerate status", "status", config.Status)
		r.writeError(w, ErrInvalidAccelerateConfiguration)
		return
	}

	// Store configuration
	if err := r.engine.PutBucketAccelerate(ctx, bucket, &config); err != nil {
		r.logger.Warnw("failed to set bucket accelerate", "bucket", bucket, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	w.WriteHeader(http.StatusOK)
	s3RequestsTotal.WithLabelValues("PutBucketAccelerate", "200").Inc()
}

// handleGetBucketInventory handles GET /bucket?inventory
func (r *Router) handleGetBucketInventory(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	inventoryID := req.URL.Query().Get("inventory-id")

	// If inventory ID is provided, get specific inventory
	if inventoryID != "" {
		config, err := r.engine.GetBucketInventory(ctx, bucket, inventoryID)
		if err != nil {
			r.logger.Warnw("failed to get bucket inventory", "bucket", bucket, "id", inventoryID, "error", err)
			r.writeError(w, ErrInternal)
			return
		}

		if config == nil {
			r.writeError(w, ErrInventoryNotFound)
			return
		}

		r.writeXML(w, http.StatusOK, config)
		s3RequestsTotal.WithLabelValues("GetBucketInventory", "200").Inc()
		return
	}

	// List all inventories
	configs, err := r.engine.ListBucketInventory(ctx, bucket)
	if err != nil {
		r.logger.Warnw("failed to list bucket inventory", "bucket", bucket, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	// Return as list
	type InventoryList struct {
		XMLName     xml.Name                   `xml:"ListInventoryConfigurationsResult"`
		Configurations []metadata.InventoryConfiguration `xml:"InventoryConfiguration"`
	}

	response := InventoryList{
		Configurations: configs,
	}

	r.writeXML(w, http.StatusOK, response)
	s3RequestsTotal.WithLabelValues("ListBucketInventory", "200").Inc()
}

// handlePutBucketInventory handles PUT /bucket?inventory
func (r *Router) handlePutBucketInventory(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	inventoryID := req.URL.Query().Get("inventory-id")
	if inventoryID == "" {
		r.writeError(w, ErrInvalidRequest)
		return
	}

	// Read body with size limit
	body, err := readLimitedBody(req.Body)
	if err != nil {
		r.logger.Warnw("failed to read request body", "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	// Parse inventory configuration
	var config metadata.InventoryConfiguration
	if err := xml.Unmarshal(body, &config); err != nil {
		r.logger.Warnw("failed to parse inventory configuration", "error", err)
		r.writeError(w, ErrMalformedXML)
		return
	}

	// Set the ID from the query parameter
	config.ID = inventoryID

	// Store configuration
	if err := r.engine.PutBucketInventory(ctx, bucket, inventoryID, &config); err != nil {
		r.logger.Warnw("failed to set bucket inventory", "bucket", bucket, "id", inventoryID, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	w.WriteHeader(http.StatusOK)
	s3RequestsTotal.WithLabelValues("PutBucketInventory", "200").Inc()
}

// handleDeleteBucketInventory handles DELETE /bucket?inventory
func (r *Router) handleDeleteBucketInventory(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	inventoryID := req.URL.Query().Get("inventory-id")
	if inventoryID == "" {
		r.writeError(w, ErrInvalidRequest)
		return
	}

	// Delete configuration
	if err := r.engine.DeleteBucketInventory(ctx, bucket, inventoryID); err != nil {
		r.logger.Warnw("failed to delete bucket inventory", "bucket", bucket, "id", inventoryID, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	w.WriteHeader(http.StatusNoContent)
	s3RequestsTotal.WithLabelValues("DeleteBucketInventory", "204").Inc()
}

// handleGetBucketAnalytics handles GET /bucket?analytics
func (r *Router) handleGetBucketAnalytics(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	analyticsID := req.URL.Query().Get("analytics-id")

	// If analytics ID is provided, get specific analytics
	if analyticsID != "" {
		config, err := r.engine.GetBucketAnalytics(ctx, bucket, analyticsID)
		if err != nil {
			r.logger.Warnw("failed to get bucket analytics", "bucket", bucket, "id", analyticsID, "error", err)
			r.writeError(w, ErrInternal)
			return
		}

		if config == nil {
			r.writeError(w, ErrAnalyticsNotFound)
			return
		}

		r.writeXML(w, http.StatusOK, config)
		s3RequestsTotal.WithLabelValues("GetBucketAnalytics", "200").Inc()
		return
	}

	// List all analytics
	configs, err := r.engine.ListBucketAnalytics(ctx, bucket)
	if err != nil {
		r.logger.Warnw("failed to list bucket analytics", "bucket", bucket, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	// Return as list
	type AnalyticsList struct {
		XMLName     xml.Name                     `xml:"ListAnalyticsConfigurationsResult"`
		Configurations []metadata.AnalyticsConfiguration `xml:"AnalyticsConfiguration"`
	}

	response := AnalyticsList{
		Configurations: configs,
	}

	r.writeXML(w, http.StatusOK, response)
	s3RequestsTotal.WithLabelValues("ListBucketAnalytics", "200").Inc()
}

// handlePutBucketAnalytics handles PUT /bucket?analytics
func (r *Router) handlePutBucketAnalytics(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	analyticsID := req.URL.Query().Get("analytics-id")
	if analyticsID == "" {
		r.writeError(w, ErrInvalidRequest)
		return
	}

	// Read body with size limit
	body, err := readLimitedBody(req.Body)
	if err != nil {
		r.logger.Warnw("failed to read request body", "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	// Parse analytics configuration
	var config metadata.AnalyticsConfiguration
	if err := xml.Unmarshal(body, &config); err != nil {
		r.logger.Warnw("failed to parse analytics configuration", "error", err)
		r.writeError(w, ErrMalformedXML)
		return
	}

	// Set the ID from the query parameter
	config.ID = analyticsID

	// Store configuration
	if err := r.engine.PutBucketAnalytics(ctx, bucket, analyticsID, &config); err != nil {
		r.logger.Warnw("failed to set bucket analytics", "bucket", bucket, "id", analyticsID, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	w.WriteHeader(http.StatusOK)
	s3RequestsTotal.WithLabelValues("PutBucketAnalytics", "200").Inc()
}

// handleDeleteBucketAnalytics handles DELETE /bucket?analytics
func (r *Router) handleDeleteBucketAnalytics(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	analyticsID := req.URL.Query().Get("analytics-id")
	if analyticsID == "" {
		r.writeError(w, ErrInvalidRequest)
		return
	}

	// Delete configuration
	if err := r.engine.DeleteBucketAnalytics(ctx, bucket, analyticsID); err != nil {
		r.logger.Warnw("failed to delete bucket analytics", "bucket", bucket, "id", analyticsID, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	w.WriteHeader(http.StatusNoContent)
	s3RequestsTotal.WithLabelValues("DeleteBucketAnalytics", "204").Inc()
}

// handleGetBucketWebsite handles GET /bucket?website
func (r *Router) handleGetBucketWebsite(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	config, err := r.engine.GetBucketWebsite(ctx, bucket)
	if err != nil {
		r.logger.Warnw("failed to get bucket website", "bucket", bucket, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	if config == nil {
		r.writeError(w, ErrWebsiteNotFound)
		return
	}

	r.writeXML(w, http.StatusOK, config)
	s3RequestsTotal.WithLabelValues("GetBucketWebsite", "200").Inc()
}

// handlePutBucketWebsite handles PUT /bucket?website
func (r *Router) handlePutBucketWebsite(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	// Read body with size limit
	body, err := readLimitedBody(req.Body)
	if err != nil {
		r.logger.Warnw("failed to read request body", "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	// Parse website configuration
	var config metadata.WebsiteConfiguration
	if err := xml.Unmarshal(body, &config); err != nil {
		r.logger.Warnw("failed to parse website configuration", "error", err)
		r.writeError(w, ErrMalformedXML)
		return
	}

	// Save configuration
	if err := r.engine.PutBucketWebsite(ctx, bucket, &config); err != nil {
		r.logger.Warnw("failed to save bucket website", "bucket", bucket, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	w.WriteHeader(http.StatusOK)
	s3RequestsTotal.WithLabelValues("PutBucketWebsite", "200").Inc()
}

// handleDeleteBucketWebsite handles DELETE /bucket?website
func (r *Router) handleDeleteBucketWebsite(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	// Delete configuration
	if err := r.engine.DeleteBucketWebsite(ctx, bucket); err != nil {
		r.logger.Warnw("failed to delete bucket website", "bucket", bucket, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	w.WriteHeader(http.StatusNoContent)
	s3RequestsTotal.WithLabelValues("DeleteBucketWebsite", "204").Inc()
}

// handleDeleteBucketPolicy handles DELETE /bucket?policy
func (r *Router) handleDeleteBucketPolicy(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	if err := r.engine.DeleteBucketPolicy(ctx, bucket); err != nil {
		r.logger.Warnw("failed to delete bucket policy", "bucket", bucket, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	w.WriteHeader(http.StatusNoContent)
	s3RequestsTotal.WithLabelValues("DeleteBucketPolicy", "204").Inc()
}

// handleDeleteBucketLifecycle handles DELETE /bucket?lifecycle
func (r *Router) handleDeleteBucketLifecycle(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	// Delete all lifecycle rules by passing empty slice
	if err := r.engine.PutBucketLifecycle(ctx, bucket, nil); err != nil {
		r.logger.Warnw("failed to delete bucket lifecycle", "bucket", bucket, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	w.WriteHeader(http.StatusNoContent)
	s3RequestsTotal.WithLabelValues("DeleteBucketLifecycle", "204").Inc()
}

// handleDeleteBucketCors handles DELETE /bucket?cors
func (r *Router) handleDeleteBucketCors(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	if err := r.engine.DeleteBucketCors(ctx, bucket); err != nil {
		r.logger.Warnw("failed to delete bucket cors", "bucket", bucket, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	w.WriteHeader(http.StatusNoContent)
	s3RequestsTotal.WithLabelValues("DeleteBucketCors", "204").Inc()
}

// handleDeleteBucketEncryption handles DELETE /bucket?encryption
func (r *Router) handleDeleteBucketEncryption(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	if err := r.engine.DeleteBucketEncryption(ctx, bucket); err != nil {
		r.logger.Warnw("failed to delete bucket encryption", "bucket", bucket, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	w.WriteHeader(http.StatusNoContent)
	s3RequestsTotal.WithLabelValues("DeleteBucketEncryption", "204").Inc()
}

// handleDeleteBucketTags handles DELETE /bucket?tagging
func (r *Router) handleDeleteBucketTags(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	if err := r.engine.DeleteBucketTags(ctx, bucket); err != nil {
		r.logger.Warnw("failed to delete bucket tags", "bucket", bucket, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	w.WriteHeader(http.StatusNoContent)
	s3RequestsTotal.WithLabelValues("DeleteBucketTags", "204").Inc()
}

// handleDeleteObjectLock handles DELETE /bucket?object-lock
func (r *Router) handleDeleteObjectLock(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	if err := r.engine.DeleteObjectLock(ctx, bucket); err != nil {
		r.logger.Warnw("failed to delete object lock", "bucket", bucket, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	w.WriteHeader(http.StatusNoContent)
	s3RequestsTotal.WithLabelValues("DeleteObjectLock", "204").Inc()
}

// handleGetObjectRetention handles GET /object?retention
func (r *Router) handleGetObjectRetention(w http.ResponseWriter, req *http.Request, bucket, key string) {
	ctx := req.Context()

	retention, err := r.engine.GetObjectRetention(ctx, bucket, key)
	if err != nil {
		r.logger.Warnw("failed to get object retention", "bucket", bucket, "key", key, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	if retention == nil {
		r.writeError(w, ErrObjectRetentionNotFound)
		return
	}

	data, err := xml.Marshal(retention)
	if err != nil {
		r.writeError(w, ErrInternal)
		return
	}

	header := `<?xml version="1.0" encoding="UTF-8"?>`
	xmlResponse := header + "\n" + string(data)

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(xmlResponse))
	s3RequestsTotal.WithLabelValues("GetObjectRetention", "200").Inc()
}

// handlePutObjectRetention handles PUT /object?retention
func (r *Router) handlePutObjectRetention(w http.ResponseWriter, req *http.Request, bucket, key string) {
	ctx := req.Context()

	body, err := io.ReadAll(req.Body)
	if err != nil {
		r.logger.Warnw("failed to read request body", "error", err)
		r.writeError(w, ErrInternal)
		return
	}
	defer req.Body.Close()

	var retention metadata.ObjectRetention
	if err := xml.Unmarshal(body, &retention); err != nil {
		r.logger.Warnw("failed to parse retention", "error", err)
		r.writeError(w, ErrMalformedXML)
		return
	}

	if err := r.engine.PutObjectRetention(ctx, bucket, key, &retention); err != nil {
		r.logger.Warnw("failed to put object retention", "bucket", bucket, "key", key, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	w.WriteHeader(http.StatusOK)
	s3RequestsTotal.WithLabelValues("PutObjectRetention", "200").Inc()
}

// handleGetObjectLegalHold handles GET /object?legal-hold
func (r *Router) handleGetObjectLegalHold(w http.ResponseWriter, req *http.Request, bucket, key string) {
	ctx := req.Context()

	legalHold, err := r.engine.GetObjectLegalHold(ctx, bucket, key)
	if err != nil {
		r.logger.Warnw("failed to get object legal hold", "bucket", bucket, "key", key, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	if legalHold == nil {
		r.writeError(w, ErrObjectLegalHoldNotFound)
		return
	}

	data, err := xml.Marshal(legalHold)
	if err != nil {
		r.writeError(w, ErrInternal)
		return
	}

	header := `<?xml version="1.0" encoding="UTF-8"?>`
	xmlResponse := header + "\n" + string(data)

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(xmlResponse))
	s3RequestsTotal.WithLabelValues("GetObjectLegalHold", "200").Inc()
}

// handlePutObjectLegalHold handles PUT /object?legal-hold
func (r *Router) handlePutObjectLegalHold(w http.ResponseWriter, req *http.Request, bucket, key string) {
	ctx := req.Context()

	body, err := io.ReadAll(req.Body)
	if err != nil {
		r.logger.Warnw("failed to read request body", "error", err)
		r.writeError(w, ErrInternal)
		return
	}
	defer req.Body.Close()

	var legalHold metadata.ObjectLegalHold
	if err := xml.Unmarshal(body, &legalHold); err != nil {
		r.logger.Warnw("failed to parse legal hold", "error", err)
		r.writeError(w, ErrMalformedXML)
		return
	}

	if err := r.engine.PutObjectLegalHold(ctx, bucket, key, &legalHold); err != nil {
		r.logger.Warnw("failed to put object legal hold", "bucket", bucket, "key", key, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	w.WriteHeader(http.StatusOK)
	s3RequestsTotal.WithLabelValues("PutObjectLegalHold", "200").Inc()
}

// handleDeletePublicAccessBlock handles DELETE /bucket?public-access-block
func (r *Router) handleDeletePublicAccessBlock(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	if err := r.engine.DeletePublicAccessBlock(ctx, bucket); err != nil {
		r.logger.Warnw("failed to delete public access block", "bucket", bucket, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	w.WriteHeader(http.StatusNoContent)
	s3RequestsTotal.WithLabelValues("DeletePublicAccessBlock", "204").Inc()
}

// handleDeleteBucketAccelerate handles DELETE /bucket?accelerate
func (r *Router) handleDeleteBucketAccelerate(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	if err := r.engine.DeleteBucketAccelerate(ctx, bucket); err != nil {
		r.logger.Warnw("failed to delete bucket accelerate", "bucket", bucket, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	w.WriteHeader(http.StatusNoContent)
	s3RequestsTotal.WithLabelValues("DeleteBucketAccelerate", "204").Inc()
}

// handleDeleteBucketNotification handles DELETE /bucket?notification
func (r *Router) handleDeleteBucketNotification(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	if err := r.engine.DeleteBucketNotification(ctx, bucket); err != nil {
		r.logger.Warnw("failed to delete bucket notification", "bucket", bucket, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	w.WriteHeader(http.StatusNoContent)
	s3RequestsTotal.WithLabelValues("DeleteBucketNotification", "204").Inc()
}

// handleDeleteBucketLogging handles DELETE /bucket?logging
func (r *Router) handleDeleteBucketLogging(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	if err := r.engine.DeleteBucketLogging(ctx, bucket); err != nil {
		r.logger.Warnw("failed to delete bucket logging", "bucket", bucket, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	w.WriteHeader(http.StatusNoContent)
	s3RequestsTotal.WithLabelValues("DeleteBucketLogging", "204").Inc()
}

// handleGetBucketLocation handles GET /bucket?location
func (r *Router) handleGetBucketLocation(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	// Check if bucket exists
	_, err := r.engine.GetBucket(ctx, bucket)
	if err != nil {
		r.logger.Warnw("bucket not found for location", "bucket", bucket, "error", err)
		r.writeError(w, ErrNoSuchBucket)
		return
	}

	location, err := r.engine.GetBucketLocation(ctx, bucket)
	if err != nil {
		r.logger.Warnw("failed to get bucket location", "bucket", bucket, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	// Default location is empty (us-east-1 in AWS)
	if location == "" {
		location = ""
	}

	// Write XML response
	xmlResponse := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">%s</LocationConstraint>`, location)

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(xmlResponse))
	s3RequestsTotal.WithLabelValues("GetBucketLocation", "200").Inc()
}

// handlePutBucketLocation handles PUT /bucket?location
func (r *Router) handlePutBucketLocation(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	body, err := io.ReadAll(req.Body)
	if err != nil {
		r.logger.Warnw("failed to read request body", "error", err)
		r.writeError(w, ErrInternal)
		return
	}
	defer req.Body.Close()

	// Parse location from XML
	var location string
	if len(body) > 0 {
		// The body should contain the location constraint
		// Could be empty string for us-east-1
		location = string(body)
		// Remove XML tags if present
		location = strings.ReplaceAll(location, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>", "")
		location = strings.ReplaceAll(location, "<LocationConstraint xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">", "")
		location = strings.ReplaceAll(location, "</LocationConstraint>", "")
		location = strings.TrimSpace(location)
	}

	// Put location
	if err := r.engine.PutBucketLocation(ctx, bucket, location); err != nil {
		r.logger.Warnw("failed to put bucket location", "bucket", bucket, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	w.WriteHeader(http.StatusOK)
	s3RequestsTotal.WithLabelValues("PutBucketLocation", "200").Inc()
}

// handleGetBucketOwnershipControls handles GET /bucket?ownership-controls
func (r *Router) handleGetBucketOwnershipControls(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	// Check if bucket exists
	_, err := r.engine.GetBucket(ctx, bucket)
	if err != nil {
		r.logger.Warnw("bucket not found for ownership controls", "bucket", bucket, "error", err)
		r.writeError(w, ErrNoSuchBucket)
		return
	}

	config, err := r.engine.GetBucketOwnershipControls(ctx, bucket)
	if err != nil {
		r.logger.Warnw("failed to get bucket ownership controls", "bucket", bucket, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	if config == nil {
		r.writeError(w, ErrOwnershipControlsNotFound)
		return
	}

	// Write XML response
	data, err := xml.Marshal(config)
	if err != nil {
		r.writeError(w, ErrInternal)
		return
	}

	header := `<?xml version="1.0" encoding="UTF-8"?>`
	xmlResponse := header + "\n" + string(data)

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(xmlResponse))
	s3RequestsTotal.WithLabelValues("GetBucketOwnershipControls", "200").Inc()
}

// handlePutBucketOwnershipControls handles PUT /bucket?ownership-controls
func (r *Router) handlePutBucketOwnershipControls(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	body, err := io.ReadAll(req.Body)
	if err != nil {
		r.logger.Warnw("failed to read request body", "error", err)
		r.writeError(w, ErrInternal)
		return
	}
	defer req.Body.Close()

	var config metadata.OwnershipControls
	if err := xml.Unmarshal(body, &config); err != nil {
		r.logger.Warnw("failed to parse ownership controls", "error", err)
		r.writeError(w, ErrMalformedXML)
		return
	}

	if err := r.engine.PutBucketOwnershipControls(ctx, bucket, &config); err != nil {
		r.logger.Warnw("failed to put bucket ownership controls", "bucket", bucket, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	w.WriteHeader(http.StatusOK)
	s3RequestsTotal.WithLabelValues("PutBucketOwnershipControls", "200").Inc()
}

// handleDeleteBucketOwnershipControls handles DELETE /bucket?ownership-controls
func (r *Router) handleDeleteBucketOwnershipControls(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	if err := r.engine.DeleteBucketOwnershipControls(ctx, bucket); err != nil {
		r.logger.Warnw("failed to delete bucket ownership controls", "bucket", bucket, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	w.WriteHeader(http.StatusNoContent)
	s3RequestsTotal.WithLabelValues("DeleteBucketOwnershipControls", "204").Inc()
}

// handleGetBucketMetrics handles GET /bucket?metrics
func (r *Router) handleGetBucketMetrics(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	// Check if bucket exists
	_, err := r.engine.GetBucket(ctx, bucket)
	if err != nil {
		r.logger.Warnw("bucket not found for metrics", "bucket", bucket, "error", err)
		r.writeError(w, ErrNoSuchBucket)
		return
	}

	id := req.URL.Query().Get("id")

	// If ID is provided, get specific metrics
	if id != "" {
		config, err := r.engine.GetBucketMetrics(ctx, bucket, id)
		if err != nil {
			r.logger.Warnw("failed to get bucket metrics", "bucket", bucket, "id", id, "error", err)
			r.writeError(w, ErrInternal)
			return
		}

		if config == nil {
			r.writeError(w, ErrMetricsNotFound)
			return
		}

		// Write XML response
		data, err := xml.Marshal(config)
		if err != nil {
			r.writeError(w, ErrInternal)
			return
		}

		header := `<?xml version="1.0" encoding="UTF-8"?>`
		xmlResponse := header + "\n" + string(data)

		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(xmlResponse))
		s3RequestsTotal.WithLabelValues("GetBucketMetrics", "200").Inc()
		return
	}

	// List all metrics
	configs, err := r.engine.ListBucketMetrics(ctx, bucket)
	if err != nil {
		r.logger.Warnw("failed to list bucket metrics", "bucket", bucket, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	// Return as list
	type MetricsList struct {
		XMLName     xml.Name                       `xml:"ListMetricsConfigurationsResult"`
		Configurations []metadata.MetricsConfiguration `xml:"MetricsConfiguration"`
	}

	response := MetricsList{
		Configurations: configs,
	}

	r.writeXML(w, http.StatusOK, response)
	s3RequestsTotal.WithLabelValues("ListBucketMetrics", "200").Inc()
}

// handlePutBucketMetrics handles PUT /bucket?metrics&id={id}
func (r *Router) handlePutBucketMetrics(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	body, err := io.ReadAll(req.Body)
	if err != nil {
		r.logger.Warnw("failed to read request body", "error", err)
		r.writeError(w, ErrInternal)
		return
	}
	defer req.Body.Close()

	var config metadata.MetricsConfiguration
	if err := xml.Unmarshal(body, &config); err != nil {
		r.logger.Warnw("failed to parse metrics", "error", err)
		r.writeError(w, ErrMalformedXML)
		return
	}

	id := req.URL.Query().Get("id")
	if id == "" {
		id = config.ID
	}
	if id == "" {
		id = "" // Default ID
	}

	if err := r.engine.PutBucketMetrics(ctx, bucket, id, &config); err != nil {
		r.logger.Warnw("failed to put bucket metrics", "bucket", bucket, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	w.WriteHeader(http.StatusOK)
	s3RequestsTotal.WithLabelValues("PutBucketMetrics", "200").Inc()
}

// handleDeleteBucketMetrics handles DELETE /bucket?metrics&id={id}
func (r *Router) handleDeleteBucketMetrics(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	id := req.URL.Query().Get("id")
	if id == "" {
		id = "" // Default ID
	}

	if err := r.engine.DeleteBucketMetrics(ctx, bucket, id); err != nil {
		r.logger.Warnw("failed to delete bucket metrics", "bucket", bucket, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	w.WriteHeader(http.StatusNoContent)
	s3RequestsTotal.WithLabelValues("DeleteBucketMetrics", "204").Inc()
}

// handleGetBucketReplication handles GET /bucket?replication
func (r *Router) handleGetBucketReplication(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	// Check if bucket exists
	_, err := r.engine.GetBucket(ctx, bucket)
	if err != nil {
		r.logger.Warnw("bucket not found for replication", "bucket", bucket, "error", err)
		r.writeError(w, ErrNoSuchBucket)
		return
	}

	config, err := r.engine.GetReplicationConfig(ctx, bucket)
	if err != nil {
		r.logger.Warnw("failed to get bucket replication", "bucket", bucket, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	if config == nil {
		r.writeError(w, ErrReplicationNotFound)
		return
	}

	// Write XML response
	data, err := xml.Marshal(config)
	if err != nil {
		r.writeError(w, ErrInternal)
		return
	}

	header := `<?xml version="1.0" encoding="UTF-8"?>`
	xmlResponse := header + "\n" + string(data)

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(xmlResponse))
	s3RequestsTotal.WithLabelValues("GetBucketReplication", "200").Inc()
}

// handlePutBucketReplication handles PUT /bucket?replication
func (r *Router) handlePutBucketReplication(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	body, err := io.ReadAll(req.Body)
	if err != nil {
		r.logger.Warnw("failed to read request body", "error", err)
		r.writeError(w, ErrInternal)
		return
	}
	defer req.Body.Close()

	var config metadata.ReplicationConfig
	if err := xml.Unmarshal(body, &config); err != nil {
		r.logger.Warnw("failed to parse replication config", "error", err)
		r.writeError(w, ErrMalformedXML)
		return
	}

	if err := r.engine.PutReplicationConfig(ctx, bucket, &config); err != nil {
		r.logger.Warnw("failed to put bucket replication", "bucket", bucket, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	w.WriteHeader(http.StatusOK)
	s3RequestsTotal.WithLabelValues("PutBucketReplication", "200").Inc()
}

// handleDeleteBucketReplication handles DELETE /bucket?replication
func (r *Router) handleDeleteBucketReplication(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	if err := r.engine.DeleteReplicationConfig(ctx, bucket); err != nil {
		r.logger.Warnw("failed to delete bucket replication", "bucket", bucket, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	w.WriteHeader(http.StatusNoContent)
	s3RequestsTotal.WithLabelValues("DeleteBucketReplication", "204").Inc()
}

// handleGetBucketAcl handles GET /bucket?acl
func (r *Router) handleGetBucketAcl(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	// Check if bucket exists
	_, err := r.engine.GetBucket(ctx, bucket)
	if err != nil {
		r.logger.Warnw("bucket not found for ACL", "bucket", bucket, "error", err)
		r.writeError(w, ErrNoSuchBucket)
		return
	}

	// Return default ACL (owner full control)
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)

	aclResponse := `<?xml version="1.0" encoding="UTF-8"?>
<AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Owner>
    <ID>owner</ID>
    <DisplayName>Owner</DisplayName>
  </Owner>
  <AccessControlList>
    <Grant>
      <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
        <ID>owner</ID>
        <DisplayName>Owner</DisplayName>
      </Grantee>
      <Permission>FULL_CONTROL</Permission>
    </Grant>
  </AccessControlList>
</AccessControlPolicy>`

	w.Write([]byte(aclResponse))
	s3RequestsTotal.WithLabelValues("GetBucketAcl", "200").Inc()
}

// handlePutBucketAcl handles PUT /bucket?acl
func (r *Router) handlePutBucketAcl(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	// Check if bucket exists
	_, err := r.engine.GetBucket(ctx, bucket)
	if err != nil {
		r.logger.Warnw("bucket not found for ACL", "bucket", bucket, "error", err)
		r.writeError(w, ErrNoSuchBucket)
		return
	}

	// For now, just acknowledge the ACL was set
	// In a full implementation, we'd parse and store the ACL
	w.WriteHeader(http.StatusOK)
	s3RequestsTotal.WithLabelValues("PutBucketAcl", "200").Inc()
}

// handleDeleteBucketAcl handles DELETE /bucket?acl
func (r *Router) handleDeleteBucketAcl(w http.ResponseWriter, req *http.Request, bucket string) {
	// ACLs cannot actually be deleted, just reset to default
	w.WriteHeader(http.StatusNoContent)
	s3RequestsTotal.WithLabelValues("DeleteBucketAcl", "204").Inc()
}

// handleGetObjectTags handles GET /bucket/key?tagging
func (r *Router) handleGetObjectTags(w http.ResponseWriter, req *http.Request, bucket, key string) {
	ctx := req.Context()

	// Check if object exists
	obj, err := r.engine.GetObject(ctx, bucket, key, engine.GetObjectOptions{})
	if err != nil {
		r.logger.Warnw("object not found for tags", "bucket", bucket, "key", key, "error", err)
		r.writeError(w, ErrNoSuchKey)
		return
	}

	// Return tags if any, otherwise empty tag set
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)

	tagsXML := `<?xml version="1.0" encoding="UTF-8"?>
<Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <TagSet>`

	if obj.Metadata != nil {
		for k, v := range obj.Metadata {
			tagsXML += fmt.Sprintf(`
    <Tag>
      <Key>%s</Key>
      <Value>%s</Value>
    </Tag>`, tags.EscapeXML(k), tags.EscapeXML(v))
		}
	}

	tagsXML += `
  </TagSet>
</Tagging>`

	w.Write([]byte(tagsXML))
	s3RequestsTotal.WithLabelValues("GetObjectTags", "200").Inc()
}

// handlePutObjectTags handles PUT /bucket/key?tagging
func (r *Router) handlePutObjectTags(w http.ResponseWriter, req *http.Request, bucket, key string) {
	ctx := req.Context()

	// Check if object exists
	_, err := r.engine.GetObject(ctx, bucket, key, engine.GetObjectOptions{})
	if err != nil {
		r.logger.Warnw("object not found for tags", "bucket", bucket, "key", key, "error", err)
		r.writeError(w, ErrNoSuchKey)
		return
	}

	// Parse tags from body (simplified - just acknowledge for now)
	// In production, we'd parse the XML and update object metadata

	w.WriteHeader(http.StatusNoContent)
	s3RequestsTotal.WithLabelValues("PutObjectTags", "204").Inc()
}

// handleDeleteObjectTags handles DELETE /bucket/key?tagging
func (r *Router) handleDeleteObjectTags(w http.ResponseWriter, req *http.Request, bucket, key string) {
	ctx := req.Context()

	// Check if object exists
	_, err := r.engine.GetObject(ctx, bucket, key, engine.GetObjectOptions{})
	if err != nil {
		r.logger.Warnw("object not found for tags", "bucket", bucket, "key", key, "error", err)
		r.writeError(w, ErrNoSuchKey)
		return
	}

	w.WriteHeader(http.StatusNoContent)
	s3RequestsTotal.WithLabelValues("DeleteObjectTags", "204").Inc()
}

// handleGetBucketNotification handles GET /bucket?notification
func (r *Router) handleGetBucketNotification(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	config, err := r.engine.GetBucketNotification(ctx, bucket)
	if err != nil {
		r.logger.Warnw("failed to get bucket notification", "bucket", bucket, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	if config == nil {
		// Return empty notification config
		config = &metadata.NotificationConfiguration{}
	}

	r.writeXML(w, http.StatusOK, config)
	s3RequestsTotal.WithLabelValues("GetBucketNotification", "200").Inc()
}

// handlePutBucketNotification handles PUT /bucket?notification
func (r *Router) handlePutBucketNotification(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	// Read body with size limit
	body, err := readLimitedBody(req.Body)
	if err != nil {
		r.logger.Warnw("failed to read request body", "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	// Parse notification configuration
	var config metadata.NotificationConfiguration
	if err := xml.Unmarshal(body, &config); err != nil {
		r.logger.Warnw("failed to parse notification configuration", "error", err)
		r.writeError(w, ErrMalformedXML)
		return
	}

	// Save configuration
	if err := r.engine.PutBucketNotification(ctx, bucket, &config); err != nil {
		r.logger.Warnw("failed to save bucket notification", "bucket", bucket, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	w.WriteHeader(http.StatusOK)
	s3RequestsTotal.WithLabelValues("PutBucketNotification", "200").Inc()
}

// handleGetBucketLogging handles GET /bucket?logging
func (r *Router) handleGetBucketLogging(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	config, err := r.engine.GetBucketLogging(ctx, bucket)
	if err != nil {
		r.logger.Warnw("failed to get bucket logging", "bucket", bucket, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	if config == nil {
		// Return empty logging config
		config = &metadata.LoggingConfiguration{}
	}

	r.writeXML(w, http.StatusOK, config)
	s3RequestsTotal.WithLabelValues("GetBucketLogging", "200").Inc()
}

// handlePutBucketLogging handles PUT /bucket?logging
func (r *Router) handlePutBucketLogging(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	// Read body with size limit
	body, err := readLimitedBody(req.Body)
	if err != nil {
		r.logger.Warnw("failed to read request body", "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	// Parse logging configuration
	var config metadata.LoggingConfiguration
	if err := xml.Unmarshal(body, &config); err != nil {
		r.logger.Warnw("failed to parse logging configuration", "error", err)
		r.writeError(w, ErrMalformedXML)
		return
	}

	// Save configuration
	if err := r.engine.PutBucketLogging(ctx, bucket, &config); err != nil {
		r.logger.Warnw("failed to save bucket logging", "bucket", bucket, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	w.WriteHeader(http.StatusOK)
	s3RequestsTotal.WithLabelValues("PutBucketLogging", "200").Inc()
}

// handleGetPresignedURL handles GET /bucket/key?presignedurl
func (r *Router) handleGetPresignedURL(w http.ResponseWriter, req *http.Request, bucket, key string) {
	ctx := req.Context()

	// Parse query parameters
	method := req.URL.Query().Get("method")
	if method == "" {
		method = "GET" // Default to GET
	}

	expires := int64(3600) // Default 1 hour
	if expiresStr := req.URL.Query().Get("expires"); expiresStr != "" {
		if parsed, err := strconv.ParseInt(expiresStr, 10, 64); err == nil {
			expires = parsed
		}
	}

	// Generate presigned URL
	url, err := r.engine.GeneratePresignedURL(ctx, bucket, key, method, expires)
	if err != nil {
		r.logger.Warnw("failed to generate presigned URL", "bucket", bucket, "key", key, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	// Write response
	response := struct {
		URL string `json:"url"`
	}{
		URL: url,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
	s3RequestsTotal.WithLabelValues("GetPresignedURL", "200").Inc()
}

// handlePutPresignedURL handles PUT /bucket/key?presignedurl (for storing custom presigned URLs)
func (r *Router) handlePutPresignedURL(w http.ResponseWriter, req *http.Request, bucket, key string) {
	ctx := req.Context()

	// Read body with size limit
	body, err := readLimitedBody(req.Body)
	if err != nil {
		r.logger.Warnw("failed to read request body", "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	var input struct {
		Method string `json:"method"`
		Expires int64 `json:"expires"`
	}

	if err := json.Unmarshal(body, &input); err != nil {
		r.logger.Warnw("failed to parse request body", "error", err)
		r.writeError(w, ErrInvalidRequest)
		return
	}

	// Generate presigned URL
	url, err := r.engine.GeneratePresignedURL(ctx, bucket, key, input.Method, input.Expires)
	if err != nil {
		r.logger.Warnw("failed to generate presigned URL", "bucket", bucket, "key", key, "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	// Write response
	response := struct {
		URL string `json:"url"`
	}{
		URL: url,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
	s3RequestsTotal.WithLabelValues("PutPresignedURL", "200").Inc()
}

// handleDeleteObjects handles POST /bucket?delete (batch delete)
func (r *Router) handleDeleteObjects(w http.ResponseWriter, req *http.Request, bucket string) {
	ctx := req.Context()

	// Read body with size limit
	body, err := readLimitedBody(req.Body)
	if err != nil {
		r.logger.Warnw("failed to read request body", "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	var input s3types.DeleteObjectsInput
	if err := xml.Unmarshal(body, &input); err != nil {
		r.logger.Warnw("failed to parse delete input", "error", err)
		r.writeError(w, ErrMalformedXML)
		return
	}

	// Delete objects
	var deleted []s3types.DeletedObject
	var errors []s3types.DeleteError

	for _, obj := range input.Objects {
		err := r.engine.DeleteObject(ctx, bucket, obj.Key, engine.DeleteObjectOptions{
			VersionID: obj.VersionID,
		})
		if err != nil {
			errors = append(errors, s3types.DeleteError{
				Key:       obj.Key,
				VersionID: obj.VersionID,
				Code:      "AccessDenied",
				Message:   err.Error(),
			})
		} else {
			deleted = append(deleted, s3types.DeletedObject{
				Key:       obj.Key,
				VersionID: obj.VersionID,
			})
		}
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)

	resp := s3types.DeleteObjectsOutput{
		Deleted: deleted,
		Errors:  errors,
	}
	xmlBytes, _ := xml.Marshal(resp)
	w.Write(xmlBytes)

	s3RequestsTotal.WithLabelValues("DeleteObjects", "200").Inc()
}

// handleSelectObjectContent handles S3 Select (POST /bucket/key?select)
func (r *Router) handleSelectObjectContent(w http.ResponseWriter, req *http.Request, bucket, key string) {
	ctx := req.Context()

	// Get the object first
	obj, err := r.engine.GetObject(ctx, bucket, key, engine.GetObjectOptions{})
	if err != nil {
		r.logger.Warnw("failed to get object for select", "bucket", bucket, "key", key, "error", err)
		r.writeError(w, ErrNoSuchKey)
		return
	}
	defer obj.Body.Close()

	// Read the body
	data, err := io.ReadAll(obj.Body)
	if err != nil {
		r.logger.Warnw("failed to read object data", "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	// Parse the S3 Select request body
	var selectInput s3types.SelectObjectContentRequest
	if err := xml.Unmarshal(data, &selectInput); err != nil {
		r.logger.Warnw("failed to parse select input", "error", err)
		r.writeError(w, ErrMalformedXML)
		return
	}

	// Determine input format (CSV or JSON)
	inputFormat := s3select.FormatJSON
	if selectInput.InputSerialization.CSV != nil {
		inputFormat = s3select.FormatCSV
	}

	// Create select request
	selectReq := &s3select.SelectRequest{
		Bucket:     bucket,
		Key:        key,
		Expression: selectInput.Expression,
		InputSerialization: s3select.InputSerialization{
			Format: inputFormat,
		},
	}

	// Execute select - pass the original object data, not the request body
	result, err := r.selectService.Execute(ctx, selectReq, bytes.NewReader(data))
	if err != nil {
		r.logger.Warnw("failed to execute select", "error", err)
		r.writeError(w, ErrInternal)
		return
	}

	// Write response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(result.Payload)

	s3RequestsTotal.WithLabelValues("SelectObjectContent", "200").Inc()
}

// handleRestoreObject handles POST /bucket/key?restore (Glacier restore)
func (r *Router) handleRestoreObject(w http.ResponseWriter, req *http.Request, bucket, key string) {
	ctx := req.Context()

	// Get the object
	obj, err := r.engine.GetObject(ctx, bucket, key, engine.GetObjectOptions{})
	if err != nil {
		r.logger.Warnw("object not found for restore", "bucket", bucket, "key", key, "error", err)
		r.writeError(w, ErrNoSuchKey)
		return
	}

	// Check if object is in Glacier storage class
	if obj.StorageClass != "GLACIER" && obj.StorageClass != "DEEP_ARCHIVE" {
		r.writeError(w, ErrInvalidObjectState)
		return
	}

	// For now, just acknowledge the restore request
	// In a full implementation, this would initiate an async restore job
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusAccepted)

	restoreResponse := `<?xml version="1.0" encoding="UTF-8"?>
<RestoreJob>
  <JobDescription>Restore initiated</JobDescription>
</RestoreJob>`

	w.Write([]byte(restoreResponse))
	s3RequestsTotal.WithLabelValues("RestoreObject", "202").Inc()
}
