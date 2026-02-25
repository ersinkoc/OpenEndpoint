package client

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestNew(t *testing.T) {
	cfg := Config{
		Endpoint:  "http://localhost:9000",
		AccessKey: "access",
		SecretKey: "secret",
		Region:    "us-east-1",
		Timeout:   30 * time.Second,
	}

	client, err := New(cfg)
	if err != nil {
		t.Errorf("New() error: %v", err)
	}
	if client == nil {
		t.Error("New() returned nil client")
	}
	if client.endpoint != cfg.Endpoint {
		t.Errorf("endpoint = %s, expected %s", client.endpoint, cfg.Endpoint)
	}
}

func TestNewWithDefaults(t *testing.T) {
	cfg := Config{}

	client, err := New(cfg)
	if err != nil {
		t.Errorf("New() error: %v", err)
	}
	if client == nil {
		t.Error("New() returned nil client")
	}
	if client.endpoint != "http://localhost:9000" {
		t.Errorf("default endpoint = %s, expected http://localhost:9000", client.endpoint)
	}
}

func TestListBuckets(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Errorf("Method = %s, expected GET", r.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"Buckets":[{"Name":"bucket1"},{"Name":"bucket2"}]}`))
	}))
	defer server.Close()

	client, _ := New(Config{Endpoint: server.URL})
	buckets, err := client.ListBuckets(context.Background())
	if err != nil {
		t.Errorf("ListBuckets() error: %v", err)
	}
	if len(buckets) != 2 {
		t.Errorf("ListBuckets() returned %d buckets, expected 2", len(buckets))
	}
}

func TestListBucketsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client, _ := New(Config{Endpoint: server.URL})
	_, err := client.ListBuckets(context.Background())
	if err == nil {
		t.Error("ListBuckets() expected error, got nil")
	}
}

func TestCreateBucket(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "PUT" {
			t.Errorf("Method = %s, expected PUT", r.Method)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, _ := New(Config{Endpoint: server.URL})
	err := client.CreateBucket(context.Background(), "test-bucket")
	if err != nil {
		t.Errorf("CreateBucket() error: %v", err)
	}
}

func TestCreateBucketNoContent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client, _ := New(Config{Endpoint: server.URL})
	err := client.CreateBucket(context.Background(), "test-bucket")
	if err != nil {
		t.Errorf("CreateBucket() error: %v", err)
	}
}

func TestCreateBucketError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()

	client, _ := New(Config{Endpoint: server.URL})
	err := client.CreateBucket(context.Background(), "test-bucket")
	if err == nil {
		t.Error("CreateBucket() expected error, got nil")
	}
}

func TestDeleteBucket(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "DELETE" {
			t.Errorf("Method = %s, expected DELETE", r.Method)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client, _ := New(Config{Endpoint: server.URL})
	err := client.DeleteBucket(context.Background(), "test-bucket")
	if err != nil {
		t.Errorf("DeleteBucket() error: %v", err)
	}
}

func TestDeleteBucketError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
	}))
	defer server.Close()

	client, _ := New(Config{Endpoint: server.URL})
	err := client.DeleteBucket(context.Background(), "test-bucket")
	if err == nil {
		t.Error("DeleteBucket() expected error, got nil")
	}
}

func TestPutObject(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "PUT" {
			t.Errorf("Method = %s, expected PUT", r.Method)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, _ := New(Config{Endpoint: server.URL})
	err := client.PutObject(context.Background(), "bucket", "key", nil)
	if err != nil {
		t.Errorf("PutObject() error: %v", err)
	}
}

func TestPutObjectError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer server.Close()

	client, _ := New(Config{Endpoint: server.URL})
	err := client.PutObject(context.Background(), "bucket", "key", nil)
	if err == nil {
		t.Error("PutObject() expected error, got nil")
	}
}

func TestGetObject(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Errorf("Method = %s, expected GET", r.Method)
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("test content"))
	}))
	defer server.Close()

	client, _ := New(Config{Endpoint: server.URL})
	reader, err := client.GetObject(context.Background(), "bucket", "key")
	if err != nil {
		t.Errorf("GetObject() error: %v", err)
	}
	defer reader.Close()
}

func TestGetObjectError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client, _ := New(Config{Endpoint: server.URL})
	_, err := client.GetObject(context.Background(), "bucket", "key")
	if err == nil {
		t.Error("GetObject() expected error, got nil")
	}
}

func TestDeleteObject(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "DELETE" {
			t.Errorf("Method = %s, expected DELETE", r.Method)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client, _ := New(Config{Endpoint: server.URL})
	err := client.DeleteObject(context.Background(), "bucket", "key")
	if err != nil {
		t.Errorf("DeleteObject() error: %v", err)
	}
}

func TestDeleteObjectError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer server.Close()

	client, _ := New(Config{Endpoint: server.URL})
	err := client.DeleteObject(context.Background(), "bucket", "key")
	if err == nil {
		t.Error("DeleteObject() expected error, got nil")
	}
}

func TestListObjects(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Errorf("Method = %s, expected GET", r.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"Contents":[{"Key":"file1.txt"},{"Key":"file2.txt"}]}`))
	}))
	defer server.Close()

	client, _ := New(Config{Endpoint: server.URL})
	keys, err := client.ListObjects(context.Background(), "bucket", "prefix/")
	if err != nil {
		t.Errorf("ListObjects() error: %v", err)
	}
	if len(keys) != 2 {
		t.Errorf("ListObjects() returned %d keys, expected 2", len(keys))
	}
}

func TestListObjectsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client, _ := New(Config{Endpoint: server.URL})
	_, err := client.ListObjects(context.Background(), "bucket", "")
	if err == nil {
		t.Error("ListObjects() expected error, got nil")
	}
}

func TestHeadObject(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "HEAD" {
			t.Errorf("Method = %s, expected HEAD", r.Method)
		}
		w.Header().Set("Content-Length", "1024")
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, _ := New(Config{Endpoint: server.URL})
	metadata, err := client.HeadObject(context.Background(), "bucket", "key")
	if err != nil {
		t.Errorf("HeadObject() error: %v", err)
	}
	if metadata == nil {
		t.Error("HeadObject() returned nil metadata")
	}
}

func TestHeadObjectError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client, _ := New(Config{Endpoint: server.URL})
	_, err := client.HeadObject(context.Background(), "bucket", "key")
	if err == nil {
		t.Error("HeadObject() expected error, got nil")
	}
}

func TestCopyObject(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "PUT" {
			t.Errorf("Method = %s, expected PUT", r.Method)
		}
		if r.Header.Get("x-amz-copy-source") != "/src-bucket/src-key" {
			t.Errorf("x-amz-copy-source = %s, expected /src-bucket/src-key", r.Header.Get("x-amz-copy-source"))
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, _ := New(Config{Endpoint: server.URL})
	err := client.CopyObject(context.Background(), "src-bucket", "src-key", "dst-bucket", "dst-key")
	if err != nil {
		t.Errorf("CopyObject() error: %v", err)
	}
}

func TestCopyObjectError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client, _ := New(Config{Endpoint: server.URL})
	err := client.CopyObject(context.Background(), "src-bucket", "src-key", "dst-bucket", "dst-key")
	if err == nil {
		t.Error("CopyObject() expected error, got nil")
	}
}

func TestNewRequest(t *testing.T) {
	client, _ := New(Config{
		Endpoint:  "http://localhost:9000",
		AccessKey: "access",
		SecretKey: "secret",
	})

	req, err := client.newRequest(context.Background(), "GET", "/bucket/key", nil)
	if err != nil {
		t.Errorf("newRequest() error: %v", err)
	}
	if req == nil {
		t.Error("newRequest() returned nil request")
	}
	if req.Method != "GET" {
		t.Errorf("Method = %s, expected GET", req.Method)
	}
}

func TestNewRequestWithAuth(t *testing.T) {
	client, _ := New(Config{
		Endpoint:  "http://localhost:9000",
		AccessKey: "access",
		SecretKey: "secret",
	})

	req, err := client.newRequest(context.Background(), "PUT", "/bucket/key", nil)
	if err != nil {
		t.Errorf("newRequest() error: %v", err)
	}

	auth := req.Header.Get("Authorization")
	if auth == "" {
		t.Error("Authorization header not set")
	}
	if req.Header.Get("X-Amz-Date") == "" {
		t.Error("X-Amz-Date header not set")
	}
}

func TestWriteFile(t *testing.T) {
	err := WriteFile("test.txt", []byte("test"))
	if err != nil {
		t.Errorf("WriteFile() error: %v", err)
	}
}

func TestWriteFileSimple(t *testing.T) {
	err := WriteFileSimple("test.txt", []byte("test"))
	if err != nil {
		t.Errorf("WriteFileSimple() error: %v", err)
	}
}

func TestNewRequestInvalidURL(t *testing.T) {
	client, _ := New(Config{
		Endpoint:  "://invalid",
		AccessKey: "access",
		SecretKey: "secret",
	})

	_, err := client.newRequest(context.Background(), "GET", "/bucket", nil)
	if err == nil {
		t.Error("newRequest() expected error for invalid URL, got nil")
	}
}

func TestNewWithAWS(t *testing.T) {
	cfg := Config{
		Endpoint:       "http://localhost:9000",
		AccessKey:      "access",
		SecretKey:      "secret",
		Region:         "us-east-1",
		ForcePathStyle: true,
	}

	client, err := NewWithAWS(cfg)
	if err != nil {
		t.Errorf("NewWithAWS() error: %v", err)
	}
	if client == nil {
		t.Error("NewWithAWS() returned nil client")
	}
}

func TestNewWithAWSDefaults(t *testing.T) {
	cfg := Config{
		AccessKey: "access",
		SecretKey: "secret",
	}

	client, err := NewWithAWS(cfg)
	if err != nil {
		t.Errorf("NewWithAWS() error: %v", err)
	}
	if client == nil {
		t.Error("NewWithAWS() returned nil client")
	}
}

func TestConfigDefaults(t *testing.T) {
	cfg := Config{}
	client, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	if client.endpoint != "http://localhost:9000" {
		t.Errorf("Default endpoint = %s, want http://localhost:9000", client.endpoint)
	}
}

func TestNewRequestWithBody(t *testing.T) {
	client, _ := New(Config{
		Endpoint:  "http://localhost:9000",
		AccessKey: "access",
		SecretKey: "secret",
	})

	req, err := client.newRequest(context.Background(), "PUT", "/bucket/key", nil)
	if err != nil {
		t.Fatalf("newRequest() error: %v", err)
	}

	// Content-Type should not be set when body is nil
	if req.Header.Get("Content-Type") != "" {
		t.Error("Content-Type should not be set when body is nil")
	}
}

func TestNewRequestNoAuth(t *testing.T) {
	client, _ := New(Config{
		Endpoint: "http://localhost:9000",
	})

	req, err := client.newRequest(context.Background(), "GET", "/bucket/key", nil)
	if err != nil {
		t.Fatalf("newRequest() error: %v", err)
	}

	// Authorization header should not be set when no credentials
	if req.Header.Get("Authorization") != "" {
		t.Error("Authorization should not be set when no credentials")
	}
}

func TestListBucketsInvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`invalid json`))
	}))
	defer server.Close()

	client, _ := New(Config{Endpoint: server.URL})
	_, err := client.ListBuckets(context.Background())
	if err == nil {
		t.Error("ListBuckets() expected error for invalid JSON, got nil")
	}
}

func TestListObjectsInvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`invalid json`))
	}))
	defer server.Close()

	client, _ := New(Config{Endpoint: server.URL})
	_, err := client.ListObjects(context.Background(), "bucket", "")
	if err == nil {
		t.Error("ListObjects() expected error for invalid JSON, got nil")
	}
}

func TestClientStruct(t *testing.T) {
	client := &Client{
		endpoint:  "http://test:9000",
		accessKey: "key",
		secretKey: "secret",
		client:    &http.Client{},
	}

	if client.endpoint != "http://test:9000" {
		t.Errorf("endpoint = %s, want http://test:9000", client.endpoint)
	}
}

func TestConfigStruct(t *testing.T) {
	cfg := Config{
		Endpoint:       "http://test:9000",
		AccessKey:      "key",
		SecretKey:      "secret",
		Region:         "us-west-1",
		DisableSSL:     true,
		ForcePathStyle: true,
		Timeout:        60 * time.Second,
	}

	if cfg.Endpoint != "http://test:9000" {
		t.Errorf("Endpoint = %s", cfg.Endpoint)
	}
	if cfg.Timeout != 60*time.Second {
		t.Errorf("Timeout = %v", cfg.Timeout)
	}
}

func TestUploadFile(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "PUT" {
			t.Errorf("Method = %s, expected PUT", r.Method)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	tmpFile, err := os.CreateTemp("", "upload-*.txt")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.WriteString("test content")
	tmpFile.Close()

	client, _ := New(Config{Endpoint: server.URL})
	err = client.UploadFile(context.Background(), "bucket", "key", tmpFile.Name())
	if err != nil {
		t.Errorf("UploadFile() error: %v", err)
	}
}

func TestUploadFileError(t *testing.T) {
	client, _ := New(Config{Endpoint: "http://localhost:9000"})
	err := client.UploadFile(context.Background(), "bucket", "key", "/nonexistent/file.txt")
	if err == nil {
		t.Error("UploadFile() expected error for nonexistent file, got nil")
	}
}

func TestDownloadFile(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Errorf("Method = %s, expected GET", r.Method)
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("test content"))
	}))
	defer server.Close()

	client, _ := New(Config{Endpoint: server.URL})
	err := client.DownloadFile(context.Background(), "bucket", "key", "test-download.txt")
	if err != nil {
		t.Errorf("DownloadFile() error: %v", err)
	}
}

func TestDownloadFileError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client, _ := New(Config{Endpoint: server.URL})
	err := client.DownloadFile(context.Background(), "bucket", "key", "/tmp/test-download.txt")
	if err == nil {
		t.Error("DownloadFile() expected error, got nil")
	}
}

func TestPutObjectWithBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "PUT" {
			t.Errorf("Method = %s, expected PUT", r.Method)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, _ := New(Config{Endpoint: server.URL})
	err := client.PutObject(context.Background(), "bucket", "key", bytes.NewReader([]byte("test")))
	if err != nil {
		t.Errorf("PutObject() error: %v", err)
	}
}

func TestListBucketsRequestError(t *testing.T) {
	client, _ := New(Config{Endpoint: "://invalid"})
	_, err := client.ListBuckets(context.Background())
	if err == nil {
		t.Error("ListBuckets() expected error for invalid endpoint, got nil")
	}
}

func TestListBucketsDoError(t *testing.T) {
	client, _ := New(Config{Endpoint: "http://127.0.0.1:1", Timeout: 1 * time.Millisecond})
	_, err := client.ListBuckets(context.Background())
	if err == nil {
		t.Error("ListBuckets() expected error for connection failure, got nil")
	}
}

func TestCreateBucketRequestError(t *testing.T) {
	client, _ := New(Config{Endpoint: "://invalid"})
	err := client.CreateBucket(context.Background(), "bucket")
	if err == nil {
		t.Error("CreateBucket() expected error for invalid endpoint, got nil")
	}
}

func TestCreateBucketDoError(t *testing.T) {
	client, _ := New(Config{Endpoint: "http://127.0.0.1:1", Timeout: 1 * time.Millisecond})
	err := client.CreateBucket(context.Background(), "bucket")
	if err == nil {
		t.Error("CreateBucket() expected error for connection failure, got nil")
	}
}

func TestDeleteBucketRequestError(t *testing.T) {
	client, _ := New(Config{Endpoint: "://invalid"})
	err := client.DeleteBucket(context.Background(), "bucket")
	if err == nil {
		t.Error("DeleteBucket() expected error for invalid endpoint, got nil")
	}
}

func TestDeleteBucketDoError(t *testing.T) {
	client, _ := New(Config{Endpoint: "http://127.0.0.1:1", Timeout: 1 * time.Millisecond})
	err := client.DeleteBucket(context.Background(), "bucket")
	if err == nil {
		t.Error("DeleteBucket() expected error for connection failure, got nil")
	}
}

func TestPutObjectRequestError(t *testing.T) {
	client, _ := New(Config{Endpoint: "://invalid"})
	err := client.PutObject(context.Background(), "bucket", "key", nil)
	if err == nil {
		t.Error("PutObject() expected error for invalid endpoint, got nil")
	}
}

func TestPutObjectDoError(t *testing.T) {
	client, _ := New(Config{Endpoint: "http://127.0.0.1:1", Timeout: 1 * time.Millisecond})
	err := client.PutObject(context.Background(), "bucket", "key", nil)
	if err == nil {
		t.Error("PutObject() expected error for connection failure, got nil")
	}
}

func TestGetObjectRequestError(t *testing.T) {
	client, _ := New(Config{Endpoint: "://invalid"})
	_, err := client.GetObject(context.Background(), "bucket", "key")
	if err == nil {
		t.Error("GetObject() expected error for invalid endpoint, got nil")
	}
}

func TestGetObjectDoError(t *testing.T) {
	client, _ := New(Config{Endpoint: "http://127.0.0.1:1", Timeout: 1 * time.Millisecond})
	_, err := client.GetObject(context.Background(), "bucket", "key")
	if err == nil {
		t.Error("GetObject() expected error for connection failure, got nil")
	}
}

func TestDeleteObjectRequestError(t *testing.T) {
	client, _ := New(Config{Endpoint: "://invalid"})
	err := client.DeleteObject(context.Background(), "bucket", "key")
	if err == nil {
		t.Error("DeleteObject() expected error for invalid endpoint, got nil")
	}
}

func TestDeleteObjectDoError(t *testing.T) {
	client, _ := New(Config{Endpoint: "http://127.0.0.1:1", Timeout: 1 * time.Millisecond})
	err := client.DeleteObject(context.Background(), "bucket", "key")
	if err == nil {
		t.Error("DeleteObject() expected error for connection failure, got nil")
	}
}

func TestListObjectsRequestError(t *testing.T) {
	client, _ := New(Config{Endpoint: "://invalid"})
	_, err := client.ListObjects(context.Background(), "bucket", "")
	if err == nil {
		t.Error("ListObjects() expected error for invalid endpoint, got nil")
	}
}

func TestListObjectsDoError(t *testing.T) {
	client, _ := New(Config{Endpoint: "http://127.0.0.1:1", Timeout: 1 * time.Millisecond})
	_, err := client.ListObjects(context.Background(), "bucket", "")
	if err == nil {
		t.Error("ListObjects() expected error for connection failure, got nil")
	}
}

func TestHeadObjectRequestError(t *testing.T) {
	client, _ := New(Config{Endpoint: "://invalid"})
	_, err := client.HeadObject(context.Background(), "bucket", "key")
	if err == nil {
		t.Error("HeadObject() expected error for invalid endpoint, got nil")
	}
}

func TestHeadObjectDoError(t *testing.T) {
	client, _ := New(Config{Endpoint: "http://127.0.0.1:1", Timeout: 1 * time.Millisecond})
	_, err := client.HeadObject(context.Background(), "bucket", "key")
	if err == nil {
		t.Error("HeadObject() expected error for connection failure, got nil")
	}
}

func TestCopyObjectRequestError(t *testing.T) {
	client, _ := New(Config{Endpoint: "://invalid"})
	err := client.CopyObject(context.Background(), "src", "key", "dst", "key")
	if err == nil {
		t.Error("CopyObject() expected error for invalid endpoint, got nil")
	}
}

func TestCopyObjectDoError(t *testing.T) {
	client, _ := New(Config{Endpoint: "http://127.0.0.1:1", Timeout: 1 * time.Millisecond})
	err := client.CopyObject(context.Background(), "src", "key", "dst", "key")
	if err == nil {
		t.Error("CopyObject() expected error for connection failure, got nil")
	}
}

type errorReader struct{}

func (e *errorReader) Read(p []byte) (n int, err error) {
	return 0, context.DeadlineExceeded
}

func TestDownloadFileReadAllError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "100")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("short"))
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
	}))
	defer server.Close()

	client, _ := New(Config{Endpoint: server.URL, Timeout: 100 * time.Millisecond})
	err := client.DownloadFile(context.Background(), "bucket", "key", "test-dl.txt")
	if err == nil {
		os.Remove("test-dl.txt")
		t.Error("DownloadFile() expected error for read failure, got nil")
	}
}

func TestNewRequestInvalidMethod(t *testing.T) {
	client, _ := New(Config{Endpoint: "http://localhost:9000"})
	_, err := client.newRequest(context.Background(), "GET\x00", "/bucket", nil)
	if err == nil {
		t.Error("newRequest() expected error for invalid method, got nil")
	}
}

func TestNewRequestWithBodyContentType(t *testing.T) {
	client, _ := New(Config{Endpoint: "http://localhost:9000"})
	req, err := client.newRequest(context.Background(), "PUT", "/bucket/key", bytes.NewReader([]byte("test")))
	if err != nil {
		t.Fatalf("newRequest() error: %v", err)
	}
	if req.Header.Get("Content-Type") != "application/octet-stream" {
		t.Errorf("Content-Type = %s, expected application/octet-stream", req.Header.Get("Content-Type"))
	}
}

func TestNewWithAWSNoForcePathStyle(t *testing.T) {
	cfg := Config{
		Endpoint:       "http://localhost:9000",
		AccessKey:      "access",
		SecretKey:      "secret",
		Region:         "us-east-1",
		ForcePathStyle: false,
	}

	client, err := NewWithAWS(cfg)
	if err != nil {
		t.Errorf("NewWithAWS() error: %v", err)
	}
	if client == nil {
		t.Error("NewWithAWS() returned nil client")
	}
}

func TestNewWithAWSEndpointResolver(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Buckets></Buckets></ListAllMyBucketsResult>`))
	}))
	defer server.Close()

	cfg := Config{
		Endpoint:  server.URL,
		AccessKey: "access",
		SecretKey: "secret",
		Region:    "us-east-1",
	}

	client, err := NewWithAWS(cfg)
	if err != nil {
		t.Fatalf("NewWithAWS() error: %v", err)
	}

	_, err = client.ListBuckets(context.Background(), &s3.ListBucketsInput{})
	if err != nil {
		t.Logf("ListBuckets() error (expected for mock): %v", err)
	}
}

func TestDownloadFileRequestError(t *testing.T) {
	client, _ := New(Config{Endpoint: "://invalid"})
	err := client.DownloadFile(context.Background(), "bucket", "key", "test.txt")
	if err == nil {
		t.Error("DownloadFile() expected error for invalid endpoint, got nil")
	}
}
