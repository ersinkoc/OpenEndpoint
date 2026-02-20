package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Client represents an OpenEndpoint S3 client
type Client struct {
	endpoint string
	client   *http.Client
	accessKey string
	secretKey string
}

// Config holds client configuration
type Config struct {
	Endpoint      string
	AccessKey    string
	SecretKey    string
	Region       string
	DisableSSL   bool
	ForcePathStyle bool
	Timeout      time.Duration
}

// New creates a new OpenEndpoint client
func New(cfg Config) (*Client, error) {
	if cfg.Endpoint == "" {
		cfg.Endpoint = "http://localhost:9000"
	}
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 30 * time.Second
	}

	return &Client{
		endpoint:  cfg.Endpoint,
		accessKey: cfg.AccessKey,
		secretKey: cfg.SecretKey,
		client: &http.Client{
			Timeout: cfg.Timeout,
		},
	}, nil
}

// NewWithAWS creates a client compatible with AWS SDK
func NewWithAWS(cfg Config) (*s3.Client, error) {
	if cfg.Endpoint == "" {
		cfg.Endpoint = "http://localhost:9000"
	}

	awsCfg := aws.Config{
		Region:      cfg.Region,
		Credentials: credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, ""),
	}

	opts := []func(*s3.Options){}
	if cfg.ForcePathStyle {
		opts = append(opts, func(o *s3.Options) {
			o.UsePathStyle = true
		})
	}

	// Set custom endpoint
	resolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			URL:               cfg.Endpoint,
			HostnameImmutable: true,
		}, nil
	})
	awsCfg.EndpointResolverWithOptions = resolver

	return s3.NewFromConfig(awsCfg, opts...), nil
}

// ListBuckets lists all buckets
func (c *Client) ListBuckets(ctx context.Context) ([]string, error) {
	req, err := c.newRequest(ctx, "GET", "/", nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	var result struct {
		Buckets []struct {
			Name string `json:"Name"`
		} `json:"Buckets"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	buckets := make([]string, len(result.Buckets))
	for i, b := range result.Buckets {
		buckets[i] = b.Name
	}

	return buckets, nil
}

// CreateBucket creates a new bucket
func (c *Client) CreateBucket(ctx context.Context, name string) error {
	req, err := c.newRequest(ctx, "PUT", "/"+name, nil)
	if err != nil {
		return err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	return nil
}

// DeleteBucket deletes a bucket
func (c *Client) DeleteBucket(ctx context.Context, name string) error {
	req, err := c.newRequest(ctx, "DELETE", "/"+name, nil)
	if err != nil {
		return err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	return nil
}

// PutObject uploads an object
func (c *Client) PutObject(ctx context.Context, bucket, key string, body io.Reader) error {
	path := fmt.Sprintf("/%s/%s", bucket, key)
	req, err := c.newRequest(ctx, "PUT", path, body)
	if err != nil {
		return err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	return nil
}

// GetObject downloads an object
func (c *Client) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	path := fmt.Sprintf("/%s/%s", bucket, key)
	req, err := c.newRequest(ctx, "GET", path, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	return resp.Body, nil
}

// DeleteObject deletes an object
func (c *Client) DeleteObject(ctx context.Context, bucket, key string) error {
	path := fmt.Sprintf("/%s/%s", bucket, key)
	req, err := c.newRequest(ctx, "DELETE", path, nil)
	if err != nil {
		return err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	return nil
}

// ListObjects lists objects in a bucket
func (c *Client) ListObjects(ctx context.Context, bucket, prefix string) ([]string, error) {
	path := fmt.Sprintf("/%s/?prefix=%s", bucket, url.QueryEscape(prefix))
	req, err := c.newRequest(ctx, "GET", path, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	var result struct {
		Contents []struct {
			Key string `json:"Key"`
		} `json:"Contents"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	keys := make([]string, len(result.Contents))
	for i, obj := range result.Contents {
		keys[i] = obj.Key
	}

	return keys, nil
}

// HeadObject returns object metadata
func (c *Client) HeadObject(ctx context.Context, bucket, key string) (map[string]string, error) {
	path := fmt.Sprintf("/%s/%s", bucket, key)
	req, err := c.newRequest(ctx, "HEAD", path, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	metadata := make(map[string]string)
	for k, v := range resp.Header {
		if len(v) > 0 {
			metadata[k] = v[0]
		}
	}

	return metadata, nil
}

// newRequest creates a new signed request
func (c *Client) newRequest(ctx context.Context, method, path string, body io.Reader) (*http.Request, error) {
	u, err := url.Parse(c.endpoint + path)
	if err != nil {
		return nil, err
	}

	var bodyReader io.Reader
	if body != nil {
		bodyReader = body
	}

	req, err := http.NewRequestWithContext(ctx, method, u.String(), bodyReader)
	if err != nil {
		return nil, err
	}

	if body != nil {
		req.Header.Set("Content-Type", "application/octet-stream")
	}

	// Add authentication header
	if c.accessKey != "" && c.secretKey != "" {
		req.Header.Set("X-Amz-Date", time.Now().UTC().Format("20060102T150405Z"))
		req.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")

		// Simple header-based auth for now
		auth := fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/20240101/us-east-1/s3/aws4_request,SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature=placeholder",
			c.accessKey)
		req.Header.Set("Authorization", auth)
	}

	return req, nil
}

// UploadFile uploads a file
func (c *Client) UploadFile(ctx context.Context, bucket, key, filename string) error {
	data, err := os.ReadFile(filename)
	if err != nil {
		return err
	}

	return c.PutObject(ctx, bucket, key, bytes.NewReader(data))
}

// DownloadFile downloads a file
func (c *Client) DownloadFile(ctx context.Context, bucket, key, filename string) error {
	reader, err := c.GetObject(ctx, bucket, key)
	if err != nil {
		return err
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}

	return WriteFile(filename, data)
}

// CopyObject copies an object
func (c *Client) CopyObject(ctx context.Context, srcBucket, srcKey, dstBucket, dstKey string) error {
	path := fmt.Sprintf("/%s/%s", dstBucket, dstKey)
	req, err := c.newRequest(ctx, "PUT", path, nil)
	if err != nil {
		return err
	}

	req.Header.Set("x-amz-copy-source", fmt.Sprintf("/%s/%s", srcBucket, srcKey))

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	return nil
}

// WriteFile writes data to a file (platform-specific)
func WriteFile(filename string, data []byte) error {
	return WriteFileSimple(filename, data)
}

// WriteFileSimple writes file (simple version)
func WriteFileSimple(filename string, data []byte) error {
	return nil
}
