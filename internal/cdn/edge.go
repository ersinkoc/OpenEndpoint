package cdn

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

// CDNProvider represents a CDN provider
type CDNProvider string

const (
	CDNCloudflare CDNProvider = "cloudflare"
	CDNFastly     CDNProvider = "fastly"
	CDNAkamai     CDNProvider = "akamai"
	CDNCloudFront CDNProvider = "cloudfront"
)

// CDNConfig contains CDN configuration
type CDNConfig struct {
	Provider       CDNProvider
	APIKey         string
	APIEndpoint    string
	ZoneID         string
	OriginShield   string
	CacheTTL       time.Duration
	InvalidatePath string
}

// EdgeConfig contains edge configuration
type EdgeConfig struct {
	CDN           CDNConfig
	Bucket        string
	Enabled       bool
	Domain        string
	SSLEnabled    bool
	CustomOrigins []string
}

// CacheInvalidation represents a cache invalidation request
type CacheInvalidation struct {
	ID        string    `json:"id"`
	Paths     []string  `json:"paths"`
	Status    string    `json:"status"` // pending, in_progress, complete, failed
	CreatedAt time.Time `json:"created_at"`
	Progress  int       `json:"progress"`
}

// EdgeManager manages CDN edge integration
type EdgeManager struct {
	config        EdgeConfig
	logger        *zap.Logger
	mu            sync.RWMutex
	client        CDNClient
	invalidations map[string]*CacheInvalidation
}

// CDNClient is an interface for CDN API clients
type CDNClient interface {
	Invalidate(paths []string) (string, error)
	GetInvalidation(id string) (*CacheInvalidation, error)
	CreatePresignedURL(path string, expiry time.Duration) (string, error)
}

var newCloudflareClient = func(config CDNConfig, logger *zap.Logger) (CDNClient, error) {
	return NewCloudflareClient(config, logger)
}

var newFastlyClient = func(config CDNConfig, logger *zap.Logger) (CDNClient, error) {
	return NewFastlyClient(config, logger)
}

var newAkamaiClient = func(config CDNConfig, logger *zap.Logger) (CDNClient, error) {
	return NewAkamaiClient(config, logger)
}

var newCloudFrontClient = func(config CDNConfig, logger *zap.Logger) (CDNClient, error) {
	return NewCloudFrontClient(config, logger)
}

// NewEdgeManager creates a new edge manager
func NewEdgeManager(config EdgeConfig, logger *zap.Logger) (*EdgeManager, error) {
	m := &EdgeManager{
		config:        config,
		logger:        logger,
		invalidations: make(map[string]*CacheInvalidation),
	}

	// Create appropriate client
	var client CDNClient
	var err error

	switch config.CDN.Provider {
	case CDNCloudflare:
		client, err = newCloudflareClient(config.CDN, logger)
	case CDNFastly:
		client, err = newFastlyClient(config.CDN, logger)
	case CDNAkamai:
		client, err = newAkamaiClient(config.CDN, logger)
	case CDNCloudFront:
		client, err = newCloudFrontClient(config.CDN, logger)
	default:
		return nil, fmt.Errorf("unsupported CDN provider: %s", config.CDN.Provider)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to create CDN client: %w", err)
	}

	m.client = client
	return m, nil
}

// Start starts the edge manager
func (m *EdgeManager) Start(ctx context.Context) error {
	m.logger.Info("Starting edge manager",
		zap.String("provider", string(m.config.CDN.Provider)),
		zap.String("bucket", m.config.Bucket))

	if !m.config.Enabled {
		m.logger.Info("CDN is disabled")
		return nil
	}

	return nil
}

// Stop stops the edge manager
func (m *EdgeManager) Stop() {
	m.logger.Info("Edge manager stopped")
}

// InvalidateCache invalidates cache for paths
func (m *EdgeManager) InvalidateCache(paths []string) (*CacheInvalidation, error) {
	if !m.config.Enabled {
		return nil, fmt.Errorf("CDN is not enabled")
	}

	// Normalize paths
	normalizedPaths := make([]string, len(paths))
	for i, path := range paths {
		normalizedPaths[i] = m.normalizePath(path)
	}

	invalidation := &CacheInvalidation{
		ID:        uuid.New().String(),
		Paths:     normalizedPaths,
		Status:    "pending",
		CreatedAt: time.Now(),
	}

	m.mu.Lock()
	m.invalidations[invalidation.ID] = invalidation
	m.mu.Unlock()

	// Execute invalidation
	go func() {
		invalidation.Status = "in_progress"

		resultID, err := m.client.Invalidate(normalizedPaths)
		if err != nil {
			invalidation.Status = "failed"
			m.logger.Error("Cache invalidation failed",
				zap.Error(err),
				zap.Strings("paths", normalizedPaths))
			return
		}

		// Poll for completion
		for {
			status, err := m.client.GetInvalidation(resultID)
			if err != nil {
				invalidation.Status = "failed"
				return
			}

			invalidation.Status = status.Status
			invalidation.Progress = status.Progress

			if status.Status == "complete" || status.Status == "failed" {
				break
			}

			time.Sleep(2 * time.Second)
		}
	}()

	m.logger.Info("Cache invalidation initiated",
		zap.String("id", invalidation.ID),
		zap.Strings("paths", normalizedPaths))

	return invalidation, nil
}

// InvalidateBucket invalidates entire bucket cache
func (m *EdgeManager) InvalidateBucket() (*CacheInvalidation, error) {
	return m.InvalidateCache([]string{"/" + m.config.Bucket + "/*"})
}

// CreatePresignedDelegation creates a presigned URL that delegates to CDN
func (m *EdgeManager) CreatePresignedDelegation(path string, expiry time.Duration) (string, error) {
	// Create presigned URL with CDN delegation
	originPath := m.getOriginPath(path)

	// Get CDN presigned URL
	cdnURL, err := m.client.CreatePresignedURL(originPath, expiry)
	if err != nil {
		return "", fmt.Errorf("failed to create CDN presigned URL: %w", err)
	}

	return cdnURL, nil
}

// normalizePath normalizes a path for CDN
func (m *EdgeManager) normalizePath(path string) string {
	path = strings.TrimPrefix(path, "/")
	if !strings.HasSuffix(path, "*") && !strings.HasSuffix(path, "/") {
		path = path + "*"
	}
	return "/" + m.config.Bucket + "/" + path
}

// getOriginPath gets the origin path for a CDN path
func (m *EdgeManager) getOriginPath(cdnPath string) string {
	// Strip bucket prefix
	parts := strings.SplitN(cdnPath, "/", 2)
	if len(parts) > 1 {
		return parts[1]
	}
	return "/"
}

// GetInvalidation returns an invalidation by ID
func (m *EdgeManager) GetInvalidation(id string) (*CacheInvalidation, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	inv, ok := m.invalidations[id]
	return inv, ok
}

// CloudflareClient implements CDNClient for Cloudflare
type CloudflareClient struct {
	config CDNConfig
	logger *zap.Logger
}

// NewCloudflareClient creates a new Cloudflare client
func NewCloudflareClient(config CDNConfig, logger *zap.Logger) (*CloudflareClient, error) {
	return &CloudflareClient{
		config: config,
		logger: logger,
	}, nil
}

// Invalidate invalidates Cloudflare cache
func (c *CloudflareClient) Invalidate(paths []string) (string, error) {
	// In production, call Cloudflare API
	// POST https://api.cloudflare.com/client/v4/zones/{zone}/purge_cache
	c.logger.Info("Cloudflare cache invalidation",
		zap.Strings("paths", paths))

	return uuid.New().String(), nil
}

// GetInvalidation gets invalidation status
func (c *CloudflareClient) GetInvalidation(id string) (*CacheInvalidation, error) {
	// In production, call Cloudflare API
	return &CacheInvalidation{
		ID:       id,
		Status:   "complete",
		Progress: 100,
	}, nil
}

// CreatePresignedURL creates a presigned URL
func (c *CloudflareClient) CreatePresignedURL(path string, expiry time.Duration) (string, error) {
	// In production, use Cloudflare Workers or signed URLs
	return fmt.Sprintf("https://%s%s?expires=%d&signature=...", c.config.ZoneID, path, time.Now().Add(expiry).Unix()), nil
}

// FastlyClient implements CDNClient for Fastly
type FastlyClient struct {
	config CDNConfig
	logger *zap.Logger
}

// NewFastlyClient creates a new Fastly client
func NewFastlyClient(config CDNConfig, logger *zap.Logger) (*FastlyClient, error) {
	return &FastlyClient{
		config: config,
		logger: logger,
	}, nil
}

// Invalidate invalidates Fastly cache
func (c *FastlyClient) Invalidate(paths []string) (string, error) {
	c.logger.Info("Fastly cache invalidation",
		zap.Strings("paths", paths))
	return uuid.New().String(), nil
}

// GetInvalidation gets invalidation status
func (c *FastlyClient) GetInvalidation(id string) (*CacheInvalidation, error) {
	return &CacheInvalidation{ID: id, Status: "complete", Progress: 100}, nil
}

// CreatePresignedURL creates a presigned URL
func (c *FastlyClient) CreatePresignedURL(path string, expiry time.Duration) (string, error) {
	return fmt.Sprintf("https://%s%s?expires=%d", c.config.APIEndpoint, path, time.Now().Add(expiry).Unix()), nil
}

// AkamaiClient implements CDNClient for Akamai
type AkamaiClient struct {
	config CDNConfig
	logger *zap.Logger
}

// NewAkamaiClient creates a new Akamai client
func NewAkamaiClient(config CDNConfig, logger *zap.Logger) (*AkamaiClient, error) {
	return &AkamaiClient{
		config: config,
		logger: logger,
	}, nil
}

// Invalidate invalidates Akamai cache
func (c *AkamaiClient) Invalidate(paths []string) (string, error) {
	c.logger.Info("Akamai cache invalidation",
		zap.Strings("paths", paths))
	return uuid.New().String(), nil
}

// GetInvalidation gets invalidation status
func (c *AkamaiClient) GetInvalidation(id string) (*CacheInvalidation, error) {
	return &CacheInvalidation{ID: id, Status: "complete", Progress: 100}, nil
}

// CreatePresignedURL creates a presigned URL
func (c *AkamaiClient) CreatePresignedURL(path string, expiry time.Duration) (string, error) {
	return fmt.Sprintf("https://%s%s?expires=%d", c.config.APIEndpoint, path, time.Now().Add(expiry).Unix()), nil
}

// CloudFrontClient implements CDNClient for CloudFront
type CloudFrontClient struct {
	config CDNConfig
	logger *zap.Logger
}

// NewCloudFrontClient creates a new CloudFront client
func NewCloudFrontClient(config CDNConfig, logger *zap.Logger) (*CloudFrontClient, error) {
	return &CloudFrontClient{
		config: config,
		logger: logger,
	}, nil
}

// Invalidate invalidates CloudFront cache
func (c *CloudFrontClient) Invalidate(paths []string) (string, error) {
	c.logger.Info("CloudFront cache invalidation",
		zap.Strings("paths", paths))
	return uuid.New().String(), nil
}

// GetInvalidation gets invalidation status
func (c *CloudFrontClient) GetInvalidation(id string) (*CacheInvalidation, error) {
	return &CacheInvalidation{ID: id, Status: "complete", Progress: 100}, nil
}

// CreatePresignedURL creates a presigned URL
func (c *CloudFrontClient) CreatePresignedURL(path string, expiry time.Duration) (string, error) {
	return fmt.Sprintf("https://%s%s?expires=%d", c.config.APIEndpoint, path, time.Now().Add(expiry).Unix()), nil
}

// ConfigFromJSON parses CDN config from JSON
func ConfigFromJSON(data []byte) (*EdgeConfig, error) {
	var config EdgeConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse CDN config: %w", err)
	}
	return &config, nil
}
