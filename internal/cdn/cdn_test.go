package cdn

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestNewHandler(t *testing.T) {
	handler := NewHandler("cloudflare")
	if handler == nil {
		t.Fatal("Handler should not be nil")
	}
	if handler.GetProvider() != "cloudflare" {
		t.Errorf("Provider = %s, want cloudflare", handler.GetProvider())
	}
}

func TestHandlerInvalidateCache(t *testing.T) {
	handler := NewHandler("cloudflare")

	err := handler.InvalidateCache([]string{"/path1", "/path2"})
	if err != nil {
		t.Fatalf("InvalidateCache failed: %v", err)
	}
}

func TestHandlerGenerateSignedURL(t *testing.T) {
	handler := NewHandler("cloudflare")

	url, err := handler.GenerateSignedURL("/test/path", 3600)
	if err != nil {
		t.Fatalf("GenerateSignedURL failed: %v", err)
	}
	if url == "" {
		t.Error("URL should not be empty")
	}
}

func TestHandlerGetProvider(t *testing.T) {
	handler := NewHandler("fastly")

	provider := handler.GetProvider()
	if provider != "fastly" {
		t.Errorf("Provider = %s, want fastly", provider)
	}
}

func TestCDNProviderConstants(t *testing.T) {
	if CDNCloudflare != "cloudflare" {
		t.Errorf("CDNCloudflare = %v, want cloudflare", CDNCloudflare)
	}
	if CDNFastly != "fastly" {
		t.Errorf("CDNFastly = %v, want fastly", CDNFastly)
	}
	if CDNAkamai != "akamai" {
		t.Errorf("CDNAkamai = %v, want akamai", CDNAkamai)
	}
	if CDNCloudFront != "cloudfront" {
		t.Errorf("CDNCloudFront = %v, want cloudfront", CDNCloudFront)
	}
}

func TestCDNConfigStruct(t *testing.T) {
	config := CDNConfig{
		Provider:       CDNCloudflare,
		APIKey:         "test-key",
		APIEndpoint:    "https://api.cloudflare.com",
		ZoneID:         "zone123",
		OriginShield:   "shield.example.com",
		CacheTTL:       time.Hour,
		InvalidatePath: "/purge",
	}

	if config.Provider != CDNCloudflare {
		t.Errorf("Provider = %v, want cloudflare", config.Provider)
	}
}

func TestEdgeConfigStruct(t *testing.T) {
	config := EdgeConfig{
		CDN:           CDNConfig{Provider: CDNFastly},
		Bucket:        "test-bucket",
		Enabled:       true,
		Domain:        "cdn.example.com",
		SSLEnabled:    true,
		CustomOrigins: []string{"origin1.example.com"},
	}

	if config.Bucket != "test-bucket" {
		t.Errorf("Bucket = %v, want test-bucket", config.Bucket)
	}
}

func TestCacheInvalidationStruct(t *testing.T) {
	inv := CacheInvalidation{
		ID:        "inv123",
		Paths:     []string{"/path1", "/path2"},
		Status:    "pending",
		CreatedAt: time.Now(),
		Progress:  0,
	}

	if inv.ID != "inv123" {
		t.Errorf("ID = %v, want inv123", inv.ID)
	}
}

func TestNewEdgeManagerCloudflare(t *testing.T) {
	logger := zap.NewNop()
	config := EdgeConfig{
		CDN: CDNConfig{
			Provider: CDNCloudflare,
			APIKey:   "test-key",
			ZoneID:   "zone123",
		},
		Bucket:  "test-bucket",
		Enabled: true,
	}

	mgr, err := NewEdgeManager(config, logger)
	if err != nil {
		t.Fatalf("NewEdgeManager failed: %v", err)
	}
	if mgr == nil {
		t.Fatal("EdgeManager should not be nil")
	}
}

func TestNewEdgeManagerFastly(t *testing.T) {
	logger := zap.NewNop()
	config := EdgeConfig{
		CDN: CDNConfig{
			Provider:    CDNFastly,
			APIKey:      "test-key",
			APIEndpoint: "https://api.fastly.com",
		},
		Bucket:  "test-bucket",
		Enabled: true,
	}

	mgr, err := NewEdgeManager(config, logger)
	if err != nil {
		t.Fatalf("NewEdgeManager failed: %v", err)
	}
	if mgr == nil {
		t.Fatal("EdgeManager should not be nil")
	}
}

func TestNewEdgeManagerAkamai(t *testing.T) {
	logger := zap.NewNop()
	config := EdgeConfig{
		CDN: CDNConfig{
			Provider:    CDNAkamai,
			APIKey:      "test-key",
			APIEndpoint: "https://api.akamai.com",
		},
		Bucket:  "test-bucket",
		Enabled: true,
	}

	mgr, err := NewEdgeManager(config, logger)
	if err != nil {
		t.Fatalf("NewEdgeManager failed: %v", err)
	}
	if mgr == nil {
		t.Fatal("EdgeManager should not be nil")
	}
}

func TestNewEdgeManagerCloudFront(t *testing.T) {
	logger := zap.NewNop()
	config := EdgeConfig{
		CDN: CDNConfig{
			Provider:    CDNCloudFront,
			APIKey:      "test-key",
			APIEndpoint: "https://cloudfront.amazonaws.com",
		},
		Bucket:  "test-bucket",
		Enabled: true,
	}

	mgr, err := NewEdgeManager(config, logger)
	if err != nil {
		t.Fatalf("NewEdgeManager failed: %v", err)
	}
	if mgr == nil {
		t.Fatal("EdgeManager should not be nil")
	}
}

func TestNewEdgeManagerUnsupportedProvider(t *testing.T) {
	logger := zap.NewNop()
	config := EdgeConfig{
		CDN: CDNConfig{
			Provider: "unsupported",
		},
		Bucket:  "test-bucket",
		Enabled: true,
	}

	_, err := NewEdgeManager(config, logger)
	if err == nil {
		t.Error("NewEdgeManager should fail for unsupported provider")
	}
}

func TestEdgeManagerStart(t *testing.T) {
	logger := zap.NewNop()
	config := EdgeConfig{
		CDN: CDNConfig{
			Provider: CDNCloudflare,
		},
		Bucket:  "test-bucket",
		Enabled: true,
	}

	mgr, _ := NewEdgeManager(config, logger)
	err := mgr.Start(context.Background())
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}
}

func TestEdgeManagerStartDisabled(t *testing.T) {
	logger := zap.NewNop()
	config := EdgeConfig{
		CDN: CDNConfig{
			Provider: CDNCloudflare,
		},
		Bucket:  "test-bucket",
		Enabled: false,
	}

	mgr, _ := NewEdgeManager(config, logger)
	err := mgr.Start(context.Background())
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}
}

func TestEdgeManagerStop(t *testing.T) {
	logger := zap.NewNop()
	config := EdgeConfig{
		CDN: CDNConfig{
			Provider: CDNCloudflare,
		},
		Bucket:  "test-bucket",
		Enabled: true,
	}

	mgr, _ := NewEdgeManager(config, logger)
	mgr.Stop()
}

func TestEdgeManagerInvalidateCache(t *testing.T) {
	logger := zap.NewNop()
	config := EdgeConfig{
		CDN: CDNConfig{
			Provider: CDNCloudflare,
		},
		Bucket:  "test-bucket",
		Enabled: true,
	}

	mgr, _ := NewEdgeManager(config, logger)
	inv, err := mgr.InvalidateCache([]string{"/path1", "/path2"})
	if err != nil {
		t.Fatalf("InvalidateCache failed: %v", err)
	}
	if inv == nil {
		t.Fatal("Invalidation should not be nil")
	}
	if inv.Status != "pending" {
		t.Errorf("Status = %v, want pending", inv.Status)
	}
}

func TestEdgeManagerInvalidateCacheDisabled(t *testing.T) {
	logger := zap.NewNop()
	config := EdgeConfig{
		CDN: CDNConfig{
			Provider: CDNCloudflare,
		},
		Bucket:  "test-bucket",
		Enabled: false,
	}

	mgr, _ := NewEdgeManager(config, logger)
	_, err := mgr.InvalidateCache([]string{"/path1"})
	if err == nil {
		t.Error("InvalidateCache should fail when CDN is disabled")
	}
}

func TestEdgeManagerInvalidateBucket(t *testing.T) {
	logger := zap.NewNop()
	config := EdgeConfig{
		CDN: CDNConfig{
			Provider: CDNCloudflare,
		},
		Bucket:  "test-bucket",
		Enabled: true,
	}

	mgr, _ := NewEdgeManager(config, logger)
	inv, err := mgr.InvalidateBucket()
	if err != nil {
		t.Fatalf("InvalidateBucket failed: %v", err)
	}
	if inv == nil {
		t.Fatal("Invalidation should not be nil")
	}
}

func TestEdgeManagerCreatePresignedDelegation(t *testing.T) {
	logger := zap.NewNop()
	config := EdgeConfig{
		CDN: CDNConfig{
			Provider: CDNCloudflare,
			ZoneID:   "zone123",
		},
		Bucket:  "test-bucket",
		Enabled: true,
	}

	mgr, _ := NewEdgeManager(config, logger)
	url, err := mgr.CreatePresignedDelegation("/test/path", time.Hour)
	if err != nil {
		t.Fatalf("CreatePresignedDelegation failed: %v", err)
	}
	if url == "" {
		t.Error("URL should not be empty")
	}
}

func TestEdgeManagerGetInvalidation(t *testing.T) {
	logger := zap.NewNop()
	config := EdgeConfig{
		CDN: CDNConfig{
			Provider: CDNCloudflare,
		},
		Bucket:  "test-bucket",
		Enabled: true,
	}

	mgr, _ := NewEdgeManager(config, logger)
	inv, _ := mgr.InvalidateCache([]string{"/path1"})

	retrieved, ok := mgr.GetInvalidation(inv.ID)
	if !ok {
		t.Fatal("Should find invalidation")
	}
	if retrieved.ID != inv.ID {
		t.Errorf("ID = %v, want %v", retrieved.ID, inv.ID)
	}
}

func TestEdgeManagerGetInvalidationNotFound(t *testing.T) {
	logger := zap.NewNop()
	config := EdgeConfig{
		CDN: CDNConfig{
			Provider: CDNCloudflare,
		},
		Bucket:  "test-bucket",
		Enabled: true,
	}

	mgr, _ := NewEdgeManager(config, logger)
	_, ok := mgr.GetInvalidation("nonexistent")
	if ok {
		t.Error("Should not find nonexistent invalidation")
	}
}

func TestEdgeManagerNormalizePath(t *testing.T) {
	logger := zap.NewNop()
	config := EdgeConfig{
		CDN:     CDNConfig{Provider: CDNCloudflare},
		Bucket:  "test-bucket",
		Enabled: true,
	}

	mgr, _ := NewEdgeManager(config, logger)

	tests := []struct {
		input    string
		expected string
	}{
		{"/path1", "/test-bucket/path1*"},
		{"path2", "/test-bucket/path2*"},
		{"/path3/", "/test-bucket/path3/"},
		{"/path4/*", "/test-bucket/path4/*"},
	}

	for _, tt := range tests {
		result := mgr.normalizePath(tt.input)
		if result != tt.expected {
			t.Errorf("normalizePath(%s) = %s, want %s", tt.input, result, tt.expected)
		}
	}
}

func TestEdgeManagerGetOriginPath(t *testing.T) {
	logger := zap.NewNop()
	config := EdgeConfig{
		CDN:     CDNConfig{Provider: CDNCloudflare},
		Bucket:  "test-bucket",
		Enabled: true,
	}

	mgr, _ := NewEdgeManager(config, logger)

	tests := []struct {
		input    string
		expected string
	}{
		{"bucket/path1", "path1"},
		{"bucket", "/"},
	}

	for _, tt := range tests {
		result := mgr.getOriginPath(tt.input)
		if result != tt.expected {
			t.Errorf("getOriginPath(%s) = %s, want %s", tt.input, result, tt.expected)
		}
	}
}

func TestNewCloudflareClient(t *testing.T) {
	logger := zap.NewNop()
	config := CDNConfig{Provider: CDNCloudflare, ZoneID: "zone123"}

	client, err := NewCloudflareClient(config, logger)
	if err != nil {
		t.Fatalf("NewCloudflareClient failed: %v", err)
	}
	if client == nil {
		t.Fatal("Client should not be nil")
	}
}

func TestCloudflareClientInvalidate(t *testing.T) {
	logger := zap.NewNop()
	config := CDNConfig{Provider: CDNCloudflare, ZoneID: "zone123"}

	client, _ := NewCloudflareClient(config, logger)
	id, err := client.Invalidate([]string{"/path1"})
	if err != nil {
		t.Fatalf("Invalidate failed: %v", err)
	}
	if id == "" {
		t.Error("ID should not be empty")
	}
}

func TestCloudflareClientGetInvalidation(t *testing.T) {
	logger := zap.NewNop()
	config := CDNConfig{Provider: CDNCloudflare, ZoneID: "zone123"}

	client, _ := NewCloudflareClient(config, logger)
	inv, err := client.GetInvalidation("inv123")
	if err != nil {
		t.Fatalf("GetInvalidation failed: %v", err)
	}
	if inv.Status != "complete" {
		t.Errorf("Status = %v, want complete", inv.Status)
	}
}

func TestCloudflareClientCreatePresignedURL(t *testing.T) {
	logger := zap.NewNop()
	config := CDNConfig{Provider: CDNCloudflare, ZoneID: "zone123"}

	client, _ := NewCloudflareClient(config, logger)
	url, err := client.CreatePresignedURL("/path1", time.Hour)
	if err != nil {
		t.Fatalf("CreatePresignedURL failed: %v", err)
	}
	if url == "" {
		t.Error("URL should not be empty")
	}
}

func TestNewFastlyClient(t *testing.T) {
	logger := zap.NewNop()
	config := CDNConfig{Provider: CDNFastly}

	client, err := NewFastlyClient(config, logger)
	if err != nil {
		t.Fatalf("NewFastlyClient failed: %v", err)
	}
	if client == nil {
		t.Fatal("Client should not be nil")
	}
}

func TestFastlyClientMethods(t *testing.T) {
	logger := zap.NewNop()
	config := CDNConfig{Provider: CDNFastly, APIEndpoint: "https://fastly.com"}
	client, _ := NewFastlyClient(config, logger)

	id, err := client.Invalidate([]string{"/path1"})
	if err != nil || id == "" {
		t.Errorf("Invalidate failed: %v", err)
	}

	inv, err := client.GetInvalidation(id)
	if err != nil || inv.Status != "complete" {
		t.Errorf("GetInvalidation failed: %v", err)
	}

	url, err := client.CreatePresignedURL("/path1", time.Hour)
	if err != nil || url == "" {
		t.Errorf("CreatePresignedURL failed: %v", err)
	}
}

func TestNewAkamaiClient(t *testing.T) {
	logger := zap.NewNop()
	config := CDNConfig{Provider: CDNAkamai}

	client, err := NewAkamaiClient(config, logger)
	if err != nil {
		t.Fatalf("NewAkamaiClient failed: %v", err)
	}
	if client == nil {
		t.Fatal("Client should not be nil")
	}
}

func TestAkamaiClientMethods(t *testing.T) {
	logger := zap.NewNop()
	config := CDNConfig{Provider: CDNAkamai, APIEndpoint: "https://akamai.com"}
	client, _ := NewAkamaiClient(config, logger)

	id, err := client.Invalidate([]string{"/path1"})
	if err != nil || id == "" {
		t.Errorf("Invalidate failed: %v", err)
	}

	inv, err := client.GetInvalidation(id)
	if err != nil || inv.Status != "complete" {
		t.Errorf("GetInvalidation failed: %v", err)
	}

	url, err := client.CreatePresignedURL("/path1", time.Hour)
	if err != nil || url == "" {
		t.Errorf("CreatePresignedURL failed: %v", err)
	}
}

func TestNewCloudFrontClient(t *testing.T) {
	logger := zap.NewNop()
	config := CDNConfig{Provider: CDNCloudFront}

	client, err := NewCloudFrontClient(config, logger)
	if err != nil {
		t.Fatalf("NewCloudFrontClient failed: %v", err)
	}
	if client == nil {
		t.Fatal("Client should not be nil")
	}
}

func TestCloudFrontClientMethods(t *testing.T) {
	logger := zap.NewNop()
	config := CDNConfig{Provider: CDNCloudFront, APIEndpoint: "https://cloudfront.amazonaws.com"}
	client, _ := NewCloudFrontClient(config, logger)

	id, err := client.Invalidate([]string{"/path1"})
	if err != nil || id == "" {
		t.Errorf("Invalidate failed: %v", err)
	}

	inv, err := client.GetInvalidation(id)
	if err != nil || inv.Status != "complete" {
		t.Errorf("GetInvalidation failed: %v", err)
	}

	url, err := client.CreatePresignedURL("/path1", time.Hour)
	if err != nil || url == "" {
		t.Errorf("CreatePresignedURL failed: %v", err)
	}
}

func TestConfigFromJSON(t *testing.T) {
	jsonData := `{
		"bucket": "test-bucket",
		"enabled": true,
		"domain": "cdn.example.com",
		"sslEnabled": true,
		"cdn": {
			"provider": "cloudflare",
			"apiKey": "test-key",
			"zoneId": "zone123"
		}
	}`

	config, err := ConfigFromJSON([]byte(jsonData))
	if err != nil {
		t.Fatalf("ConfigFromJSON failed: %v", err)
	}
	if config.Bucket != "test-bucket" {
		t.Errorf("Bucket = %v, want test-bucket", config.Bucket)
	}
}

func TestConfigFromJSONInvalid(t *testing.T) {
	_, err := ConfigFromJSON([]byte("invalid json"))
	if err == nil {
		t.Error("ConfigFromJSON should fail for invalid JSON")
	}
}

type mockCDNClient struct {
	invalidateErr         error
	getInvalidationErr    error
	createPresignedURLErr error
	invalidationStatus    string
}

func (m *mockCDNClient) Invalidate(paths []string) (string, error) {
	if m.invalidateErr != nil {
		return "", m.invalidateErr
	}
	return "mock-inv-id", nil
}

func (m *mockCDNClient) GetInvalidation(id string) (*CacheInvalidation, error) {
	if m.getInvalidationErr != nil {
		return nil, m.getInvalidationErr
	}
	status := "complete"
	if m.invalidationStatus != "" {
		status = m.invalidationStatus
	}
	return &CacheInvalidation{ID: id, Status: status, Progress: 100}, nil
}

func (m *mockCDNClient) CreatePresignedURL(path string, expiry time.Duration) (string, error) {
	if m.createPresignedURLErr != nil {
		return "", m.createPresignedURLErr
	}
	return "https://mock-cdn.example.com" + path, nil
}

func TestEdgeManagerInvalidateCacheClientError(t *testing.T) {
	logger := zap.NewNop()
	config := EdgeConfig{
		CDN:     CDNConfig{Provider: CDNCloudflare},
		Bucket:  "test-bucket",
		Enabled: true,
	}

	mgr, _ := NewEdgeManager(config, logger)
	mgr.client = &mockCDNClient{invalidateErr: fmt.Errorf("client error")}

	inv, err := mgr.InvalidateCache([]string{"/path1"})
	if err != nil {
		t.Fatalf("InvalidateCache should not fail immediately: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	retrieved, ok := mgr.GetInvalidation(inv.ID)
	if !ok {
		t.Fatal("Should find invalidation")
	}
	if retrieved.Status != "failed" {
		t.Errorf("Status = %v, want failed", retrieved.Status)
	}
}

func TestEdgeManagerInvalidateCacheGetInvalidationError(t *testing.T) {
	logger := zap.NewNop()
	config := EdgeConfig{
		CDN:     CDNConfig{Provider: CDNCloudflare},
		Bucket:  "test-bucket",
		Enabled: true,
	}

	mgr, _ := NewEdgeManager(config, logger)
	mgr.client = &mockCDNClient{getInvalidationErr: fmt.Errorf("status error")}

	inv, err := mgr.InvalidateCache([]string{"/path1"})
	if err != nil {
		t.Fatalf("InvalidateCache should not fail immediately: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	retrieved, ok := mgr.GetInvalidation(inv.ID)
	if !ok {
		t.Fatal("Should find invalidation")
	}
	if retrieved.Status != "failed" {
		t.Errorf("Status = %v, want failed", retrieved.Status)
	}
}

func TestEdgeManagerInvalidateCachePollingLoop(t *testing.T) {
	logger := zap.NewNop()
	config := EdgeConfig{
		CDN:     CDNConfig{Provider: CDNCloudflare},
		Bucket:  "test-bucket",
		Enabled: true,
	}

	mgr, _ := NewEdgeManager(config, logger)
	mgr.client = &mockCDNClient{invalidationStatus: "in_progress"}

	inv, err := mgr.InvalidateCache([]string{"/path1"})
	if err != nil {
		t.Fatalf("InvalidateCache failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	retrieved, _ := mgr.GetInvalidation(inv.ID)
	if retrieved.Status != "in_progress" && retrieved.Status != "complete" {
		t.Errorf("Unexpected status: %v", retrieved.Status)
	}
}

func TestEdgeManagerCreatePresignedDelegationError(t *testing.T) {
	logger := zap.NewNop()
	config := EdgeConfig{
		CDN:     CDNConfig{Provider: CDNCloudflare, ZoneID: "zone123"},
		Bucket:  "test-bucket",
		Enabled: true,
	}

	mgr, _ := NewEdgeManager(config, logger)
	mgr.client = &mockCDNClient{createPresignedURLErr: fmt.Errorf("presigned error")}

	_, err := mgr.CreatePresignedDelegation("/test/path", time.Hour)
	if err == nil {
		t.Error("CreatePresignedDelegation should fail when client returns error")
	}
}

func TestNewEdgeManagerClientCreationError(t *testing.T) {
	logger := zap.NewNop()

	originalCloudflareClient := newCloudflareClient
	newCloudflareClient = func(config CDNConfig, logger *zap.Logger) (CDNClient, error) {
		return nil, fmt.Errorf("client creation failed")
	}
	defer func() { newCloudflareClient = originalCloudflareClient }()

	config := EdgeConfig{
		CDN:     CDNConfig{Provider: CDNCloudflare},
		Bucket:  "test-bucket",
		Enabled: true,
	}

	_, err := NewEdgeManager(config, logger)
	if err == nil {
		t.Error("NewEdgeManager should fail when client creation fails")
	}
}
