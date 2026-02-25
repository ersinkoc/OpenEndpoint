package providers

import (
	"context"
	"testing"
	"time"
)

func TestNewCloudFlare(t *testing.T) {
	provider := NewCloudFlare("api-key", "email@example.com", "zone-id")
	if provider == nil {
		t.Error("NewCloudFlare returned nil")
	}
	if provider.APIKey != "api-key" {
		t.Errorf("APIKey = %s, expected api-key", provider.APIKey)
	}
}

func TestNewAWSCloudFront(t *testing.T) {
	provider := NewAWSCloudFront("access-key", "secret-key", "us-east-1")
	if provider == nil {
		t.Error("NewAWSCloudFront returned nil")
	}
	if provider.AccessKey != "access-key" {
		t.Errorf("AccessKey = %s, expected access-key", provider.AccessKey)
	}
}

func TestCloudFlareCreateDistribution(t *testing.T) {
	provider := NewCloudFlare("key", "email", "zone")
	ctx := context.Background()

	config := DistributionConfig{
		Enabled:  true,
		Provider: "cloudflare",
		Domain:   "example.com",
		ZoneID:   "zone-123",
		CacheBehavior: CacheBehavior{
			TTL:          time.Hour,
			CachePolicy:  "public",
			QueryStrings: "all",
		},
	}

	id, err := provider.CreateDistribution(ctx, config)
	if err != nil {
		t.Errorf("CreateDistribution error: %v", err)
	}
	if id == "" {
		t.Error("CreateDistribution returned empty ID")
	}
}

func TestCloudFlareGetDistribution(t *testing.T) {
	provider := NewCloudFlare("key", "email", "zone")
	ctx := context.Background()

	info, err := provider.GetDistribution(ctx, "dist-123")
	if err != nil {
		t.Errorf("GetDistribution error: %v", err)
	}
	if info == nil {
		t.Fatal("GetDistribution returned nil")
	}
	if info.ID != "dist-123" {
		t.Errorf("ID = %s, expected dist-123", info.ID)
	}
	if info.Status != "Deployed" {
		t.Errorf("Status = %s, expected Deployed", info.Status)
	}
}

func TestCloudFlareDeleteDistribution(t *testing.T) {
	provider := NewCloudFlare("key", "email", "zone")
	ctx := context.Background()

	err := provider.DeleteDistribution(ctx, "dist-123")
	if err != nil {
		t.Errorf("DeleteDistribution error: %v", err)
	}
}

func TestCloudFlareInvalidateCache(t *testing.T) {
	provider := NewCloudFlare("key", "email", "zone")
	ctx := context.Background()

	paths := []string{"/path1", "/path2"}
	id, err := provider.InvalidateCache(ctx, "dist-123", paths)
	if err != nil {
		t.Errorf("InvalidateCache error: %v", err)
	}
	if id == "" {
		t.Error("InvalidateCache returned empty ID")
	}
}

func TestCloudFlareGetInvalidationStatus(t *testing.T) {
	provider := NewCloudFlare("key", "email", "zone")
	ctx := context.Background()

	status, err := provider.GetInvalidationStatus(ctx, "dist-123", "inv-456")
	if err != nil {
		t.Errorf("GetInvalidationStatus error: %v", err)
	}
	if status != "Complete" {
		t.Errorf("Status = %s, expected Complete", status)
	}
}

func TestAWSCloudFrontCreateDistribution(t *testing.T) {
	provider := NewAWSCloudFront("access", "secret", "us-east-1")
	ctx := context.Background()

	config := DistributionConfig{
		Enabled:  true,
		Provider: "cloudfront",
		Domain:   "example.com",
		ZoneID:   "zone-123",
	}

	id, err := provider.CreateDistribution(ctx, config)
	if err != nil {
		t.Errorf("CreateDistribution error: %v", err)
	}
	if id == "" {
		t.Error("CreateDistribution returned empty ID")
	}
}

func TestAWSCloudFrontGetDistribution(t *testing.T) {
	provider := NewAWSCloudFront("access", "secret", "us-east-1")
	ctx := context.Background()

	info, err := provider.GetDistribution(ctx, "dist-123")
	if err != nil {
		t.Errorf("GetDistribution error: %v", err)
	}
	if info == nil {
		t.Fatal("GetDistribution returned nil")
	}
	if info.ID != "dist-123" {
		t.Errorf("ID = %s, expected dist-123", info.ID)
	}
	if info.Status != "Deployed" {
		t.Errorf("Status = %s, expected Deployed", info.Status)
	}
}

func TestAWSCloudFrontDeleteDistribution(t *testing.T) {
	provider := NewAWSCloudFront("access", "secret", "us-east-1")
	ctx := context.Background()

	err := provider.DeleteDistribution(ctx, "dist-123")
	if err != nil {
		t.Errorf("DeleteDistribution error: %v", err)
	}
}

func TestAWSCloudFrontInvalidateCache(t *testing.T) {
	provider := NewAWSCloudFront("access", "secret", "us-east-1")
	ctx := context.Background()

	paths := []string{"/path1", "/path2"}
	id, err := provider.InvalidateCache(ctx, "dist-123", paths)
	if err != nil {
		t.Errorf("InvalidateCache error: %v", err)
	}
	if id == "" {
		t.Error("InvalidateCache returned empty ID")
	}
}

func TestAWSCloudFrontGetInvalidationStatus(t *testing.T) {
	provider := NewAWSCloudFront("access", "secret", "us-east-1")
	ctx := context.Background()

	status, err := provider.GetInvalidationStatus(ctx, "dist-123", "inv-456")
	if err != nil {
		t.Errorf("GetInvalidationStatus error: %v", err)
	}
	if status != "Complete" {
		t.Errorf("Status = %s, expected Complete", status)
	}
}

func TestDistributionConfigDefaults(t *testing.T) {
	config := DistributionConfig{
		Enabled:    true,
		Provider:   "cloudflare",
		PriceClass: "100",
		SSLMethod:  "sni-only",
	}

	if !config.Enabled {
		t.Error("Enabled should be true")
	}
	if config.Provider != "cloudflare" {
		t.Errorf("Provider = %s, expected cloudflare", config.Provider)
	}
}

func TestCacheBehaviorConfig(t *testing.T) {
	behavior := CacheBehavior{
		TTL:          time.Hour,
		CachePolicy:  "public",
		Cookies:      "none",
		QueryStrings: "all",
	}

	if behavior.TTL != time.Hour {
		t.Errorf("TTL = %v, expected 1 hour", behavior.TTL)
	}
	if behavior.CachePolicy != "public" {
		t.Errorf("CachePolicy = %s, expected public", behavior.CachePolicy)
	}
}

func TestOriginAccessConfig(t *testing.T) {
	origin := OriginAccess{
		Protocol: "https",
		AuthType: "aws-sig",
	}
	origin.Credentials.AccessKey = "key"
	origin.Credentials.SecretKey = "secret"

	if origin.Protocol != "https" {
		t.Errorf("Protocol = %s, expected https", origin.Protocol)
	}
	if origin.Credentials.AccessKey != "key" {
		t.Errorf("AccessKey = %s, expected key", origin.Credentials.AccessKey)
	}
}

func TestDistributionInfoFields(t *testing.T) {
	info := DistributionInfo{
		ID:           "dist-123",
		Status:       "Deployed",
		DomainName:   "d123.cloudfront.net",
		ARN:          "arn:aws:cloudfront::123:distribution/d123",
		LastModified: time.Now(),
	}

	if info.ID != "dist-123" {
		t.Errorf("ID = %s, expected dist-123", info.ID)
	}
	if info.DomainName != "d123.cloudfront.net" {
		t.Errorf("DomainName = %s, expected d123.cloudfront.net", info.DomainName)
	}
}

func TestProviderInterface(t *testing.T) {
	var _ Provider = NewCloudFlare("key", "email", "zone")
	var _ Provider = NewAWSCloudFront("access", "secret", "us-east-1")
}
