package bucketconfig

import (
	"errors"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	cfg := New()
	if cfg == nil {
		t.Fatal("Config should not be nil")
	}

	if cfg.buckets == nil {
		t.Error("Buckets map should be initialized")
	}

	if cfg.versioning == nil {
		t.Error("Versioning map should be initialized")
	}

	if cfg.cors == nil {
		t.Error("CORS map should be initialized")
	}

	if cfg.policies == nil {
		t.Error("Policies map should be initialized")
	}
}

func TestSetVersioningConfig(t *testing.T) {
	cfg := New()

	versioning := &VersioningConfig{
		Status:    "Enabled",
		MFADelete: "Disabled",
	}

	err := cfg.SetVersioningConfig("test-bucket", versioning)
	if err != nil {
		t.Fatalf("SetVersioningConfig failed: %v", err)
	}
}

func TestGetVersioningConfig(t *testing.T) {
	cfg := New()

	// Test non-existent bucket
	_, ok := cfg.GetVersioningConfig("non-existent")
	if ok {
		t.Error("Should not find versioning for non-existent bucket")
	}

	// Test existing bucket
	versioning := &VersioningConfig{Status: "Enabled"}
	cfg.SetVersioningConfig("test-bucket", versioning)

	result, ok := cfg.GetVersioningConfig("test-bucket")
	if !ok {
		t.Fatal("Should find versioning")
	}

	if result.Status != "Enabled" {
		t.Errorf("Status = %s, want Enabled", result.Status)
	}
}

func TestIsVersioningEnabled(t *testing.T) {
	cfg := New()

	// Test disabled (default)
	if cfg.IsVersioningEnabled("test-bucket") {
		t.Error("Versioning should not be enabled by default")
	}

	// Test enabled
	versioning := &VersioningConfig{Status: "Enabled"}
	cfg.SetVersioningConfig("test-bucket", versioning)

	if !cfg.IsVersioningEnabled("test-bucket") {
		t.Error("Versioning should be enabled")
	}
}

func TestSetCORSConfig(t *testing.T) {
	cfg := New()

	cors := &CORSConfig{
		Bucket: "test-bucket",
		CORSRules: []*CORSRule{
			{
				AllowedOrigins: []string{"*"},
				AllowedMethods: []string{"GET", "PUT"},
			},
		},
	}

	err := cfg.SetCORSConfig("test-bucket", cors)
	if err != nil {
		t.Fatalf("SetCORSConfig failed: %v", err)
	}
}

func TestGetCORSConfig(t *testing.T) {
	cfg := New()

	// Test non-existent
	_, ok := cfg.GetCORSConfig("non-existent")
	if ok {
		t.Error("Should not find CORS for non-existent bucket")
	}

	// Test existing
	cors := &CORSConfig{
		Bucket: "test-bucket",
		CORSRules: []*CORSRule{
			{
				AllowedOrigins: []string{"https://example.com"},
			},
		},
	}
	cfg.SetCORSConfig("test-bucket", cors)

	result, ok := cfg.GetCORSConfig("test-bucket")
	if !ok {
		t.Fatal("Should find CORS config")
	}

	if len(result.CORSRules) != 1 {
		t.Errorf("CORSRules length = %d, want 1", len(result.CORSRules))
	}
}

func TestDeleteCORSConfig(t *testing.T) {
	cfg := New()

	cors := &CORSConfig{
		Bucket: "test-bucket",
		CORSRules: []*CORSRule{
			{AllowedOrigins: []string{"*"}},
		},
	}
	cfg.SetCORSConfig("test-bucket", cors)

	err := cfg.DeleteCORSConfig("test-bucket")
	if err != nil {
		t.Fatalf("DeleteCORSConfig failed: %v", err)
	}

	_, ok := cfg.GetCORSConfig("test-bucket")
	if ok {
		t.Error("CORS config should be deleted")
	}
}

func TestSetBucketPolicy(t *testing.T) {
	cfg := New()

	policy := &BucketPolicy{
		Version: "2012-10-17",
		Statement: []*PolicyStatement{
			{
				Effect:    "Allow",
				Principal: "*",
				Action:    "s3:GetObject",
				Resource:  "arn:aws:s3:::test-bucket/*",
			},
		},
	}

	err := cfg.SetBucketPolicy("test-bucket", policy)
	if err != nil {
		t.Fatalf("SetBucketPolicy failed: %v", err)
	}
}

func TestGetBucketPolicy(t *testing.T) {
	cfg := New()

	// Test non-existent
	_, ok := cfg.GetBucketPolicy("non-existent")
	if ok {
		t.Error("Should not find policy for non-existent bucket")
	}

	// Test existing
	policy := &BucketPolicy{Version: "2012-10-17"}
	cfg.SetBucketPolicy("test-bucket", policy)

	result, ok := cfg.GetBucketPolicy("test-bucket")
	if !ok {
		t.Fatal("Should find policy")
	}

	if result.Version != "2012-10-17" {
		t.Errorf("Version = %s, want 2012-10-17", result.Version)
	}
}

func TestDeleteBucketPolicy(t *testing.T) {
	cfg := New()

	policy := &BucketPolicy{Version: "2012-10-17"}
	cfg.SetBucketPolicy("test-bucket", policy)

	err := cfg.DeleteBucketPolicy("test-bucket")
	if err != nil {
		t.Fatalf("DeleteBucketPolicy failed: %v", err)
	}

	_, ok := cfg.GetBucketPolicy("test-bucket")
	if ok {
		t.Error("Policy should be deleted")
	}
}

func TestSetObjectLockConfig(t *testing.T) {
	cfg := New()

	lock := &ObjectLockConfig{
		Enabled:          true,
		RetentionMode:    "Compliance",
		RetentionDays:    30,
		LegalHoldEnabled: false,
	}

	err := cfg.SetObjectLockConfig("test-bucket", lock)
	if err != nil {
		t.Fatalf("SetObjectLockConfig failed: %v", err)
	}
}

func TestGetObjectLockConfig(t *testing.T) {
	cfg := New()

	// Test non-existent
	_, ok := cfg.GetObjectLockConfig("non-existent")
	if ok {
		t.Error("Should not find object lock for non-existent bucket")
	}

	// Test existing
	lock := &ObjectLockConfig{Enabled: true}
	cfg.SetObjectLockConfig("test-bucket", lock)

	result, ok := cfg.GetObjectLockConfig("test-bucket")
	if !ok {
		t.Fatal("Should find object lock config")
	}

	if !result.Enabled {
		t.Error("Object lock should be enabled")
	}
}

func TestIsObjectLockEnabled(t *testing.T) {
	cfg := New()

	// Test disabled (default)
	if cfg.IsObjectLockEnabled("test-bucket") {
		t.Error("Object lock should not be enabled by default")
	}

	// Test enabled
	lock := &ObjectLockConfig{Enabled: true}
	cfg.SetObjectLockConfig("test-bucket", lock)

	if !cfg.IsObjectLockEnabled("test-bucket") {
		t.Error("Object lock should be enabled")
	}
}

func TestSetBucketTags(t *testing.T) {
	cfg := New()

	tags := map[string]string{
		"environment": "production",
		"team":        "backend",
	}

	err := cfg.SetBucketTags("test-bucket", tags)
	if err != nil {
		t.Fatalf("SetBucketTags failed: %v", err)
	}
}

func TestGetBucketTags(t *testing.T) {
	cfg := New()

	// Test non-existent
	_, ok := cfg.GetBucketTags("non-existent")
	if ok {
		t.Error("Should not find tags for non-existent bucket")
	}

	// Test existing
	tags := map[string]string{"key": "value"}
	cfg.SetBucketTags("test-bucket", tags)

	result, ok := cfg.GetBucketTags("test-bucket")
	if !ok {
		t.Fatal("Should find tags")
	}

	if result["key"] != "value" {
		t.Errorf("Tag value = %s, want value", result["key"])
	}
}

func TestDeleteBucketTags(t *testing.T) {
	cfg := New()

	tags := map[string]string{"key": "value"}
	cfg.SetBucketTags("test-bucket", tags)

	err := cfg.DeleteBucketTags("test-bucket")
	if err != nil {
		t.Fatalf("DeleteBucketTags failed: %v", err)
	}

	_, ok := cfg.GetBucketTags("test-bucket")
	if ok {
		t.Error("Tags should be deleted")
	}
}

func TestGenerateVersionID(t *testing.T) {
	id1 := GenerateVersionID()
	id2 := GenerateVersionID()

	if id1 == "" {
		t.Error("Version ID should not be empty")
	}

	if id1 == id2 {
		t.Error("Version IDs should be unique")
	}
}

func TestGenerateRandomID(t *testing.T) {
	id1 := generateRandomID()
	id2 := generateRandomID()

	if id1 == "" {
		t.Error("Random ID should not be empty")
	}

	if len(id1) != 8 {
		t.Errorf("Random ID length = %d, want 8", len(id1))
	}

	if id1 == id2 {
		t.Error("Random IDs should be unique")
	}
}

func TestGenerateRandomID_Fallback(t *testing.T) {
	original := randRead
	defer func() { randRead = original }()

	randRead = func(b []byte) (int, error) {
		return 0, errors.New("forced error")
	}

	id := generateRandomID()
	if len(id) != 8 {
		t.Errorf("Fallback ID length = %d, want 8", len(id))
	}
}

func TestBucketPolicy_JSON(t *testing.T) {
	policy := &BucketPolicy{
		Version: "2012-10-17",
		Statement: []*PolicyStatement{
			{
				Sid:       "PublicRead",
				Effect:    "Allow",
				Principal: "*",
				Action:    "s3:GetObject",
				Resource:  "arn:aws:s3:::my-bucket/*",
			},
		},
	}

	// Test GetJSON
	jsonStr, err := policy.GetJSON()
	if err != nil {
		t.Fatalf("GetJSON failed: %v", err)
	}

	if len(jsonStr) == 0 {
		t.Error("JSON should not be empty")
	}

	// Test SetPolicyFromJSON
	newPolicy := &BucketPolicy{}
	err = newPolicy.SetPolicyFromJSON(jsonStr)
	if err != nil {
		t.Fatalf("SetPolicyFromJSON failed: %v", err)
	}

	if newPolicy.Version != "2012-10-17" {
		t.Errorf("Version = %s, want 2012-10-17", newPolicy.Version)
	}
}

func TestValidateCORS(t *testing.T) {
	tests := []struct {
		name    string
		config  *CORSConfig
		wantErr bool
	}{
		{
			name: "valid CORS",
			config: &CORSConfig{
				CORSRules: []*CORSRule{
					{
						AllowedOrigins: []string{"*"},
						AllowedMethods: []string{"GET"},
					},
				},
			},
			wantErr: false,
		},
		{
			name:    "nil config",
			config:  nil,
			wantErr: true,
		},
		{
			name: "no allowed methods",
			config: &CORSConfig{
				CORSRules: []*CORSRule{
					{
						AllowedOrigins: []string{"*"},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "invalid method",
			config: &CORSConfig{
				CORSRules: []*CORSRule{
					{
						AllowedOrigins: []string{"*"},
						AllowedMethods: []string{"INVALID"},
					},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateCORS(tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateCORS() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidatePolicy(t *testing.T) {
	tests := []struct {
		name    string
		policy  *BucketPolicy
		wantErr bool
	}{
		{
			name: "valid policy",
			policy: &BucketPolicy{
				Version: "2012-10-17",
				Statement: []*PolicyStatement{
					{
						Effect:    "Allow",
						Principal: "*",
						Action:    "s3:GetObject",
						Resource:  "arn:aws:s3:::bucket/*",
					},
				},
			},
			wantErr: false,
		},
		{
			name:    "nil policy",
			policy:  nil,
			wantErr: true,
		},
		{
			name: "no statements",
			policy: &BucketPolicy{
				Version:   "2012-10-17",
				Statement: []*PolicyStatement{},
			},
			wantErr: true,
		},
		{
			name: "invalid effect",
			policy: &BucketPolicy{
				Version: "2012-10-17",
				Statement: []*PolicyStatement{
					{
						Effect:    "Invalid",
						Principal: "*",
						Action:    "s3:GetObject",
						Resource:  "arn:aws:s3:::bucket/*",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "no principal",
			policy: &BucketPolicy{
				Version: "2012-10-17",
				Statement: []*PolicyStatement{
					{
						Effect:   "Allow",
						Action:   "s3:GetObject",
						Resource: "arn:aws:s3:::bucket/*",
					},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidatePolicy(tt.policy)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidatePolicy() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestConcurrentAccess(t *testing.T) {
	cfg := New()
	done := make(chan bool)

	// Concurrent writes
	for i := 0; i < 100; i++ {
		go func(id int) {
			bucket := string(rune('A' + id%26))
			cfg.SetVersioningConfig(bucket, &VersioningConfig{Status: "Enabled"})
			cfg.SetCORSConfig(bucket, &CORSConfig{
				Bucket:    bucket,
				CORSRules: []*CORSRule{{AllowedOrigins: []string{"*"}}},
			})
			cfg.SetBucketPolicy(bucket, &BucketPolicy{Version: "2012-10-17"})
			cfg.SetBucketTags(bucket, map[string]string{"id": string(rune('0' + id%10))})
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 100; i++ {
		<-done
	}
}

func TestModifiedTimestamp(t *testing.T) {
	cfg := New()

	before := time.Now()
	cfg.SetVersioningConfig("test-bucket", &VersioningConfig{Status: "Enabled"})
	after := time.Now()

	result, _ := cfg.GetVersioningConfig("test-bucket")

	if result.ModifiedDate.Before(before) || result.ModifiedDate.After(after) {
		t.Error("ModifiedDate should be set to current time")
	}
}

func TestToJSON(t *testing.T) {
	cfg := New()

	cfg.SetVersioningConfig("test-bucket", &VersioningConfig{Status: "Enabled"})
	cfg.SetCORSConfig("test-bucket", &CORSConfig{
		Bucket:    "test-bucket",
		CORSRules: []*CORSRule{{AllowedOrigins: []string{"*"}}},
	})

	data, err := cfg.ToJSON("test-bucket")
	if err != nil {
		t.Fatalf("ToJSON failed: %v", err)
	}

	if len(data) == 0 {
		t.Error("JSON should not be empty")
	}
}

func TestDeleteBucketConfig(t *testing.T) {
	cfg := New()

	cfg.SetVersioningConfig("test-bucket", &VersioningConfig{Status: "Enabled"})
	cfg.SetCORSConfig("test-bucket", &CORSConfig{Bucket: "test-bucket"})
	cfg.SetBucketPolicy("test-bucket", &BucketPolicy{Version: "2012-10-17"})
	cfg.SetObjectLockConfig("test-bucket", &ObjectLockConfig{Enabled: true})
	cfg.SetBucketTags("test-bucket", map[string]string{"key": "value"})

	err := cfg.DeleteBucketConfig("test-bucket")
	if err != nil {
		t.Fatalf("DeleteBucketConfig failed: %v", err)
	}

	// Verify all configs are deleted
	_, ok := cfg.GetVersioningConfig("test-bucket")
	if ok {
		t.Error("Versioning should be deleted")
	}
	_, ok = cfg.GetCORSConfig("test-bucket")
	if ok {
		t.Error("CORS should be deleted")
	}
	_, ok = cfg.GetBucketPolicy("test-bucket")
	if ok {
		t.Error("Policy should be deleted")
	}
	_, ok = cfg.GetObjectLockConfig("test-bucket")
	if ok {
		t.Error("Object lock should be deleted")
	}
	_, ok = cfg.GetBucketTags("test-bucket")
	if ok {
		t.Error("Tags should be deleted")
	}
}

func TestSetBucketConfig(t *testing.T) {
	cfg := New()

	bucketCfg := &BucketConfig{
		Name:     "test-bucket",
		Location: "us-east-1",
		Owner:    "test-user",
	}

	cfg.SetBucketConfig("test-bucket", bucketCfg)

	result, ok := cfg.GetBucketConfig("test-bucket")
	if !ok {
		t.Fatal("Should find bucket config")
	}

	if result.Name != "test-bucket" {
		t.Errorf("Name = %s, want test-bucket", result.Name)
	}
}

func TestListBucketConfigs(t *testing.T) {
	cfg := New()

	cfg.SetBucketConfig("bucket1", &BucketConfig{Name: "bucket1"})
	cfg.SetBucketConfig("bucket2", &BucketConfig{Name: "bucket2"})
	cfg.SetBucketConfig("bucket3", &BucketConfig{Name: "bucket3"})

	configs := cfg.ListBucketConfigs()
	if len(configs) != 3 {
		t.Errorf("ListBucketConfigs() = %d, want 3", len(configs))
	}
}

func TestSetBucketTags_ExistingBucket(t *testing.T) {
	cfg := New()

	cfg.SetBucketConfig("test-bucket", &BucketConfig{Name: "test-bucket", Location: "us-east-1"})

	tags := map[string]string{"environment": "production"}
	err := cfg.SetBucketTags("test-bucket", tags)
	if err != nil {
		t.Fatalf("SetBucketTags failed: %v", err)
	}

	bucketCfg, ok := cfg.GetBucketConfig("test-bucket")
	if !ok {
		t.Fatal("Bucket config should exist")
	}
	if bucketCfg.Tags["environment"] != "production" {
		t.Errorf("Tags not updated in bucket config")
	}
}

func TestDeleteBucketTags_ExistingBucket(t *testing.T) {
	cfg := New()

	cfg.SetBucketConfig("test-bucket", &BucketConfig{Name: "test-bucket"})
	cfg.SetBucketTags("test-bucket", map[string]string{"key": "value"})

	err := cfg.DeleteBucketTags("test-bucket")
	if err != nil {
		t.Fatalf("DeleteBucketTags failed: %v", err)
	}

	bucketCfg, ok := cfg.GetBucketConfig("test-bucket")
	if !ok {
		t.Fatal("Bucket config should exist")
	}
	if bucketCfg.Tags != nil {
		t.Errorf("Tags should be nil in bucket config, got %v", bucketCfg.Tags)
	}
}

func TestValidateCORS_NoAllowedOrigins(t *testing.T) {
	config := &CORSConfig{
		CORSRules: []*CORSRule{
			{
				AllowedMethods: []string{"GET"},
				AllowedOrigins: []string{},
			},
		},
	}

	err := ValidateCORS(config)
	if err == nil {
		t.Error("ValidateCORS should return error for empty AllowedOrigins")
	}
}

func TestValidatePolicy_EmptyVersion(t *testing.T) {
	policy := &BucketPolicy{
		Version: "",
		Statement: []*PolicyStatement{
			{
				Effect:    "Allow",
				Principal: "*",
				Action:    "s3:GetObject",
				Resource:  "arn:aws:s3:::bucket/*",
			},
		},
	}

	err := ValidatePolicy(policy)
	if err != nil {
		t.Fatalf("ValidatePolicy failed: %v", err)
	}

	if policy.Version != "2012-10-17" {
		t.Errorf("Version = %s, want 2012-10-17", policy.Version)
	}
}

func TestValidatePolicy_NoAction(t *testing.T) {
	policy := &BucketPolicy{
		Version: "2012-10-17",
		Statement: []*PolicyStatement{
			{
				Effect:    "Allow",
				Principal: "*",
				Resource:  "arn:aws:s3:::bucket/*",
			},
		},
	}

	err := ValidatePolicy(policy)
	if err == nil {
		t.Error("ValidatePolicy should return error for nil Action")
	}
}

func TestValidatePolicy_NoResource(t *testing.T) {
	policy := &BucketPolicy{
		Version: "2012-10-17",
		Statement: []*PolicyStatement{
			{
				Effect:    "Allow",
				Principal: "*",
				Action:    "s3:GetObject",
			},
		},
	}

	err := ValidatePolicy(policy)
	if err == nil {
		t.Error("ValidatePolicy should return error for nil Resource")
	}
}
