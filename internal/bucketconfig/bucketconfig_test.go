package bucketconfig

import (
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

func TestSetVersioning(t *testing.T) {
	cfg := New()

	versioning := &VersioningConfig{
		Status:    "Enabled",
		MfaDelete: "Disabled",
	}

	err := cfg.SetVersioning("test-bucket", versioning)
	if err != nil {
		t.Fatalf("SetVersioning failed: %v", err)
	}
}

func TestGetVersioning(t *testing.T) {
	cfg := New()

	// Test non-existent bucket
	_, ok := cfg.GetVersioning("non-existent")
	if ok {
		t.Error("Should not find versioning for non-existent bucket")
	}

	// Test existing bucket
	versioning := &VersioningConfig{Status: "Enabled"}
	cfg.SetVersioning("test-bucket", versioning)

	result, ok := cfg.GetVersioning("test-bucket")
	if !ok {
		t.Fatal("Should find versioning")
	}

	if result.Status != "Enabled" {
		t.Errorf("Status = %s, want Enabled", result.Status)
	}
}

func TestSetCORSConfig(t *testing.T) {
	cfg := New()

	cors := &CORSConfig{
		AllowedOrigins: []string{"*"},
		AllowedMethods: []string{"GET", "PUT"},
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
	cors := &CORSConfig{AllowedOrigins: []string{"https://example.com"}}
	cfg.SetCORSConfig("test-bucket", cors)

	result, ok := cfg.GetCORSConfig("test-bucket")
	if !ok {
		t.Fatal("Should find CORS config")
	}

	if len(result.AllowedOrigins) != 1 {
		t.Errorf("AllowedOrigins length = %d, want 1", len(result.AllowedOrigins))
	}
}

func TestDeleteCORSConfig(t *testing.T) {
	cfg := New()

	cors := &CORSConfig{AllowedOrigins: []string{"*"}}
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
		Statement: []PolicyStatement{
			{
				Effect:   "Allow",
				Actions:  []string{"s3:GetObject"},
				Resources: []string{"arn:aws:s3:::test-bucket/*"},
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
		ObjectLockEnabled: "Enabled",
		DefaultRetention: &DefaultRetention{
			Mode: "COMPLIANCE",
			Days: 30,
		},
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
	lock := &ObjectLockConfig{ObjectLockEnabled: "Enabled"}
	cfg.SetObjectLockConfig("test-bucket", lock)

	result, ok := cfg.GetObjectLockConfig("test-bucket")
	if !ok {
		t.Fatal("Should find object lock config")
	}

	if result.ObjectLockEnabled != "Enabled" {
		t.Errorf("ObjectLockEnabled = %s, want Enabled", result.ObjectLockEnabled)
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

func TestBucketPolicy_JSON(t *testing.T) {
	policy := &BucketPolicy{
		Version: "2012-10-17",
		Statement: []PolicyStatement{
			{
				Sid:       "PublicRead",
				Effect:    "Allow",
				Actions:   []string{"s3:GetObject"},
				Resources: []string{"arn:aws:s3:::my-bucket/*"},
			},
		},
	}

	// Test ToJSON
	jsonBytes, err := policy.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON failed: %v", err)
	}

	if len(jsonBytes) == 0 {
		t.Error("JSON should not be empty")
	}

	// Test FromJSON
	newPolicy := &BucketPolicy{}
	err = newPolicy.FromJSON(jsonBytes)
	if err != nil {
		t.Fatalf("FromJSON failed: %v", err)
	}

	if newPolicy.Version != "2012-10-17" {
		t.Errorf("Version = %s, want 2012-10-17", newPolicy.Version)
	}
}

func TestCORSConfig_JSON(t *testing.T) {
	cors := &CORSConfig{
		AllowedOrigins: []string{"https://example.com"},
		AllowedMethods: []string{"GET", "PUT", "POST"},
		AllowedHeaders: []string{"*"},
		MaxAgeSeconds:  3600,
	}

	jsonBytes, err := cors.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON failed: %v", err)
	}

	newCors := &CORSConfig{}
	err = newCors.FromJSON(jsonBytes)
	if err != nil {
		t.Fatalf("FromJSON failed: %v", err)
	}

	if newCors.MaxAgeSeconds != 3600 {
		t.Errorf("MaxAgeSeconds = %d, want 3600", newCors.MaxAgeSeconds)
	}
}

func TestVersioningConfig_JSON(t *testing.T) {
	versioning := &VersioningConfig{
		Status:    "Enabled",
		MfaDelete: "Disabled",
	}

	jsonBytes, err := versioning.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON failed: %v", err)
	}

	newVersioning := &VersioningConfig{}
	err = newVersioning.FromJSON(jsonBytes)
	if err != nil {
		t.Fatalf("FromJSON failed: %v", err)
	}

	if newVersioning.Status != "Enabled" {
		t.Errorf("Status = %s, want Enabled", newVersioning.Status)
	}
}

func TestConcurrentAccess(t *testing.T) {
	cfg := New()
	done := make(chan bool)

	// Concurrent writes
	for i := 0; i < 100; i++ {
		go func(id int) {
			bucket := string(rune('A' + id))
			cfg.SetVersioning(bucket, &VersioningConfig{Status: "Enabled"})
			cfg.SetCORSConfig(bucket, &CORSConfig{})
			cfg.SetBucketPolicy(bucket, &BucketPolicy{})
			cfg.SetBucketTags(bucket, map[string]string{"id": string(rune('0' + id))})
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 100; i++ {
		<-done
	}

	// Verify no data corruption
	stats := cfg.GetStats()
	if stats.TotalBuckets != 100 {
		t.Errorf("TotalBuckets = %d, want 100", stats.TotalBuckets)
	}
}

func TestConfig_GetStats(t *testing.T) {
	cfg := New()

	// Empty stats
	stats := cfg.GetStats()
	if stats.TotalBuckets != 0 {
		t.Errorf("Empty TotalBuckets = %d, want 0", stats.TotalBuckets)
	}

	// Add some buckets
	cfg.SetVersioning("bucket1", &VersioningConfig{Status: "Enabled"})
	cfg.SetVersioning("bucket2", &VersioningConfig{Status: "Disabled"})
	cfg.SetCORSConfig("bucket3", &CORSConfig{})

	stats = cfg.GetStats()
	if stats.TotalBuckets < 3 {
		t.Errorf("TotalBuckets = %d, want at least 3", stats.TotalBuckets)
	}
}

func TestModifiedTimestamp(t *testing.T) {
	cfg := New()

	before := time.Now()
	cfg.SetVersioning("test-bucket", &VersioningConfig{Status: "Enabled"})
	after := time.Now()

	result, _ := cfg.GetVersioning("test-bucket")

	if result.ModifiedDate.Before(before) || result.ModifiedDate.After(after) {
		t.Error("ModifiedDate should be set to current time")
	}
}
