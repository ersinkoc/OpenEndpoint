package tiering

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestNewManager(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)
	if mgr == nil {
		t.Fatal("Manager should not be nil")
	}
	if mgr.policies == nil {
		t.Error("policies map should be initialized")
	}
	if mgr.tierUsage == nil {
		t.Error("tierUsage map should be initialized")
	}
	if mgr.objectTiers == nil {
		t.Error("objectTiers map should be initialized")
	}
}

func TestNewAnalyzer(t *testing.T) {
	logger := zap.NewNop()
	analyzer := NewAnalyzer(logger)
	if analyzer == nil {
		t.Fatal("Analyzer should not be nil")
	}
}

func TestTierConstants(t *testing.T) {
	if TierHot != "hot" {
		t.Errorf("TierHot = %v, want hot", TierHot)
	}
	if TierWarm != "warm" {
		t.Errorf("TierWarm = %v, want warm", TierWarm)
	}
	if TierCold != "cold" {
		t.Errorf("TierCold = %v, want cold", TierCold)
	}
	if TierGlacier != "glacier" {
		t.Errorf("TierGlacier = %v, want glacier", TierGlacier)
	}
}

func TestDefaultTierConfigs(t *testing.T) {
	configs := DefaultTierConfigs()
	if len(configs) != 4 {
		t.Errorf("len(DefaultTierConfigs()) = %d, want 4", len(configs))
	}

	for i, cfg := range configs {
		if cfg.Priority != i {
			t.Errorf("config[%d].Priority = %d, want %d", i, cfg.Priority, i)
		}
	}
}

func TestCreatePolicy(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	policy, err := mgr.CreatePolicy("test-policy", "test-bucket", "prefix/")
	if err != nil {
		t.Fatalf("CreatePolicy failed: %v", err)
	}

	if policy.ID == "" {
		t.Error("policy ID should not be empty")
	}
	if policy.Name != "test-policy" {
		t.Errorf("Name = %v, want test-policy", policy.Name)
	}
	if policy.Bucket != "test-bucket" {
		t.Errorf("Bucket = %v, want test-bucket", policy.Bucket)
	}
	if policy.Prefix != "prefix/" {
		t.Errorf("Prefix = %v, want prefix/", policy.Prefix)
	}
	if !policy.Enabled {
		t.Error("policy should be enabled by default")
	}
	if len(policy.TierConfigs) != 4 {
		t.Errorf("len(TierConfigs) = %d, want 4", len(policy.TierConfigs))
	}
}

func TestGetPolicy(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	policy, _ := mgr.CreatePolicy("test-policy", "test-bucket", "")

	got, ok := mgr.GetPolicy(policy.ID)
	if !ok {
		t.Fatal("GetPolicy should return ok=true")
	}
	if got.ID != policy.ID {
		t.Errorf("ID = %v, want %v", got.ID, policy.ID)
	}
}

func TestGetPolicyNotFound(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	_, ok := mgr.GetPolicy("nonexistent")
	if ok {
		t.Error("GetPolicy should return ok=false for nonexistent policy")
	}
}

func TestListPolicies(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	mgr.CreatePolicy("policy1", "bucket1", "")
	mgr.CreatePolicy("policy2", "bucket2", "")

	policies := mgr.ListPolicies()
	if len(policies) != 2 {
		t.Errorf("len(policies) = %d, want 2", len(policies))
	}
}

func TestListPoliciesEmpty(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	policies := mgr.ListPolicies()
	if len(policies) != 0 {
		t.Errorf("len(policies) = %d, want 0", len(policies))
	}
}

func TestUpdateObjectTier(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	mgr.UpdateObjectTier("bucket", "key", TierHot)

	tier, ok := mgr.GetObjectTier("bucket", "key")
	if !ok {
		t.Fatal("GetObjectTier should return ok=true")
	}
	if tier != TierHot {
		t.Errorf("tier = %v, want hot", tier)
	}
}

func TestUpdateObjectTierChange(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	mgr.UpdateObjectTier("bucket", "key", TierHot)
	mgr.UpdateObjectTier("bucket", "key", TierCold)

	tier, _ := mgr.GetObjectTier("bucket", "key")
	if tier != TierCold {
		t.Errorf("tier = %v, want cold", tier)
	}
}

func TestGetObjectTierNotFound(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	_, ok := mgr.GetObjectTier("bucket", "nonexistent")
	if ok {
		t.Error("GetObjectTier should return ok=false for nonexistent object")
	}
}

func TestGetTierUsage(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	mgr.UpdateObjectTier("bucket", "key1", TierHot)
	mgr.UpdateObjectTier("bucket", "key2", TierHot)
	mgr.UpdateObjectTier("bucket", "key3", TierCold)

	usage := mgr.GetTierUsage()
	if usage[TierHot] != 2 {
		t.Errorf("usage[hot] = %d, want 2", usage[TierHot])
	}
	if usage[TierCold] != 1 {
		t.Errorf("usage[cold] = %d, want 1", usage[TierCold])
	}
}

func TestGetCostEstimate(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	mgr.UpdateObjectTier("bucket", "key1", TierHot)

	total, tierCosts := mgr.GetCostEstimate()

	if total < 0 {
		t.Errorf("total = %v, want >= 0", total)
	}

	if len(tierCosts) == 0 {
		t.Error("tierCosts should not be empty")
	}
}

func TestRecommendTierHighAccess(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	obj := &ObjectInfo{
		Bucket:      "bucket",
		Key:         "key",
		CreatedAt:   time.Now().Add(-1 * time.Hour),
		AccessCount: 100,
	}

	tier := mgr.RecommendTier(obj)
	if tier != TierHot {
		t.Errorf("RecommendTier(high access) = %v, want hot", tier)
	}
}

func TestRecommendTierModerateAccess(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	obj := &ObjectInfo{
		Bucket:      "bucket",
		Key:         "key",
		CreatedAt:   time.Now().Add(-10 * time.Hour),
		AccessCount: 20,
	}

	tier := mgr.RecommendTier(obj)
	if tier != TierWarm {
		t.Errorf("RecommendTier(moderate access) = %v, want warm", tier)
	}
}

func TestRecommendTierOld(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	obj := &ObjectInfo{
		Bucket:      "bucket",
		Key:         "key",
		CreatedAt:   time.Now().Add(-100 * 24 * time.Hour),
		AccessCount: 1,
	}

	tier := mgr.RecommendTier(obj)
	if tier != TierCold {
		t.Errorf("RecommendTier(old) = %v, want cold", tier)
	}
}

func TestRecommendTierVeryOld(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	obj := &ObjectInfo{
		Bucket:      "bucket",
		Key:         "key",
		CreatedAt:   time.Now().Add(-200 * 24 * time.Hour),
		AccessCount: 0,
	}

	tier := mgr.RecommendTier(obj)
	if tier != TierGlacier {
		t.Errorf("RecommendTier(very old) = %v, want glacier", tier)
	}
}

func TestRecommendTierDefault(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	obj := &ObjectInfo{
		Bucket:      "bucket",
		Key:         "key",
		CreatedAt:   time.Now().Add(-10 * 24 * time.Hour),
		AccessCount: 1,
	}

	tier := mgr.RecommendTier(obj)
	if tier != TierWarm {
		t.Errorf("RecommendTier(default) = %v, want warm", tier)
	}
}

func TestRecommendTierGlacier(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	obj := &ObjectInfo{
		Bucket:      "bucket",
		Key:         "key",
		CreatedAt:   time.Now().Add(-365 * 24 * time.Hour),
		AccessCount: 0,
	}

	tier := mgr.RecommendTier(obj)
	if tier != TierGlacier {
		t.Errorf("RecommendTier(glacier) = %v, want glacier", tier)
	}
}

func TestTransitionTier(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	err := mgr.TransitionTier(context.Background(), "bucket", "key", TierCold)
	if err != nil {
		t.Fatalf("TransitionTier failed: %v", err)
	}

	tier, ok := mgr.GetObjectTier("bucket", "key")
	if !ok || tier != TierCold {
		t.Errorf("GetObjectTier = %v, %v, want cold, true", tier, ok)
	}
}

func TestManagerStop(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	mgr.Stop()
}

func TestManagerStart(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go mgr.Start(ctx)

	time.Sleep(10 * time.Millisecond)
}

func TestManagerStartWithTicker(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	mgr.CreatePolicy("test-policy", "test-bucket", "")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go mgr.Start(ctx)

	time.Sleep(20 * time.Millisecond)
	cancel()
	time.Sleep(10 * time.Millisecond)
}

func TestManagerTieringWorkerCtxCancel(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	ctx, cancel := context.WithCancel(context.Background())

	go mgr.Start(ctx)

	time.Sleep(10 * time.Millisecond)
	cancel()
	time.Sleep(10 * time.Millisecond)
}

func TestManagerTieringWorkerStopCh(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	ctx := context.Background()

	go mgr.Start(ctx)

	time.Sleep(10 * time.Millisecond)
	mgr.Stop()
	time.Sleep(10 * time.Millisecond)
}

func TestManagerTieringWorkerTicker(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)
	mgr.SetTickerInterval(10 * time.Millisecond)

	mgr.CreatePolicy("test-policy", "test-bucket", "")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go mgr.Start(ctx)

	time.Sleep(50 * time.Millisecond)
	cancel()
	time.Sleep(10 * time.Millisecond)
}

func TestAnalyzeAccessPatternHighRate(t *testing.T) {
	logger := zap.NewNop()
	analyzer := NewAnalyzer(logger)

	objects := []*ObjectInfo{
		{
			Bucket:      "bucket",
			Key:         "key1",
			Size:        1024,
			CreatedAt:   time.Now().Add(-1 * time.Hour),
			AccessCount: 100,
			Tier:        TierWarm,
		},
	}

	recommendations := analyzer.AnalyzeAccessPattern(objects)
	if len(recommendations) != 1 {
		t.Errorf("len(recommendations) = %d, want 1", len(recommendations))
	}

	rec, ok := recommendations["bucket/key1"]
	if !ok {
		t.Fatal("recommendation for bucket/key1 should exist")
	}

	if rec.Recommended != TierHot {
		t.Errorf("Recommended = %v, want hot", rec.Recommended)
	}
	if rec.Reason != "High access rate" {
		t.Errorf("Reason = %v, want 'High access rate'", rec.Reason)
	}
}

func TestAnalyzeAccessPatternModerateRate(t *testing.T) {
	logger := zap.NewNop()
	analyzer := NewAnalyzer(logger)

	objects := []*ObjectInfo{
		{
			Bucket:      "bucket",
			Key:         "key1",
			Size:        1024,
			CreatedAt:   time.Now().Add(-10 * time.Hour),
			AccessCount: 50,
			Tier:        TierHot,
		},
	}

	recommendations := analyzer.AnalyzeAccessPattern(objects)
	rec := recommendations["bucket/key1"]

	if rec.Recommended != TierWarm {
		t.Errorf("Recommended = %v, want warm", rec.Recommended)
	}
}

func TestAnalyzeAccessPatternOld(t *testing.T) {
	logger := zap.NewNop()
	analyzer := NewAnalyzer(logger)

	objects := []*ObjectInfo{
		{
			Bucket:      "bucket",
			Key:         "key1",
			Size:        1024,
			CreatedAt:   time.Now().Add(-100 * 24 * time.Hour),
			AccessCount: 1,
			Tier:        TierWarm,
		},
	}

	recommendations := analyzer.AnalyzeAccessPattern(objects)
	rec := recommendations["bucket/key1"]

	if rec.Recommended != TierCold {
		t.Errorf("Recommended = %v, want cold", rec.Recommended)
	}
}

func TestAnalyzeAccessPatternVeryOld(t *testing.T) {
	logger := zap.NewNop()
	analyzer := NewAnalyzer(logger)

	objects := []*ObjectInfo{
		{
			Bucket:      "bucket",
			Key:         "key1",
			Size:        1024,
			CreatedAt:   time.Now().Add(-200 * 24 * time.Hour),
			AccessCount: 1,
			Tier:        TierCold,
		},
	}

	recommendations := analyzer.AnalyzeAccessPattern(objects)
	rec := recommendations["bucket/key1"]

	if rec.Recommended != TierGlacier {
		t.Errorf("Recommended = %v, want glacier", rec.Recommended)
	}
}

func TestAnalyzeAccessPatternDefault(t *testing.T) {
	logger := zap.NewNop()
	analyzer := NewAnalyzer(logger)

	objects := []*ObjectInfo{
		{
			Bucket:      "bucket",
			Key:         "key1",
			Size:        1024,
			CreatedAt:   time.Now().Add(-10 * 24 * time.Hour),
			AccessCount: 1,
			Tier:        TierHot,
		},
	}

	recommendations := analyzer.AnalyzeAccessPattern(objects)
	rec := recommendations["bucket/key1"]

	if rec.Recommended != TierWarm {
		t.Errorf("Recommended = %v, want warm", rec.Recommended)
	}
	if rec.Reason != "Default tier" {
		t.Errorf("Reason = %v, want 'Default tier'", rec.Reason)
	}
}

func TestTierConfig(t *testing.T) {
	cfg := TierConfig{
		Name:           "test",
		Tier:           TierHot,
		MinAge:         24 * time.Hour,
		MaxSizeGB:      100,
		CostPerGBMonth: 0.023,
		Priority:       0,
	}

	if cfg.Name != "test" {
		t.Errorf("Name = %v, want test", cfg.Name)
	}
}

func TestObjectInfo(t *testing.T) {
	obj := ObjectInfo{
		Bucket:       "bucket",
		Key:          "key",
		Size:         1024,
		StorageClass: "STANDARD",
		LastAccess:   time.Now(),
		CreatedAt:    time.Now(),
		AccessCount:  10,
		Tier:         TierHot,
	}

	if obj.Bucket != "bucket" {
		t.Errorf("Bucket = %v, want bucket", obj.Bucket)
	}
}

func TestTieringPolicy(t *testing.T) {
	policy := TieringPolicy{
		ID:          "test-id",
		Name:        "test-policy",
		Bucket:      "bucket",
		Prefix:      "prefix/",
		TierConfigs: DefaultTierConfigs(),
		Enabled:     true,
		CreatedAt:   time.Now(),
	}

	if policy.ID != "test-id" {
		t.Errorf("ID = %v, want test-id", policy.ID)
	}
}

func TestTierRecommendation(t *testing.T) {
	rec := TierRecommendation{
		Object:      &ObjectInfo{Key: "test"},
		Recommended: TierCold,
		Current:     TierHot,
		Reason:      "Old age",
		SavingsGB:   1.5,
	}

	if rec.Recommended != TierCold {
		t.Errorf("Recommended = %v, want cold", rec.Recommended)
	}
}

func TestManager_EvaluateTiering(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	// Add an enabled policy
	policy := &TieringPolicy{
		ID:      "test-policy",
		Bucket:  "test-bucket",
		Enabled: true,
		TierConfigs: []TierConfig{
			{Tier: TierHot, MinAge: time.Hour},
			{Tier: TierCold, MinAge: 24 * time.Hour},
		},
	}
	mgr.policies["test-policy"] = policy

	// This should not panic
	mgr.evaluateTiering()
}

func TestManager_EvaluatePolicy(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	policy := &TieringPolicy{
		ID:      "test-policy",
		Bucket:  "test-bucket",
		Enabled: true,
		TierConfigs: []TierConfig{
			{Tier: TierHot, MinAge: time.Hour},
			{Tier: TierWarm, MinAge: 7 * 24 * time.Hour},
			{Tier: TierCold, MinAge: 30 * 24 * time.Hour},
		},
	}

	// This should not panic
	mgr.evaluatePolicy(policy)
}

func TestManager_EvaluateTiering_NoPolicies(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	// No policies added - should complete without panic
	mgr.evaluateTiering()
}

func TestManager_EvaluateTiering_DisabledPolicy(t *testing.T) {
	logger := zap.NewNop()
	mgr := NewManager(logger)

	// Add a disabled policy
	policy := &TieringPolicy{
		ID:      "disabled-policy",
		Bucket:  "test-bucket",
		Enabled: false,
		TierConfigs: []TierConfig{
			{Tier: TierHot, MinAge: time.Hour},
		},
	}
	mgr.policies["disabled-policy"] = policy

	// Should skip disabled policies without panic
	mgr.evaluateTiering()
}
