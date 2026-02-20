package tiering

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

// Tier represents a storage tier
type Tier string

const (
	TierHot    Tier = "hot"    // SSD/NVMe
	TierWarm   Tier = "warm"   // HDD
	TierCold   Tier = "cold"   // Object Archive
	TierGlacier Tier = "glacier" // Deep Archive
)

// TierConfig contains configuration for a tier
type TierConfig struct {
	Name           string        `json:"name"`
	Tier           Tier          `json:"tier"`
	MinAge         time.Duration `json:"min_age"` // Minimum age before moving to this tier
	MaxSizeGB      int64         `json:"max_size_gb"`
	CostPerGBMonth float64       `json:"cost_per_gb_month"`
	Priority       int           `json:"priority"` // 0 = highest
}

// DefaultTierConfigs returns default tier configurations
func DefaultTierConfigs() []TierConfig {
	return []TierConfig{
		{
			Name:           "hot",
			Tier:           TierHot,
			MinAge:         0,
			MaxSizeGB:      100,
			CostPerGBMonth: 0.023,
			Priority:       0,
		},
		{
			Name:           "warm",
			Tier:           TierWarm,
			MinAge:         30 * 24 * time.Hour, // 30 days
			MaxSizeGB:      500,
			CostPerGBMonth: 0.0125,
			Priority:       1,
		},
		{
			Name:           "cold",
			Tier:           TierCold,
			MinAge:         90 * 24 * time.Hour, // 90 days
			MaxSizeGB:      2000,
			CostPerGBMonth: 0.004,
			Priority:       2,
		},
		{
			Name:           "glacier",
			Tier:           TierGlacier,
			MinAge:         180 * 24 * time.Hour, // 180 days
			MaxSizeGB:      -1, // Unlimited
			CostPerGBMonth: 0.00099,
			Priority:       3,
		},
	}
}

// ObjectInfo contains object metadata for tiering decisions
type ObjectInfo struct {
	Bucket       string
	Key          string
	Size         int64
	StorageClass string
	LastAccess   time.Time
	CreatedAt    time.Time
	AccessCount  int64
	Tier         Tier
}

// TieringPolicy defines when to move objects between tiers
type TieringPolicy struct {
	ID          string     `json:"id"`
	Name        string     `json:"name"`
	Bucket      string     `json:"bucket"`
	Prefix      string     `json:"prefix"`
	TierConfigs []TierConfig `json:"tier_configs"`
	Enabled     bool       `json:"enabled"`
	CreatedAt   time.Time  `json:"created_at"`
}

// Manager manages intelligent tiering
type Manager struct {
	logger     *zap.Logger
	mu         sync.RWMutex
	policies   map[string]*TieringPolicy
	tierUsage  map[Tier]int64 // Total bytes per tier
	objectTiers map[string]Tier // object key -> current tier
	stopCh     chan struct{}
}

// NewManager creates a new tiering manager
func NewManager(logger *zap.Logger) *Manager {
	return &Manager{
		logger:     logger,
		policies:   make(map[string]*TieringPolicy),
		tierUsage:  make(map[Tier]int64),
		objectTiers: make(map[string]Tier),
		stopCh:    make(chan struct{}),
	}
}

// Start starts the tiering manager
func (m *Manager) Start(ctx context.Context) {
	m.logger.Info("Starting tiering manager")

	// Initialize tier usage
	for _, tier := range []Tier{TierHot, TierWarm, TierCold, TierGlacier} {
		m.tierUsage[tier] = 0
	}

	// Start tiering worker
	go m.tieringWorker(ctx)
}

// Stop stops the tiering manager
func (m *Manager) Stop() {
	close(m.stopCh)
	m.logger.Info("Tiering manager stopped")
}

// CreatePolicy creates a tiering policy
func (m *Manager) CreatePolicy(name, bucket, prefix string) (*TieringPolicy, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	policy := &TieringPolicy{
		ID:          uuid.New().String(),
		Name:        name,
		Bucket:      bucket,
		Prefix:      prefix,
		TierConfigs: DefaultTierConfigs(),
		Enabled:     true,
		CreatedAt:   time.Now(),
	}

	m.policies[policy.ID] = policy

	m.logger.Info("Tiering policy created",
		zap.String("id", policy.ID),
		zap.String("name", name))

	return policy, nil
}

// GetPolicy returns a policy by ID
func (m *Manager) GetPolicy(policyID string) (*TieringPolicy, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	p, ok := m.policies[policyID]
	return p, ok
}

// ListPolicies lists all policies
func (m *Manager) ListPolicies() []*TieringPolicy {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]*TieringPolicy, 0, len(m.policies))
	for _, p := range m.policies {
		result = append(result, p)
	}
	return result
}

// UpdateObjectTier updates the tier of an object
func (m *Manager) UpdateObjectTier(bucket, key string, tier Tier) {
	m.mu.Lock()
	defer m.mu.Unlock()

	objectKey := bucket + "/" + key
	oldTier := m.objectTiers[objectKey]

	// Update usage
	if oldTier != "" {
		m.tierUsage[oldTier] -= 1 // Decrement count (simplified)
	}
	m.tierUsage[tier] += 1 // Increment count
	m.objectTiers[objectKey] = tier

	m.logger.Debug("Object tier updated",
		zap.String("bucket", bucket),
		zap.String("key", key),
		zap.String("old_tier", string(oldTier)),
		zap.String("new_tier", string(tier)))
}

// GetObjectTier returns the current tier of an object
func (m *Manager) GetObjectTier(bucket, key string) (Tier, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	objectKey := bucket + "/" + key
	tier, ok := m.objectTiers[objectKey]
	return tier, ok
}

// GetTierUsage returns usage per tier
func (m *Manager) GetTierUsage() map[Tier]int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[Tier]int64)
	for k, v := range m.tierUsage {
		result[k] = v
	}
	return result
}

// GetCostEstimate returns monthly cost estimate
func (m *Manager) GetCostEstimate() (float64, map[Tier]float64) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	tierCosts := make(map[Tier]float64)
	var total float64

	// Calculate per-tier costs
	tierConfigMap := make(map[Tier]TierConfig)
	for _, cfg := range DefaultTierConfigs() {
		tierConfigMap[cfg.Tier] = cfg
	}

	for tier, bytes := range m.tierUsage {
		cfg := tierConfigMap[tier]
		gb := float64(bytes) / (1024 * 1024 * 1024)
		cost := gb * cfg.CostPerGBMonth
		tierCosts[tier] = cost
		total += cost
	}

	return total, tierCosts
}

// tieringWorker runs periodic tiering
func (m *Manager) tieringWorker(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.evaluateTiering()
		}
	}
}

// evaluateTiering evaluates and moves objects between tiers
func (m *Manager) evaluateTiering() {
	m.logger.Info("Evaluating tiering policies")

	m.mu.RLock()
	policies := make([]*TieringPolicy, 0, len(m.policies))
	for _, p := range m.policies {
		if p.Enabled {
			policies = append(policies, p)
		}
	}
	m.mu.RUnlock()

	for _, policy := range policies {
		m.evaluatePolicy(policy)
	}
}

// evaluatePolicy evaluates a single policy
func (m *Manager) evaluatePolicy(policy *TieringPolicy) {
	m.logger.Debug("Evaluating policy",
		zap.String("policy_id", policy.ID),
		zap.String("bucket", policy.Bucket))

	// In production, this would:
	// 1. Scan objects in the bucket/prefix
	// 2. Check age and access patterns
	// 3. Determine optimal tier
	// 4. Schedule tier change

	// For now, log the evaluation
	for _, cfg := range policy.TierConfigs {
		m.logger.Debug("Tier config",
			zap.String("tier", string(cfg.Tier)),
			zap.Duration("min_age", cfg.MinAge))
	}
}

// RecommendTier recommends the best tier for an object
func (m *Manager) RecommendTier(obj *ObjectInfo) Tier {
	// Simple algorithm based on age and access patterns
	age := time.Since(obj.CreatedAt)
	accessRate := float64(obj.AccessCount) / age.Hours()

	// Very frequently accessed -> hot
	if accessRate > 10 {
		return TierHot
	}

	// Frequently accessed -> warm
	if accessRate > 1 {
		return TierWarm
	}

	// Not accessed recently -> cold
	if age > 90*24*time.Hour {
		return TierCold
	}

	// Very old -> glacier
	if age > 180*24*time.Hour {
		return TierGlacier
	}

	// Default
	return TierWarm
}

// TransitionTier transitions an object to a new tier
func (m *Manager) TransitionTier(ctx context.Context, bucket, key string, targetTier Tier) error {
	m.logger.Info("Transitioning object tier",
		zap.String("bucket", bucket),
		zap.String("key", key),
		zap.String("target_tier", string(targetTier)))

	// In production, this would:
	// 1. Copy object to new tier location
	// 2. Update metadata
	// 3. Delete from old location

	// Update in-memory state
	m.UpdateObjectTier(bucket, key, targetTier)

	return nil
}

// Analyzer analyzes access patterns for tiering
type Analyzer struct {
	logger *zap.Logger
}

// NewAnalyzer creates a new access pattern analyzer
func NewAnalyzer(logger *zap.Logger) *Analyzer {
	return &Analyzer{logger: logger}
}

// AnalyzeAccessPattern analyzes access patterns for objects
func (a *Analyzer) AnalyzeAccessPattern(objects []*ObjectInfo) map[string]TierRecommendation {
	recommendations := make(map[string]TierRecommendation)

	for _, obj := range objects {
		age := time.Since(obj.CreatedAt)
		accessRate := float64(obj.AccessCount) / (age.Hours() + 1)

		var recommendedTier Tier
		var reason string

		switch {
		case accessRate > 10:
			recommendedTier = TierHot
			reason = "High access rate"
		case accessRate > 1:
			recommendedTier = TierWarm
			reason = "Moderate access rate"
		case age > 180*24*time.Hour:
			recommendedTier = TierGlacier
			reason = "No access for 180+ days"
		case age > 90*24*time.Hour:
			recommendedTier = TierCold
			reason = "No access for 90+ days"
		default:
			recommendedTier = TierWarm
			reason = "Default tier"
		}

		recommendations[obj.Bucket+"/"+obj.Key] = TierRecommendation{
			Object:      obj,
			Recommended: recommendedTier,
			Current:     obj.Tier,
			Reason:      reason,
			SavingsGB:   float64(obj.Size) / (1024 * 1024 * 1024),
		}
	}

	return recommendations
}

// TierRecommendation contains tier recommendation
type TierRecommendation struct {
	Object      *ObjectInfo
	Recommended Tier
	Current     Tier
	Reason      string
	SavingsGB   float64
}
