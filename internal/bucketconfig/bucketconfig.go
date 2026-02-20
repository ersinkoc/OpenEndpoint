package bucketconfig

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// Config manages bucket configuration
type Config struct {
	mu           sync.RWMutex
	buckets      map[string]*BucketConfig
	versioning   map[string]*VersioningConfig
	cors         map[string]*CORSConfig
	policies     map[string]*BucketPolicy
	objectLock   map[string]*ObjectLockConfig
	tags         map[string]map[string]string
}

// BucketConfig represents bucket configuration
type BucketConfig struct {
	Name                string            `json:"name" yaml:"name"`
	CreationDate        time.Time         `json:"creationDate" yaml:"creationDate"`
	Location            string            `json:"location" yaml:"location"`
	Owner               string            `json:"owner" yaml:"owner"`
	EndpointType        string            `json:"endpointType" yaml:"endpointType"`
	AccelerateConfig    string            `json:"accelerateConfig" yaml:"accelerateConfig"`
	Policy              *BucketPolicy     `json:"policy,omitempty" yaml:"policy,omitempty"`
	Tags                map[string]string `json:"tags,omitempty" yaml:"tags,omitempty"`
}

// VersioningConfig represents bucket versioning configuration
type VersioningConfig struct {
	Status          string    `json:"status" yaml:"status"` // Enabled, Suspended, Disabled
	MFADelete       string    `json:"mfaDelete" yaml:"mfaDelete"` // Enabled, Disabled
	ModifiedDate    time.Time `json:"modifiedDate" yaml:"modifiedDate"`
}

// VersionInfo represents version information for an object
type VersionInfo struct {
	VersionID     string    `json:"versionId" yaml:"versionId"`
	IsLatest      bool      `json:"isLatest" yaml:"isLatest"`
	Key           string    `json:"key" yaml:"key"`
	Size          int64     `json:"size" yaml:"size"`
	ETag          string    `json:"etag" yaml:"etag"`
	LastModified  time.Time `json:"lastModified" yaml:"lastModified"`
	StorageClass  string    `json:"storageClass" yaml:"storageClass"`
	Owner         string    `json:"owner" yaml:"owner"`
	IsDeleteMarker bool     `json:"isDeleteMarker" yaml:"isDeleteMarker"`
}

// CORSConfig represents CORS configuration
type CORSConfig struct {
	Bucket      string     `json:"bucket" yaml:"bucket"`
	CORSRules   []*CORSRule `json:"corsRules" yaml:"corsRules"`
	ModifiedAt  time.Time  `json:"modifiedAt" yaml:"modifiedAt"`
}

// CORSRule represents a CORS rule
type CORSRule struct {
	ID             string   `json:"id,omitempty" yaml:"id,omitempty"`
	AllowedMethods []string `json:"allowedMethods" yaml:"allowedMethods"`
	AllowedOrigins []string `json:"allowedOrigins" yaml:"allowedOrigins"`
	AllowedHeaders []string `json:"allowedHeaders,omitempty" yaml:"allowedHeaders,omitempty"`
	ExposeHeaders  []string `json:"exposeHeaders,omitempty" yaml:"exposeHeaders,omitempty"`
	MaxAgeSeconds  int      `json:"maxAgeSeconds,omitempty" yaml:"maxAgeSeconds,omitempty"`
}

// BucketPolicy represents a bucket policy
type BucketPolicy struct {
	Version      string           `json:"version" yaml:"version"`
	ID           string           `json:"id,omitempty" yaml:"id,omitempty"`
	Statement    []*PolicyStatement `json:"statement" yaml:"statement"`
	ModifiedDate time.Time        `json:"modifiedDate" yaml:"modifiedDate"`
}

// PolicyStatement represents a policy statement
type PolicyStatement struct {
	Sid        string      `json:"sid,omitempty" yaml:"sid,omitempty"`
	Effect     string      `json:"effect" yaml:"effect"` // Allow, Deny
	Principal  interface{} `json:"principal" yaml:"principal"`
	Action     interface{} `json:"action" yaml:"action"`
	Resource   interface{} `json:"resource" yaml:"resource"`
	Condition  interface{} `json:"condition,omitempty" yaml:"condition,omitempty"`
}

// ObjectLockConfig represents object lock configuration
type ObjectLockConfig struct {
	Enabled          bool   `json:"enabled" yaml:"enabled"`
	RetentionMode    string `json:"retentionMode" yaml:"retentionMode"` // Governance, Compliance
	RetentionDays    int    `json:"retentionDays" yaml:"retentionDays"`
	LegalHoldEnabled bool   `json:"legalHoldEnabled" yaml:"legalHoldEnabled"`
	ModifiedDate     time.Time `json:"modifiedDate" yaml:"modifiedDate"`
}

// ObjectLockRetention represents object lock retention
type ObjectLockRetention struct {
	Mode          string    `json:"mode" yaml:"mode"`
	RetainUntil   time.Time `json:"retainUntil" yaml:"retainUntil"`
	RetainedBy    string    `json:"retainedBy" yaml:"retainedBy"`
	CreatedAt     time.Time `json:"createdAt" yaml:"createdAt"`
}

// ObjectLockLegalHold represents legal hold status
type ObjectLockLegalHold struct {
	Status   string    `json:"status" yaml:"status"` // ON, OFF
	AppliedBy string   `json:"appliedBy" yaml:"appliedBy"`
	AppliedAt time.Time `json:"appliedAt" yaml:"appliedAt"`
}

// New creates a new bucket config manager
func New() *Config {
	return &Config{
		buckets:    make(map[string]*BucketConfig),
		versioning: make(map[string]*VersioningConfig),
		cors:       make(map[string]*CORSConfig),
		policies:   make(map[string]*BucketPolicy),
		objectLock: make(map[string]*ObjectLockConfig),
		tags:       make(map[string]map[string]string),
	}
}

// SetBucketConfig sets bucket configuration
func (c *Config) SetBucketConfig(name string, config *BucketConfig) {
	c.mu.Lock()
	defer c.mu.Unlock()

	config.Name = name
	config.CreationDate = time.Now()
	c.buckets[name] = config
}

// GetBucketConfig returns bucket configuration
func (c *Config) GetBucketConfig(name string) (*BucketConfig, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	config, ok := c.buckets[name]
	return config, ok
}

// ListBucketConfigs returns all bucket configurations
func (c *Config) ListBucketConfigs() []*BucketConfig {
	c.mu.RLock()
	defer c.mu.RUnlock()

	configs := make([]*BucketConfig, 0, len(c.buckets))
	for _, config := range c.buckets {
		configs = append(configs, config)
	}
	return configs
}

// SetVersioningConfig sets versioning configuration
func (c *Config) SetVersioningConfig(bucket string, config *VersioningConfig) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.buckets[bucket]; !ok {
		return fmt.Errorf("bucket not found: %s", bucket)
	}

	config.ModifiedDate = time.Now()
	c.versioning[bucket] = config
	return nil
}

// GetVersioningConfig returns versioning configuration
func (c *Config) GetVersioningConfig(bucket string) (*VersioningConfig, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	config, ok := c.versioning[bucket]
	return config, ok
}

// IsVersioningEnabled returns true if versioning is enabled
func (c *Config) IsVersioningEnabled(bucket string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if config, ok := c.versioning[bucket]; ok {
		return config.Status == "Enabled"
	}
	return false
}

// SetCORSConfig sets CORS configuration
func (c *Config) SetCORSConfig(bucket string, config *CORSConfig) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.buckets[bucket]; !ok {
		return fmt.Errorf("bucket not found: %s", bucket)
	}

	config.Bucket = bucket
	config.ModifiedAt = time.Now()
	c.cors[bucket] = config
	return nil
}

// GetCORSConfig returns CORS configuration
func (c *Config) GetCORSConfig(bucket string) (*CORSConfig, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	config, ok := c.cors[bucket]
	return config, ok
}

// DeleteCORSConfig deletes CORS configuration
func (c *Config) DeleteCORSConfig(bucket string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.cors, bucket)
	return nil
}

// SetBucketPolicy sets bucket policy
func (c *Config) SetBucketPolicy(bucket string, policy *BucketPolicy) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.buckets[bucket]; !ok {
		return fmt.Errorf("bucket not found: %s", bucket)
	}

	policy.ModifiedDate = time.Now()
	c.policies[bucket] = policy
	return nil
}

// GetBucketPolicy returns bucket policy
func (c *Config) GetBucketPolicy(bucket string) (*BucketPolicy, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	policy, ok := c.policies[bucket]
	return policy, ok
}

// DeleteBucketPolicy deletes bucket policy
func (c *Config) DeleteBucketPolicy(bucket string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.policies, bucket)
	return nil
}

// SetObjectLockConfig sets object lock configuration
func (c *Config) SetObjectLockConfig(bucket string, config *ObjectLockConfig) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.buckets[bucket]; !ok {
		return fmt.Errorf("bucket not found: %s", bucket)
	}

	config.ModifiedDate = time.Now()
	c.objectLock[bucket] = config
	return nil
}

// GetObjectLockConfig returns object lock configuration
func (c *Config) GetObjectLockConfig(bucket string) (*ObjectLockConfig, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	config, ok := c.objectLock[bucket]
	return config, ok
}

// IsObjectLockEnabled returns true if object lock is enabled
func (c *Config) IsObjectLockEnabled(bucket string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if config, ok := c.objectLock[bucket]; ok {
		return config.Enabled
	}
	return false
}

// SetBucketTags sets bucket tags
func (c *Config) SetBucketTags(bucket string, tags map[string]string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.buckets[bucket]; !ok {
		return fmt.Errorf("bucket not found: %s", bucket)
	}

	c.tags[bucket] = tags

	// Update in bucket config
	if config, ok := c.buckets[bucket]; ok {
		config.Tags = tags
	}

	return nil
}

// GetBucketTags returns bucket tags
func (c *Config) GetBucketTags(bucket string) (map[string]string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	tags, ok := c.tags[bucket]
	return tags, ok
}

// DeleteBucketTags deletes bucket tags
func (c *Config) DeleteBucketTags(bucket string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.tags, bucket)

	// Update in bucket config
	if config, ok := c.buckets[bucket]; ok {
		config.Tags = nil
	}

	return nil
}

// DeleteBucketConfig deletes bucket configuration
func (c *Config) DeleteBucketConfig(bucket string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.buckets, bucket)
	delete(c.versioning, bucket)
	delete(c.cors, bucket)
	delete(c.policies, bucket)
	delete(c.objectLock, bucket)
	delete(c.tags, bucket)

	return nil
}

// ValidateCORS validates CORS configuration
func ValidateCORS(config *CORSConfig) error {
	if config == nil {
		return fmt.Errorf("CORS configuration is nil")
	}

	for _, rule := range config.CORSRules {
		if len(rule.AllowedMethods) == 0 {
			return fmt.Errorf("CORS rule must have at least one allowed method")
		}
		if len(rule.AllowedOrigins) == 0 {
			return fmt.Errorf("CORS rule must have at least one allowed origin")
		}

		// Validate methods
		validMethods := map[string]bool{
			"GET": true, "HEAD": true, "POST": true,
			"PUT": true, "DELETE": true, "PATCH": true,
		}
		for _, method := range rule.AllowedMethods {
			if !validMethods[method] {
				return fmt.Errorf("invalid CORS method: %s", method)
			}
		}
	}

	return nil
}

// ValidatePolicy validates bucket policy
func ValidatePolicy(policy *BucketPolicy) error {
	if policy == nil {
		return fmt.Errorf("policy is nil")
	}

	if policy.Version == "" {
		policy.Version = "2012-10-17"
	}

	if len(policy.Statement) == 0 {
		return fmt.Errorf("policy must have at least one statement")
	}

	for _, stmt := range policy.Statement {
		if stmt.Effect != "Allow" && stmt.Effect != "Deny" {
			return fmt.Errorf("invalid effect: %s", stmt.Effect)
		}
		if stmt.Principal == nil {
			return fmt.Errorf("principal is required")
		}
		if stmt.Action == nil {
			return fmt.Errorf("action is required")
		}
		if stmt.Resource == nil {
			return fmt.Errorf("resource is required")
		}
	}

	return nil
}

// ToJSON returns bucket config as JSON
func (c *Config) ToJSON(bucket string) ([]byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := map[string]interface{}{
		"versioning": c.versioning[bucket],
		"cors":       c.cors[bucket],
		"policy":     c.policies[bucket],
		"objectLock": c.objectLock[bucket],
		"tags":       c.tags[bucket],
	}

	return json.MarshalIndent(result, "", "  ")
}

// GenerateVersionID generates a version ID
func GenerateVersionID() string {
	return fmt.Sprintf("%d-%s", time.Now().UnixNano(), generateRandomID())
}

func generateRandomID() string {
	const hexChars = "0123456789abcdef"
	bytes := make([]byte, 8)
	for i := range bytes {
		bytes[i] = hexChars[time.Now().UnixNano()%16]
		time.Sleep(time.Nanosecond)
	}
	return string(bytes)
}

// GetPolicyJSON returns policy as JSON string
func (p *BucketPolicy) GetJSON() (string, error) {
	data, err := json.MarshalIndent(p, "", "  ")
	return string(data), err
}

// SetPolicyFromJSON sets policy from JSON string
func (p *BucketPolicy) SetPolicyFromJSON(jsonStr string) error {
	return json.Unmarshal([]byte(jsonStr), p)
}
