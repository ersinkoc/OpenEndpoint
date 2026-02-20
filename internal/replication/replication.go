package replication

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// Replication manages bucket replication
type Replication struct {
	mu        sync.RWMutex
	rules     map[string][]*Rule // bucketName -> rules
	stats     map[string]*Stats  // bucketName -> stats
	status    map[string]string  // bucketName -> status
}

// Rule represents a replication rule
type Rule struct {
	ID                    string         `json:"id" yaml:"id"`
	Name                  string         `json:"name" yaml:"name"`
	Priority              int            `json:"priority" yaml:"priority"`
	Status                string         `json:"status" yaml:"status"` // Enabled, Disabled
	Filter               *Filter        `json:"filter" yaml:"filter"`
	Destination          *Destination   `json:"destination" yaml:"destination"`
	DeleteMarkerReplication *DeleteMarkerReplication `json:"deleteMarkerReplication" yaml:"deleteMarkerReplication"`
	ExistingObjectReplication *ExistingObjectReplication `json:"existingObjectReplication" yaml:"existingObjectReplication"`
	ReplicaModifications *ReplicaModifications `json:"replicaModifications" yaml:"replicaModifications"`
	ReplicationTime      *ReplicationTime `json:"replicationTime" yaml:"replicationTime"`
	CreatedAt            time.Time       `json:"createdAt" yaml:"createdAt"`
	ModifiedAt           time.Time       `json:"modifiedAt" yaml:"modifiedAt"`
}

// Filter represents the filter for a rule
type Filter struct {
	Prefix            string            `json:"prefix,omitempty" yaml:"prefix,omitempty"`
	Tag              *Tag              `json:"tag,omitempty" yaml:"tag,omitempty"`
	And              *AndFilter        `json:"and,omitempty" yaml:"and,omitempty"`
}

// Tag represents a tag filter
type Tag struct {
	Key   string `json:"key" yaml:"key"`
	Value string `json:"value" yaml:"value"`
}

// AndFilter represents an AND filter
type AndFilter struct {
	Prefix string `json:"prefix,omitempty" yaml:"prefix,omitempty"`
	Tags   []*Tag `json:"tags,omitempty" yaml:"tags,omitempty"`
}

// Destination represents the replication destination
type Destination struct {
	Bucket             string   `json:"bucket" yaml:"bucket"`
	StorageClass       string   `json:"storageClass,omitempty" yaml:"storageClass,omitempty"`
	EncryptionType     string   `json:"encryptionType,omitempty" yaml:"encryptionType,omitempty"`
	ReplicaKmsKeyID    string   `json:"replicaKmsKeyID,omitempty" yaml:"replicaKmsKeyID,omitempty"`
	AccessControlTranslation *AccessControlTranslation `json:"accessControlTranslation,omitempty" yaml:"accessControlTranslation,omitempty"`
	Metrics            *Metrics `json:"metrics,omitempty" yaml:"metrics,omitempty"`
	ReplicationTime    *ReplicationTime `json:"replicationTime,omitempty" yaml:"replicationTime,omitempty"`
}

// AccessControlTranslation represents access control translation
type AccessControlTranslation struct {
	Owner string `json:"owner" yaml:"owner"`
}

// Metrics represents replication metrics
type Metrics struct {
	Status               string `json:"status" yaml:"status"`
	EventThresholdMinutes int   `json:"eventThresholdMinutes" yaml:"eventThresholdMinutes"`
}

// ReplicationTime represents replication time configuration
type ReplicationTime struct {
	Status string `json:"status" yaml:"status"`
	Minutes int   `json:"minutes" yaml:"minutes"`
}

// DeleteMarkerReplication represents delete marker replication
type DeleteMarkerReplication struct {
	Status string `json:"status" yaml:"status"`
}

// ExistingObjectReplication represents existing object replication
type ExistingObjectReplication struct {
	Status string `json:"status" yaml:"status"`
}

// ReplicaModifications represents replica modifications
type ReplicaModifications struct {
	Status string `json:"status" yaml:"status"`
}

// Stats represents replication statistics
type Stats struct {
	ReplicatedObjects      int64     `json:"replicatedObjects" yaml:"replicatedObjects"`
	ReplicatedBytes       int64     `json:"replicatedBytes" yaml:"replicatedBytes"`
	PendingReplication    int64     `json:"pendingReplication" yaml:"pendingReplication"`
	FailedReplication     int64     `json:"failedReplication" yaml:"failedReplication"`
	LastReplicationTime   time.Time `json:"lastReplicationTime" yaml:"lastReplicationTime"`
	Latency                int64     `json:"latency" yaml:"latency"` // in milliseconds
}

// DestinationStatus represents the status of a destination
type DestinationStatus struct {
	Bucket      string    `json:"bucket" yaml:"bucket"`
	Status      string    `json:"status" yaml:"status"`
	LastSync    time.Time `json:"lastSync" yaml:"lastSync"`
	ObjectCount int64     `json:"objectCount" yaml:"objectCount"`
	BytesUsed   int64     `json:"bytesUsed" yaml:"bytesUsed"`
}

// New creates a new Replication manager
func New() *Replication {
	return &Replication{
		rules:  make(map[string][]*Rule),
		stats:  make(map[string]*Stats),
		status: make(map[string]string),
	}
}

// AddRule adds a replication rule
func (r *Replication) AddRule(bucket string, rule *Rule) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if rule.ID == "" {
		rule.ID = generateRuleID()
	}
	if rule.Status == "" {
		rule.Status = "Enabled"
	}
	rule.CreatedAt = time.Now()
	rule.ModifiedAt = time.Now()

	rules := r.rules[bucket]
	for _, existing := range rules {
		if existing.ID == rule.ID {
			return fmt.Errorf("rule already exists: %s", rule.ID)
		}
	}

	r.rules[bucket] = append(rules, rule)
	r.stats[bucket] = &Stats{}
	r.status[bucket] = "Enabled"

	return nil
}

// GetRule returns a rule by bucket and rule ID
func (r *Replication) GetRule(bucket, ruleID string) (*Rule, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	rules, ok := r.rules[bucket]
	if !ok {
		return nil, false
	}

	for _, rule := range rules {
		if rule.ID == ruleID {
			return rule, true
		}
	}
	return nil, false
}

// ListRules returns all rules for a bucket
func (r *Replication) ListRules(bucket string) []*Rule {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.rules[bucket]
}

// ListAllRules returns all replication rules
func (r *Replication) ListAllRules() map[string][]*Rule {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make(map[string][]*Rule)
	for bucket, rules := range r.rules {
		result[bucket] = rules
	}
	return result
}

// UpdateRule updates a replication rule
func (r *Replication) UpdateRule(bucket string, ruleID string, updates *Rule) (*Rule, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	rules, ok := r.rules[bucket]
	if !ok {
		return nil, fmt.Errorf("bucket not found: %s", bucket)
	}

	for i, rule := range rules {
		if rule.ID == ruleID {
			if updates.Name != "" {
				rules[i].Name = updates.Name
			}
			if updates.Priority > 0 {
				rules[i].Priority = updates.Priority
			}
			if updates.Status != "" {
				rules[i].Status = updates.Status
			}
			if updates.Filter != nil {
				rules[i].Filter = updates.Filter
			}
			if updates.Destination != nil {
				rules[i].Destination = updates.Destination
			}
			rules[i].ModifiedAt = time.Now()
			return rules[i], nil
		}
	}

	return nil, fmt.Errorf("rule not found: %s", ruleID)
}

// DeleteRule deletes a replication rule
func (r *Replication) DeleteRule(bucket, ruleID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	rules, ok := r.rules[bucket]
	if !ok {
		return fmt.Errorf("bucket not found: %s", bucket)
	}

	for i, rule := range rules {
		if rule.ID == ruleID {
			r.rules[bucket] = append(rules[:i], rules[i+1:]...)

			// Clean up stats if no more rules
			if len(r.rules[bucket]) == 0 {
				delete(r.stats, bucket)
				delete(r.status, bucket)
			}

			return nil
		}
	}

	return fmt.Errorf("rule not found: %s", ruleID)
}

// DeleteBucketRules deletes all rules for a bucket
func (r *Replication) DeleteBucketRules(bucket string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.rules, bucket)
	delete(r.stats, bucket)
	delete(r.status, bucket)

	return nil
}

// GetStats returns replication statistics for a bucket
func (r *Replication) GetStats(bucket string) (*Stats, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	stats, ok := r.stats[bucket]
	return stats, ok
}

// UpdateStats updates replication statistics
func (r *Replication) UpdateStats(bucket string, update *Stats) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if stats, ok := r.stats[bucket]; ok {
		stats.ReplicatedObjects += update.ReplicatedObjects
		stats.ReplicatedBytes += update.ReplicatedBytes
		stats.PendingReplication = update.PendingReplication
		stats.FailedReplication = update.FailedReplication
		stats.LastReplicationTime = time.Now()
		stats.Latency = update.Latency
	}
}

// GetStatus returns replication status for a bucket
func (r *Replication) GetStatus(bucket string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.status[bucket]
}

// SetStatus sets replication status for a bucket
func (r *Replication) SetStatus(bucket, status string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.status[bucket] = status
}

// IsEnabled returns true if replication is enabled for a bucket
func (r *Replication) IsEnabled(bucket string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if status, ok := r.status[bucket]; ok {
		return status == "Enabled"
	}
	return false
}

// GetReplicationDestination returns the destination for a bucket
func (r *Replication) GetReplicationDestination(bucket string) (*Destination, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	rules, ok := r.rules[bucket]
	if !ok || len(rules) == 0 {
		return nil, false
	}

	// Return the first enabled rule's destination
	for _, rule := range rules {
		if rule.Status == "Enabled" && rule.Destination != nil {
			return rule.Destination, true
		}
	}

	return nil, false
}

// GetDestinationStatus returns status for all destinations
func (r *Replication) GetDestinationStatus(bucket string) []DestinationStatus {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var statuses []DestinationStatus
	rules, ok := r.rules[bucket]
	if !ok {
		return statuses
	}

	for _, rule := range rules {
		if rule.Status == "Enabled" && rule.Destination != nil {
			statuses = append(statuses, DestinationStatus{
				Bucket:      rule.Destination.Bucket,
				Status:      rule.Status,
				LastSync:    time.Now(),
				ObjectCount: 0,
				BytesUsed:   0,
			})
		}
	}

	return statuses
}

// GetEnabledRules returns enabled rules for a bucket
func (r *Replication) GetEnabledRules(bucket string) []*Rule {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var enabled []*Rule
	rules, ok := r.rules[bucket]
	if !ok {
		return enabled
	}

	for _, rule := range rules {
		if rule.Status == "Enabled" {
			enabled = append(enabled, rule)
		}
	}

	return enabled
}

// BucketHasReplication checks if a bucket has replication enabled
func (r *Replication) BucketHasReplication(bucket string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	rules, ok := r.rules[bucket]
	if !ok {
		return false
	}

	for _, rule := range rules {
		if rule.Status == "Enabled" {
			return true
		}
	}

	return false
}

// GetRuleCount returns the number of rules for a bucket
func (r *Replication) GetRuleCount(bucket string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return len(r.rules[bucket])
}

// GetTotalReplicatedObjects returns total replicated objects across all buckets
func (r *Replication) GetTotalReplicatedObjects() int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var total int64
	for _, stats := range r.stats {
		total += stats.ReplicatedObjects
	}
	return total
}

// GetTotalReplicatedBytes returns total replicated bytes across all buckets
func (r *Replication) GetTotalReplicatedBytes() int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var total int64
	for _, stats := range r.stats {
		total += stats.ReplicatedBytes
	}
	return total
}

// generateRuleID generates a unique rule ID
func generateRuleID() string {
	return fmt.Sprintf("replication-rule-%d", time.Now().UnixNano())
}

// ToJSON returns replication rules as JSON
func (r *Replication) ToJSON(bucket string) ([]byte, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	rules := r.rules[bucket]
	return json.MarshalIndent(rules, "", "  ")
}

// FromJSON loads replication rules from JSON
func (r *Replication) FromJSON(bucket string, data []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var rules []*Rule
	if err := json.Unmarshal(data, &rules); err != nil {
		return err
	}

	r.rules[bucket] = rules
	if len(rules) > 0 {
		r.stats[bucket] = &Stats{}
		r.status[bucket] = "Enabled"
	}

	return nil
}
