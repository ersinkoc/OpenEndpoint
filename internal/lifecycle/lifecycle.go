package lifecycle

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// Lifecycle manages lifecycle rules for buckets
type Lifecycle struct {
	mu    sync.RWMutex
	rules map[string][]*Rule // bucketName -> rules
}

// Rule represents a lifecycle rule
type Rule struct {
	ID         string         `json:"id" yaml:"id"`
	Name       string         `json:"name" yaml:"name"`
	Enabled    bool           `json:"enabled" yaml:"enabled"`
	Priority   int            `json:"priority" yaml:"priority"`
	Filter     *Filter        `json:"filter" yaml:"filter"`
	Actions    []*Action      `json:"actions" yaml:"actions"`
	CreatedAt  time.Time      `json:"createdAt" yaml:"createdAt"`
	ModifiedAt time.Time      `json:"modifiedAt" yaml:"modifiedAt"`
	Status     string         `json:"status" yaml:"status"` // Enabled, Disabled
}

// Filter represents the filter for a rule
type Filter struct {
	Prefix            string            `json:"prefix,omitempty" yaml:"prefix,omitempty"`
	Tag              *Tag              `json:"tag,omitempty" yaml:"tag,omitempty"`
	And              *AndFilter        `json:"and,omitempty" yaml:"and,omitempty"`
	ObjectSizeGreater *int64            `json:"objectSizeGreaterThan,omitempty" yaml:"objectSizeGreaterThan,omitempty"`
	ObjectSizeLesser  *int64            `json:"objectSizeLessThan,omitempty" yaml:"objectSizeLessThan,omitempty"`
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

// Action represents a lifecycle action
type Action struct {
	Name          string `json:"name" yaml:"name"` // Delete, Transition, ExpiredObjectDeleteMarker, AbortIncompleteMultipartUpload
	Days          *int   `json:"days,omitempty" yaml:"days,omitempty"`
	Date          *string `json:"date,omitempty" yaml:"date,omitempty"`
	StorageClass  *string `json:"storageClass,omitempty" yaml:"storageClass,omitempty"`
	DeleteMarkerReplication *bool `json:"deleteMarkerReplication,omitempty" yaml:"deleteMarkerReplication,omitempty"`
}

// StorageClassTransition represents storage class transitions
var StorageClasses = map[string]string{
	"STANDARD":          "STANDARD",
	"STANDARD_IA":       "STANDARD_IA",
	"INTELLIGENT_TIERING": "INTELLIGENT_TIERING",
	"GLACIER":           "GLACIER",
	"DEEP_ARCHIVE":      "DEEP_ARCHIVE",
	"REDUCED_REDUNDANCY": "REDUCED_REDUNDANCY",
}

// Transition represents a transition action
type Transition struct {
	Days          int    `json:"days" yaml:"days"`
	StorageClass string `json:"storageClass" yaml:"storageClass"`
}

// Expiration represents an expiration action
type Expiration struct {
	Days            int    `json:"days" yaml:"days"`
	Date            string `json:"date" yaml:"date"`
	ExpiredObjectDeleteMarker bool `json:"expiredObjectDeleteMarker" yaml:"expiredObjectDeleteMarker"`
}

// AbortIncompleteMultipartUpload represents abort incomplete multipart upload
type AbortIncompleteMultipartUpload struct {
	DaysAfterInitiation int `json:"daysAfterInitiation" yaml:"daysAfterInitiation"`
}

// New creates a new Lifecycle manager
func New() *Lifecycle {
	return &Lifecycle{
		rules: make(map[string][]*Rule),
	}
}

// AddRule adds a lifecycle rule to a bucket
func (l *Lifecycle) AddRule(bucket string, rule *Rule) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if rule.ID == "" {
		rule.ID = generateRuleID()
	}
	if rule.Status == "" {
		rule.Status = "Enabled"
	}
	rule.CreatedAt = time.Now()
	rule.ModifiedAt = time.Now()

	rules := l.rules[bucket]
	for _, r := range rules {
		if r.ID == rule.ID {
			return fmt.Errorf("rule already exists: %s", rule.ID)
		}
	}

	l.rules[bucket] = append(rules, rule)
	return nil
}

// GetRule returns a rule by bucket and rule ID
func (l *Lifecycle) GetRule(bucket, ruleID string) (*Rule, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	rules, ok := l.rules[bucket]
	if !ok {
		return nil, false
	}

	for _, r := range rules {
		if r.ID == ruleID {
			return r, true
		}
	}
	return nil, false
}

// ListRules returns all rules for a bucket
func (l *Lifecycle) ListRules(bucket string) []*Rule {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.rules[bucket]
}

// ListAllRules returns all lifecycle rules
func (l *Lifecycle) ListAllRules() map[string][]*Rule {
	l.mu.RLock()
	defer l.mu.RUnlock()

	result := make(map[string][]*Rule)
	for bucket, rules := range l.rules {
		result[bucket] = rules
	}
	return result
}

// UpdateRule updates a lifecycle rule
func (l *Lifecycle) UpdateRule(bucket string, ruleID string, updates *Rule) (*Rule, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	rules, ok := l.rules[bucket]
	if !ok {
		return nil, fmt.Errorf("bucket not found: %s", bucket)
	}

	for i, r := range rules {
		if r.ID == ruleID {
			if updates.Name != "" {
				rules[i].Name = updates.Name
			}
			if updates.Enabled {
				rules[i].Enabled = updates.Enabled
				rules[i].Status = "Enabled"
			} else {
				rules[i].Status = "Disabled"
			}
			if updates.Priority > 0 {
				rules[i].Priority = updates.Priority
			}
			if updates.Filter != nil {
				rules[i].Filter = updates.Filter
			}
			if updates.Actions != nil {
				rules[i].Actions = updates.Actions
			}
			rules[i].ModifiedAt = time.Now()
			return rules[i], nil
		}
	}

	return nil, fmt.Errorf("rule not found: %s", ruleID)
}

// DeleteRule deletes a lifecycle rule
func (l *Lifecycle) DeleteRule(bucket, ruleID string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	rules, ok := l.rules[bucket]
	if !ok {
		return fmt.Errorf("bucket not found: %s", bucket)
	}

	for i, r := range rules {
		if r.ID == ruleID {
			l.rules[bucket] = append(rules[:i], rules[i+1:]...)
			return nil
		}
	}

	return fmt.Errorf("rule not found: %s", ruleID)
}

// DeleteBucketRules deletes all rules for a bucket
func (l *Lifecycle) DeleteBucketRules(bucket string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	delete(l.rules, bucket)
	return nil
}

// GetApplicableRules returns rules applicable to an object
func (l *Lifecycle) GetApplicableRules(bucket, objectKey string, objectSize int64, tags map[string]string, createdAt time.Time) []*Rule {
	l.mu.RLock()
	defer l.mu.RUnlock()

	rules, ok := l.rules[bucket]
	if !ok {
		return nil
	}

	var applicable []*Rule
	for _, rule := range rules {
		if rule.Status != "Enabled" {
			continue
		}

		if ruleMatchesObject(rule, objectKey, objectSize, tags) {
			applicable = append(applicable, rule)
		}
	}

	// Sort by priority
	for i := 0; i < len(applicable)-1; i++ {
		for j := i + 1; j < len(applicable); j++ {
			if applicable[j].Priority < applicable[i].Priority {
				applicable[i], applicable[j] = applicable[j], applicable[i]
			}
		}
	}

	return applicable
}

// ruleMatchesObject checks if a rule matches an object
func ruleMatchesObject(rule *Rule, objectKey string, objectSize int64, tags map[string]string) bool {
	if rule.Filter == nil {
		return true
	}

	filter := rule.Filter

	// Check prefix
	if filter.Prefix != "" {
		if len(objectKey) < len(filter.Prefix) {
			return false
		}
		if objectKey[:len(filter.Prefix)] != filter.Prefix {
			return false
		}
	}

	// Check object size
	if filter.ObjectSizeGreater != nil && objectSize <= *filter.ObjectSizeGreater {
		return false
	}
	if filter.ObjectSizeLesser != nil && objectSize >= *filter.ObjectSizeLesser {
		return false
	}

	// Check tags
	if filter.Tag != nil {
		if tagVal, ok := tags[filter.Tag.Key]; !ok || tagVal != filter.Tag.Value {
			return false
		}
	}

	// Check AND filter
	if filter.And != nil {
		if filter.And.Prefix != "" {
			if len(objectKey) < len(filter.And.Prefix) || objectKey[:len(filter.And.Prefix)] != filter.And.Prefix {
				return false
			}
		}
		for _, tag := range filter.And.Tags {
			if tagVal, ok := tags[tag.Key]; !ok || tagVal != tag.Value {
				return false
			}
		}
	}

	return true
}

// GetExpiredObjects returns objects that should be expired
func (l *Lifecycle) GetExpiredObjects(bucket string, now time.Time) []ObjectExpiry {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var expired []ObjectExpiry

	rules, ok := l.rules[bucket]
	if !ok {
		return expired
	}

	for _, rule := range rules {
		if rule.Status != "Enabled" {
			continue
		}

		for _, action := range rule.Actions {
			if action.Name == "Expiration" || action.Name == "Delete" {
				if action.Days != nil {
					expiryDate := now.AddDate(0, 0, -*action.Days)
					expired = append(expired, ObjectExpiry{
						RuleID:      rule.ID,
						ObjectKey:   rule.Filter.Prefix,
						ExpiryDate:  expiryDate,
						ActionType:  "Expiration",
					})
				}
			}
		}
	}

	return expired
}

// GetTransitions returns objects that should be transitioned
func (l *Lifecycle) GetTransitions(bucket string, now time.Time) []ObjectTransition {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var transitions []ObjectTransition

	rules, ok := l.rules[bucket]
	if !ok {
		return transitions
	}

	for _, rule := range rules {
		if rule.Status != "Enabled" {
			continue
		}

		for _, action := range rule.Actions {
			if action.Name == "Transition" || action.Name == "StorageClass" {
				if action.Days != nil && action.StorageClass != nil {
					transitionDate := now.AddDate(0, 0, -*action.Days)
					transitions = append(transitions, ObjectTransition{
						RuleID:       rule.ID,
						ObjectKey:    rule.Filter.Prefix,
						TransitionDate: transitionDate,
						StorageClass: *action.StorageClass,
						ActionType:   "Transition",
					})
				}
			}
		}
	}

	return transitions
}

// ObjectExpiry represents an object that should be expired
type ObjectExpiry struct {
	RuleID     string
	ObjectKey  string
	ExpiryDate time.Time
	ActionType string
}

// ObjectTransition represents an object that should be transitioned
type ObjectTransition struct {
	RuleID          string
	ObjectKey       string
	TransitionDate  time.Time
	StorageClass    string
	ActionType      string
}

// generateRuleID generates a unique rule ID
func generateRuleID() string {
	return fmt.Sprintf("rule-%d", time.Now().UnixNano())
}

// ToJSON returns lifecycle rules as JSON
func (l *Lifecycle) ToJSON(bucket string) ([]byte, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	rules := l.rules[bucket]
	return json.MarshalIndent(rules, "", "  ")
}

// FromJSON loads lifecycle rules from JSON
func (l *Lifecycle) FromJSON(bucket string, data []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	var rules []*Rule
	if err := json.Unmarshal(data, &rules); err != nil {
		return err
	}

	l.rules[bucket] = rules
	return nil
}

// BucketHasRules checks if a bucket has lifecycle rules
func (l *Lifecycle) BucketHasRules(bucket string) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()

	rules, ok := l.rules[bucket]
	if !ok || len(rules) == 0 {
		return false
	}

	return true
}

// GetRuleCount returns the number of rules for a bucket
func (l *Lifecycle) GetRuleCount(bucket string) int {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return len(l.rules[bucket])
}
