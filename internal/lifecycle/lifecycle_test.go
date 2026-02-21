package lifecycle

import (
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	lc := New()
	if lc == nil {
		t.Fatal("Lifecycle should not be nil")
	}

	if lc.rules == nil {
		t.Error("Rules map should be initialized")
	}
}

func TestAddRule(t *testing.T) {
	lc := New()

	rule := &Rule{
		Name:    "test-rule",
		Enabled: true,
	}

	err := lc.AddRule("test-bucket", rule)
	if err != nil {
		t.Fatalf("AddRule failed: %v", err)
	}

	if rule.ID == "" {
		t.Error("Rule ID should be generated")
	}

	if rule.Status != "Enabled" {
		t.Errorf("Status = %s, want Enabled", rule.Status)
	}
}

func TestAddRule_Duplicate(t *testing.T) {
	lc := New()

	rule1 := &Rule{
		ID:      "rule-123",
		Name:    "test-rule",
		Enabled: true,
	}

	rule2 := &Rule{
		ID:      "rule-123",
		Name:    "duplicate-rule",
		Enabled: true,
	}

	lc.AddRule("test-bucket", rule1)
	err := lc.AddRule("test-bucket", rule2)

	if err == nil {
		t.Error("Should return error for duplicate rule ID")
	}
}

func TestGetRule(t *testing.T) {
	lc := New()

	rule := &Rule{
		ID:   "rule-123",
		Name: "test-rule",
	}
	lc.AddRule("test-bucket", rule)

	found, ok := lc.GetRule("test-bucket", "rule-123")
	if !ok {
		t.Fatal("Rule should be found")
	}

	if found.Name != "test-rule" {
		t.Errorf("Name = %s, want test-rule", found.Name)
	}
}

func TestGetRule_NotFound(t *testing.T) {
	lc := New()

	_, ok := lc.GetRule("non-existent-bucket", "rule-123")
	if ok {
		t.Error("Should not find rule in non-existent bucket")
	}

	lc.AddRule("test-bucket", &Rule{ID: "rule-123"})
	_, ok = lc.GetRule("test-bucket", "non-existent-rule")
	if ok {
		t.Error("Should not find non-existent rule")
	}
}

func TestListRules(t *testing.T) {
	lc := New()

	// Empty bucket
	rules := lc.ListRules("empty-bucket")
	if len(rules) != 0 {
		t.Errorf("Empty bucket should have 0 rules, got %d", len(rules))
	}

	// Add rules
	lc.AddRule("test-bucket", &Rule{ID: "rule-1", Name: "Rule 1"})
	lc.AddRule("test-bucket", &Rule{ID: "rule-2", Name: "Rule 2"})
	lc.AddRule("test-bucket", &Rule{ID: "rule-3", Name: "Rule 3"})

	rules = lc.ListRules("test-bucket")
	if len(rules) != 3 {
		t.Errorf("Rules count = %d, want 3", len(rules))
	}
}

func TestListAllRules(t *testing.T) {
	lc := New()

	lc.AddRule("bucket1", &Rule{ID: "rule-1"})
	lc.AddRule("bucket2", &Rule{ID: "rule-2"})
	lc.AddRule("bucket3", &Rule{ID: "rule-3"})

	allRules := lc.ListAllRules()
	if len(allRules) != 3 {
		t.Errorf("Buckets with rules = %d, want 3", len(allRules))
	}
}

func TestUpdateRule(t *testing.T) {
	lc := New()

	rule := &Rule{
		ID:     "rule-123",
		Name:   "Original Name",
		Enabled: true,
	}
	lc.AddRule("test-bucket", rule)

	updates := &Rule{
		Name:   "Updated Name",
		Enabled: false,
	}

	updated, err := lc.UpdateRule("test-bucket", "rule-123", updates)
	if err != nil {
		t.Fatalf("UpdateRule failed: %v", err)
	}

	if updated.Name != "Updated Name" {
		t.Errorf("Name = %s, want Updated Name", updated.Name)
	}

	if updated.Status != "Disabled" {
		t.Errorf("Status = %s, want Disabled", updated.Status)
	}
}

func TestUpdateRule_NotFound(t *testing.T) {
	lc := New()

	_, err := lc.UpdateRule("non-existent-bucket", "rule-123", &Rule{})
	if err == nil {
		t.Error("Should return error for non-existent bucket")
	}

	lc.AddRule("test-bucket", &Rule{ID: "rule-123"})
	_, err = lc.UpdateRule("test-bucket", "non-existent-rule", &Rule{})
	if err == nil {
		t.Error("Should return error for non-existent rule")
	}
}

func TestDeleteRule(t *testing.T) {
	lc := New()

	lc.AddRule("test-bucket", &Rule{ID: "rule-123"})

	err := lc.DeleteRule("test-bucket", "rule-123")
	if err != nil {
		t.Fatalf("DeleteRule failed: %v", err)
	}

	_, ok := lc.GetRule("test-bucket", "rule-123")
	if ok {
		t.Error("Rule should be deleted")
	}
}

func TestDeleteRule_NotFound(t *testing.T) {
	lc := New()

	err := lc.DeleteRule("non-existent-bucket", "rule-123")
	if err == nil {
		t.Error("Should return error for non-existent bucket")
	}

	lc.AddRule("test-bucket", &Rule{ID: "rule-123"})
	err = lc.DeleteRule("test-bucket", "non-existent-rule")
	if err == nil {
		t.Error("Should return error for non-existent rule")
	}
}

func TestDeleteBucketRules(t *testing.T) {
	lc := New()

	lc.AddRule("test-bucket", &Rule{ID: "rule-1"})
	lc.AddRule("test-bucket", &Rule{ID: "rule-2"})

	err := lc.DeleteBucketRules("test-bucket")
	if err != nil {
		t.Fatalf("DeleteBucketRules failed: %v", err)
	}

	rules := lc.ListRules("test-bucket")
	if len(rules) != 0 {
		t.Errorf("Rules count = %d, want 0", len(rules))
	}
}

func TestGetApplicableRules(t *testing.T) {
	lc := New()

	// Add enabled rule
	rule1 := &Rule{
		ID:       "rule-1",
		Status:   "Enabled",
		Priority: 1,
		Filter:   &Filter{Prefix: "photos/"},
	}
	lc.AddRule("test-bucket", rule1)

	// Add disabled rule (should not be applicable)
	rule2 := &Rule{
		ID:       "rule-2",
		Status:   "Disabled",
		Priority: 2,
	}
	lc.AddRule("test-bucket", rule2)

	// Add rule with matching prefix
	rule3 := &Rule{
		ID:       "rule-3",
		Status:   "Enabled",
		Priority: 3,
		Filter:   &Filter{Prefix: "photos/2023/"},
	}
	lc.AddRule("test-bucket", rule3)

	applicable := lc.GetApplicableRules("test-bucket", "photos/2023/vacation.jpg", 1000, nil, time.Now())

	if len(applicable) != 2 {
		t.Errorf("Applicable rules = %d, want 2", len(applicable))
	}

	// Should be sorted by priority
	if applicable[0].Priority > applicable[1].Priority {
		t.Error("Rules should be sorted by priority")
	}
}

func TestRuleMatchesObject_NoFilter(t *testing.T) {
	rule := &Rule{
		Filter: nil,
	}

	matches := ruleMatchesObject(rule, "any/key", 1000, nil)
	if !matches {
		t.Error("Rule with no filter should match all objects")
	}
}

func TestRuleMatchesObject_Prefix(t *testing.T) {
	rule := &Rule{
		Filter: &Filter{Prefix: "photos/"},
	}

	tests := []struct {
		key      string
		expected bool
	}{
		{"photos/vacation.jpg", true},
		{"photos/2023/pic.jpg", true},
		{"documents/report.pdf", false},
		{"other/file.txt", false},
	}

	for _, test := range tests {
		result := ruleMatchesObject(rule, test.key, 1000, nil)
		if result != test.expected {
			t.Errorf("Key %s: matches = %v, want %v", test.key, result, test.expected)
		}
	}
}

func TestRuleMatchesObject_Size(t *testing.T) {
	rule := &Rule{
		Filter: &Filter{
			ObjectSizeGreater: intPtr(1000),
			ObjectSizeLesser:  intPtr(10000),
		},
	}

	tests := []struct {
		size     int64
		expected bool
	}{
		{500, false},    // Too small
		{1000, false},   // Equal to min (not greater)
		{1001, true},    // Just above min
		{5000, true},    // In range
		{9999, true},    // Just below max
		{10000, false},  // Equal to max (not lesser)
		{20000, false},  // Too large
	}

	for _, test := range tests {
		result := ruleMatchesObject(rule, "key", test.size, nil)
		if result != test.expected {
			t.Errorf("Size %d: matches = %v, want %v", test.size, result, test.expected)
		}
	}
}

func TestRuleMatchesObject_Tags(t *testing.T) {
	rule := &Rule{
		Filter: &Filter{
			Tag: &Tag{Key: "environment", Value: "production"},
		},
	}

	tests := []struct {
		tags     map[string]string
		expected bool
	}{
		{map[string]string{"environment": "production"}, true},
		{map[string]string{"environment": "staging"}, false},
		{map[string]string{"other": "value"}, false},
		{nil, false},
	}

	for _, test := range tests {
		result := ruleMatchesObject(rule, "key", 1000, test.tags)
		if result != test.expected {
			t.Errorf("Tags %v: matches = %v, want %v", test.tags, result, test.expected)
		}
	}
}

func TestGenerateRuleID(t *testing.T) {
	id1 := generateRuleID()
	id2 := generateRuleID()

	if id1 == "" {
		t.Error("Rule ID should not be empty")
	}

	if id1 == id2 {
		t.Error("Rule IDs should be unique")
	}

	// Should start with "rule-"
	if len(id1) < 5 || id1[:5] != "rule-" {
		t.Errorf("Rule ID should start with 'rule-', got %s", id1)
	}
}

func TestToJSON(t *testing.T) {
	lc := New()

	lc.AddRule("test-bucket", &Rule{
		ID:   "rule-1",
		Name: "Test Rule",
	})

	data, err := lc.ToJSON("test-bucket")
	if err != nil {
		t.Fatalf("ToJSON failed: %v", err)
	}

	if len(data) == 0 {
		t.Error("JSON data should not be empty")
	}
}

func TestFromJSON(t *testing.T) {
	lc := New()

	jsonData := `[{"id":"rule-1","name":"Test Rule","status":"Enabled"}]`

	err := lc.FromJSON("test-bucket", []byte(jsonData))
	if err != nil {
		t.Fatalf("FromJSON failed: %v", err)
	}

	rules := lc.ListRules("test-bucket")
	if len(rules) != 1 {
		t.Errorf("Rules count = %d, want 1", len(rules))
	}

	if rules[0].Name != "Test Rule" {
		t.Errorf("Rule name = %s, want Test Rule", rules[0].Name)
	}
}

func TestBucketHasRules(t *testing.T) {
	lc := New()

	if lc.BucketHasRules("empty-bucket") {
		t.Error("Empty bucket should not have rules")
	}

	lc.AddRule("test-bucket", &Rule{ID: "rule-1"})

	if !lc.BucketHasRules("test-bucket") {
		t.Error("Bucket with rule should have rules")
	}
}

func TestGetRuleCount(t *testing.T) {
	lc := New()

	if lc.GetRuleCount("empty-bucket") != 0 {
		t.Error("Empty bucket should have 0 rules")
	}

	lc.AddRule("test-bucket", &Rule{ID: "rule-1"})
	lc.AddRule("test-bucket", &Rule{ID: "rule-2"})

	if lc.GetRuleCount("test-bucket") != 2 {
		t.Errorf("Rule count = %d, want 2", lc.GetRuleCount("test-bucket"))
	}
}

func TestGetExpiredObjects(t *testing.T) {
	lc := New()

	days := 30
	rule := &Rule{
		ID:     "rule-1",
		Status: "Enabled",
		Actions: []*Action{
			{
				Name: "Expiration",
				Days: &days,
			},
		},
	}
	lc.AddRule("test-bucket", rule)

	expired := lc.GetExpiredObjects("test-bucket", time.Now())

	if len(expired) != 1 {
		t.Errorf("Expired objects = %d, want 1", len(expired))
	}
}

func TestGetTransitions(t *testing.T) {
	lc := New()

	days := 90
	storageClass := "GLACIER"
	rule := &Rule{
		ID:     "rule-1",
		Status: "Enabled",
		Actions: []*Action{
			{
				Name:         "Transition",
				Days:         &days,
				StorageClass: &storageClass,
			},
		},
	}
	lc.AddRule("test-bucket", rule)

	transitions := lc.GetTransitions("test-bucket", time.Now())

	if len(transitions) != 1 {
		t.Errorf("Transitions = %d, want 1", len(transitions))
	}

	if transitions[0].StorageClass != "GLACIER" {
		t.Errorf("Storage class = %s, want GLACIER", transitions[0].StorageClass)
	}
}

func intPtr(i int64) *int64 {
	return &i
}
