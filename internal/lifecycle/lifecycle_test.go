package lifecycle

import (
	"fmt"
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
		ID:      "rule-123",
		Name:    "Original Name",
		Enabled: true,
	}
	lc.AddRule("test-bucket", rule)

	updates := &Rule{
		Name:    "Updated Name",
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
		{500, false},   // Too small
		{1000, false},  // Equal to min (not greater)
		{1001, true},   // Just above min
		{5000, true},   // In range
		{9999, true},   // Just below max
		{10000, false}, // Equal to max (not lesser)
		{20000, false}, // Too large
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
		Filter: &Filter{Prefix: "archive/"},
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

	if expired[0].RuleID != "rule-1" {
		t.Errorf("RuleID = %s, want rule-1", expired[0].RuleID)
	}
}

func TestGetExpiredObjectsNoBucket(t *testing.T) {
	lc := New()

	expired := lc.GetExpiredObjects("nonexistent-bucket", time.Now())

	if len(expired) != 0 {
		t.Errorf("Expected 0 expired objects for nonexistent bucket, got %d", len(expired))
	}
}

func TestGetExpiredObjectsDisabledRule(t *testing.T) {
	lc := New()

	days := 30
	rule := &Rule{
		ID:     "rule-1",
		Status: "Disabled",
		Filter: &Filter{Prefix: "archive/"},
		Actions: []*Action{
			{
				Name: "Expiration",
				Days: &days,
			},
		},
	}
	lc.AddRule("test-bucket", rule)

	expired := lc.GetExpiredObjects("test-bucket", time.Now())

	if len(expired) != 0 {
		t.Errorf("Expected 0 expired objects for disabled rule, got %d", len(expired))
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

func TestGetTransitionsNoBucket(t *testing.T) {
	lc := New()

	transitions := lc.GetTransitions("nonexistent-bucket", time.Now())

	if len(transitions) != 0 {
		t.Errorf("Expected 0 transitions for nonexistent bucket, got %d", len(transitions))
	}
}

func TestGetTransitionsDisabledRule(t *testing.T) {
	lc := New()

	days := 90
	storageClass := "GLACIER"
	rule := &Rule{
		ID:     "rule-1",
		Status: "Disabled",
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

	if len(transitions) != 0 {
		t.Errorf("Expected 0 transitions for disabled rule, got %d", len(transitions))
	}
}

func TestRuleMatchesObject_AndFilter(t *testing.T) {
	rule := &Rule{
		Filter: &Filter{
			And: &AndFilter{
				Prefix: "photos/",
				Tags: []*Tag{
					{Key: "type", Value: "image"},
				},
			},
		},
	}

	tests := []struct {
		key      string
		tags     map[string]string
		expected bool
	}{
		{"photos/vacation.jpg", map[string]string{"type": "image"}, true},
		{"photos/vacation.jpg", map[string]string{"type": "video"}, false},
		{"photos/vacation.jpg", map[string]string{"other": "value"}, false},
		{"docs/report.pdf", map[string]string{"type": "image"}, false},
	}

	for _, test := range tests {
		result := ruleMatchesObject(rule, test.key, 1000, test.tags)
		if result != test.expected {
			t.Errorf("Key %s, tags %v: matches = %v, want %v", test.key, test.tags, result, test.expected)
		}
	}
}

func TestRuleMatchesObject_ShortPrefix(t *testing.T) {
	rule := &Rule{
		Filter: &Filter{Prefix: "photos/2023/"},
	}

	result := ruleMatchesObject(rule, "short", 1000, nil)
	if result {
		t.Error("Short key should not match long prefix")
	}
}

func TestRuleMatchesObject_AndFilterShortPrefix(t *testing.T) {
	rule := &Rule{
		Filter: &Filter{
			And: &AndFilter{
				Prefix: "photos/2023/",
			},
		},
	}

	result := ruleMatchesObject(rule, "short", 1000, nil)
	if result {
		t.Error("Short key should not match long AND prefix")
	}
}

func TestUpdateRuleWithFilter(t *testing.T) {
	lc := New()

	rule := &Rule{
		ID:      "rule-123",
		Name:    "Original",
		Enabled: true,
	}
	lc.AddRule("test-bucket", rule)

	updates := &Rule{
		Name: "Updated",
		Filter: &Filter{
			Prefix: "photos/",
		},
		Actions: []*Action{
			{Name: "Expiration"},
		},
		Priority: 10,
	}

	updated, err := lc.UpdateRule("test-bucket", "rule-123", updates)
	if err != nil {
		t.Fatalf("UpdateRule failed: %v", err)
	}

	if updated.Filter == nil || updated.Filter.Prefix != "photos/" {
		t.Error("Filter should be updated")
	}
	if len(updated.Actions) != 1 {
		t.Error("Actions should be updated")
	}
	if updated.Priority != 10 {
		t.Errorf("Priority = %d, want 10", updated.Priority)
	}
}

func TestLifecycleConcurrent(t *testing.T) {
	lc := New()
	done := make(chan bool)

	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				rule := &Rule{
					ID:   fmt.Sprintf("rule-%d-%d", id, j),
					Name: fmt.Sprintf("Rule %d-%d", id, j),
				}
				lc.AddRule("test-bucket", rule)
				lc.GetRule("test-bucket", rule.ID)
				lc.ListRules("test-bucket")
			}
			done <- true
		}(i)
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestGetApplicableRulesNoBucket(t *testing.T) {
	lc := New()

	rules := lc.GetApplicableRules("nonexistent-bucket", "key", 1000, nil, time.Now())

	if rules != nil {
		t.Errorf("Expected nil rules for nonexistent bucket, got %d", len(rules))
	}
}

func intPtr(i int64) *int64 {
	return &i
}

func TestStorageClasses(t *testing.T) {
	for key, value := range StorageClasses {
		if key != value {
			t.Errorf("StorageClasses[%s] = %s, want %s", key, value, key)
		}
	}

	required := []string{"STANDARD", "STANDARD_IA", "GLACIER", "DEEP_ARCHIVE"}
	for _, sc := range required {
		if _, ok := StorageClasses[sc]; !ok {
			t.Errorf("StorageClasses missing required class: %s", sc)
		}
	}
}

func TestRuleFields(t *testing.T) {
	lc := New()

	rule := &Rule{
		ID:       "rule-1",
		Name:     "Test Rule",
		Enabled:  true,
		Priority: 5,
		Filter:   &Filter{Prefix: "test/"},
		Actions:  []*Action{{Name: "Expiration"}},
	}

	lc.AddRule("bucket", rule)

	found, _ := lc.GetRule("bucket", "rule-1")
	if found.Name != "Test Rule" {
		t.Errorf("Name = %s, want Test Rule", found.Name)
	}
	if found.Priority != 5 {
		t.Errorf("Priority = %d, want 5", found.Priority)
	}
	if found.CreatedAt.IsZero() {
		t.Error("CreatedAt should be set")
	}
	if found.ModifiedAt.IsZero() {
		t.Error("ModifiedAt should be set")
	}
}

func TestActionFields(t *testing.T) {
	days := 30
	date := "2024-01-01"
	storageClass := "GLACIER"
	deleteMarkerReplication := true

	action := &Action{
		Name:                    "Transition",
		Days:                    &days,
		Date:                    &date,
		StorageClass:            &storageClass,
		DeleteMarkerReplication: &deleteMarkerReplication,
	}

	if action.Name != "Transition" {
		t.Errorf("Name = %s, want Transition", action.Name)
	}
	if *action.Days != 30 {
		t.Errorf("Days = %d, want 30", *action.Days)
	}
	if *action.StorageClass != "GLACIER" {
		t.Errorf("StorageClass = %s, want GLACIER", *action.StorageClass)
	}
}

func TestFilterFields(t *testing.T) {
	filter := &Filter{
		Prefix:            "photos/",
		Tag:               &Tag{Key: "type", Value: "image"},
		ObjectSizeGreater: intPtr(100),
		ObjectSizeLesser:  intPtr(10000),
	}

	if filter.Prefix != "photos/" {
		t.Errorf("Prefix = %s, want photos/", filter.Prefix)
	}
	if filter.Tag.Key != "type" {
		t.Errorf("Tag.Key = %s, want type", filter.Tag.Key)
	}
}

func TestExpirationStruct(t *testing.T) {
	exp := Expiration{
		Days:                      30,
		Date:                      "2024-01-01",
		ExpiredObjectDeleteMarker: true,
	}

	if exp.Days != 30 {
		t.Errorf("Days = %d, want 30", exp.Days)
	}
	if !exp.ExpiredObjectDeleteMarker {
		t.Error("ExpiredObjectDeleteMarker should be true")
	}
}

func TestTransitionStruct(t *testing.T) {
	trans := Transition{
		Days:         90,
		StorageClass: "GLACIER",
	}

	if trans.Days != 90 {
		t.Errorf("Days = %d, want 90", trans.Days)
	}
	if trans.StorageClass != "GLACIER" {
		t.Errorf("StorageClass = %s, want GLACIER", trans.StorageClass)
	}
}

func TestAbortIncompleteMultipartUploadStruct(t *testing.T) {
	abort := AbortIncompleteMultipartUpload{
		DaysAfterInitiation: 7,
	}

	if abort.DaysAfterInitiation != 7 {
		t.Errorf("DaysAfterInitiation = %d, want 7", abort.DaysAfterInitiation)
	}
}

func TestObjectExpiryStruct(t *testing.T) {
	exp := ObjectExpiry{
		RuleID:     "rule-1",
		ObjectKey:  "test/key",
		ExpiryDate: time.Now(),
		ActionType: "Expiration",
	}

	if exp.RuleID != "rule-1" {
		t.Errorf("RuleID = %s, want rule-1", exp.RuleID)
	}
}

func TestObjectTransitionStruct(t *testing.T) {
	trans := ObjectTransition{
		RuleID:         "rule-1",
		ObjectKey:      "test/key",
		TransitionDate: time.Now(),
		StorageClass:   "GLACIER",
		ActionType:     "Transition",
	}

	if trans.StorageClass != "GLACIER" {
		t.Errorf("StorageClass = %s, want GLACIER", trans.StorageClass)
	}
}

func TestToJSONEmptyBucket(t *testing.T) {
	lc := New()

	data, err := lc.ToJSON("empty-bucket")
	if err != nil {
		t.Fatalf("ToJSON failed: %v", err)
	}

	if string(data) != "null" {
		t.Errorf("Empty bucket JSON = %s, want null", string(data))
	}
}

func TestFromJSONInvalid(t *testing.T) {
	lc := New()

	err := lc.FromJSON("bucket", []byte("invalid json"))
	if err == nil {
		t.Error("FromJSON should fail with invalid JSON")
	}
}

func TestGetExpiredObjectsWithDate(t *testing.T) {
	lc := New()

	date := "2024-01-01"
	rule := &Rule{
		ID:     "rule-1",
		Status: "Enabled",
		Actions: []*Action{
			{
				Name: "Expiration",
				Date: &date,
			},
		},
	}
	lc.AddRule("test-bucket", rule)

	expired := lc.GetExpiredObjects("test-bucket", time.Now())
	if len(expired) != 0 {
		t.Errorf("Expected 0 expired objects with date action, got %d", len(expired))
	}
}

func TestGetTransitionsWithDate(t *testing.T) {
	lc := New()

	date := "2024-01-01"
	storageClass := "GLACIER"
	rule := &Rule{
		ID:     "rule-1",
		Status: "Enabled",
		Actions: []*Action{
			{
				Name:         "Transition",
				Date:         &date,
				StorageClass: &storageClass,
			},
		},
	}
	lc.AddRule("test-bucket", rule)

	transitions := lc.GetTransitions("test-bucket", time.Now())
	if len(transitions) != 0 {
		t.Errorf("Expected 0 transitions with date action, got %d", len(transitions))
	}
}

func TestGetTransitionsStorageClassAction(t *testing.T) {
	lc := New()

	days := 90
	storageClass := "GLACIER"
	rule := &Rule{
		ID:     "rule-1",
		Status: "Enabled",
		Actions: []*Action{
			{
				Name:         "StorageClass",
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
}

func TestGetExpiredObjectsDeleteAction(t *testing.T) {
	lc := New()

	days := 30
	rule := &Rule{
		ID:     "rule-1",
		Status: "Enabled",
		Filter: &Filter{Prefix: "archive/"},
		Actions: []*Action{
			{
				Name: "Delete",
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

func TestRuleMatchesObjectAllFilters(t *testing.T) {
	rule := &Rule{
		Filter: &Filter{
			Prefix:            "photos/",
			Tag:               &Tag{Key: "type", Value: "image"},
			ObjectSizeGreater: intPtr(100),
			ObjectSizeLesser:  intPtr(10000),
		},
	}

	tests := []struct {
		key      string
		size     int64
		tags     map[string]string
		expected bool
	}{
		{"photos/vacation.jpg", 1000, map[string]string{"type": "image"}, true},
		{"photos/vacation.jpg", 50, map[string]string{"type": "image"}, false},
		{"photos/vacation.jpg", 10001, map[string]string{"type": "image"}, false},
		{"photos/vacation.jpg", 1000, map[string]string{"type": "video"}, false},
		{"docs/file.pdf", 1000, map[string]string{"type": "image"}, false},
	}

	for _, test := range tests {
		result := ruleMatchesObject(rule, test.key, test.size, test.tags)
		if result != test.expected {
			t.Errorf("Key %s, size %d, tags %v: matches = %v, want %v", test.key, test.size, test.tags, result, test.expected)
		}
	}
}

func TestAndFilterWithNoTags(t *testing.T) {
	rule := &Rule{
		Filter: &Filter{
			And: &AndFilter{
				Prefix: "photos/",
			},
		},
	}

	result := ruleMatchesObject(rule, "photos/vacation.jpg", 1000, nil)
	if !result {
		t.Error("AND filter with only prefix should match")
	}
}

func TestUpdateRuleWithEnabledTrue(t *testing.T) {
	lc := New()

	rule := &Rule{
		ID:      "rule-123",
		Name:    "Original",
		Enabled: false,
		Status:  "Disabled",
	}
	lc.AddRule("test-bucket", rule)

	updates := &Rule{
		Name:    "Updated",
		Enabled: true,
	}

	updated, err := lc.UpdateRule("test-bucket", "rule-123", updates)
	if err != nil {
		t.Fatalf("UpdateRule failed: %v", err)
	}

	if updated.Status != "Enabled" {
		t.Errorf("Status = %s, want Enabled", updated.Status)
	}
	if !updated.Enabled {
		t.Error("Enabled should be true")
	}
}

func TestGetTransitionsWithFilter(t *testing.T) {
	lc := New()

	days := 90
	storageClass := "GLACIER"
	rule := &Rule{
		ID:     "rule-1",
		Status: "Enabled",
		Filter: &Filter{Prefix: "archive/"},
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

	if transitions[0].ObjectKey != "archive/" {
		t.Errorf("ObjectKey = %s, want archive/", transitions[0].ObjectKey)
	}
}

func TestGetApplicableRulesSorting(t *testing.T) {
	lc := New()

	rule1 := &Rule{
		ID:       "rule-1",
		Status:   "Enabled",
		Priority: 3,
		Filter:   &Filter{Prefix: "test/"},
	}
	rule2 := &Rule{
		ID:       "rule-2",
		Status:   "Enabled",
		Priority: 1,
		Filter:   &Filter{Prefix: "test/"},
	}
	rule3 := &Rule{
		ID:       "rule-3",
		Status:   "Enabled",
		Priority: 2,
		Filter:   &Filter{Prefix: "test/"},
	}
	lc.AddRule("test-bucket", rule1)
	lc.AddRule("test-bucket", rule2)
	lc.AddRule("test-bucket", rule3)

	applicable := lc.GetApplicableRules("test-bucket", "test/file.txt", 1000, nil, time.Now())

	if len(applicable) != 3 {
		t.Errorf("Applicable rules = %d, want 3", len(applicable))
	}

	if applicable[0].Priority != 1 || applicable[1].Priority != 2 || applicable[2].Priority != 3 {
		t.Errorf("Rules not sorted by priority: got %d, %d, %d", applicable[0].Priority, applicable[1].Priority, applicable[2].Priority)
	}
}

func TestBucketHasRulesEmpty(t *testing.T) {
	lc := New()

	lc.AddRule("test-bucket", &Rule{ID: "rule-1"})
	lc.DeleteBucketRules("test-bucket")

	if lc.BucketHasRules("test-bucket") {
		t.Error("Bucket should not have rules after DeleteBucketRules")
	}
}

func TestGetExpiredObjectsWithoutDays(t *testing.T) {
	lc := New()

	rule := &Rule{
		ID:     "rule-1",
		Status: "Enabled",
		Filter: &Filter{Prefix: "archive/"},
		Actions: []*Action{
			{
				Name: "Expiration",
			},
		},
	}
	lc.AddRule("test-bucket", rule)

	expired := lc.GetExpiredObjects("test-bucket", time.Now())

	if len(expired) != 0 {
		t.Errorf("Expired objects = %d, want 0 (no days specified)", len(expired))
	}
}

func TestGetTransitionsWithoutDays(t *testing.T) {
	lc := New()

	storageClass := "GLACIER"
	rule := &Rule{
		ID:     "rule-1",
		Status: "Enabled",
		Actions: []*Action{
			{
				Name:         "Transition",
				StorageClass: &storageClass,
			},
		},
	}
	lc.AddRule("test-bucket", rule)

	transitions := lc.GetTransitions("test-bucket", time.Now())

	if len(transitions) != 0 {
		t.Errorf("Transitions = %d, want 0 (no days specified)", len(transitions))
	}
}

func TestGetTransitionsWithoutStorageClass(t *testing.T) {
	lc := New()

	days := 90
	rule := &Rule{
		ID:     "rule-1",
		Status: "Enabled",
		Actions: []*Action{
			{
				Name: "Transition",
				Days: &days,
			},
		},
	}
	lc.AddRule("test-bucket", rule)

	transitions := lc.GetTransitions("test-bucket", time.Now())

	if len(transitions) != 0 {
		t.Errorf("Transitions = %d, want 0 (no storage class specified)", len(transitions))
	}
}

func TestGenerateRuleID_Fallback(t *testing.T) {
	originalRandRead := randRead
	defer func() { randRead = originalRandRead }()

	randRead = func(b []byte) (n int, err error) {
		return 0, fmt.Errorf("simulated rand.Read failure")
	}

	id := generateRuleID()

	if id == "" {
		t.Error("Rule ID should not be empty on fallback")
	}
	if len(id) < 5 || id[:5] != "rule-" {
		t.Errorf("Rule ID should start with 'rule-', got %s", id)
	}

	if _, err := fmt.Sscanf(id, "rule-%d", new(int64)); err != nil {
		t.Errorf("Fallback ID should be timestamp-based (rule-<unixnano>), got %s", id)
	}
}
