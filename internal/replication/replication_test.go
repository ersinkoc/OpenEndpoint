package replication

import (
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	rep := New()
	if rep == nil {
		t.Fatal("Replication should not be nil")
	}

	if rep.rules == nil {
		t.Error("Rules map should be initialized")
	}

	if rep.stats == nil {
		t.Error("Stats map should be initialized")
	}

	if rep.status == nil {
		t.Error("Status map should be initialized")
	}
}

func TestAddRule(t *testing.T) {
	rep := New()

	rule := &Rule{
		Name:     "test-rule",
		Priority: 1,
		Status:   "Enabled",
	}

	err := rep.AddRule("test-bucket", rule)
	if err != nil {
		t.Fatalf("AddRule failed: %v", err)
	}

	if rule.ID == "" {
		t.Error("Rule ID should be generated")
	}

	if rule.CreatedAt.IsZero() {
		t.Error("CreatedAt should be set")
	}
}

func TestAddRule_Duplicate(t *testing.T) {
	rep := New()

	rule1 := &Rule{ID: "rule-123", Name: "rule1"}
	rule2 := &Rule{ID: "rule-123", Name: "rule2"}

	rep.AddRule("test-bucket", rule1)
	err := rep.AddRule("test-bucket", rule2)

	if err == nil {
		t.Error("Should return error for duplicate rule ID")
	}
}

func TestGetRule(t *testing.T) {
	rep := New()

	rule := &Rule{ID: "rule-123", Name: "test-rule"}
	rep.AddRule("test-bucket", rule)

	found, ok := rep.GetRule("test-bucket", "rule-123")
	if !ok {
		t.Fatal("Rule should be found")
	}

	if found.Name != "test-rule" {
		t.Errorf("Name = %s, want test-rule", found.Name)
	}
}

func TestGetRule_NotFound(t *testing.T) {
	rep := New()

	_, ok := rep.GetRule("non-existent", "rule-123")
	if ok {
		t.Error("Should not find rule in non-existent bucket")
	}

	rep.AddRule("test-bucket", &Rule{ID: "rule-123"})
	_, ok = rep.GetRule("test-bucket", "non-existent")
	if ok {
		t.Error("Should not find non-existent rule")
	}
}

func TestListRules(t *testing.T) {
	rep := New()

	// Empty bucket
	rules := rep.ListRules("empty-bucket")
	if len(rules) != 0 {
		t.Errorf("Empty bucket should have 0 rules")
	}

	// Add rules
	rep.AddRule("test-bucket", &Rule{ID: "rule-1"})
	rep.AddRule("test-bucket", &Rule{ID: "rule-2"})

	rules = rep.ListRules("test-bucket")
	if len(rules) != 2 {
		t.Errorf("Rules count = %d, want 2", len(rules))
	}
}

func TestListAllRules(t *testing.T) {
	rep := New()

	rep.AddRule("bucket1", &Rule{ID: "rule-1"})
	rep.AddRule("bucket2", &Rule{ID: "rule-2"})
	rep.AddRule("bucket3", &Rule{ID: "rule-3"})

	allRules := rep.ListAllRules()
	if len(allRules) != 3 {
		t.Errorf("Buckets with rules = %d, want 3", len(allRules))
	}
}

func TestUpdateRule(t *testing.T) {
	rep := New()

	rule := &Rule{
		ID:       "rule-123",
		Name:     "Original",
		Priority: 1,
		Status:   "Enabled",
	}
	rep.AddRule("test-bucket", rule)

	updates := &Rule{
		Name:     "Updated",
		Priority: 10,
		Status:   "Disabled",
	}

	updated, err := rep.UpdateRule("test-bucket", "rule-123", updates)
	if err != nil {
		t.Fatalf("UpdateRule failed: %v", err)
	}

	if updated.Name != "Updated" {
		t.Errorf("Name = %s, want Updated", updated.Name)
	}

	if updated.Priority != 10 {
		t.Errorf("Priority = %d, want 10", updated.Priority)
	}

	if updated.Status != "Disabled" {
		t.Errorf("Status = %s, want Disabled", updated.Status)
	}
}

func TestUpdateRule_NotFound(t *testing.T) {
	rep := New()

	_, err := rep.UpdateRule("non-existent", "rule-123", &Rule{})
	if err == nil {
		t.Error("Should return error for non-existent bucket")
	}

	rep.AddRule("test-bucket", &Rule{ID: "rule-123"})
	_, err = rep.UpdateRule("test-bucket", "non-existent", &Rule{})
	if err == nil {
		t.Error("Should return error for non-existent rule")
	}
}

func TestDeleteRule(t *testing.T) {
	rep := New()

	rep.AddRule("test-bucket", &Rule{ID: "rule-123"})

	err := rep.DeleteRule("test-bucket", "rule-123")
	if err != nil {
		t.Fatalf("DeleteRule failed: %v", err)
	}

	_, ok := rep.GetRule("test-bucket", "rule-123")
	if ok {
		t.Error("Rule should be deleted")
	}

	// Stats should be cleaned up when no rules left
	stats, _ := rep.GetStats("test-bucket")
	if stats != nil {
		t.Error("Stats should be nil when no rules")
	}
}

func TestDeleteRule_NotFound(t *testing.T) {
	rep := New()

	err := rep.DeleteRule("non-existent", "rule-123")
	if err == nil {
		t.Error("Should return error for non-existent bucket")
	}

	rep.AddRule("test-bucket", &Rule{ID: "rule-123"})
	err = rep.DeleteRule("test-bucket", "non-existent")
	if err == nil {
		t.Error("Should return error for non-existent rule")
	}
}

func TestDeleteBucketRules(t *testing.T) {
	rep := New()

	rep.AddRule("test-bucket", &Rule{ID: "rule-1"})
	rep.AddRule("test-bucket", &Rule{ID: "rule-2"})

	err := rep.DeleteBucketRules("test-bucket")
	if err != nil {
		t.Fatalf("DeleteBucketRules failed: %v", err)
	}

	rules := rep.ListRules("test-bucket")
	if len(rules) != 0 {
		t.Errorf("Rules count = %d, want 0", len(rules))
	}
}

func TestGetStats(t *testing.T) {
	rep := New()

	// Non-existent bucket
	_, ok := rep.GetStats("non-existent")
	if ok {
		t.Error("Should not find stats for non-existent bucket")
	}

	// Add rule to create stats
	rep.AddRule("test-bucket", &Rule{ID: "rule-1"})

	stats, ok := rep.GetStats("test-bucket")
	if !ok {
		t.Fatal("Should find stats")
	}

	if stats == nil {
		t.Fatal("Stats should not be nil")
	}
}

func TestUpdateStats(t *testing.T) {
	rep := New()

	rep.AddRule("test-bucket", &Rule{ID: "rule-1"})

	update := &Stats{
		ReplicatedObjects: 10,
		ReplicatedBytes:   1024,
		PendingReplication: 5,
	}

	rep.UpdateStats("test-bucket", update)

	stats, _ := rep.GetStats("test-bucket")
	if stats.ReplicatedObjects != 10 {
		t.Errorf("ReplicatedObjects = %d, want 10", stats.ReplicatedObjects)
	}
}

func TestGetStatus(t *testing.T) {
	rep := New()

	// Non-existent bucket
	status := rep.GetStatus("non-existent")
	if status != "" {
		t.Errorf("Status should be empty, got %s", status)
	}

	rep.AddRule("test-bucket", &Rule{ID: "rule-1"})

	status = rep.GetStatus("test-bucket")
	if status != "Enabled" {
		t.Errorf("Status = %s, want Enabled", status)
	}
}

func TestSetStatus(t *testing.T) {
	rep := New()

	rep.AddRule("test-bucket", &Rule{ID: "rule-1"})
	rep.SetStatus("test-bucket", "Disabled")

	status := rep.GetStatus("test-bucket")
	if status != "Disabled" {
		t.Errorf("Status = %s, want Disabled", status)
	}
}

func TestIsEnabled(t *testing.T) {
	rep := New()

	if rep.IsEnabled("non-existent") {
		t.Error("Non-existent bucket should not be enabled")
	}

	rep.AddRule("test-bucket", &Rule{ID: "rule-1", Status: "Enabled"})
	if !rep.IsEnabled("test-bucket") {
		t.Error("Bucket with enabled rule should be enabled")
	}

	rep.SetStatus("test-bucket", "Disabled")
	if rep.IsEnabled("test-bucket") {
		t.Error("Disabled bucket should not be enabled")
	}
}

func TestGetReplicationDestination(t *testing.T) {
	rep := New()

	// Non-existent
	_, ok := rep.GetReplicationDestination("non-existent")
	if ok {
		t.Error("Should not find destination for non-existent bucket")
	}

	// Bucket with disabled rule
	rule := &Rule{
		ID:     "rule-1",
		Status: "Disabled",
		Destination: &Destination{
			Bucket: "dest-bucket",
		},
	}
	rep.AddRule("test-bucket", rule)

	_, ok = rep.GetReplicationDestination("test-bucket")
	if ok {
		t.Error("Should not find destination for disabled rule")
	}

	// Bucket with enabled rule
	rule2 := &Rule{
		ID:     "rule-2",
		Status: "Enabled",
		Destination: &Destination{
			Bucket: "dest-bucket",
		},
	}
	rep.AddRule("bucket2", rule2)

	dest, ok := rep.GetReplicationDestination("bucket2")
	if !ok {
		t.Fatal("Should find destination")
	}

	if dest.Bucket != "dest-bucket" {
		t.Errorf("Destination bucket = %s, want dest-bucket", dest.Bucket)
	}
}

func TestGetDestinationStatus(t *testing.T) {
	rep := New()

	// Non-existent bucket
	statuses := rep.GetDestinationStatus("non-existent")
	if len(statuses) != 0 {
		t.Errorf("Should have 0 statuses, got %d", len(statuses))
	}

	// Bucket with enabled rule
	rule := &Rule{
		ID:     "rule-1",
		Status: "Enabled",
		Destination: &Destination{
			Bucket: "dest-bucket",
		},
	}
	rep.AddRule("test-bucket", rule)

	statuses = rep.GetDestinationStatus("test-bucket")
	if len(statuses) != 1 {
		t.Errorf("Should have 1 status, got %d", len(statuses))
	}
}

func TestGetEnabledRules(t *testing.T) {
	rep := New()

	// Add mixed rules
	rep.AddRule("test-bucket", &Rule{ID: "rule-1", Status: "Enabled"})
	rep.AddRule("test-bucket", &Rule{ID: "rule-2", Status: "Disabled"})
	rep.AddRule("test-bucket", &Rule{ID: "rule-3", Status: "Enabled"})

	enabled := rep.GetEnabledRules("test-bucket")
	if len(enabled) != 2 {
		t.Errorf("Enabled rules = %d, want 2", len(enabled))
	}

	for _, rule := range enabled {
		if rule.Status != "Enabled" {
			t.Error("All returned rules should be enabled")
		}
	}
}

func TestBucketHasReplication(t *testing.T) {
	rep := New()

	if rep.BucketHasReplication("non-existent") {
		t.Error("Non-existent bucket should not have replication")
	}

	rep.AddRule("test-bucket", &Rule{ID: "rule-1", Status: "Disabled"})
	if rep.BucketHasReplication("test-bucket") {
		t.Error("Bucket with disabled rule should not have replication")
	}

	rep.AddRule("bucket2", &Rule{ID: "rule-2", Status: "Enabled"})
	if !rep.BucketHasReplication("bucket2") {
		t.Error("Bucket with enabled rule should have replication")
	}
}

func TestGetRuleCount(t *testing.T) {
	rep := New()

	if rep.GetRuleCount("non-existent") != 0 {
		t.Error("Non-existent bucket should have 0 rules")
	}

	rep.AddRule("test-bucket", &Rule{ID: "rule-1"})
	rep.AddRule("test-bucket", &Rule{ID: "rule-2"})

	if rep.GetRuleCount("test-bucket") != 2 {
		t.Errorf("Rule count = %d, want 2", rep.GetRuleCount("test-bucket"))
	}
}

func TestGetTotalReplicatedObjects(t *testing.T) {
	rep := New()

	if rep.GetTotalReplicatedObjects() != 0 {
		t.Error("Should have 0 replicated objects")
	}

	rep.AddRule("bucket1", &Rule{ID: "rule-1"})
	rep.AddRule("bucket2", &Rule{ID: "rule-2"})

	rep.UpdateStats("bucket1", &Stats{ReplicatedObjects: 10})
	rep.UpdateStats("bucket2", &Stats{ReplicatedObjects: 20})

	total := rep.GetTotalReplicatedObjects()
	if total != 30 {
		t.Errorf("Total replicated = %d, want 30", total)
	}
}

func TestGetTotalReplicatedBytes(t *testing.T) {
	rep := New()

	if rep.GetTotalReplicatedBytes() != 0 {
		t.Error("Should have 0 replicated bytes")
	}

	rep.AddRule("bucket1", &Rule{ID: "rule-1"})
	rep.UpdateStats("bucket1", &Stats{ReplicatedBytes: 1024})

	total := rep.GetTotalReplicatedBytes()
	if total != 1024 {
		t.Errorf("Total bytes = %d, want 1024", total)
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
}

func TestToJSON(t *testing.T) {
	rep := New()

	rep.AddRule("test-bucket", &Rule{
		ID:   "rule-1",
		Name: "Test Rule",
	})

	data, err := rep.ToJSON("test-bucket")
	if err != nil {
		t.Fatalf("ToJSON failed: %v", err)
	}

	if len(data) == 0 {
		t.Error("JSON data should not be empty")
	}
}

func TestFromJSON(t *testing.T) {
	rep := New()

	jsonData := `[{"id":"rule-1","name":"Test Rule","status":"Enabled"}]`

	err := rep.FromJSON("test-bucket", []byte(jsonData))
	if err != nil {
		t.Fatalf("FromJSON failed: %v", err)
	}

	rules := rep.ListRules("test-bucket")
	if len(rules) != 1 {
		t.Errorf("Rules count = %d, want 1", len(rules))
	}

	if rules[0].Name != "Test Rule" {
		t.Errorf("Rule name = %s, want Test Rule", rules[0].Name)
	}
}

func TestConcurrentAccess(t *testing.T) {
	rep := New()
	done := make(chan bool)

	// Concurrent writes
	for i := 0; i < 10; i++ {
		go func(id int) {
			bucket := string(rune('A' + id))
			rep.AddRule(bucket, &Rule{ID: string(rune('0' + id))})
			rep.ListRules(bucket)
			rep.GetRule(bucket, string(rune('0' + id)))
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestRuleWithFilter(t *testing.T) {
	rep := New()

	rule := &Rule{
		ID:     "rule-1",
		Status: "Enabled",
		Filter: &Filter{
			Prefix: "photos/",
			Tag: &Tag{
				Key:   "type",
				Value: "image",
			},
		},
	}

	err := rep.AddRule("test-bucket", rule)
	if err != nil {
		t.Fatalf("AddRule failed: %v", err)
	}

	found, _ := rep.GetRule("test-bucket", "rule-1")
	if found.Filter == nil {
		t.Fatal("Filter should not be nil")
	}

	if found.Filter.Prefix != "photos/" {
		t.Errorf("Filter prefix = %s, want photos/", found.Filter.Prefix)
	}
}

func TestRuleWithDestination(t *testing.T) {
	rep := New()

	rule := &Rule{
		ID:     "rule-1",
		Status: "Enabled",
		Destination: &Destination{
			Bucket:       "dest-bucket",
			StorageClass: "STANDARD",
		},
	}

	err := rep.AddRule("test-bucket", rule)
	if err != nil {
		t.Fatalf("AddRule failed: %v", err)
	}

	found, _ := rep.GetRule("test-bucket", "rule-1")
	if found.Destination == nil {
		t.Fatal("Destination should not be nil")
	}

	if found.Destination.Bucket != "dest-bucket" {
		t.Errorf("Destination bucket = %s, want dest-bucket", found.Destination.Bucket)
	}
}

func TestRuleTimestamps(t *testing.T) {
	rep := New()

	before := time.Now()
	rule := &Rule{ID: "rule-1"}
	rep.AddRule("test-bucket", rule)
	after := time.Now()

	found, _ := rep.GetRule("test-bucket", "rule-1")

	if found.CreatedAt.Before(before) || found.CreatedAt.After(after) {
		t.Error("CreatedAt should be set to current time")
	}

	if found.ModifiedAt.Before(before) || found.ModifiedAt.After(after) {
		t.Error("ModifiedAt should be set to current time")
	}
}
