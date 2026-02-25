package s3types

import (
	"encoding/xml"
	"testing"
)

func TestBucketLifecycleConfigurationXML(t *testing.T) {
	config := BucketLifecycleConfiguration{
		Rules: []LifecycleRule{
			{
				ID:     "rule1",
				Prefix: "logs/",
				Status: "Enabled",
				Expiration: &Expiration{
					Days: 30,
				},
			},
		},
	}

	data, err := xml.Marshal(config)
	if err != nil {
		t.Errorf("Failed to marshal: %v", err)
	}

	var unmarshaled BucketLifecycleConfiguration
	if err := xml.Unmarshal(data, &unmarshaled); err != nil {
		t.Errorf("Failed to unmarshal: %v", err)
	}

	if len(unmarshaled.Rules) != 1 {
		t.Errorf("Rules length mismatch")
	}
}

func TestLifecycleRuleXML(t *testing.T) {
	rule := LifecycleRule{
		ID:     "test-rule",
		Prefix: "temp/",
		Status: "Enabled",
		Transitions: []Transition{
			{Days: 30, StorageClass: "STANDARD_IA"},
		},
		Expiration: &Expiration{
			Days: 90,
		},
	}

	data, err := xml.Marshal(rule)
	if err != nil {
		t.Errorf("Failed to marshal: %v", err)
	}

	var unmarshaled LifecycleRule
	if err := xml.Unmarshal(data, &unmarshaled); err != nil {
		t.Errorf("Failed to unmarshal: %v", err)
	}

	if unmarshaled.ID != rule.ID {
		t.Errorf("ID mismatch")
	}
}

func TestTransitionXML(t *testing.T) {
	transition := Transition{
		Date:         "2024-01-01",
		Days:         30,
		StorageClass: "STANDARD_IA",
	}

	data, err := xml.Marshal(transition)
	if err != nil {
		t.Errorf("Failed to marshal: %v", err)
	}

	var unmarshaled Transition
	if err := xml.Unmarshal(data, &unmarshaled); err != nil {
		t.Errorf("Failed to unmarshal: %v", err)
	}

	if unmarshaled.Days != transition.Days {
		t.Errorf("Days mismatch")
	}
}

func TestExpirationXML(t *testing.T) {
	exp := Expiration{
		Days: 30,
	}

	data, err := xml.Marshal(exp)
	if err != nil {
		t.Errorf("Failed to marshal: %v", err)
	}

	var unmarshaled Expiration
	if err := xml.Unmarshal(data, &unmarshaled); err != nil {
		t.Errorf("Failed to unmarshal: %v", err)
	}

	if unmarshaled.Days != exp.Days {
		t.Errorf("Days mismatch")
	}
}

func TestNoncurrentVersionExpirationXML(t *testing.T) {
	nve := NoncurrentVersionExpiration{
		NoncurrentDays: 30,
	}

	data, err := xml.Marshal(nve)
	if err != nil {
		t.Errorf("Failed to marshal: %v", err)
	}

	var unmarshaled NoncurrentVersionExpiration
	if err := xml.Unmarshal(data, &unmarshaled); err != nil {
		t.Errorf("Failed to unmarshal: %v", err)
	}

	if unmarshaled.NoncurrentDays != nve.NoncurrentDays {
		t.Errorf("NoncurrentDays mismatch")
	}
}

func TestAbortIncompleteMultipartUploadXML(t *testing.T) {
	aimu := AbortIncompleteMultipartUpload{
		DaysAfterInitiation: 7,
	}

	data, err := xml.Marshal(aimu)
	if err != nil {
		t.Errorf("Failed to marshal: %v", err)
	}

	var unmarshaled AbortIncompleteMultipartUpload
	if err := xml.Unmarshal(data, &unmarshaled); err != nil {
		t.Errorf("Failed to unmarshal: %v", err)
	}

	if unmarshaled.DaysAfterInitiation != aimu.DaysAfterInitiation {
		t.Errorf("DaysAfterInitiation mismatch")
	}
}

func TestBucketCorsXML(t *testing.T) {
	cors := BucketCors{
		CorsRules: []CorsRule{
			{
				ID:             "rule1",
				AllowedMethods: []string{"GET", "PUT"},
				AllowedOrigins: []string{"*"},
				AllowedHeaders: []string{"*"},
				ExposeHeaders:  []string{"ETag"},
				MaxAgeSeconds:  3600,
			},
		},
	}

	data, err := xml.Marshal(cors)
	if err != nil {
		t.Errorf("Failed to marshal: %v", err)
	}

	var unmarshaled BucketCors
	if err := xml.Unmarshal(data, &unmarshaled); err != nil {
		t.Errorf("Failed to unmarshal: %v", err)
	}

	if len(unmarshaled.CorsRules) != 1 {
		t.Errorf("CorsRules length mismatch")
	}
}

func TestCorsRuleXML(t *testing.T) {
	rule := CorsRule{
		ID:             "test-rule",
		AllowedMethods: []string{"GET", "POST", "PUT"},
		AllowedOrigins: []string{"https://example.com"},
		AllowedHeaders: []string{"Content-Type"},
		ExposeHeaders:  []string{"X-Custom-Header"},
		MaxAgeSeconds:  3000,
	}

	data, err := xml.Marshal(rule)
	if err != nil {
		t.Errorf("Failed to marshal: %v", err)
	}

	if len(data) == 0 {
		t.Error("Marshaled data is empty")
	}
}

func TestBucketReplicationConfigurationXML(t *testing.T) {
	config := BucketReplicationConfiguration{
		Role: "arn:aws:iam::123456789:role/replication-role",
		Rules: []ReplicationRule{
			{
				ID:       "rule1",
				Priority: 1,
				Status:   "Enabled",
				Destination: Destination{
					Bucket:       "arn:aws:s3:::dest-bucket",
					StorageClass: "STANDARD",
				},
				Filter: Filter{
					Prefix: "data/",
				},
				DeleteMarkerReplication: DeleteMarkerReplication{
					Status: "Disabled",
				},
			},
		},
	}

	data, err := xml.Marshal(config)
	if err != nil {
		t.Errorf("Failed to marshal: %v", err)
	}

	if len(data) == 0 {
		t.Error("Marshaled data is empty")
	}
}

func TestDestinationXML(t *testing.T) {
	dest := Destination{
		Bucket:       "arn:aws:s3:::dest-bucket",
		StorageClass: "STANDARD",
		EncryptionConfiguration: EncryptionConfiguration{
			ReplicaKmsKeyID: "key-id",
		},
	}

	data, err := xml.Marshal(dest)
	if err != nil {
		t.Errorf("Failed to marshal: %v", err)
	}

	var unmarshaled Destination
	if err := xml.Unmarshal(data, &unmarshaled); err != nil {
		t.Errorf("Failed to unmarshal: %v", err)
	}

	if unmarshaled.Bucket != dest.Bucket {
		t.Errorf("Bucket mismatch")
	}
}

func TestFilterXML(t *testing.T) {
	filter := Filter{
		Prefix: "logs/",
		Tag: Tag{
			Key:   "environment",
			Value: "production",
		},
	}

	data, err := xml.Marshal(filter)
	if err != nil {
		t.Errorf("Failed to marshal: %v", err)
	}

	if len(data) == 0 {
		t.Error("Marshaled data is empty")
	}
}

func TestTagXML(t *testing.T) {
	tag := Tag{
		Key:   "name",
		Value: "value",
	}

	data, err := xml.Marshal(tag)
	if err != nil {
		t.Errorf("Failed to marshal: %v", err)
	}

	var unmarshaled Tag
	if err := xml.Unmarshal(data, &unmarshaled); err != nil {
		t.Errorf("Failed to unmarshal: %v", err)
	}

	if unmarshaled.Key != tag.Key {
		t.Errorf("Key mismatch")
	}
}

func TestDeleteMarkerReplicationXML(t *testing.T) {
	dmr := DeleteMarkerReplication{
		Status: "Enabled",
	}

	data, err := xml.Marshal(dmr)
	if err != nil {
		t.Errorf("Failed to marshal: %v", err)
	}

	var unmarshaled DeleteMarkerReplication
	if err := xml.Unmarshal(data, &unmarshaled); err != nil {
		t.Errorf("Failed to unmarshal: %v", err)
	}

	if unmarshaled.Status != dmr.Status {
		t.Errorf("Status mismatch")
	}
}

func TestListBucketInventoryConfigurationsOutputXML(t *testing.T) {
	output := ListBucketInventoryConfigurationsOutput{
		InventoryConfigurationList: []InventoryConfiguration{
			{
				ID:                     "inventory-1",
				IncludedObjectVersions: "Current",
				Enabled:                true,
			},
		},
		IsTruncated: false,
	}

	data, err := xml.Marshal(output)
	if err != nil {
		t.Errorf("Failed to marshal: %v", err)
	}

	if len(data) == 0 {
		t.Error("Marshaled data is empty")
	}
}

func TestInventoryConfigurationXML(t *testing.T) {
	config := InventoryConfiguration{
		ID:                     "daily-inventory",
		IncludedObjectVersions: "Current",
		Filter: InventoryFilter{
			Prefix: "data/",
		},
		Destination: InventoryDestination{
			S3BucketDestination: S3InventoryDestination{
				Format: "CSV",
				Bucket: "arn:aws:s3:::inventory-bucket",
				Prefix: "inventory/",
			},
		},
		Schedule: InventorySchedule{
			Frequency: "Daily",
		},
		Enabled:        true,
		OptionalFields: []string{"Size", "LastModifiedDate"},
	}

	data, err := xml.Marshal(config)
	if err != nil {
		t.Errorf("Failed to marshal: %v", err)
	}

	if len(data) == 0 {
		t.Error("Marshaled data is empty")
	}
}

func TestAnalyticsConfigurationXML(t *testing.T) {
	config := AnalyticsConfiguration{
		ID: "analytics-1",
		Filter: AnalyticsFilter{
			Prefix: "data/",
		},
		StorageClassAnalysis: StorageClassAnalysis{
			DataExport: DataExportAnalysis{
				Destination: AnalyticsDestination{
					S3BucketDestination: S3AnalyticsDestination{
						Format: "CSV",
						Bucket: "arn:aws:s3:::analytics-bucket",
					},
				},
			},
		},
	}

	data, err := xml.Marshal(config)
	if err != nil {
		t.Errorf("Failed to marshal: %v", err)
	}

	if len(data) == 0 {
		t.Error("Marshaled data is empty")
	}
}

func TestDeleteObjectsInputXML(t *testing.T) {
	input := DeleteObjectsInput{
		Quiet: false,
		Objects: []ObjectID{
			{Key: "file1.txt"},
			{Key: "file2.txt", VersionID: "v1"},
		},
	}

	data, err := xml.Marshal(input)
	if err != nil {
		t.Errorf("Failed to marshal: %v", err)
	}

	var unmarshaled DeleteObjectsInput
	if err := xml.Unmarshal(data, &unmarshaled); err != nil {
		t.Errorf("Failed to unmarshal: %v", err)
	}

	if len(unmarshaled.Objects) != 2 {
		t.Errorf("Objects length mismatch")
	}
}

func TestObjectIDXML(t *testing.T) {
	objID := ObjectID{
		Key:       "test.txt",
		VersionID: "v1",
	}

	data, err := xml.Marshal(objID)
	if err != nil {
		t.Errorf("Failed to marshal: %v", err)
	}

	var unmarshaled ObjectID
	if err := xml.Unmarshal(data, &unmarshaled); err != nil {
		t.Errorf("Failed to unmarshal: %v", err)
	}

	if unmarshaled.Key != objID.Key {
		t.Errorf("Key mismatch")
	}
}

func TestDeleteObjectsOutputXML(t *testing.T) {
	output := DeleteObjectsOutput{
		Deleted: []DeletedObject{
			{Key: "file1.txt"},
			{Key: "file2.txt", VersionID: "v1"},
		},
		Errors: []DeleteError{
			{Key: "file3.txt", Code: "AccessDenied", Message: "Access denied"},
		},
	}

	data, err := xml.Marshal(output)
	if err != nil {
		t.Errorf("Failed to marshal: %v", err)
	}

	var unmarshaled DeleteObjectsOutput
	if err := xml.Unmarshal(data, &unmarshaled); err != nil {
		t.Errorf("Failed to unmarshal: %v", err)
	}

	if len(unmarshaled.Deleted) != 2 {
		t.Errorf("Deleted length mismatch")
	}
	if len(unmarshaled.Errors) != 1 {
		t.Errorf("Errors length mismatch")
	}
}

func TestDeletedObjectXML(t *testing.T) {
	deleted := DeletedObject{
		Key:                   "deleted.txt",
		VersionID:             "v1",
		DeleteMarker:          true,
		DeleteMarkerVersionID: "dm-v1",
	}

	data, err := xml.Marshal(deleted)
	if err != nil {
		t.Errorf("Failed to marshal: %v", err)
	}

	var unmarshaled DeletedObject
	if err := xml.Unmarshal(data, &unmarshaled); err != nil {
		t.Errorf("Failed to unmarshal: %v", err)
	}

	if unmarshaled.Key != deleted.Key {
		t.Errorf("Key mismatch")
	}
}

func TestDeleteErrorXML(t *testing.T) {
	delErr := DeleteError{
		Key:       "error.txt",
		VersionID: "v1",
		Code:      "AccessDenied",
		Message:   "Access denied",
	}

	data, err := xml.Marshal(delErr)
	if err != nil {
		t.Errorf("Failed to marshal: %v", err)
	}

	var unmarshaled DeleteError
	if err := xml.Unmarshal(data, &unmarshaled); err != nil {
		t.Errorf("Failed to unmarshal: %v", err)
	}

	if unmarshaled.Code != delErr.Code {
		t.Errorf("Code mismatch")
	}
}

func TestBucketPolicyJSON(t *testing.T) {
	policy := BucketPolicy{
		Version: "2012-10-17",
		Statement: []Statement{
			{
				Sid:       "PublicRead",
				Effect:    "Allow",
				Principal: "*",
				Action:    []string{"s3:GetObject"},
				Resource:  "arn:aws:s3:::mybucket/*",
			},
		},
	}

	if policy.Version != "2012-10-17" {
		t.Errorf("Version mismatch")
	}
	if len(policy.Statement) != 1 {
		t.Errorf("Statement length mismatch")
	}
}

func TestStatement(t *testing.T) {
	stmt := Statement{
		Sid:       "test",
		Effect:    "Allow",
		Principal: "*",
		Action:    []string{"s3:GetObject", "s3:PutObject"},
		Resource:  "arn:aws:s3:::bucket/*",
		Condition: Condition{
			StringEquals: StringEqualsCondition{
				"aws:SourceIp": "192.168.1.0/24",
			},
		},
	}

	if stmt.Effect != "Allow" {
		t.Errorf("Effect mismatch")
	}
	if len(stmt.Action) != 2 {
		t.Errorf("Action length mismatch")
	}
}
