package events

import (
	"strings"
	"testing"
	"time"
)

func TestNewEventNotifier(t *testing.T) {
	notifier := NewEventNotifier()
	if notifier == nil {
		t.Fatal("EventNotifier should not be nil")
	}
	if notifier.configs == nil {
		t.Error("configs map should be initialized")
	}
	if notifier.subscribers == nil {
		t.Error("subscribers map should be initialized")
	}
}

func TestSetNotificationConfig(t *testing.T) {
	notifier := NewEventNotifier()

	config := &NotificationConfig{
		QueueConfigurations: []QueueConfiguration{
			{
				ID:       "queue-1",
				Event:    []string{"s3:ObjectCreated:*"},
				Queue:    "https://sqs.us-east-1.amazonaws.com/123456789012/myqueue",
				QueueArn: "arn:aws:sqs:us-east-1:123456789012:myqueue",
			},
		},
	}

	err := notifier.SetNotificationConfig("test-bucket", config)
	if err != nil {
		t.Fatalf("SetNotificationConfig failed: %v", err)
	}

	retrieved, ok := notifier.GetNotificationConfig("test-bucket")
	if !ok {
		t.Fatal("Should find notification config")
	}
	if len(retrieved.QueueConfigurations) != 1 {
		t.Errorf("QueueConfigurations count = %d, want 1", len(retrieved.QueueConfigurations))
	}
}

func TestSetNotificationConfigNil(t *testing.T) {
	notifier := NewEventNotifier()

	err := notifier.SetNotificationConfig("test-bucket", nil)
	if err == nil {
		t.Error("SetNotificationConfig should fail for nil config")
	}
}

func TestGetNotificationConfigNotFound(t *testing.T) {
	notifier := NewEventNotifier()

	_, ok := notifier.GetNotificationConfig("non-existent")
	if ok {
		t.Error("Should not find config for non-existent bucket")
	}
}

func TestDeleteNotificationConfig(t *testing.T) {
	notifier := NewEventNotifier()

	config := &NotificationConfig{
		QueueConfigurations: []QueueConfiguration{{ID: "queue-1"}},
	}

	notifier.SetNotificationConfig("test-bucket", config)
	err := notifier.DeleteNotificationConfig("test-bucket")
	if err != nil {
		t.Fatalf("DeleteNotificationConfig failed: %v", err)
	}

	_, ok := notifier.GetNotificationConfig("test-bucket")
	if ok {
		t.Error("Config should be deleted")
	}
}

func TestSubscribe(t *testing.T) {
	notifier := NewEventNotifier()

	ch := notifier.Subscribe("test-bucket")
	if ch == nil {
		t.Fatal("Subscribe should return a channel")
	}
}

func TestUnsubscribe(t *testing.T) {
	notifier := NewEventNotifier()

	ch := notifier.Subscribe("test-bucket")
	notifier.Unsubscribe("test-bucket", ch)

	notifier.mu.RLock()
	subs := notifier.subscribers["test-bucket"]
	notifier.mu.RUnlock()

	for _, sub := range subs {
		if sub == ch {
			t.Error("Channel should be unsubscribed")
		}
	}
}

func TestNotify(t *testing.T) {
	notifier := NewEventNotifier()

	config := &NotificationConfig{
		QueueConfigurations: []QueueConfiguration{
			{
				ID:    "queue-1",
				Event: []string{"s3:ObjectCreated:*"},
				Queue: "https://sqs.example.com/queue",
			},
		},
	}
	notifier.SetNotificationConfig("test-bucket", config)

	ch := notifier.Subscribe("test-bucket")

	event := CreateEvent("s3:ObjectCreated:Put", "test-bucket", "test-key", "etag", 100)
	notifier.Notify("test-bucket", event)

	select {
	case received := <-ch:
		if received.EventName != "s3:ObjectCreated:Put" {
			t.Errorf("EventName = %s, want s3:ObjectCreated:Put", received.EventName)
		}
	default:
	}
}

func TestNotifyNoConfig(t *testing.T) {
	notifier := NewEventNotifier()

	event := CreateEvent("s3:ObjectCreated:Put", "test-bucket", "test-key", "etag", 100)
	notifier.Notify("test-bucket", event)
}

func TestNotifyEventMismatch(t *testing.T) {
	notifier := NewEventNotifier()

	config := &NotificationConfig{
		QueueConfigurations: []QueueConfiguration{
			{
				ID:    "queue-1",
				Event: []string{"s3:ObjectRemoved:*"},
				Queue: "https://sqs.example.com/queue",
			},
		},
	}
	notifier.SetNotificationConfig("test-bucket", config)

	ch := notifier.Subscribe("test-bucket")

	event := CreateEvent("s3:ObjectCreated:Put", "test-bucket", "test-key", "etag", 100)
	notifier.Notify("test-bucket", event)

	select {
	case <-ch:
		t.Error("Should not receive event that doesn't match config")
	default:
	}
}

func TestMatchesEvent(t *testing.T) {
	tests := []struct {
		pattern  string
		event    string
		expected bool
	}{
		{"s3:ObjectCreated:*", "s3:ObjectCreated:Put", true},
		{"s3:ObjectCreated:*", "s3:ObjectCreated:Copy", true},
		{"s3:ObjectCreated:*", "s3:ObjectRemoved:Delete", false},
		{"s3:*", "s3:ObjectCreated:Put", true},
		{"*", "any.event", true},
		{"s3:ObjectCreated:Put", "s3:ObjectCreated:Put", true},
		{"s3:ObjectCreated:Put", "s3:ObjectCreated:Copy", false},
		{"", "s3:ObjectCreated:Put", false},
	}

	for _, tt := range tests {
		result := matchesEvent(tt.pattern, tt.event)
		if result != tt.expected {
			t.Errorf("matchesEvent(%s, %s) = %v, want %v", tt.pattern, tt.event, result, tt.expected)
		}
	}
}

func TestCreateEvent(t *testing.T) {
	event := CreateEvent("s3:ObjectCreated:Put", "my-bucket", "my-key", "etag123", 2048)

	if event.EventName != "s3:ObjectCreated:Put" {
		t.Errorf("EventName = %s, want s3:ObjectCreated:Put", event.EventName)
	}
	if event.EventVersion != "2.1" {
		t.Errorf("EventVersion = %s, want 2.1", event.EventVersion)
	}
	if event.EventSource != "aws:s3" {
		t.Errorf("EventSource = %s, want aws:s3", event.EventSource)
	}
	if event.S3.Bucket.Name != "my-bucket" {
		t.Errorf("Bucket = %s, want my-bucket", event.S3.Bucket.Name)
	}
	if event.S3.Object.Key != "my-key" {
		t.Errorf("Object key = %s, want my-key", event.S3.Object.Key)
	}
	if event.S3.Object.Size != 2048 {
		t.Errorf("Size = %d, want 2048", event.S3.Object.Size)
	}
	if event.S3.Object.ETag != "etag123" {
		t.Errorf("ETag = %s, want etag123", event.S3.Object.ETag)
	}
	if event.S3.Bucket.ARN != "arn:aws:s3:::my-bucket" {
		t.Errorf("ARN = %s, want arn:aws:s3:::my-bucket", event.S3.Bucket.ARN)
	}
}

func TestEventToJSON(t *testing.T) {
	event := CreateEvent("s3:ObjectCreated:Put", "my-bucket", "my-key", "etag", 100)

	json, err := event.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON failed: %v", err)
	}

	if len(json) == 0 {
		t.Error("JSON should not be empty")
	}

	if !strings.Contains(string(json), "s3:ObjectCreated:Put") {
		t.Error("JSON should contain event name")
	}
}

func TestToXML(t *testing.T) {
	events := []Event{
		CreateEvent("s3:ObjectCreated:Put", "bucket", "key1", "etag1", 100),
		CreateEvent("s3:ObjectRemoved:Delete", "bucket", "key2", "etag2", 200),
	}

	xml := ToXML(events)

	if xml == "" {
		t.Error("XML should not be empty")
	}
	if !strings.Contains(xml, "<?xml") {
		t.Error("XML should contain XML header")
	}
	if !strings.Contains(xml, "Notification") {
		t.Error("XML should contain Notification element")
	}
}

func TestEventTypeConstants(t *testing.T) {
	if EventObjectCreated != "s3:ObjectCreated:*" {
		t.Errorf("EventObjectCreated = %v", EventObjectCreated)
	}
	if EventObjectUploaded != "s3:ObjectCreated:Put" {
		t.Errorf("EventObjectUploaded = %v", EventObjectUploaded)
	}
	if EventObjectCopied != "s3:ObjectCreated:Copy" {
		t.Errorf("EventObjectCopied = %v", EventObjectCopied)
	}
	if EventObjectDeleted != "s3:ObjectRemoved:*" {
		t.Errorf("EventObjectDeleted = %v", EventObjectDeleted)
	}
	if EventBucketCreated != "s3:BucketCreated" {
		t.Errorf("EventBucketCreated = %v", EventBucketCreated)
	}
	if EventBucketDeleted != "s3:BucketRemoved" {
		t.Errorf("EventBucketDeleted = %v", EventBucketDeleted)
	}
}

func TestEventStruct(t *testing.T) {
	event := Event{
		EventVersion: "2.1",
		EventSource:  "aws:s3",
		EventTime:    time.Now(),
		EventName:    "s3:ObjectCreated:Put",
		UserIdentity: UserIdentity{PrincipalID: "user1"},
		RequestParameters: RequestParameters{
			SourceIPAddress: "192.168.1.1",
		},
		ResponseElements: ResponseElements{
			RequestID:  "req123",
			APIVersion: "2.0",
		},
		S3: S3EventEntity{
			S3SchemaVersion: "1.0",
			ConfigurationID: "config1",
			Bucket: BucketInfo{
				Name:          "bucket",
				OwnerIdentity: UserIdentity{PrincipalID: "owner"},
				ARN:           "arn:aws:s3:::bucket",
			},
			Object: ObjectInfo{
				Key:       "key",
				Size:      100,
				ETag:      "etag",
				VersionID: "v1",
				Sequencer: "seq",
			},
		},
		AwsRegion: "us-east-1",
	}

	if event.EventName != "s3:ObjectCreated:Put" {
		t.Errorf("EventName = %v", event.EventName)
	}
}

func TestUserIdentityStruct(t *testing.T) {
	ui := UserIdentity{PrincipalID: "user123"}
	if ui.PrincipalID != "user123" {
		t.Errorf("PrincipalID = %v, want user123", ui.PrincipalID)
	}
}

func TestRequestParametersStruct(t *testing.T) {
	rp := RequestParameters{SourceIPAddress: "10.0.0.1"}
	if rp.SourceIPAddress != "10.0.0.1" {
		t.Errorf("SourceIPAddress = %v, want 10.0.0.1", rp.SourceIPAddress)
	}
}

func TestResponseElementsStruct(t *testing.T) {
	re := ResponseElements{RequestID: "req1", APIVersion: "2.0"}
	if re.RequestID != "req1" {
		t.Errorf("RequestID = %v, want req1", re.RequestID)
	}
}

func TestS3EventEntityStruct(t *testing.T) {
	s3 := S3EventEntity{
		S3SchemaVersion: "1.0",
		ConfigurationID: "config1",
		Bucket:          BucketInfo{Name: "bucket"},
		Object:          ObjectInfo{Key: "key"},
	}

	if s3.S3SchemaVersion != "1.0" {
		t.Errorf("S3SchemaVersion = %v", s3.S3SchemaVersion)
	}
}

func TestBucketInfoStruct(t *testing.T) {
	bi := BucketInfo{
		Name:          "my-bucket",
		OwnerIdentity: UserIdentity{PrincipalID: "owner"},
		ARN:           "arn:aws:s3:::my-bucket",
	}

	if bi.Name != "my-bucket" {
		t.Errorf("Name = %v, want my-bucket", bi.Name)
	}
}

func TestObjectInfoStruct(t *testing.T) {
	oi := ObjectInfo{
		Key:       "object-key",
		Size:      1024,
		ETag:      "abc123",
		VersionID: "v1",
		Sequencer: "seq123",
	}

	if oi.Key != "object-key" {
		t.Errorf("Key = %v, want object-key", oi.Key)
	}
}

func TestNotificationConfigStruct(t *testing.T) {
	config := NotificationConfig{
		QueueConfigurations:          []QueueConfiguration{{ID: "q1"}},
		TopicConfigurations:          []TopicConfiguration{{ID: "t1"}},
		LambdaFunctionConfigurations: []LambdaConfiguration{{ID: "l1"}},
	}

	if len(config.QueueConfigurations) != 1 {
		t.Errorf("QueueConfigurations count = %d, want 1", len(config.QueueConfigurations))
	}
}

func TestQueueConfigurationStruct(t *testing.T) {
	qc := QueueConfiguration{
		ID:       "queue-1",
		Event:    []string{"s3:ObjectCreated:*"},
		Filter:   &Filter{},
		Queue:    "https://sqs.example.com/queue",
		QueueArn: "arn:aws:sqs:us-east-1:123:queue",
	}

	if qc.ID != "queue-1" {
		t.Errorf("ID = %v, want queue-1", qc.ID)
	}
}

func TestTopicConfigurationStruct(t *testing.T) {
	tc := TopicConfiguration{
		ID:       "topic-1",
		Event:    []string{"s3:ObjectRemoved:*"},
		Topic:    "arn:aws:sns:us-east-1:123:topic",
		TopicArn: "arn:aws:sns:us-east-1:123:topic",
	}

	if tc.ID != "topic-1" {
		t.Errorf("ID = %v, want topic-1", tc.ID)
	}
}

func TestLambdaConfigurationStruct(t *testing.T) {
	lc := LambdaConfiguration{
		ID:          "lambda-1",
		Event:       []string{"s3:ObjectCreated:*"},
		Function:    "myFunction",
		FunctionArn: "arn:aws:lambda:us-east-1:123:function:myFunction",
	}

	if lc.ID != "lambda-1" {
		t.Errorf("ID = %v, want lambda-1", lc.ID)
	}
}

func TestFilterStruct(t *testing.T) {
	filter := Filter{
		Key: FilterKey{
			FilterRules: []FilterRule{
				{Name: "prefix", Value: "images/"},
			},
		},
	}

	if len(filter.Key.FilterRules) != 1 {
		t.Errorf("FilterRules count = %d, want 1", len(filter.Key.FilterRules))
	}
}

func TestFilterRuleStruct(t *testing.T) {
	fr := FilterRule{Name: "suffix", Value: ".jpg"}
	if fr.Name != "suffix" {
		t.Errorf("Name = %v, want suffix", fr.Name)
	}
}

func TestMatchesConfigQueue(t *testing.T) {
	notifier := NewEventNotifier()
	config := &NotificationConfig{
		QueueConfigurations: []QueueConfiguration{
			{Event: []string{"s3:ObjectCreated:*"}},
		},
	}

	if !notifier.matchesConfig("s3:ObjectCreated:Put", config) {
		t.Error("Should match queue config")
	}
}

func TestMatchesConfigTopic(t *testing.T) {
	notifier := NewEventNotifier()
	config := &NotificationConfig{
		TopicConfigurations: []TopicConfiguration{
			{Event: []string{"s3:ObjectRemoved:*"}},
		},
	}

	if !notifier.matchesConfig("s3:ObjectRemoved:Delete", config) {
		t.Error("Should match topic config")
	}
}

func TestMatchesConfigLambda(t *testing.T) {
	notifier := NewEventNotifier()
	config := &NotificationConfig{
		LambdaFunctionConfigurations: []LambdaConfiguration{
			{Event: []string{"s3:ObjectCreated:*"}},
		},
	}

	if !notifier.matchesConfig("s3:ObjectCreated:Put", config) {
		t.Error("Should match lambda config")
	}
}

func TestMatchesConfigNoMatch(t *testing.T) {
	notifier := NewEventNotifier()
	config := &NotificationConfig{
		QueueConfigurations: []QueueConfiguration{
			{Event: []string{"s3:ObjectRemoved:*"}},
		},
	}

	if notifier.matchesConfig("s3:ObjectCreated:Put", config) {
		t.Error("Should not match")
	}
}

func TestMatchesConfigEmpty(t *testing.T) {
	notifier := NewEventNotifier()
	config := &NotificationConfig{}

	if notifier.matchesConfig("s3:ObjectCreated:Put", config) {
		t.Error("Empty config should not match")
	}
}
