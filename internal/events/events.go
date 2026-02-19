package events

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// EventType represents the type of event
type EventType string

const (
	// Object events
	EventObjectCreated    EventType = "s3:ObjectCreated:*"
	EventObjectUploaded   EventType = "s3:ObjectCreated:Put"
	EventObjectCopied    EventType = "s3:ObjectCreated:Copy"
	EventObjectMultipart  EventType = "s3:ObjectCreated:CompleteMultipartUpload"
	EventObjectDeleted    EventType = "s3:ObjectRemoved:*"
	EventObjectRemoved    EventType = "s3:ObjectRemoved:Delete"
	EventObjectRemovedTag  EventType = "s3:ObjectRemoved:DeleteTagging"

	// Object ACL events
	EventObjectAclPut    EventType = "s3:ObjectAcl:Put"

	// Bucket events
	EventBucketCreated   EventType = "s3:BucketCreated"
	EventBucketDeleted   EventType = "s3:BucketRemoved"

	// Bucket ACL events
	EventBucketAclPut    EventType = "s3:BucketAcl:Put"

	// Bucket policy events
	EventBucketPolicyPut EventType = "s3:BucketPolicyPut"

	// Lifecycle events
	EventLifecycleExpiration EventType = "s3:LifecycleExpiration:*"
)

// Event represents an S3 event
type Event struct {
	EventVersion      string            `json:"eventVersion"`
	EventSource      string            `json:"eventSource"`
	EventTime        time.Time         `json:"eventTime"`
	EventName        string            `json:"eventName"`
	UserIdentity      UserIdentity      `json:"userIdentity"`
	RequestParameters RequestParameters `json:"requestParameters"`
	ResponseElements ResponseElements  `json:"responseElements"`
	S3               S3EventEntity     `json:"s3"`
	AwsRegion        string            `json:"awsRegion"`
}

// UserIdentity represents the user identity
type UserIdentity struct {
	PrincipalID string `json:"principalId"`
}

// RequestParameters represents request parameters
type RequestParameters struct {
	SourceIPAddress string `json:"sourceIPAddress"`
}

// ResponseElements represents response elements
type ResponseElements struct {
	RequestID string `json:"requestId"`
	APIVersion string `json:"apiVersion"`
}

// S3EventEntity represents the S3 entity in an event
type S3EventEntity struct {
	S3SchemaVersion string    `json:"s3SchemaVersion"`
	ConfigurationID string   `json:"configurationId"`
	Bucket          BucketInfo `json:"bucket"`
	Object          ObjectInfo `json:"object"`
}

// BucketInfo represents bucket information in event
type BucketInfo struct {
	Name          string `json:"name"`
	OwnerIdentity UserIdentity `json:"ownerIdentity"`
	ARN           string `json:"arn"`
}

// ObjectInfo represents object information in event
type ObjectInfo struct {
	Key       string `json:"key"`
	Size      int64  `json:"size,omitempty"`
	ETag      string `json:"eTag,omitempty"`
	VersionID string `json:"versionId,omitempty"`
	Sequencer string `json:"sequencer,omitempty"`
}

// NotificationConfig represents notification configuration
type NotificationConfig struct {
	QueueConfigurations []QueueConfiguration `json:"QueueConfiguration,omitempty"`
	TopicConfigurations []TopicConfiguration `json:"TopicConfiguration,omitempty"`
	LambdaFunctionConfigurations []LambdaConfiguration `json:"LambdaFunctionConfiguration,omitempty"`
}

// QueueConfiguration represents SQS queue configuration
type QueueConfiguration struct {
	ID        string   `json:"Id"`
	Event     []string `json:"Event"`
	Filter    *Filter  `json:"Filter,omitempty"`
	Queue     string   `json:"Queue"`
	QueueArn  string   `json:"QueueArn"`
}

// TopicConfiguration represents SNS topic configuration
type TopicConfiguration struct {
	ID        string   `json:"Id"`
	Event     []string `json:"Event"`
	Filter    *Filter  `json:"Filter,omitempty"`
	Topic     string   `json:"Topic"`
	TopicArn  string   `json:"TopicArn"`
}

// LambdaConfiguration represents Lambda function configuration
type LambdaConfiguration struct {
	ID        string   `json:"Id"`
	Event     []string `json:"Event"`
	Filter    *Filter  `json:"Filter,omitempty"`
	Function  string   `json:"Function"`
	FunctionArn string `json:"FunctionArn"`
}

// Filter represents event filter
type Filter struct {
	Key FilterKey `json:"S3Key"`
}

// FilterKey represents S3 key filter
type FilterKey struct {
	FilterRules []FilterRule `json:"FilterRules"`
}

// FilterRule represents a filter rule
type FilterRule struct {
	Name  string `json:"Name"` // prefix or suffix
	Value string `json:"Value"`
}

// EventNotifier handles event notifications
type EventNotifier struct {
	mu           sync.RWMutex
	configs      map[string]*NotificationConfig
	subscribers map[string][]chan Event
}

// NewEventNotifier creates a new event notifier
func NewEventNotifier() *EventNotifier {
	return &EventNotifier{
		configs:      make(map[string]*NotificationConfig),
		subscribers:  make(map[string][]chan Event),
	}
}

// SetNotificationConfig sets notification configuration for a bucket
func (en *EventNotifier) SetNotificationConfig(bucket string, config *NotificationConfig) error {
	en.mu.Lock()
	defer en.mu.Unlock()

	// Validate configuration
	if config == nil {
		return fmt.Errorf("notification config cannot be nil")
	}

	en.configs[bucket] = config

	// Initialize subscribers for each configuration
	en.initSubscribers(bucket, config)

	return nil
}

// GetNotificationConfig gets notification configuration
func (en *EventNotifier) GetNotificationConfig(bucket string) (*NotificationConfig, bool) {
	en.mu.RLock()
	defer en.mu.RUnlock()

	config, ok := en.configs[bucket]
	return config, ok
}

// DeleteNotificationConfig deletes notification configuration
func (en *EventNotifier) DeleteNotificationConfig(bucket string) error {
	en.mu.Lock()
	defer en.mu.Unlock()

	delete(en.configs, bucket)
	delete(en.subscribers, bucket)

	return nil
}

// Subscribe subscribes to events for a bucket
func (en *EventNotifier) Subscribe(bucket string) chan Event {
	en.mu.Lock()
	defer en.mu.Unlock()

	ch := make(chan Event, 100) // Buffered channel
	en.subscribers[bucket] = append(en.subscribers[bucket], ch)

	return ch
}

// Unsubscribe unsubscribes from events
func (en *EventNotifier) Unsubscribe(bucket string, ch chan Event) {
	en.mu.Lock()
	defer en.mu.Unlock()

	subscribers := en.subscribers[bucket]
	for i, sub := range subscribers {
		if sub == ch {
			en.subscribers[bucket] = append(subscribers[:i], subscribers[i+1:]...)
			close(ch)
			break
		}
	}
}

// Notify notifies subscribers about an event
func (en *EventNotifier) Notify(bucket string, event Event) {
	en.mu.RLock()
	config, ok := en.configs[bucket]
	subscribers := en.subscribers[bucket]
	en.mu.RUnlock()

	if !ok {
		return
	}

	// Check if event matches any configuration
	if !en.matchesConfig(event.EventName, config) {
		return
	}

	// Notify all subscribers
	for _, ch := range subscribers {
		select {
		case ch <- event:
		default:
			// Channel full, skip
		}
	}
}

// matchesConfig checks if event matches configuration
func (en *EventNotifier) matchesConfig(eventName string, config *NotificationConfig) bool {
	// Check queue configurations
	for _, qc := range config.QueueConfigurations {
		for _, event := range qc.Event {
			if matchesEvent(event, eventName) {
				return true
			}
		}
	}

	// Check topic configurations
	for _, tc := range config.TopicConfigurations {
		for _, event := range tc.Event {
			if matchesEvent(event, eventName) {
				return true
			}
		}
	}

	// Check lambda configurations
	for _, lc := range config.LambdaFunctionConfigurations {
		for _, event := range lc.Event {
			if matchesEvent(event, eventName) {
				return true
			}
		}
	}

	return false
}

// matchesEvent checks if event name matches pattern
func matchesEvent(pattern, eventName string) bool {
	if pattern == eventName {
		return true
	}

	// Handle wildcards
	if len(pattern) > 0 && pattern[len(pattern)-1] == '*' {
		prefix := pattern[:len(pattern)-1]
		if len(eventName) >= len(prefix) && eventName[:len(prefix)] == prefix {
			return true
		}
	}

	return false
}

// initSubscribers initializes subscribers for configurations
func (en *EventNotifier) initSubscribers(bucket string, config *NotificationConfig) {
	// This would create actual subscribers based on configuration
	// For now, just initialize the slice
	if en.subscribers[bucket] == nil {
		en.subscribers[bucket] = make([]chan Event, 0)
	}
}

// CreateEvent creates a new S3 event
func CreateEvent(eventName, bucket, key, etag string, size int64) Event {
	return Event{
		EventVersion:   "2.1",
		EventSource:    "aws:s3",
		EventTime:      time.Now(),
		EventName:      eventName,
		UserIdentity:   UserIdentity{PrincipalID: "OpenEndpoint"},
		ResponseElements: ResponseElements{
			RequestID: fmt.Sprintf("%d", time.Now().UnixNano()),
		},
		S3: S3EventEntity{
			S3SchemaVersion: "1.0",
			ConfigurationID:  "notification",
			Bucket: BucketInfo{
				Name: bucket,
				ARN:  fmt.Sprintf("arn:aws:s3:::%s", bucket),
			},
			Object: ObjectInfo{
				Key:  key,
				ETag: etag,
				Size: size,
			},
		},
		AwsRegion: "us-east-1",
	}
}

// ToJSON converts event to JSON
func (e *Event) ToJSON() ([]byte, error) {
	return json.Marshal(e)
}

// ToXML converts events to S3 notification XML format
func ToXML(events []Event) string {
	xml := `<?xml version="1.0" encoding="UTF-8"?>`
	xml += `<Notification xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`

	for _, event := range events {
		xml += `<Event>`
		xml += event.EventName
		xml += `</Event>`
	}

	xml += `</Notification>`

	return xml
}
