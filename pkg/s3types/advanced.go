package s3types

import "encoding/xml"

// Additional S3 types

// BucketLifecycleConfiguration is the request/response for lifecycle config
type BucketLifecycleConfiguration struct {
	XMLName xml.Name        `xml:"LifecycleConfiguration"`
	Rules   []LifecycleRule `xml:"Rule"`
}

// LifecycleRule represents a lifecycle rule
type LifecycleRule struct {
	XMLName                   xml.Name                    `xml:"Rule"`
	ID                        string                      `xml:"ID"`
	Prefix                   string                      `xml:"Prefix"`
	Status                   string                      `xml:"Status"`
	Transitions              []Transition                `xml:"Transition,omitempty"`
	Expiration               *Expiration                 `xml:"Expiration,omitempty"`
	NoncurrentVersionExpiration *NoncurrentVersionExpiration `xml:"NoncurrentVersionExpiration,omitempty"`
	AbortIncompleteMultipartUpload *AbortIncompleteMultipartUpload `xml:"AbortIncompleteMultipartUpload,omitempty"`
}

// Transition represents storage class transition
type Transition struct {
	XMLName         xml.Name `xml:"Transition"`
	Date           string   `xml:"Date,omitempty"`
	Days           int      `xml:"Days,omitempty"`
	StorageClass   string   `xml:"StorageClass"`
}

// Expiration represents object expiration
type Expiration struct {
	XMLName                      xml.Name `xml:"Expiration"`
	Date                        string   `xml:"Date,omitempty"`
	Days                        int      `xml:"Days,omitempty"`
	ExpiredObjectDeleteMarker   *bool    `xml:"ExpiredObjectDeleteMarker"`
}

// NoncurrentVersionExpiration represents noncurrent version expiration
type NoncurrentVersionExpiration struct {
	XMLName            xml.Name `xml:"NoncurrentVersionExpiration"`
	NoncurrentDays     int      `xml:"NoncurrentDays"`
}

// AbortIncompleteMultipartUpload represents abort incomplete multipart upload
type AbortIncompleteMultipartUpload struct {
	XMLName           xml.Name `xml:"AbortIncompleteMultipartUpload"`
	DaysAfterInitiation int    `xml:"DaysAfterInitiation"`
}

// BucketCors is the request/response for CORS
type BucketCors struct {
	XMLName xml.Name   `xml:"CORSConfiguration"`
	CorsRules []CorsRule `xml:"CORSRule"`
}

// CorsRule represents a CORS rule
type CorsRule struct {
	XMLName           xml.Name   `xml:"CORSRule"`
	ID                string     `xml:"ID,omitempty"`
	AllowedMethods   []string   `xml:"AllowedMethod"`
	AllowedOrigins   []string   `xml:"AllowedOrigin"`
	AllowedHeaders   []string   `xml:"AllowedHeader,omitempty"`
	ExposeHeaders    []string   `xml:"ExposeHeader,omitempty"`
	MaxAgeSeconds    int        `xml:"MaxAgeSeconds,omitempty"`
}

// BucketPolicy is the request/response for bucket policy
type BucketPolicy struct {
	XMLName   xml.Name `xml:"Policy"`
	Version   string   `json:"Version,omitempty"`
	Statement []Statement `json:"Statement,omitempty"`
}

// Statement represents an IAM statement
type Statement struct {
	Sid       string   `json:"Sid,omitempty"`
	Effect    string   `json:"Effect"`
	Principal string   `json:"Principal,omitempty"`
	Action    []string `json:"Action"`
	Resource  string   `json:"Resource"`
	Condition Condition `json:"Condition,omitempty"`
}

// Condition represents an IAM condition
type Condition struct {
	StringEquals StringEqualsCondition `json:"StringEquals,omitempty"`
	IpAddress    IpAddressCondition    `json:"IpAddress,omitempty"`
}

// StringEqualsCondition represents string equals condition
type StringEqualsCondition map[string]string

// IpAddressCondition represents IP address condition
type IpAddressCondition map[string]string

// BucketReplicationConfiguration is the request/response for replication
type BucketReplicationConfiguration struct {
	XMLName     xml.Name      `xml:"ReplicationConfiguration"`
	Role        string       `xml:"Role"`
	Rules       []ReplicationRule `xml:"Rule"`
}

// ReplicationRule represents a replication rule
type ReplicationRule struct {
	XMLName             xml.Name   `xml:"Rule"`
	ID                  string     `xml:"ID"`
	Priority           int        `xml:"Priority"`
	Status             string     `xml:"Status"`
	Destination        Destination `xml:"Destination"`
	Filter             Filter     `xml:"Filter"`
	DeleteMarkerReplication DeleteMarkerReplication `xml:"DeleteMarkerReplication"`
}

// Destination represents replication destination
type Destination struct {
	XMLName             xml.Name `xml:"Destination"`
	Bucket             string   `xml:"Bucket"`
	StorageClass       string   `xml:"StorageClass,omitempty"`
	EncryptionConfiguration EncryptionConfiguration `xml:"EncryptionConfiguration,omitempty"`
}

// EncryptionConfiguration represents encryption configuration
type EncryptionConfiguration struct {
	ReplicaKmsKeyID string `xml:"ReplicaKmsKeyID"`
}

// Filter represents replication filter
type Filter struct {
	XMLName   xml.Name `xml:"Filter"`
	Prefix    string   `xml:"Prefix,omitempty"`
	Tag       Tag      `xml:"Tag,omitempty"`
}

// Tag represents a tag
type Tag struct {
	XMLName xml.Name `xml:"Tag"`
	Key     string   `xml:"Key"`
	Value   string   `xml:"Value"`
}

// DeleteMarkerReplication represents delete marker replication
type DeleteMarkerReplication struct {
	XMLName string `xml:"DeleteMarkerReplication"`
	Status  string `xml:"Status"`
}

// ListBucketInventoryConfigurationsOutput is the response for ListBucketInventoryConfigurations
type ListBucketInventoryConfigurationsOutput struct {
	XMLName                  xml.Name                     `xml:"ListBucketInventoryConfigurationsResult"`
	xmlns                   string                       `xml:"xmlns,attr"`
	InventoryConfigurationList []InventoryConfiguration `xml:"InventoryConfiguration"`
	IsTruncated             bool                         `xml:"IsTruncated"`
	ContinuationToken       string                       `xml:"ContinuationToken"`
	NextContinuationToken   string                       `xml:"NextContinuationToken"`
}

// InventoryConfiguration represents an inventory configuration
type InventoryConfiguration struct {
	XMLName              xml.Name  `xml:"InventoryConfiguration"`
	ID                   string    `xml:"Id"`
	IncludedObjectVersions string  `xml:"IncludedObjectVersions"`
	Filter               InventoryFilter `xml:"Filter"`
	Destination          InventoryDestination `xml:"Destination"`
	Schedule             InventorySchedule `xml:"Schedule"`
	Enabled              bool      `xml:"Enabled"`
	OptionalFields      []string  `xml:"OptionalFields>Field"`
}

// InventoryFilter represents inventory filter
type InventoryFilter struct {
	XMLName xml.Name `xml:"Filter"`
	Prefix  string  `xml:"Prefix,omitempty"`
}

// InventoryDestination represents inventory destination
type InventoryDestination struct {
	XMLName    xml.Name              `xml:"Destination"`
	S3BucketDestination S3InventoryDestination `xml:"S3BucketDestination"`
}

// S3InventoryDestination represents S3 inventory destination
type S3InventoryDestination struct {
	XMLName      xml.Name `xml:"S3BucketDestination"`
	Format       string   `xml:"Format"`
	Bucket       string   `xml:"Bucket"`
	Prefix       string   `xml:"Prefix,omitempty"`
	EncryptionID string   `xml:"EncryptionConfiguration>ReplicaKmsKeyID,omitempty"`
}

// InventorySchedule represents inventory schedule
type InventorySchedule struct {
	XMLName xml.Name `xml:"Schedule"`
	Frequency string `xml:"Frequency"`
}

// AnalyticsConfiguration represents analytics configuration
type AnalyticsConfiguration struct {
	XMLName   xml.Name   `xml:"AnalyticsConfiguration"`
	ID        string     `xml:"Id"`
	Filter    AnalyticsFilter `xml:"Filter"`
	StorageClassAnalysis StorageClassAnalysis `xml:"StorageClassAnalysis"`
}

// AnalyticsFilter represents analytics filter
type AnalyticsFilter struct {
	XMLName xml.Name `xml:"Filter"`
	Prefix  string  `xml:"Prefix,omitempty"`
	Tag     Tag     `xml:"Tag,omitempty"`
}

// StorageClassAnalysis represents storage class analysis
type StorageClassAnalysis struct {
	XMLName xml.Name            `xml:"StorageClassAnalysis"`
	DataExport DataExportAnalysis `xml:"DataExport"`
}

// DataExportAnalysis represents data export analysis
type DataExportAnalysis struct {
	XMLName             xml.Name `xml:"DataExport"`
	Destination         AnalyticsDestination `xml:"Destination"`
}

// AnalyticsDestination represents analytics destination
type AnalyticsDestination struct {
	XMLName             xml.Name `xml:"Destination"`
	S3BucketDestination S3AnalyticsDestination `xml:"S3BucketDestination"`
}

// S3AnalyticsDestination represents S3 analytics destination
type S3AnalyticsDestination struct {
	XMLName      xml.Name `xml:"S3BucketDestination"`
	Format       string   `xml:"Format"`
	Bucket       string   `xml:"Bucket"`
	Prefix       string   `xml:"Prefix,omitempty"`
}

// DeleteObjectsInput is the request for DeleteObjects
type DeleteObjectsInput struct {
	XMLName xml.Name    `xml:"Delete"`
	Quiet   bool        `xml:"Quiet"`
	Objects []ObjectID  `xml:"Object"`
}

// ObjectID represents an object to delete
type ObjectID struct {
	XMLName   xml.Name `xml:"Object"`
	Key       string   `xml:"Key"`
	VersionID string   `xml:"VersionId,omitempty"`
}

// DeleteObjectsOutput is the response for DeleteObjects
type DeleteObjectsOutput struct {
	XMLName      xml.Name        `xml:"DeleteResult"`
	Deleted      []DeletedObject `xml:"Deleted"`
	Errors       []DeleteError   `xml:"Error"`
}

// DeletedObject represents a successfully deleted object
type DeletedObject struct {
	XMLName   xml.Name `xml:"Deleted"`
	Key       string   `xml:"Key"`
	VersionID string   `xml:"VersionId,omitempty"`
	DeleteMarker          bool   `xml:"DeleteMarker,omitempty"`
	DeleteMarkerVersionID string `xml:"DeleteMarkerVersionId,omitempty"`
}

// DeleteError represents a delete error
type DeleteError struct {
	XMLName   xml.Name `xml:"Error"`
	Key       string   `xml:"Key"`
	VersionID string   `xml:"VersionId,omitempty"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
}
