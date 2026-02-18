package s3types

import "encoding/xml"

// Common S3 XML types

// Error represents an S3 error response
type Error struct {
	Code      string `xml:"Code"`
	Message   string `xml:"Message"`
	Resource  string `xml:"Resource,omitempty"`
	RequestID string `xml:"RequestId,omitempty"`
}

// ListAllMyBucketsResult is the response for ListBuckets
type ListAllMyBucketsResult struct {
	XMLName   struct{}   `xml:"ListAllMyBucketsResult"`
	xmlns     string     `xml:"xmlns,attr"`
	Owner     *Owner     `xml:"Owner"`
	Buckets   *Buckets   `xml:"Buckets"`
}

// Owner represents bucket owner
type Owner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

// Buckets contains list of buckets
type Buckets struct {
	Bucket []Bucket `xml:"Bucket"`
}

// Bucket represents a bucket
type Bucket struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

// ListObjectsV2Output is the response for ListObjectsV2
type ListObjectsV2Output struct {
	XMLName               string  `xml:"ListObjectsV2Output"`
	xmlns                 string  `xml:"xmlns,attr"`
	Name                  string  `xml:"Name"`
	Prefix                string  `xml:"Prefix,omitempty"`
	Delimiter             string  `xml:"Delimiter,omitempty"`
	MaxKeys               string  `xml:"MaxKeys"`
	KeyCount              string  `xml:"KeyCount"`
	IsTruncated           bool    `xml:"IsTruncated"`
	Contents              []Object `xml:"Contents"`
	CommonPrefixes        []string `xml:"CommonPrefixes>Prefix"`
	NextContinuationToken string   `xml:"NextContinuationToken"`
}

// Object represents an object
type Object struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         string `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
	Owner        *Owner `xml:"Owner"`
}

// InitiateMultipartUploadResult is the response for InitiateMultipartUpload
type InitiateMultipartUploadResult struct {
	XMLName  string `xml:"InitiateMultipartUploadResult"`
	xmlns    string `xml:"xmlns,attr"`
	Bucket   string `xml:"Bucket"`
	Key      string `xml:"Key"`
	UploadID string `xml:"UploadId"`
}

// CompleteMultipartUploadInput is the request for CompleteMultipartUpload
type CompleteMultipartUploadInput struct {
	XMLName string   `xml:"CompleteMultipartUpload"`
	Parts   []Part  `xml:"Part"`
}

// Part represents a part in CompleteMultipartUpload
type Part struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

// CompleteMultipartUploadResult is the response for CompleteMultipartUpload
type CompleteMultipartUploadResult struct {
	XMLName   string `xml:"CompleteMultipartUploadResult"`
	Location  string `xml:"Location"`
	Bucket    string `xml:"Bucket"`
	Key       string `xml:"Key"`
	ETag      string `xml:"ETag"`
	RequestID string `xml:"RequestId"`
}

// ListPartsOutput is the response for ListParts
type ListPartsOutput struct {
	XMLName   string `xml:"ListPartsOutput"`
	xmlns     string `xml:"xmlns,attr"`
	Bucket    string `xml:"Bucket"`
	Key       string `xml:"Key"`
	UploadID  string `xml:"UploadId"`
	StorageClass string `xml:"StorageClass"`
	IsTruncated bool   `xml:"IsTruncated"`
	Parts     []Part `xml:"Part"`
}

// ListMultipartUploadsOutput is the response for ListMultipartUploads
type ListMultipartUploadsOutput struct {
	XMLName             string           `xml:"ListMultipartUploadsOutput"`
	xmlns               string           `xml:"xmlns,attr"`
	Bucket              string           `xml:"Bucket"`
	KeyMarker           string           `xml:"KeyMarker"`
	UploadIDMarker      string           `xml:"UploadIdMarker"`
	NextKeyMarker       string           `xml:"NextKeyMarker"`
	NextUploadIDMarker  string           `xml:"NextUploadIdMarker"`
	MaxUploads          string           `xml:"MaxUploads"`
	IsTruncated         bool             `xml:"IsTruncated"`
	Upload              []Upload         `xml:"Upload"`
	CommonPrefixes      []string         `xml:"CommonPrefixes>Prefix"`
}

// Upload represents a multipart upload
type Upload struct {
	Key       string `xml:"Key"`
	UploadID  string `xml:"UploadId"`
	Initiated string `xml:"Initiated"`
	Initiator *Owner `xml:"Initiator"`
	Owner     *Owner `xml:"Owner"`
	StorageClass string `xml:"StorageClass"`
}

// CopyObjectResult is the response for CopyObject
type CopyObjectResult struct {
	XMLName           string `xml:"CopyObjectResult"`
	xmlns             string `xml:"xmlns,attr"`
	LastModified      string `xml:"LastModified"`
	ETag              string `xml:"ETag"`
	RequestID         string `xml:"RequestId"`
	ServerSideEncryption string `xml:"ServerSideEncryption,omitempty"`
}

// CreateBucketConfiguration is the request for CreateBucket
type CreateBucketConfiguration struct {
	XMLName      string `xml:"CreateBucketConfiguration"`
	xmlns        string `xml:"xmlns,attr"`
	LocationConstraint string `xml:"LocationConstraint"`
}

// GetBucketLocationOutput is the response for GetBucketLocation
type GetBucketLocationOutput struct {
	XMLName string `xml:"GetBucketLocationOutput"`
	xmlns   string `xml:"xmlns,attr"`
	Region  string `xml:"LocationConstraint"`
}

// GetBucketVersioningOutput is the response for GetBucketVersioning
type GetBucketVersioningOutput struct {
	XMLName    string `xml:"GetBucketVersioningOutput"`
	xmlns      string `xml:"xmlns,attr"`
	Status     string `xml:"Status"`
	MFADelete  string `xml:"MFADelete"`
}

// PutBucketVersioningInput is the request for PutBucketVersioning
type PutBucketVersioningInput struct {
	XMLName    string            `xml:"VersioningConfiguration"`
	xmlns      string            `xml:"xmlns,attr"`
	Status     string            `xml:"Status"`
	MFADelete  string            `xml:"MFADelete"`
}

// GetBucketLifecycleOutput is the response for GetBucketLifecycle
type GetBucketLifecycleOutput struct {
	XMLName string          `xml:"GetBucketLifecycleOutput"`
	xmlns   string          `xml:"xmlns,attr"`
	Rules   []LifecycleRule `xml:"Rule"`
}

// PutBucketLifecycleInput is the request for PutBucketLifecycle
type PutBucketLifecycleInput struct {
	XMLName string          `xml:"LifecycleConfiguration"`
	xmlns   string          `xml:"xmlns,attr"`
	Rules   []LifecycleRule `xml:"Rule"`
}

// GetObjectAttributesOutput is the response for GetObjectAttributes
type GetObjectAttributesOutput struct {
	XMLName             string             `xml:"GetObjectAttributesOutput"`
	xmlns               string             `xml:"xmlns,attr"`
	ETag                string             `xml:"ETag"`
	Checksum            *Checksum          `xml:"Checksum"`
	ObjectParts         *ObjectParts       `xml:"ObjectParts,omitempty"`
	StorageClass        string             `xml:"StorageClass"`
	LastModified        string             `xml:"LastModified"`
	ObjectSize          string             `xml:"ObjectSize"`
	VersionId           string             `xml:"VersionId,omitempty"`
	RequestCharged      string             `xml:"RequestCharged,omitempty"`
	ServerSideEncryption string           `xml:"ServerSideEncryption,omitempty"`
}

// Checksum represents checksum information
type Checksum struct {
	ChecksumSHA1   string `xml:"ChecksumSHA1,omitempty"`
	ChecksumSHA256 string `xml:"ChecksumSHA256,omitempty"`
	ChecksumCRC32  string `xml:"ChecksumCRC32,omitempty"`
}

// ObjectParts represents object parts information
type ObjectParts struct {
	TotalPartsCount int     `xml:"TotalPartsCount"`
	IsTruncated     bool    `xml:"IsTruncated"`
	Parts           []Part  `xml:"Part"`
}

// SelectObjectContentRequest is the request for SelectObjectContent
type SelectObjectContentRequest struct {
	XMLName      xml.Name `xml:"SelectObjectContentRequest"`
	Expression   string   `xml:"Expression"`
	ExpressionType string `xml:"ExpressionType"`
	InputSerialization  InputSerialization  `xml:"InputSerialization"`
	OutputSerialization OutputSerialization `xml:"OutputSerialization"`
	RequestProgress RequestProgress `xml:"RequestProgress,omitempty"`
}

// InputSerialization defines input serialization
type InputSerialization struct {
	CSV    *CSVInput  `xml:"CSV,omitempty"`
	JSON   *JSONInput `xml:"JSON,omitempty"`
}

// CSVInput defines CSV input serialization
type CSVInput struct {
	FileHeaderInfo string `xml:"FileHeaderInfo"`
	RecordDelimiter string `xml:"RecordDelimiter,omitempty"`
	FieldDelimiter string `xml:"FieldDelimiter,omitempty"`
	QuoteCharacter string `xml:"QuoteCharacter,omitempty"`
}

// JSONInput defines JSON input serialization
type JSONInput struct {
	Type string `xml:"Type"`
}

// OutputSerialization defines output serialization
type OutputSerialization struct {
	CSV    *CSVOutput  `xml:"CSV,omitempty"`
	JSON   *JSONOutput `xml:"JSON,omitempty"`
}

// CSVOutput defines CSV output serialization
type CSVOutput struct {
	RecordDelimiter string `xml:"RecordDelimiter,omitempty"`
	FieldDelimiter string `xml:"FieldDelimiter,omitempty"`
	QuoteCharacter string `xml:"QuoteCharacter,omitempty"`
}

// JSONOutput defines JSON output serialization
type JSONOutput struct {
	RecordDelimiter string `xml:"RecordDelimiter,omitempty"`
}

// RequestProgress defines request progress
type RequestProgress struct {
	Enabled bool `xml:"Enabled"`
}

// SelectObjectContentOutput is the response for SelectObjectContent
type SelectObjectContentOutput struct {
	XMLName xml.Name `xml:"SelectObjectContentResult"`
	Payload SelectObjectContentPayload `xml:"Payload"`
}

// SelectObjectContentPayload contains the actual data
type SelectObjectContentPayload struct {
	Records    *RecordsEvent `xml:"Records,omitempty"`
	Stats      *StatsEvent   `xml:"Stats,omitempty"`
	Progress   *ProgressEvent `xml:"Progress,omitempty"`
	End        *EndEvent     `xml:"End,omitempty"`
}

// RecordsEvent contains data records
type RecordsEvent struct {
	Body string `xml:"Body"`
}

// StatsEvent contains statistics
type StatsEvent struct {
	Details StatsDetails `xml:"Details"`
}

// StatsDetails contains stats details
type StatsDetails struct {
	BytesScanned    int64 `xml:"BytesScanned"`
	BytesProcessed  int64 `xml:"BytesProcessed"`
	BytesReturned   int64 `xml:"BytesReturned"`
}

// ProgressEvent contains progress information
type ProgressEvent struct {
	Details ProgressDetails `xml:"Details"`
}

// ProgressDetails contains progress details
type ProgressDetails struct {
	BytesScanned    int64 `xml:"BytesScanned"`
	BytesProcessed  int64 `xml:"BytesProcessed"`
	BytesReturned   int64 `xml:"BytesReturned"`
}

// EndEvent indicates end of data
type EndEvent struct {
}
