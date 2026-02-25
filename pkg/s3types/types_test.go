package s3types

import (
	"encoding/xml"
	"testing"
)

func TestErrorXML(t *testing.T) {
	s3err := Error{
		Code:      "NoSuchBucket",
		Message:   "The specified bucket does not exist",
		Resource:  "/mybucket",
		RequestID: "12345",
	}

	data, err := xml.Marshal(s3err)
	if err != nil {
		t.Errorf("Failed to marshal Error: %v", err)
	}

	var unmarshaled Error
	if err := xml.Unmarshal(data, &unmarshaled); err != nil {
		t.Errorf("Failed to unmarshal Error: %v", err)
	}

	if unmarshaled.Code != s3err.Code {
		t.Errorf("Code mismatch: %s != %s", unmarshaled.Code, s3err.Code)
	}
}

func TestListAllMyBucketsResultXML(t *testing.T) {
	result := ListAllMyBucketsResult{
		Owner: &Owner{
			ID:          "owner-id",
			DisplayName: "owner-name",
		},
		Buckets: &Buckets{
			Bucket: []Bucket{
				{Name: "bucket1", CreationDate: "2024-01-01"},
				{Name: "bucket2", CreationDate: "2024-01-02"},
			},
		},
	}

	data, err := xml.Marshal(result)
	if err != nil {
		t.Errorf("Failed to marshal ListAllMyBucketsResult: %v", err)
	}

	if len(data) == 0 {
		t.Error("Marshaled data is empty")
	}
}

func TestOwnerXML(t *testing.T) {
	owner := Owner{
		ID:          "owner-id",
		DisplayName: "owner-name",
	}

	data, err := xml.Marshal(owner)
	if err != nil {
		t.Errorf("Failed to marshal Owner: %v", err)
	}

	var unmarshaled Owner
	if err := xml.Unmarshal(data, &unmarshaled); err != nil {
		t.Errorf("Failed to unmarshal Owner: %v", err)
	}

	if unmarshaled.ID != owner.ID {
		t.Errorf("ID mismatch: %s != %s", unmarshaled.ID, owner.ID)
	}
}

func TestBucketXML(t *testing.T) {
	bucket := Bucket{
		Name:         "mybucket",
		CreationDate: "2024-01-01",
	}

	data, err := xml.Marshal(bucket)
	if err != nil {
		t.Errorf("Failed to marshal Bucket: %v", err)
	}

	var unmarshaled Bucket
	if err := xml.Unmarshal(data, &unmarshaled); err != nil {
		t.Errorf("Failed to unmarshal Bucket: %v", err)
	}

	if unmarshaled.Name != bucket.Name {
		t.Errorf("Name mismatch: %s != %s", unmarshaled.Name, bucket.Name)
	}
}

func TestListObjectsV2OutputXML(t *testing.T) {
	output := ListObjectsV2Output{
		Name:        "mybucket",
		Prefix:      "prefix/",
		MaxKeys:     "1000",
		KeyCount:    "2",
		IsTruncated: false,
		Contents: []Object{
			{Key: "file1.txt", Size: "100", ETag: "\"etag1\""},
			{Key: "file2.txt", Size: "200", ETag: "\"etag2\""},
		},
		CommonPrefixes: []string{"prefix/subdir/"},
	}

	data, err := xml.Marshal(output)
	if err != nil {
		t.Errorf("Failed to marshal ListObjectsV2Output: %v", err)
	}

	if len(data) == 0 {
		t.Error("Marshaled data is empty")
	}
}

func TestObjectXML(t *testing.T) {
	obj := Object{
		Key:          "test.txt",
		LastModified: "2024-01-01T00:00:00Z",
		ETag:         "\"abc123\"",
		Size:         "1024",
		StorageClass: "STANDARD",
		Owner: &Owner{
			ID:          "owner-id",
			DisplayName: "owner",
		},
	}

	data, err := xml.Marshal(obj)
	if err != nil {
		t.Errorf("Failed to marshal Object: %v", err)
	}

	var unmarshaled Object
	if err := xml.Unmarshal(data, &unmarshaled); err != nil {
		t.Errorf("Failed to unmarshal Object: %v", err)
	}

	if unmarshaled.Key != obj.Key {
		t.Errorf("Key mismatch: %s != %s", unmarshaled.Key, obj.Key)
	}
}

func TestInitiateMultipartUploadResultXML(t *testing.T) {
	result := InitiateMultipartUploadResult{
		Bucket:   "mybucket",
		Key:      "largefile.bin",
		UploadID: "upload-123",
	}

	data, err := xml.Marshal(result)
	if err != nil {
		t.Errorf("Failed to marshal InitiateMultipartUploadResult: %v", err)
	}

	var unmarshaled InitiateMultipartUploadResult
	if err := xml.Unmarshal(data, &unmarshaled); err != nil {
		t.Errorf("Failed to unmarshal: %v", err)
	}

	if unmarshaled.Bucket != result.Bucket {
		t.Errorf("Bucket mismatch")
	}
}

func TestCompleteMultipartUploadInputXML(t *testing.T) {
	input := CompleteMultipartUploadInput{
		Parts: []Part{
			{PartNumber: 1, ETag: "\"etag1\""},
			{PartNumber: 2, ETag: "\"etag2\""},
		},
	}

	data, err := xml.Marshal(input)
	if err != nil {
		t.Errorf("Failed to marshal CompleteMultipartUploadInput: %v", err)
	}

	var unmarshaled CompleteMultipartUploadInput
	if err := xml.Unmarshal(data, &unmarshaled); err != nil {
		t.Errorf("Failed to unmarshal: %v", err)
	}

	if len(unmarshaled.Parts) != len(input.Parts) {
		t.Errorf("Parts length mismatch")
	}
}

func TestPartXML(t *testing.T) {
	part := Part{
		PartNumber: 1,
		ETag:       "\"etag123\"",
	}

	data, err := xml.Marshal(part)
	if err != nil {
		t.Errorf("Failed to marshal Part: %v", err)
	}

	var unmarshaled Part
	if err := xml.Unmarshal(data, &unmarshaled); err != nil {
		t.Errorf("Failed to unmarshal Part: %v", err)
	}

	if unmarshaled.PartNumber != part.PartNumber {
		t.Errorf("PartNumber mismatch")
	}
}

func TestCompleteMultipartUploadResultXML(t *testing.T) {
	result := CompleteMultipartUploadResult{
		Location:  "http://mybucket.s3.amazonaws.com/largefile.bin",
		Bucket:    "mybucket",
		Key:       "largefile.bin",
		ETag:      "\"combined-etag\"",
		RequestID: "req-123",
	}

	data, err := xml.Marshal(result)
	if err != nil {
		t.Errorf("Failed to marshal: %v", err)
	}

	if len(data) == 0 {
		t.Error("Marshaled data is empty")
	}
}

func TestListPartsOutputXML(t *testing.T) {
	output := ListPartsOutput{
		Bucket:       "mybucket",
		Key:          "largefile.bin",
		UploadID:     "upload-123",
		StorageClass: "STANDARD",
		IsTruncated:  false,
		Parts: []Part{
			{PartNumber: 1, ETag: "\"etag1\""},
		},
	}

	data, err := xml.Marshal(output)
	if err != nil {
		t.Errorf("Failed to marshal: %v", err)
	}

	if len(data) == 0 {
		t.Error("Marshaled data is empty")
	}
}

func TestListMultipartUploadsOutputXML(t *testing.T) {
	output := ListMultipartUploadsOutput{
		Bucket:         "mybucket",
		KeyMarker:      "",
		UploadIDMarker: "",
		MaxUploads:     "1000",
		IsTruncated:    false,
		Upload: []Upload{
			{
				Key:          "file1.bin",
				UploadID:     "upload-1",
				Initiated:    "2024-01-01T00:00:00Z",
				StorageClass: "STANDARD",
			},
		},
		CommonPrefixes: []string{"prefix/"},
	}

	data, err := xml.Marshal(output)
	if err != nil {
		t.Errorf("Failed to marshal: %v", err)
	}

	if len(data) == 0 {
		t.Error("Marshaled data is empty")
	}
}

func TestUploadXML(t *testing.T) {
	upload := Upload{
		Key:          "file.bin",
		UploadID:     "upload-123",
		Initiated:    "2024-01-01T00:00:00Z",
		StorageClass: "STANDARD",
		Initiator: &Owner{
			ID:          "initiator-id",
			DisplayName: "initiator",
		},
		Owner: &Owner{
			ID:          "owner-id",
			DisplayName: "owner",
		},
	}

	data, err := xml.Marshal(upload)
	if err != nil {
		t.Errorf("Failed to marshal: %v", err)
	}

	var unmarshaled Upload
	if err := xml.Unmarshal(data, &unmarshaled); err != nil {
		t.Errorf("Failed to unmarshal: %v", err)
	}

	if unmarshaled.Key != upload.Key {
		t.Errorf("Key mismatch")
	}
}

func TestCopyObjectResultXML(t *testing.T) {
	result := CopyObjectResult{
		LastModified:         "2024-01-01T00:00:00Z",
		ETag:                 "\"etag123\"",
		RequestID:            "req-123",
		ServerSideEncryption: "AES256",
	}

	data, err := xml.Marshal(result)
	if err != nil {
		t.Errorf("Failed to marshal: %v", err)
	}

	if len(data) == 0 {
		t.Error("Marshaled data is empty")
	}
}

func TestCreateBucketConfigurationXML(t *testing.T) {
	config := CreateBucketConfiguration{
		LocationConstraint: "us-west-2",
	}

	data, err := xml.Marshal(config)
	if err != nil {
		t.Errorf("Failed to marshal: %v", err)
	}

	var unmarshaled CreateBucketConfiguration
	if err := xml.Unmarshal(data, &unmarshaled); err != nil {
		t.Errorf("Failed to unmarshal: %v", err)
	}

	if unmarshaled.LocationConstraint != config.LocationConstraint {
		t.Errorf("LocationConstraint mismatch")
	}
}

func TestGetBucketLocationOutputXML(t *testing.T) {
	output := GetBucketLocationOutput{
		Region: "us-west-2",
	}

	data, err := xml.Marshal(output)
	if err != nil {
		t.Errorf("Failed to marshal: %v", err)
	}

	var unmarshaled GetBucketLocationOutput
	if err := xml.Unmarshal(data, &unmarshaled); err != nil {
		t.Errorf("Failed to unmarshal: %v", err)
	}

	if unmarshaled.Region != output.Region {
		t.Errorf("Region mismatch")
	}
}

func TestGetBucketVersioningOutputXML(t *testing.T) {
	output := GetBucketVersioningOutput{
		Status:    "Enabled",
		MFADelete: "Disabled",
	}

	data, err := xml.Marshal(output)
	if err != nil {
		t.Errorf("Failed to marshal: %v", err)
	}

	var unmarshaled GetBucketVersioningOutput
	if err := xml.Unmarshal(data, &unmarshaled); err != nil {
		t.Errorf("Failed to unmarshal: %v", err)
	}

	if unmarshaled.Status != output.Status {
		t.Errorf("Status mismatch")
	}
}

func TestChecksumXML(t *testing.T) {
	checksum := Checksum{
		ChecksumSHA1:   "sha1hash",
		ChecksumSHA256: "sha256hash",
		ChecksumCRC32:  "crc32hash",
	}

	data, err := xml.Marshal(checksum)
	if err != nil {
		t.Errorf("Failed to marshal: %v", err)
	}

	var unmarshaled Checksum
	if err := xml.Unmarshal(data, &unmarshaled); err != nil {
		t.Errorf("Failed to unmarshal: %v", err)
	}

	if unmarshaled.ChecksumSHA256 != checksum.ChecksumSHA256 {
		t.Errorf("ChecksumSHA256 mismatch")
	}
}

func TestObjectPartsXML(t *testing.T) {
	parts := ObjectParts{
		TotalPartsCount: 2,
		IsTruncated:     false,
		Parts: []Part{
			{PartNumber: 1, ETag: "\"etag1\""},
			{PartNumber: 2, ETag: "\"etag2\""},
		},
	}

	data, err := xml.Marshal(parts)
	if err != nil {
		t.Errorf("Failed to marshal: %v", err)
	}

	var unmarshaled ObjectParts
	if err := xml.Unmarshal(data, &unmarshaled); err != nil {
		t.Errorf("Failed to unmarshal: %v", err)
	}

	if unmarshaled.TotalPartsCount != parts.TotalPartsCount {
		t.Errorf("TotalPartsCount mismatch")
	}
}

func TestSelectObjectContentRequestXML(t *testing.T) {
	req := SelectObjectContentRequest{
		Expression:     "SELECT * FROM s3object s WHERE s._1 > 100",
		ExpressionType: "SQL",
		InputSerialization: InputSerialization{
			CSV: &CSVInput{
				FileHeaderInfo: "USE",
			},
		},
		OutputSerialization: OutputSerialization{
			JSON: &JSONOutput{
				RecordDelimiter: "\n",
			},
		},
		RequestProgress: RequestProgress{
			Enabled: true,
		},
	}

	data, err := xml.Marshal(req)
	if err != nil {
		t.Errorf("Failed to marshal: %v", err)
	}

	if len(data) == 0 {
		t.Error("Marshaled data is empty")
	}
}

func TestStatsDetailsXML(t *testing.T) {
	stats := StatsDetails{
		BytesScanned:   1000,
		BytesProcessed: 800,
		BytesReturned:  500,
	}

	data, err := xml.Marshal(stats)
	if err != nil {
		t.Errorf("Failed to marshal: %v", err)
	}

	var unmarshaled StatsDetails
	if err := xml.Unmarshal(data, &unmarshaled); err != nil {
		t.Errorf("Failed to unmarshal: %v", err)
	}

	if unmarshaled.BytesScanned != stats.BytesScanned {
		t.Errorf("BytesScanned mismatch")
	}
}

func TestProgressDetailsXML(t *testing.T) {
	progress := ProgressDetails{
		BytesScanned:   500,
		BytesProcessed: 400,
		BytesReturned:  300,
	}

	data, err := xml.Marshal(progress)
	if err != nil {
		t.Errorf("Failed to marshal: %v", err)
	}

	var unmarshaled ProgressDetails
	if err := xml.Unmarshal(data, &unmarshaled); err != nil {
		t.Errorf("Failed to unmarshal: %v", err)
	}

	if unmarshaled.BytesScanned != progress.BytesScanned {
		t.Errorf("BytesScanned mismatch")
	}
}

func TestEndEventXML(t *testing.T) {
	event := EndEvent{}

	data, err := xml.Marshal(event)
	if err != nil {
		t.Errorf("Failed to marshal: %v", err)
	}

	var unmarshaled EndEvent
	if err := xml.Unmarshal(data, &unmarshaled); err != nil {
		t.Errorf("Failed to unmarshal: %v", err)
	}
}
