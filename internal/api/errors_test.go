package api

import (
	"net/http"
	"testing"
)

func TestS3ErrorAllMethods(t *testing.T) {
	err := &s3Error{
		code:       "TestCode",
		message:    "Test message",
		statusCode: 400,
	}

	if err.Code() != "TestCode" {
		t.Errorf("Code() = %v, want TestCode", err.Code())
	}

	if err.Message() != "Test message" {
		t.Errorf("Message() = %v, want Test message", err.Message())
	}

	if err.StatusCode() != 400 {
		t.Errorf("StatusCode() = %v, want 400", err.StatusCode())
	}

	expectedError := "TestCode: Test message"
	if err.Error() != expectedError {
		t.Errorf("Error() = %v, want %v", err.Error(), expectedError)
	}
}

func TestAllS3ErrorsComplete(t *testing.T) {
	tests := []struct {
		name       string
		err        *s3Error
		wantCode   string
		wantStatus int
		wantMsg    string
	}{
		{"InternalError", ErrInternal, "InternalError", http.StatusInternalServerError, "An internal error occurred."},
		{"InvalidURI", ErrInvalidURI, "InvalidURI", http.StatusBadRequest, "The specified URI could not be parsed."},
		{"MethodNotAllowed", ErrMethodNotAllowed, "MethodNotAllowed", http.StatusMethodNotAllowed, "The specified method is not allowed."},
		{"NoSuchBucket", ErrNoSuchBucket, "NoSuchBucket", http.StatusNotFound, "The specified bucket does not exist."},
		{"NoSuchKey", ErrNoSuchKey, "NoSuchKey", http.StatusNotFound, "The specified key does not exist."},
		{"InvalidObjectState", ErrInvalidObjectState, "InvalidObjectState", http.StatusBadRequest, "The operation is not valid for the object's storage class."},
		{"OwnershipControlsNotFound", ErrOwnershipControlsNotFound, "OwnershipControlsNotFound", http.StatusNotFound, "The ownership controls for this bucket do not exist."},
		{"MetricsNotFound", ErrMetricsNotFound, "MetricsNotFound", http.StatusNotFound, "The metrics configuration for this bucket does not exist."},
		{"ReplicationNotFound", ErrReplicationNotFound, "ReplicationNotFound", http.StatusNotFound, "The replication configuration for this bucket does not exist."},
		{"ObjectRetentionNotFound", ErrObjectRetentionNotFound, "ObjectRetentionNotFound", http.StatusNotFound, "The object retention configuration does not exist."},
		{"ObjectLegalHoldNotFound", ErrObjectLegalHoldNotFound, "ObjectLegalHoldNotFound", http.StatusNotFound, "The object legal hold does not exist."},
		{"NoSuchUpload", ErrNoSuchUpload, "NoSuchUpload", http.StatusNotFound, "The specified multipart upload does not exist."},
		{"BucketNotEmpty", ErrBucketNotEmpty, "BucketNotEmpty", http.StatusConflict, "The bucket you tried to delete is not empty."},
		{"InvalidBucketName", ErrInvalidBucketName, "InvalidBucketName", http.StatusBadRequest, "The specified bucket name is invalid."},
		{"InvalidObjectName", ErrInvalidObjectName, "InvalidObjectName", http.StatusBadRequest, "The specified object name is invalid."},
		{"InvalidArgument", ErrInvalidArgument, "InvalidArgument", http.StatusBadRequest, "An invalid argument was specified."},
		{"AccessDenied", ErrAccessDenied, "AccessDenied", http.StatusForbidden, "Access Denied."},
		{"SignatureDoesNotMatch", ErrSignatureDoesNotMatch, "SignatureDoesNotMatch", http.StatusForbidden, "The request signature we calculated does not match the signature you provided."},
		{"MalformedXML", ErrMalformedXML, "MalformedXML", http.StatusBadRequest, "The XML you provided was not well-formed or did not validate against our published schema."},
		{"MissingContentLength", ErrMissingContentLength, "MissingContentLength", http.StatusLengthRequired, "You must provide the Content-Length HTTP header."},
		{"InvalidContentLength", ErrInvalidContentLength, "InvalidContentLength", http.StatusBadRequest, "The Content-Length HTTP header was not specified or is invalid."},
		{"PreconditionFailed", ErrPreconditionFailed, "PreconditionFailed", http.StatusPreconditionFailed, "At least one of the preconditions you specified did not hold."},
		{"NotImplemented", ErrNotImplemented, "NotImplemented", http.StatusNotImplemented, "A header you provided implies functionality that is not implemented."},
		{"TooManyBuckets", ErrTooManyBuckets, "TooManyBuckets", http.StatusBadRequest, "You have attempted to create more buckets than allowed."},
		{"BucketAlreadyExists", ErrBucketAlreadyExists, "BucketAlreadyExists", http.StatusConflict, "The bucket you tried to create already exists."},
		{"BucketAlreadyOwnedByYou", ErrBucketAlreadyOwnedByYou, "BucketAlreadyOwnedByYou", http.StatusConflict, "The bucket you tried to create already exists and you own it."},
		{"MaxMessageLengthExceeded", ErrMaxMessageLengthExceeded, "MaxMessageLengthExceeded", http.StatusBadRequest, "Your request was too large."},
		{"MaxUploadLengthExceeded", ErrMaxUploadLengthExceeded, "MaxUploadLengthExceeded", http.StatusBadRequest, "Your upload exceeds the maximum allowed object size."},
		{"EntityTooSmall", ErrEntityTooSmall, "EntityTooSmall", http.StatusBadRequest, "Your proposed upload is smaller than the minimum allowed object size."},
		{"EntityTooLarge", ErrEntityTooLarge, "EntityTooLarge", http.StatusBadRequest, "Your proposed upload exceeds the maximum allowed object size."},
		{"InvalidRequest", ErrInvalidRequest, "InvalidRequest", http.StatusBadRequest, "The request is invalid."},
		{"InvalidAccelerateConfiguration", ErrInvalidAccelerateConfiguration, "InvalidAccelerateConfiguration", http.StatusBadRequest, "The accelerate configuration is invalid."},
		{"InventoryNotFound", ErrInventoryNotFound, "InventoryConfigurationNotFoundError", http.StatusNotFound, "The specified inventory configuration does not exist."},
		{"AnalyticsNotFound", ErrAnalyticsNotFound, "AnalyticsConfigurationNotFoundError", http.StatusNotFound, "The specified analytics configuration does not exist."},
		{"PresignedURLExpired", ErrPresignedURLExpired, "PresignedURLExpired", http.StatusForbidden, "The presigned URL has expired."},
		{"PresignedURLNotFound", ErrPresignedURLNotFound, "PresignedURLNotFoundError", http.StatusNotFound, "The specified presigned URL does not exist."},
		{"InvalidPresignedURL", ErrInvalidPresignedURL, "InvalidPresignedURL", http.StatusBadRequest, "The presigned URL is invalid."},
		{"WebsiteNotFound", ErrWebsiteNotFound, "NoSuchWebsiteConfiguration", http.StatusNotFound, "The specified bucket website configuration does not exist."},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.err.Code() != tt.wantCode {
				t.Errorf("Code() = %v, want %v", tt.err.Code(), tt.wantCode)
			}
			if tt.err.Message() != tt.wantMsg {
				t.Errorf("Message() = %v, want %v", tt.err.Message(), tt.wantMsg)
			}
			if tt.err.StatusCode() != tt.wantStatus {
				t.Errorf("StatusCode() = %v, want %v", tt.err.StatusCode(), tt.wantStatus)
			}
		})
	}
}

func TestS3ErrorInterfaceAssertion(t *testing.T) {
	var _ S3Error = ErrInternal
	var _ S3Error = ErrNoSuchBucket
	var _ S3Error = ErrAccessDenied
	var _ S3Error = &s3Error{}

	var err error = ErrNoSuchBucket
	s3Err, ok := err.(S3Error)
	if !ok {
		t.Error("error should implement S3Error interface")
	}
	if s3Err.Code() != "NoSuchBucket" {
		t.Errorf("Code() = %v, want NoSuchBucket", s3Err.Code())
	}
}

func TestS3ErrorFormat(t *testing.T) {
	err := ErrNoSuchBucket
	errStr := err.Error()
	if errStr == "" {
		t.Error("Error() should not return empty string")
	}
	if len(errStr) < len(ErrNoSuchBucket.code)+len(ErrNoSuchBucket.message) {
		t.Error("Error() should contain code and message")
	}
}
