package api

import (
	"fmt"
)

// S3Error represents an S3 API error
type S3Error interface {
	Code() string
	Message() string
	StatusCode() int
	error
}

type s3Error struct {
	code       string
	message    string
	statusCode int
}

func (e *s3Error) Code() string      { return e.code }
func (e *s3Error) Message() string   { return e.message }
func (e *s3Error) StatusCode() int    { return e.statusCode }
func (e *s3Error) Error() string     { return fmt.Sprintf("%s: %s", e.code, e.message) }

// Common S3 errors
var (
	ErrInternal = &s3Error{
		code:       "InternalError",
		message:    "An internal error occurred.",
		statusCode: 500,
	}

	ErrInvalidURI = &s3Error{
		code:       "InvalidURI",
		message:    "The specified URI could not be parsed.",
		statusCode: 400,
	}

	ErrMethodNotAllowed = &s3Error{
		code:       "MethodNotAllowed",
		message:    "The specified method is not allowed.",
		statusCode: 405,
	}

	ErrNoSuchBucket = &s3Error{
		code:       "NoSuchBucket",
		message:    "The specified bucket does not exist.",
		statusCode: 404,
	}

	ErrNoSuchKey = &s3Error{
		code:       "NoSuchKey",
		message:    "The specified key does not exist.",
		statusCode: 404,
	}

	ErrNoSuchUpload = &s3Error{
		code:       "NoSuchUpload",
		message:    "The specified multipart upload does not exist.",
		statusCode: 404,
	}

	ErrBucketNotEmpty = &s3Error{
		code:       "BucketNotEmpty",
		message:    "The bucket you tried to delete is not empty.",
		statusCode: 409,
	}

	ErrInvalidBucketName = &s3Error{
		code:       "InvalidBucketName",
		message:    "The specified bucket name is invalid.",
		statusCode: 400,
	}

	ErrInvalidObjectName = &s3Error{
		code:       "InvalidObjectName",
		message:    "The specified object name is invalid.",
		statusCode: 400,
	}

	ErrInvalidArgument = &s3Error{
		code:       "InvalidArgument",
		message:    "An invalid argument was specified.",
		statusCode: 400,
	}

	ErrAccessDenied = &s3Error{
		code:       "AccessDenied",
		message:    "Access Denied.",
		statusCode: 403,
	}

	ErrSignatureDoesNotMatch = &s3Error{
		code:       "SignatureDoesNotMatch",
		message:    "The request signature we calculated does not match the signature you provided.",
		statusCode: 403,
	}

	ErrMalformedXML = &s3Error{
		code:       "MalformedXML",
		message:    "The XML you provided was not well-formed or did not validate against our published schema.",
		statusCode: 400,
	}

	ErrMissingContentLength = &s3Error{
		code:       "MissingContentLength",
		message:    "You must provide the Content-Length HTTP header.",
		statusCode: 411,
	}

	ErrInvalidContentLength = &s3Error{
		code:       "InvalidContentLength",
		message:    "The Content-Length HTTP header was not specified or is invalid.",
		statusCode: 400,
	}

	ErrPreconditionFailed = &s3Error{
		code:       "PreconditionFailed",
		message:    "At least one of the preconditions you specified did not hold.",
		statusCode: 412,
	}

	ErrNotImplemented = &s3Error{
		code:       "NotImplemented",
		message:    "A header you provided implies functionality that is not implemented.",
		statusCode: 501,
	}

	ErrTooManyBuckets = &s3Error{
		code:       "TooManyBuckets",
		message:    "You have attempted to create more buckets than allowed.",
		statusCode: 400,
	}

	ErrBucketAlreadyExists = &s3Error{
		code:       "BucketAlreadyExists",
		message:    "The bucket you tried to create already exists.",
		statusCode: 409,
	}

	ErrBucketAlreadyOwnedByYou = &s3Error{
		code:       "BucketAlreadyOwnedByYou",
		message:    "The bucket you tried to create already exists and you own it.",
		statusCode: 409,
	}

	ErrMaxMessageLengthExceeded = &s3Error{
		code:       "MaxMessageLengthExceeded",
		message:    "Your request was too large.",
		statusCode: 400,
	}

	ErrMaxUploadLengthExceeded = &s3Error{
		code:       "MaxUploadLengthExceeded",
		message:    "Your upload exceeds the maximum allowed object size.",
		statusCode: 400,
	}

	ErrEntityTooSmall = &s3Error{
		code:       "EntityTooSmall",
		message:    "Your proposed upload is smaller than the minimum allowed object size.",
		statusCode: 400,
	}

	ErrEntityTooLarge = &s3Error{
		code:       "EntityTooLarge",
		message:    "Your proposed upload exceeds the maximum allowed object size.",
		statusCode: 400,
	}

	ErrInvalidRequest = &s3Error{
		code:       "InvalidRequest",
		message:    "The request is invalid.",
		statusCode: 400,
	}
)
