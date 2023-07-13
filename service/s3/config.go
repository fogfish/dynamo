//
// Copyright (C) 2022 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

package s3

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/fogfish/curie"
)

// S3 declares AWS API used by the library
type S3 interface {
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	DeleteObject(context.Context, *s3.DeleteObjectInput, ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

// Option type to configure the S3
type Option func(*Config)

// Config Options
type Config struct {
	prefixes curie.Prefixes
	bucket   string
	service  S3
}

// NewConfig creates Config with default options
func defaultConfig() *Config {
	return &Config{
		prefixes: curie.Namespaces{},
	}
}

// WithPrefixes defines prefixes for CURIEs
func WithPrefixes(prefixes curie.Prefixes) Option {
	return func(c *Config) {
		c.prefixes = prefixes
	}
}

// WithBucket defined bucket for I/O
func WithBucket(bucket string) Option {
	return func(c *Config) {
		c.bucket = bucket
	}
}

// Configure AWS Service for broker instance
func WithService(service S3) Option {
	return func(c *Config) {
		c.service = service
	}
}
