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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/fogfish/curie"
	"github.com/fogfish/opts"
)

// S3 declares AWS API used by the library
type S3 interface {
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	DeleteObject(context.Context, *s3.DeleteObjectInput, ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

// Option type to configure the S3
type Option = opts.Option[Options]

// Config Options
type Options struct {
	prefixes curie.Prefixes
	service  S3
}

func (c *Options) checkRequired() error {
	return opts.Required(c,
		WithS3(nil),
	)
}

var (
	// Configure CURIE prefixes
	WithPrefixes = opts.ForType[Options, curie.Prefixes]()

	// Set DynamoDB client for the client
	WithService = opts.ForType[Options, S3]()

	// Set DynamoDB client for the client
	WithS3 = opts.ForType[Options, S3]()

	// Configure client's DynamoDB to use provided the aws.Config
	WithConfig = opts.FMap(optsFromConfig)

	// Use default aws.Config for all DynamoDB clients
	WithDefaultS3 = opts.From(optsDefaultS3)
)

// NewConfig creates Config with default options
func optsDefault() Options {
	return Options{
		prefixes: curie.Namespaces{},
	}
}

func optsDefaultS3(c *Options) error {
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return err
	}
	return optsFromConfig(c, cfg)
}

func optsFromConfig(c *Options, cfg aws.Config) error {
	if c.service == nil {
		c.service = s3.NewFromConfig(cfg)
	}
	return nil
}
