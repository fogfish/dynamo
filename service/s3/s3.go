//
// Copyright (C) 2019 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

package s3

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/fogfish/dynamo/v2"
)

type Storage[T dynamo.Thing] struct {
	service   S3
	bucket    *string
	codec     *codec[T]
	schema    *schema[T]
	undefined T
}

// Must constraint for api factory
func Must[T dynamo.Thing](keyval *Storage[T], err error) *Storage[T] {
	if err != nil {
		panic(err)
	}

	return keyval
}

// New creates instance of S3 api
func New[T dynamo.Thing](opts ...Option) (*Storage[T], error) {
	conf := defaultConfig()
	for _, opt := range opts {
		opt(conf)
	}

	aws, err := newService(conf)
	if err != nil {
		return nil, err
	}

	bucket := conf.bucket
	if bucket == "" {
		return nil, errUndefinedBucket.New(nil)
	}

	return &Storage[T]{
		service: aws,
		bucket:  &bucket,
		codec:   newCodec[T](conf.prefixes),
		schema:  newSchema[T](),
	}, nil
}

func newService(conf *Config) (S3, error) {
	if conf.service != nil {
		return conf.service, nil
	}

	aws, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, err
	}

	return s3.NewFromConfig(aws), nil
}
