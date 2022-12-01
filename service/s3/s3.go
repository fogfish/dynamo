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
	"net/url"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/fogfish/dynamo/v2"
)

type Storage[T dynamo.Thing] struct {
	service   dynamo.S3
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
func New[T dynamo.Thing](connector string, opts ...dynamo.Option) (*Storage[T], error) {
	conf := dynamo.NewConfig()
	for _, opt := range opts {
		opt(&conf)
	}

	aws, err := newService(&conf)
	if err != nil {
		return nil, err
	}

	var bucket *string
	uri, err := newURI(connector)
	if err != nil || len(uri.Path) < 2 {
		return nil, errInvalidConnectorURL.New(nil, connector)
	}

	seq := uri.Segments()
	bucket = &seq[0]

	return &Storage[T]{
		service: aws,
		bucket:  bucket,
		codec:   newCodec[T](conf.Prefixes),
		schema:  newSchema[T](),
	}, nil
}

func newService(conf *dynamo.Config) (dynamo.S3, error) {
	if conf.Service != nil {
		service, ok := conf.Service.(dynamo.S3)
		if ok {
			return service, nil
		}
	}

	aws, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, err
	}

	return s3.NewFromConfig(aws), nil
}

func newURI(uri string) (*dynamo.URL, error) {
	spec, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}

	return (*dynamo.URL)(spec), nil
}
