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
	"fmt"
	"net/url"
	"runtime"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/fogfish/curie"
	"github.com/fogfish/dynamo"
	ds3 "github.com/fogfish/dynamo/internal/s3"
)

// Must constraint for api factory
func Must[T dynamo.Thing](keyval dynamo.KeyVal[T], err error) dynamo.KeyVal[T] {
	if err != nil {
		panic(err)
	}

	return keyval
}

// New creates instance of S3 api
func New[T dynamo.Thing](
	service dynamo.S3,
	connector string,
	prefixes curie.Prefixes,
) (dynamo.KeyVal[T], error) {
	aws, err := newService(service)
	if err != nil {
		return nil, err
	}

	var bucket *string
	uri, err := newURI(connector)
	if err != nil || len(uri.Path) < 2 {
		return nil, errInvalidConnectorURL(connector)
	}

	seq := uri.Segments()
	bucket = &seq[0]

	return &ds3.Storage[T]{
		Service: aws,
		Bucket:  bucket,
		Codec:   ds3.NewCodec[T](prefixes),
		Schema:  ds3.NewSchema[T](),
	}, nil
}

func newService(service dynamo.S3) (dynamo.S3, error) {
	if service != nil {
		return service, nil
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

func errInvalidConnectorURL(url string) error {
	var name string

	if pc, _, _, ok := runtime.Caller(1); ok {
		name = runtime.FuncForPC(pc).Name()
	}

	return fmt.Errorf("[%s] invalid connector url: %s", name, url)
}
