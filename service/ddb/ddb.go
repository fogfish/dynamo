//
// Copyright (C) 2019 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

package ddb

import (
	"context"
	"fmt"
	"net/url"
	"runtime"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/fogfish/dynamo/v2"
	"github.com/fogfish/dynamo/v2/internal/ddb"
)

func Must[T dynamo.Thing](keyval dynamo.KeyVal[T], err error) dynamo.KeyVal[T] {
	if err != nil {
		panic(err)
	}

	return keyval
}

// New creates instance of DynamoDB api
func New[T dynamo.Thing](connector string, opts ...dynamo.Option) (dynamo.KeyVal[T], error) {
	conf := dynamo.NewConfig()
	for _, opt := range opts {
		opt(&conf)
	}

	aws, err := newService(&conf)
	if err != nil {
		return nil, err
	}

	var table, index *string
	uri, err := newURI(connector)
	if err != nil || len(uri.Path) < 2 {
		return nil, errInvalidConnectorURL(connector)
	}

	seq := uri.Segments()
	table = &seq[0]
	if len(seq) > 1 {
		index = &seq[1]
	}

	return &ddb.Storage[T]{
		Service: aws,
		Table:   table,
		Index:   index,
		Codec:   ddb.NewCodec[T](uri),
		Schema:  ddb.NewSchema[T](),
	}, nil
}

func newService(conf *dynamo.Config) (dynamo.DynamoDB, error) {
	if conf.Service != nil {
		service, ok := conf.Service.(dynamo.DynamoDB)
		if ok {
			return service, nil
		}
	}

	aws, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, err
	}

	return dynamodb.NewFromConfig(aws), nil
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
