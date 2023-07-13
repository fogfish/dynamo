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

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/fogfish/dynamo/v3"
)

// Storage type
type Storage[T dynamo.Thing] struct {
	service   DynamoDB
	table     *string
	index     *string
	codec     *codec[T]
	schema    *schema[T]
	undefined T
}

func Must[T dynamo.Thing](keyval *Storage[T], err error) *Storage[T] {
	if err != nil {
		panic(err)
	}

	return keyval
}

// New creates instance of DynamoDB api
func New[T dynamo.Thing](opts ...Option) (*Storage[T], error) {
	conf := defaultOptions()
	for _, opt := range opts {
		opt(conf)
	}

	aws, err := newService(conf)
	if err != nil {
		return nil, err
	}

	table := conf.table
	if table == "" {
		return nil, errUndefinedTable.New(nil)
	}

	var index *string
	if conf.index != "" {
		index = &conf.index
	}

	return &Storage[T]{
		service: aws,
		table:   &table,
		index:   index,
		codec:   newCodec[T](conf),
		schema:  newSchema[T](conf.useStrictType),
	}, nil
}

func newService(conf *Options) (DynamoDB, error) {
	if conf.service != nil {
		return conf.service, nil
	}

	aws, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, err
	}

	return dynamodb.NewFromConfig(aws), nil
}
