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
	"net/url"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/fogfish/dynamo/v2"
)

// DynamoDB declares interface of original AWS DynamoDB API used by the library
type DynamoDB interface {
	GetItem(context.Context, *dynamodb.GetItemInput, ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	PutItem(context.Context, *dynamodb.PutItemInput, ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	DeleteItem(context.Context, *dynamodb.DeleteItemInput, ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
	UpdateItem(context.Context, *dynamodb.UpdateItemInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
	Query(context.Context, *dynamodb.QueryInput, ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
	BatchGetItem(context.Context, *dynamodb.BatchGetItemInput, ...func(*dynamodb.Options)) (*dynamodb.BatchGetItemOutput, error)
}

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
func New[T dynamo.Thing](connector string, opts ...dynamo.Option) (*Storage[T], error) {
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
		return nil, errInvalidConnectorURL.New(nil, connector)
	}

	seq := uri.Segments()
	table = &seq[0]
	if len(seq) > 1 {
		index = &seq[1]
	}

	return &Storage[T]{
		service: aws,
		table:   table,
		index:   index,
		codec:   newCodec[T](uri),
		schema:  newSchema[T](),
	}, nil
}

func newService(conf *dynamo.Config) (DynamoDB, error) {
	if conf.Service != nil {
		service, ok := conf.Service.(DynamoDB)
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
