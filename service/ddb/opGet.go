//
// Copyright (C) 2022 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

package ddb

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Get item from storage
func (db *Storage[T]) Get(ctx context.Context, key T, opts ...interface{ GetterOpt(T) }) (T, error) {
	gen, err := db.codec.EncodeKey(key)
	if err != nil {
		return db.undefined, errInvalidKey.New(err)
	}

	req := &dynamodb.GetItemInput{
		Key:                      gen,
		TableName:                aws.String(db.table),
		ProjectionExpression:     db.schema.Projection,
		ExpressionAttributeNames: db.schema.ExpectedAttributeNames,
	}

	val, err := db.service.GetItem(ctx, req)
	if err != nil {
		return db.undefined, errServiceIO.New(err)
	}

	if val.Item == nil {
		return db.undefined, errNotFound(nil, key)
	}

	obj, err := db.codec.Decode(val.Item)
	if err != nil {
		return db.undefined, errInvalidEntity.New(err)
	}

	return obj, nil
}

func (db *Storage[T]) BatchGet(ctx context.Context, keys []T, opts ...interface{ GetterOpt(T) }) ([]T, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	seq := make([]map[string]types.AttributeValue, len(keys))
	for i := 0; i < len(keys); i++ {
		gen, err := db.codec.EncodeKey(keys[i])
		if err != nil {
			return nil, errInvalidKey.New(err)
		}
		seq[i] = gen
	}

	req := &dynamodb.BatchGetItemInput{
		RequestItems: map[string]types.KeysAndAttributes{
			db.table: {
				Keys:                     seq,
				ProjectionExpression:     db.schema.Projection,
				ExpressionAttributeNames: db.schema.ExpectedAttributeNames,
			},
		},
	}

	val, err := db.service.BatchGetItem(ctx, req)
	if err != nil {
		return nil, errServiceIO.New(err)
	}

	rsp, exists := val.Responses[db.table]
	if !exists {
		return make([]T, 0), nil
	}

	items := make([]T, len(rsp))
	for i := 0; i < len(rsp); i++ {
		obj, err := db.codec.Decode(rsp[i])
		if err != nil {
			return nil, errInvalidEntity.New(err)
		}
		items[i] = obj
	}

	return items, nil
}
