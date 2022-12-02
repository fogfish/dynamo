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

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// Get item from storage
func (db *Storage[T]) Get(ctx context.Context, key T) (T, error) {
	gen, err := db.codec.EncodeKey(key)
	if err != nil {
		return db.undefined, errInvalidKey.New(err)
	}

	req := &dynamodb.GetItemInput{
		Key:                      gen,
		TableName:                db.table,
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
