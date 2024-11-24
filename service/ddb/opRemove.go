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
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Remove discards the entity from the table
func (db *Storage[T]) Remove(ctx context.Context, key T, opts ...interface{ WriterOpt(T) }) (T, error) {
	gen, err := db.codec.EncodeKey(key)
	if err != nil {
		return db.undefined, errInvalidKey.New(err)
	}

	req := &dynamodb.DeleteItemInput{
		Key:          gen,
		TableName:    aws.String(db.table),
		ReturnValues: "ALL_OLD",
	}
	names, values := maybeConditionExpression(&req.ConditionExpression, opts)
	req.ExpressionAttributeValues = values
	req.ExpressionAttributeNames = names

	val, err := db.service.DeleteItem(ctx, req)
	if err != nil {
		if recoverConditionalCheckFailedException(err) {
			return db.undefined, errPreConditionFailed(err, key,
				strings.Contains(*req.ConditionExpression, "attribute_not_exists") || strings.Contains(*req.ConditionExpression, "="),
				strings.Contains(*req.ConditionExpression, "attribute_exists") || strings.Contains(*req.ConditionExpression, "<>"),
			)
		}
		return db.undefined, errServiceIO.New(err)
	}

	obj, err := db.codec.Decode(val.Attributes)
	if err != nil {
		return db.undefined, errInvalidEntity.New(err)
	}

	return obj, nil
}

// Remove multiple items at once
func (db *Storage[T]) BatchRemove(ctx context.Context, keys []T, opts ...interface{ WriterOpt(T) }) ([]T, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	seq := make([]types.WriteRequest, len(keys))
	for i := 0; i < len(keys); i++ {
		gen, err := db.codec.EncodeKey(keys[i])
		if err != nil {
			return nil, errInvalidEntity.New(err)
		}
		seq[i] = types.WriteRequest{DeleteRequest: &types.DeleteRequest{Key: gen}}
	}

	req := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			db.table: seq,
		},
	}

	val, err := db.service.BatchWriteItem(ctx, req)
	if err != nil {
		return nil, errServiceIO.New(err)
	}

	if len(val.UnprocessedItems) != 0 {
		items := val.UnprocessedItems[db.table]
		fails := make([]T, len(items))
		for i, r := range items {
			fails[i], _ = db.codec.Decode(r.PutRequest.Item)
		}
		return fails, errBatchPartialIO.New(nil)
	}

	return nil, nil
}
