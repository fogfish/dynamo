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

// Put writes entity
func (db *Storage[T]) Put(ctx context.Context, entity T, opts ...interface{ WriterOpt(T) }) error {
	gen, err := db.codec.Encode(entity)
	if err != nil {
		return errInvalidEntity.New(err)
	}

	req := &dynamodb.PutItemInput{
		Item:      gen,
		TableName: aws.String(db.table),
	}

	names, values := maybeConditionExpression(&req.ConditionExpression, opts)
	req.ExpressionAttributeValues = values
	req.ExpressionAttributeNames = names

	_, err = db.service.PutItem(ctx, req)
	if err != nil {
		if recoverConditionalCheckFailedException(err) {
			return errPreConditionFailed(err, entity,
				strings.Contains(*req.ConditionExpression, "attribute_not_exists") || strings.Contains(*req.ConditionExpression, "="),
				strings.Contains(*req.ConditionExpression, "attribute_exists") || strings.Contains(*req.ConditionExpression, "<>"),
			)
		}
		return errServiceIO.New(err)
	}

	return nil
}

// Put multiple items at once
func (db *Storage[T]) BatchPut(ctx context.Context, entities []T, opts ...interface{ WriterOpt(T) }) ([]T, error) {
	if len(entities) == 0 {
		return nil, nil
	}

	seq := make([]types.WriteRequest, len(entities))
	for i := 0; i < len(entities); i++ {
		gen, err := db.codec.Encode(entities[i])
		if err != nil {
			return nil, errInvalidEntity.New(err)
		}
		seq[i] = types.WriteRequest{PutRequest: &types.PutRequest{Item: gen}}
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
