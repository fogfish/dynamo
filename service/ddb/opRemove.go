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

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// Remove discards the entity from the table
func (db *Storage[T]) Remove(ctx context.Context, key T, config ...interface{ Constraint(T) }) error {
	gen, err := db.codec.EncodeKey(key)
	if err != nil {
		return errInvalidKey.New(err)
	}

	req := &dynamodb.DeleteItemInput{
		Key:       gen,
		TableName: db.table,
	}
	names, values := maybeConditionExpression(&req.ConditionExpression, config)
	req.ExpressionAttributeValues = values
	req.ExpressionAttributeNames = names

	_, err = db.service.DeleteItem(ctx, req)
	if err != nil {
		if recoverConditionalCheckFailedException(err) {
			return errPreConditionFailed(err, key,
				strings.Contains(*req.ConditionExpression, "attribute_not_exists") || strings.Contains(*req.ConditionExpression, "="),
				strings.Contains(*req.ConditionExpression, "attribute_exists") || strings.Contains(*req.ConditionExpression, "<>"),
			)
		}
		return errServiceIO.New(err)
	}

	return nil
}
