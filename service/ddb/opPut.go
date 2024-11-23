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
