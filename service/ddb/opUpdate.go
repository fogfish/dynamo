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
	"github.com/fogfish/dynamo/v2"
)

// Update applies a partial patch to entity using update expression abstraction
func (db *Storage[T]) UpdateWith(ctx context.Context, expression UpdateItemExpression[T], opts ...interface{ ConditionExpression(T) }) (T, error) {
	gen, err := db.codec.Encode(expression.entity)
	if err != nil {
		return db.undefined, errInvalidEntity.New(err)
	}
	req := expression.request
	req.Key = db.codec.KeyOnly(gen)
	req.TableName = db.table
	req.ReturnValues = "ALL_NEW"

	maybeUpdateConditionExpression(
		&req.ConditionExpression,
		req.ExpressionAttributeNames,
		req.ExpressionAttributeValues,
		opts,
	)

	return db.update(ctx, expression.entity, req)
}

// Update applies a partial patch to entity and returns new values
func (db *Storage[T]) Update(ctx context.Context, entity T, opts ...interface{ ConditionExpression(T) }) (T, error) {
	gen, err := db.codec.Encode(entity)
	if err != nil {
		return db.undefined, errInvalidEntity.New(err)
	}

	names := map[string]string{}
	values := map[string]types.AttributeValue{}
	update := make([]string, 0)
	for k, v := range gen {
		if k != db.codec.pkPrefix && k != db.codec.skSuffix && k != "id" {
			names["#__"+k+"__"] = k
			values[":__"+k+"__"] = v
			update = append(update, "#__"+k+"__="+":__"+k+"__")
		}
	}
	expression := aws.String("SET " + strings.Join(update, ","))

	req := &dynamodb.UpdateItemInput{
		Key:                       db.codec.KeyOnly(gen),
		ExpressionAttributeNames:  names,
		ExpressionAttributeValues: values,
		UpdateExpression:          expression,
		TableName:                 db.table,
		ReturnValues:              "ALL_NEW",
	}

	maybeUpdateConditionExpression(
		&req.ConditionExpression,
		req.ExpressionAttributeNames,
		req.ExpressionAttributeValues,
		opts,
	)

	return db.update(ctx, entity, req)
}

func (db *Storage[T]) update(ctx context.Context, key dynamo.Thing, req *dynamodb.UpdateItemInput) (T, error) {
	val, err := db.service.UpdateItem(ctx, req)
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
