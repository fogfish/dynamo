//
// Copyright (C) 2022 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

//
// The file declares key/value interface for dynamodb
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

/*

ddb internal handler for dynamo I/O
*/
type Storage[T dynamo.Thing] struct {
	Service   dynamo.DynamoDB
	Table     *string
	Index     *string
	Codec     *Codec[T]
	Schema    *Schema[T]
	undefined T
}

//-----------------------------------------------------------------------------
//
// Key Value
//
//-----------------------------------------------------------------------------

// Get item from storage
func (db *Storage[T]) Get(ctx context.Context, key T) (T, error) {
	gen, err := db.Codec.EncodeKey(key)
	if err != nil {
		return db.undefined, errInvalidKey(err)
	}

	req := &dynamodb.GetItemInput{
		Key:                      gen,
		TableName:                db.Table,
		ProjectionExpression:     db.Schema.Projection,
		ExpressionAttributeNames: db.Schema.ExpectedAttributeNames,
	}

	val, err := db.Service.GetItem(ctx, req)
	if err != nil {
		return db.undefined, errServiceIO(err)
	}

	if val.Item == nil {
		return db.undefined, errNotFound(nil, key)
	}

	obj, err := db.Codec.Decode(val.Item)
	if err != nil {
		return db.undefined, errInvalidEntity(err)
	}

	return obj, nil
}

// Put writes entity
func (db *Storage[T]) Put(ctx context.Context, entity T, config ...dynamo.Constraint[T]) error {
	gen, err := db.Codec.Encode(entity)
	if err != nil {
		return errInvalidEntity(err)
	}

	req := &dynamodb.PutItemInput{
		Item:      gen,
		TableName: db.Table,
	}

	names, values := maybeConditionExpression(&req.ConditionExpression, config)
	req.ExpressionAttributeValues = values
	req.ExpressionAttributeNames = names

	_, err = db.Service.PutItem(ctx, req)
	if err != nil {
		if recoverConditionalCheckFailedException(err) {
			return errPreConditionFailed(err, entity,
				strings.Contains(*req.ConditionExpression, "attribute_not_exists") || strings.Contains(*req.ConditionExpression, "="),
				strings.Contains(*req.ConditionExpression, "attribute_exists") || strings.Contains(*req.ConditionExpression, "<>"),
			)
		}
		return errServiceIO(err)
	}

	return nil
}

// Remove discards the entity from the table
func (db *Storage[T]) Remove(ctx context.Context, key T, config ...dynamo.Constraint[T]) error {
	gen, err := db.Codec.EncodeKey(key)
	if err != nil {
		return errInvalidKey(err)
	}

	req := &dynamodb.DeleteItemInput{
		Key:       gen,
		TableName: db.Table,
	}
	names, values := maybeConditionExpression(&req.ConditionExpression, config)
	req.ExpressionAttributeValues = values
	req.ExpressionAttributeNames = names

	_, err = db.Service.DeleteItem(ctx, req)
	if err != nil {
		if recoverConditionalCheckFailedException(err) {
			return errPreConditionFailed(err, key,
				strings.Contains(*req.ConditionExpression, "attribute_not_exists") || strings.Contains(*req.ConditionExpression, "="),
				strings.Contains(*req.ConditionExpression, "attribute_exists") || strings.Contains(*req.ConditionExpression, "<>"),
			)
		}
		return errServiceIO(err)
	}

	return nil
}

// Update applies a partial patch to entity and returns new values
func (db *Storage[T]) Update(ctx context.Context, entity T, config ...dynamo.Constraint[T]) (T, error) {
	gen, err := db.Codec.Encode(entity)
	if err != nil {
		return db.undefined, errInvalidEntity(err)
	}

	names := map[string]string{}
	values := map[string]types.AttributeValue{}
	update := make([]string, 0)
	for k, v := range gen {
		if k != db.Codec.pkPrefix && k != db.Codec.skSuffix && k != "id" {
			names["#__"+k+"__"] = k
			values[":__"+k+"__"] = v
			update = append(update, "#__"+k+"__="+":__"+k+"__")
		}
	}
	expression := aws.String("SET " + strings.Join(update, ","))

	req := &dynamodb.UpdateItemInput{
		Key:                       db.Codec.KeyOnly(gen),
		ExpressionAttributeNames:  names,
		ExpressionAttributeValues: values,
		UpdateExpression:          expression,
		TableName:                 db.Table,
		ReturnValues:              "ALL_NEW",
	}

	maybeUpdateConditionExpression(
		&req.ConditionExpression,
		req.ExpressionAttributeNames,
		req.ExpressionAttributeValues,
		config,
	)

	val, err := db.Service.UpdateItem(ctx, req)
	if err != nil {
		if recoverConditionalCheckFailedException(err) {
			return db.undefined, errPreConditionFailed(err, entity,
				strings.Contains(*req.ConditionExpression, "attribute_not_exists") || strings.Contains(*req.ConditionExpression, "="),
				strings.Contains(*req.ConditionExpression, "attribute_exists") || strings.Contains(*req.ConditionExpression, "<>"),
			)
		}
		return db.undefined, errServiceIO(err)
	}

	obj, err := db.Codec.Decode(val.Attributes)
	if err != nil {
		return db.undefined, errInvalidEntity(err)
	}

	return obj, nil
}

// Match applies a pattern matching to elements in the table
func (db *Storage[T]) Match(ctx context.Context, key T) dynamo.Seq[T] {
	gen, err := db.Codec.EncodeKey(key)
	if err != nil {
		return newSeq[T](ctx, nil, nil, errInvalidKey(err))
	}

	suffix, isSuffix := gen[db.Codec.skSuffix]
	switch v := suffix.(type) {
	case *types.AttributeValueMemberS:
		if v.Value == "_" {
			delete(gen, db.Codec.skSuffix)
			isSuffix = false
		}
	}

	expr := db.Codec.pkPrefix + " = :__" + db.Codec.pkPrefix + "__"
	if isSuffix {
		expr = expr + " and begins_with(" + db.Codec.skSuffix + ", :__" + db.Codec.skSuffix + "__)"
	}

	q := &dynamodb.QueryInput{
		KeyConditionExpression:    aws.String(expr),
		ExpressionAttributeValues: exprOf(gen),
		ProjectionExpression:      db.Schema.Projection,
		ExpressionAttributeNames:  db.Schema.ExpectedAttributeNames,
		TableName:                 db.Table,
		IndexName:                 db.Index,
	}

	return newSeq(ctx, db, q, err)
}

//
func exprOf(gen map[string]types.AttributeValue) (val map[string]types.AttributeValue) {
	val = map[string]types.AttributeValue{}
	for k, v := range gen {
		switch v.(type) {
		case *types.AttributeValueMemberNULL:
			// No Update is applied for nil attributes
			break
		default:
			val[":__"+k+"__"] = v
		}
	}

	return
}
