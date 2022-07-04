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
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/fogfish/dynamo"
)

/*

DynamoDB declares API used by the library
*/
type DynamoDB interface {
	GetItem(context.Context, *dynamodb.GetItemInput, ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	PutItem(context.Context, *dynamodb.PutItemInput, ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	DeleteItem(context.Context, *dynamodb.DeleteItemInput, ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
	UpdateItem(context.Context, *dynamodb.UpdateItemInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
	Query(context.Context, *dynamodb.QueryInput, ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
}

/*

ddb internal handler for dynamo I/O
*/
type ddb[T dynamo.Thing] struct {
	dynamo    DynamoDB
	codec     Codec[T]
	table     *string
	index     *string
	schema    *Schema[T]
	undefined T
}

func New[T dynamo.Thing](cfg *dynamo.Config) dynamo.KeyVal[T] {
	db := &ddb[T]{
		dynamo: dynamodb.NewFromConfig(cfg.AWS),
	}

	// config table name and index name
	seq := cfg.URI.Segments()
	db.table = &seq[0]
	if len(seq) > 1 {
		db.index = &seq[1]
	}
	db.schema = NewSchema[T]()

	// config mapping of Indentity to table attributes
	db.codec = Codec[T]{
		pkPrefix: cfg.URI.Query("prefix", "prefix"),
		skSuffix: cfg.URI.Query("suffix", "suffix"),
	}

	return db
}

// Mock dynamoDB I/O channel
func (db *ddb[T]) Mock(dynamo DynamoDB) {
	db.dynamo = dynamo
	db.codec = Codec[T]{
		pkPrefix: "prefix",
		skSuffix: "suffix",
	}
}

//-----------------------------------------------------------------------------
//
// Key Value
//
//-----------------------------------------------------------------------------

// Get item from storage
func (db *ddb[T]) Get(ctx context.Context, key T) (T, error) {
	gen, err := db.codec.EncodeKey(key)
	if err != nil {
		return db.undefined, errInvalidKey(err, "Get")
	}

	req := &dynamodb.GetItemInput{
		Key:                      gen,
		TableName:                db.table,
		ProjectionExpression:     db.schema.Projection,
		ExpressionAttributeNames: db.schema.ExpectedAttributeNames,
	}

	val, err := db.dynamo.GetItem(ctx, req)
	if err != nil {
		return db.undefined, errServiceIO(err, "Get")
	}

	if val.Item == nil {
		return db.undefined, dynamo.ErrNotFound(nil, key)
	}

	obj, err := db.codec.Decode(val.Item)
	if err != nil {
		return db.undefined, errInvalidEntity(err, "Get")
	}

	return obj, nil
}

// Put writes entity
func (db *ddb[T]) Put(ctx context.Context, entity T, config ...dynamo.Constraint[T]) error {
	gen, err := db.codec.Encode(entity)
	if err != nil {
		return errInvalidEntity(err, "Put")
	}

	req := &dynamodb.PutItemInput{
		Item:      gen,
		TableName: db.table,
	}

	names, values := maybeConditionExpression(&req.ConditionExpression, config)
	req.ExpressionAttributeValues = values
	req.ExpressionAttributeNames = names

	_, err = db.dynamo.PutItem(ctx, req)
	if err != nil {
		var pce *types.ConditionalCheckFailedException
		if errors.As(err, &pce) {
			return dynamo.ErrPreConditionFailed(err, entity, strings.Contains(*req.ConditionExpression, "attribute_exists"))
		}

		return errServiceIO(err, "Put")
	}

	return nil
}

// Remove discards the entity from the table
func (db *ddb[T]) Remove(ctx context.Context, key T, config ...dynamo.Constraint[T]) error {
	gen, err := db.codec.EncodeKey(key)
	if err != nil {
		return errInvalidKey(err, "Remove")
	}

	req := &dynamodb.DeleteItemInput{
		Key:       gen,
		TableName: db.table,
	}
	names, values := maybeConditionExpression(&req.ConditionExpression, config)
	req.ExpressionAttributeValues = values
	req.ExpressionAttributeNames = names

	_, err = db.dynamo.DeleteItem(ctx, req)
	if err != nil {
		var pce *types.ConditionalCheckFailedException
		if errors.As(err, &pce) {
			return dynamo.ErrPreConditionFailed(err, key, strings.Contains(*req.ConditionExpression, "attribute_not_exists"))
		}

		return errServiceIO(err, "Remove")
	}

	return nil
}

// Update applies a partial patch to entity and returns new values
func (db *ddb[T]) Update(ctx context.Context, entity T, config ...dynamo.Constraint[T]) (T, error) {
	gen, err := db.codec.Encode(entity)
	if err != nil {
		return db.undefined, errInvalidEntity(err, "Update")
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
		config,
	)

	val, err := db.dynamo.UpdateItem(ctx, req)
	if err != nil {
		var pce *types.ConditionalCheckFailedException
		if errors.As(err, &pce) {
			return db.undefined, dynamo.ErrPreConditionFailed(err, entity, strings.Contains(*req.ConditionExpression, "attribute_exists"))
		}

		return db.undefined, errServiceIO(err, "Update")
	}

	return db.codec.Decode(val.Attributes)
}

// Match applies a pattern matching to elements in the table
func (db *ddb[T]) Match(ctx context.Context, key T) dynamo.Seq[T] {
	gen, err := db.codec.EncodeKey(key)
	if err != nil {
		return newSeq[T](nil, nil, nil, errInvalidKey(err, "Match"))
	}

	suffix, isSuffix := gen[db.codec.skSuffix]
	switch v := suffix.(type) {
	case *types.AttributeValueMemberS:
		if v.Value == "_" {
			delete(gen, db.codec.skSuffix)
			isSuffix = false
		}
	}

	expr := db.codec.pkPrefix + " = :__" + db.codec.pkPrefix + "__"
	if isSuffix {
		expr = expr + " and begins_with(" + db.codec.skSuffix + ", :__" + db.codec.skSuffix + "__)"
	}

	q := &dynamodb.QueryInput{
		KeyConditionExpression:    aws.String(expr),
		ExpressionAttributeValues: exprOf(gen),
		ProjectionExpression:      db.schema.Projection,
		ExpressionAttributeNames:  db.schema.ExpectedAttributeNames,
		TableName:                 db.table,
		IndexName:                 db.index,
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

//
// Error types
//

func errServiceIO(err error, op string) error {
	return fmt.Errorf("[dynamo.ddb.%s] service i/o failed: %w", op, err)
}

func errInvalidKey(err error, op string) error {
	return fmt.Errorf("[dynamo.ddb.%s] invalid key: %w", op, err)
}

func errInvalidEntity(err error, op string) error {
	return fmt.Errorf("[dynamo.ddb.%s] invalid entity: %w", op, err)
}

func errProcessEntity(err error, op string, thing dynamo.Thing) error {
	return fmt.Errorf("[dynamo.ddb.%s] can't process (%s, %s) : %w", op, thing.HashKey(), thing.SortKey(), err)
}
