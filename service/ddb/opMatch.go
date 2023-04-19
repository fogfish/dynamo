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
	"github.com/fogfish/curie"
	"github.com/fogfish/dynamo/v2"
)

// Match applies a pattern matching to elements in the table
func (db *Storage[T]) MatchKey(ctx context.Context, key dynamo.Thing, opts ...dynamo.MatchOpt) ([]T, dynamo.MatchOpt, error) {
	gen, err := db.codec.EncodeKey(key)
	if err != nil {
		return nil, nil, errInvalidKey.New(err)
	}
	return db.match(ctx, gen, opts)
}

// Match applies a pattern matching to elements in the table
func (db *Storage[T]) Match(ctx context.Context, key T, opts ...dynamo.MatchOpt) ([]T, dynamo.MatchOpt, error) {
	gen, err := db.codec.EncodeKey(key)
	if err != nil {
		return nil, nil, errInvalidKey.New(err)
	}
	return db.match(ctx, gen, opts)
}

// Match applies a pattern matching to elements in the table
func (db *Storage[T]) match(ctx context.Context, gen map[string]types.AttributeValue, opts []dynamo.MatchOpt) ([]T, dynamo.MatchOpt, error) {
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

	q := db.reqQuery(gen, expr, opts)
	val, err := db.service.Query(ctx, q)
	if err != nil {
		return nil, nil, errServiceIO.New(err)
	}

	seq := make([]T, val.Count)
	for i := 0; i < int(val.Count); i++ {
		obj, err := db.codec.Decode(val.Items[i])
		if err != nil {
			return nil, nil, errInvalidEntity.New(err)
		}
		seq[i] = obj
	}

	return seq, lastKeyToCursor(db.codec, val), nil
}

func (db *Storage[T]) reqQuery(
	gen map[string]types.AttributeValue,
	expr string,
	opts []interface{ MatchOpt() },
) *dynamodb.QueryInput {
	var (
		limit             *int32                          = nil
		exclusiveStartKey map[string]types.AttributeValue = nil
	)
	for _, opt := range opts {
		switch v := opt.(type) {
		case interface{ Limit() int32 }:
			limit = aws.Int32(v.Limit())
		case dynamo.Thing:
			prefix := v.HashKey()
			suffix := v.SortKey()

			if prefix != "" {
				key := map[string]types.AttributeValue{}

				key[db.codec.pkPrefix] = &types.AttributeValueMemberS{Value: string(prefix)}
				if suffix != "" {
					key[db.codec.skSuffix] = &types.AttributeValueMemberS{Value: string(suffix)}
				} else {
					key[db.codec.skSuffix] = &types.AttributeValueMemberS{Value: "_"}
				}
				exclusiveStartKey = key
			}
		}
	}

	req := &dynamodb.QueryInput{
		KeyConditionExpression:    aws.String(expr),
		ExpressionAttributeValues: exprOf(gen),
		ProjectionExpression:      db.schema.Projection,
		ExpressionAttributeNames:  db.schema.ExpectedAttributeNames,
		TableName:                 db.table,
		IndexName:                 db.index,
		Limit:                     limit,
		ExclusiveStartKey:         exclusiveStartKey,
	}

	return req
}

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

type cursor struct{ hashKey, sortKey string }

func (c cursor) HashKey() curie.IRI { return curie.IRI(c.hashKey) }
func (c cursor) SortKey() curie.IRI { return curie.IRI(c.sortKey) }

func lastKeyToCursor[T dynamo.Thing](codec *codec[T], val *dynamodb.QueryOutput) dynamo.MatchOpt {
	if val.LastEvaluatedKey == nil {
		return nil
	}

	var hkey, skey string

	key := val.LastEvaluatedKey
	prefix, isPrefix := key[codec.pkPrefix]
	if isPrefix {
		switch v := prefix.(type) {
		case *types.AttributeValueMemberS:
			hkey = v.Value
		}
	}

	suffix, isSuffix := key[codec.skSuffix]
	if isSuffix {
		switch v := suffix.(type) {
		case *types.AttributeValueMemberS:
			skey = v.Value
		}
	}

	return dynamo.Cursor(&cursor{hashKey: hkey, sortKey: skey})
}
