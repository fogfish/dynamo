//
// Copyright (C) 2022 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

//
// The file implements dynamodb specific constraints
//

package ddb

import (
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/fogfish/dynamo/v2"
	"github.com/fogfish/golem/pure/hseq"
)

// Constraint ...
type Constraint[T any] struct {
	fun string
	key string
	val any
}

func (Constraint[T]) Constraint(T) {}

// Schema ...
func Schema[T dynamo.Thing, A any](a string) Effect[T, A] {
	return hseq.FMap1(
		generic[T](a),
		mkEffect[T, A],
	)
}

// generic[T] filters hseq.Generic[T] list with defined fields
func generic[T any](fs ...string) hseq.Seq[T] {
	seq := make(hseq.Seq[T], 0)
	for _, t := range hseq.Generic[T]() {
		for _, f := range fs {
			if t.Name == f {
				seq = append(seq, t)
			}
		}
	}
	return seq
}

// Builds TypeOf constrain
func mkEffect[T dynamo.Thing, A any](t hseq.Type[T]) Effect[T, A] {
	tag := t.Tag.Get("dynamodbav")
	if tag == "" {
		return Effect[T, A]{""}
	}

	return Effect[T, A]{strings.Split(tag, ",")[0]}
}

// Internal implementation of Constrain effects for storage
type Effect[T dynamo.Thing, A any] struct{ key string }

// Eq is equal constrain
//
//	name.Eq(x) ⟼ Field = :value
func (eff Effect[T, A]) Eq(val A) Constraint[T] {
	return Constraint[T]{fun: "=", key: eff.key, val: val}
}

/*
Internal implementation of conditional expressions for dynamo db
*/
func maybeConditionExpression[T dynamo.Thing](
	conditionExpression **string,
	config []interface{ Constraint(T) },
) (
	expressionAttributeNames map[string]string,
	expressionAttributeValues map[string]types.AttributeValue,
) {
	if len(config) > 0 {
		expressionAttributeNames = map[string]string{}
		expressionAttributeValues = map[string]types.AttributeValue{}

		switch c := config[0].(type) {
		case Constraint[T]:
			if c.val != nil {
				dyadic(c,
					conditionExpression,
					expressionAttributeNames,
					expressionAttributeValues,
				)
			} else {
				unary(c,
					conditionExpression,
					expressionAttributeNames,
					expressionAttributeValues,
				)
			}
		}

		// Unfortunately empty maps are not accepted by DynamoDB
		if len(expressionAttributeNames) == 0 {
			expressionAttributeNames = nil
		}
		if len(expressionAttributeValues) == 0 {
			expressionAttributeValues = nil
		}
	}
	return
}

/*
Internal implementation of conditional expressions for dynamo db in the case of
update.
*/
func maybeUpdateConditionExpression[T dynamo.Thing](
	conditionExpression **string,
	expressionAttributeNames map[string]string,
	expressionAttributeValues map[string]types.AttributeValue,
	config []interface{ Constraint(T) },
) {
	if len(config) > 0 {
		switch c := config[0].(type) {
		case Constraint[T]:
			if c.val != nil {
				dyadic(c,
					conditionExpression,
					expressionAttributeNames,
					expressionAttributeValues,
				)
			} else {
				unary(c,
					conditionExpression,
					expressionAttributeNames,
					expressionAttributeValues,
				)
			}
		}
	}
}

/*
dyadic translate expression to dynamo format
*/
func dyadic[T dynamo.Thing](
	op Constraint[T],
	conditionExpression **string,
	expressionAttributeNames map[string]string,
	expressionAttributeValues map[string]types.AttributeValue,
) {
	if op.key == "" {
		return
	}

	lit, err := attributevalue.Marshal(op.val)
	if err != nil {
		return
	}

	key := "#__" + op.key + "__"
	let := ":__" + op.key + "__"
	expressionAttributeValues[let] = lit
	expressionAttributeNames[key] = op.key
	*conditionExpression = aws.String(key + " " + op.fun + " " + let)
}

/*
unary translate expression to dynamo format
*/
func unary[T dynamo.Thing](
	op Constraint[T],
	conditionExpression **string,
	expressionAttributeNames map[string]string,
	expressionAttributeValues map[string]types.AttributeValue,
) {
	if op.key == "" {
		return
	}

	key := "#__" + op.key + "__"
	expressionAttributeNames[key] = op.key

	*conditionExpression = aws.String(op.fun + "(" + key + ")")
}
