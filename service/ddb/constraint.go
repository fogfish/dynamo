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

// Constraint is a function that applies conditional expression to storage request.
// Each storage implements own constrains protocols. The module here defines a few
// constrain protocol. The structure of the constrain is abstracted away from the client.
type Constraint[T any] struct {
	fun string
	key string
	val any
}

func (Constraint[T]) Constraint(T) {}

// Schema declares type descriptor to express Storage I/O Constrains.
//
// Let's consider a following example:
//
//	type Person struct {
//	  curie.ID
//	  Name    string `dynamodbav:"anothername,omitempty"`
//	}
//
// How to define a condition expression on the field Name? Golang struct defines
// and refers the field by `Name` but DynamoDB stores it under the attribute
// `anothername`. Struct field dynamodbav tag specifies serialization rules.
// Golang does not support a typesafe approach to build a correspondence between
// `Name` ⟷ `anothername`. Developers have to utilize dynamodb attribute
// name(s) in conditional expression and Golang struct name in rest of the code.
// It becomes confusing and hard to maintain.
//
// The types Effect and Schema are helpers to declare builders for conditional
// expressions. Just declare a global variables next to type definition and
// use them across the application.
//
//	var name = dynamo.Schema[Person, string]("Name")
//
//	name.Eq("Joe Doe")
//	name.NotExists()
func Schema[T dynamo.Thing, A any](a string) Constraints[T, A] {
	return hseq.FMap1(
		generic[T](a),
		mkConstraints[T, A],
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

// Builds Constrains
func mkConstraints[T dynamo.Thing, A any](t hseq.Type[T]) Constraints[T, A] {
	tag := t.Tag.Get("dynamodbav")
	if tag == "" {
		return Constraints[T, A]{""}
	}

	return Constraints[T, A]{strings.Split(tag, ",")[0]}
}

// Internal implementation of Constrain effects for storage
type Constraints[T dynamo.Thing, A any] struct{ key string }

// Eq is equal constrain
//
//	name.Eq(x) ⟼ Field = :value
func (eff Constraints[T, A]) Eq(val A) Constraint[T] {
	return Constraint[T]{fun: "=", key: eff.key, val: val}
}

// Ne is non equal constraint
//
//	name.Ne(x) ⟼ Field <> :value
func (eff Constraints[T, A]) Ne(val A) Constraint[T] {
	return Constraint[T]{fun: "<>", key: eff.key, val: val}
}

// Lt is less than constraint
//
//	name.Lt(x) ⟼ Field < :value
func (eff Constraints[T, A]) Lt(val A) Constraint[T] {
	return Constraint[T]{fun: "<", key: eff.key, val: val}
}

// Le is less or equal constain
//
//	name.Le(x) ⟼ Field <= :value
func (eff Constraints[T, A]) Le(val A) Constraint[T] {
	return Constraint[T]{fun: "<=", key: eff.key, val: val}
}

// Gt is greater than constrain
//
//	name.Le(x) ⟼ Field > :value
func (eff Constraints[T, A]) Gt(val A) Constraint[T] {
	return Constraint[T]{fun: ">", key: eff.key, val: val}
}

// Ge is greater or equal constrain
//
//	name.Le(x) ⟼ Field >= :value
func (eff Constraints[T, A]) Ge(val A) Constraint[T] {
	return Constraint[T]{fun: ">=", key: eff.key, val: val}
}

// Is matches either Eq or NotExists if value is not defined
func (eff Constraints[T, A]) Is(val string) Constraint[T] {
	if val == "_" {
		return eff.NotExists()
	}

	return Constraint[T]{fun: "=", key: eff.key, val: val}
}

// Exists attribute constrain
//
//	name.Exists(x) ⟼ attribute_exists(name)
func (eff Constraints[T, A]) Exists() Constraint[T] {
	return Constraint[T]{fun: "attribute_exists", key: eff.key}
}

// NotExists attribute constrain
//
//	name.NotExists(x) ⟼ attribute_not_exists(name)
func (eff Constraints[T, A]) NotExists() Constraint[T] {
	return Constraint[T]{fun: "attribute_not_exists", key: eff.key}
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
