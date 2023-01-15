//
// Copyright (C) 2022 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

package ddb

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/fogfish/dynamo/v2"
	"github.com/fogfish/golem/pure/hseq"
)

//
//
//

func UpdateFor[T dynamo.Thing, A any](attr ...string) UpdateExpression[T, A] {
	var seq hseq.Seq[T]

	if len(attr) == 0 {
		seq = hseq.New1[T, A]()
	} else {
		seq = hseq.New[T](attr[0])
	}

	return hseq.FMap1(seq, newUpdateExpression[T, A])
}

type UpdateItemExpression[T dynamo.Thing] struct {
	entity  T
	request *dynamodb.UpdateItemInput
}

func Updater[T dynamo.Thing](entity T, opts ...interface{ UpdateExpression(T) }) UpdateItemExpression[T] {
	request := &dynamodb.UpdateItemInput{
		ExpressionAttributeNames:  map[string]string{},
		ExpressionAttributeValues: map[string]types.AttributeValue{},
	}
	for _, opt := range opts {
		if ap, ok := opt.(interface {
			Apply(*dynamodb.UpdateItemInput)
		}); ok {
			ap.Apply(request)
		}
	}

	if len(request.ExpressionAttributeValues) == 0 {
		request.ExpressionAttributeValues = nil
	}

	return UpdateItemExpression[T]{entity: entity, request: request}
}

//
//
//

type UpdateExpression[T dynamo.Thing, A any] struct{ key string }

func newUpdateExpression[T dynamo.Thing, A any](t hseq.Type[T]) UpdateExpression[T, A] {
	tag := t.Tag.Get("dynamodbav")
	if tag == "" {
		panic(fmt.Errorf("field %s of type %T do not have `dynamodbav` tag", t.Name, *new(T)))
	}

	return UpdateExpression[T, A]{strings.Split(tag, ",")[0]}
}

// Set attribute
//
//	name.Inc(x) ⟼ SET Field = :value
func (ue UpdateExpression[T, A]) Set(val A) interface{ UpdateExpression(T) } {
	return &updateSetter[T, A]{notExists: false, key: ue.key, val: val}
}

// Set attribute if not exists
//
//	name.Inc(x) ⟼ SET Field = if_not_exists(Field, :value)
func (ue UpdateExpression[T, A]) SetNotExists(val A) interface{ UpdateExpression(T) } {
	return &updateSetter[T, A]{notExists: true, key: ue.key, val: val}
}

type updateSetter[T any, A any] struct {
	notExists bool
	key       string
	val       A
}

func (op updateSetter[T, A]) UpdateExpression(T) {}

func (op updateSetter[T, A]) Apply(req *dynamodb.UpdateItemInput) {
	val, err := attributevalue.Marshal(op.val)
	if err != nil {
		return
	}

	ekey := "#__" + op.key + "__"
	eval := ":__" + op.key + "__"

	req.ExpressionAttributeNames[ekey] = op.key
	req.ExpressionAttributeValues[eval] = val
	expr := ekey + " = " + eval
	if op.notExists {
		expr = ekey + " = if_not_exists(" + ekey + "," + eval + ")"
	}

	if req.UpdateExpression == nil {
		req.UpdateExpression = aws.String("SET " + expr)
	} else {
		req.UpdateExpression = aws.String(*req.UpdateExpression + "," + expr)
	}
}

// Add new attribute and increment value
//
//	name.Add(x) ⟼ ADD Field :value
func (ue UpdateExpression[T, A]) Add(val A) interface{ UpdateExpression(T) } {
	return &updateAdder[T, A]{key: ue.key, val: val}
}

type updateAdder[T any, A any] struct {
	key string
	val A
}

func (op updateAdder[T, A]) UpdateExpression(T) {}

func (op updateAdder[T, A]) Apply(req *dynamodb.UpdateItemInput) {
	val, err := attributevalue.Marshal(op.val)
	if err != nil {
		return
	}

	ekey := "#__" + op.key + "__"
	eval := ":__" + op.key + "__"

	req.ExpressionAttributeNames[ekey] = op.key
	req.ExpressionAttributeValues[eval] = val
	expr := ekey + " " + eval

	if req.UpdateExpression == nil {
		req.UpdateExpression = aws.String("ADD " + expr)
	} else {
		req.UpdateExpression = aws.String(*req.UpdateExpression + "," + expr)
	}
}

// Increment attribute
//
//	name.Inc(x) ⟼ SET Field = Field + :value
func (ue UpdateExpression[T, A]) Inc(val A) interface{ UpdateExpression(T) } {
	return &updateIncrement[T, A]{op: " + ", key: ue.key, val: val}
}

// Decrement attribute
//
//	name.Inc(x) ⟼ SET Field = Field - :value
func (ue UpdateExpression[T, A]) Dec(val A) interface{ UpdateExpression(T) } {
	return &updateIncrement[T, A]{op: " - ", key: ue.key, val: val}
}

type updateIncrement[T any, A any] struct {
	op  string
	key string
	val A
}

func (op updateIncrement[T, A]) UpdateExpression(T) {}

func (op updateIncrement[T, A]) Apply(req *dynamodb.UpdateItemInput) {
	val, err := attributevalue.Marshal(op.val)
	if err != nil {
		return
	}

	ekey := "#__" + op.key + "__"
	eval := ":__" + op.key + "__"

	req.ExpressionAttributeNames[ekey] = op.key
	req.ExpressionAttributeValues[eval] = val

	if req.UpdateExpression == nil {
		req.UpdateExpression = aws.String("SET " + ekey + " = " + ekey + op.op + eval)
	} else {
		req.UpdateExpression = aws.String(*req.UpdateExpression + "," + ekey + " = " + ekey + op.op + eval)
	}
}

// Append element to list
//
//	name.Inc(x) ⟼ SET Field = list_append (Field, :value)
func (ue UpdateExpression[T, A]) Append(val A) interface{ UpdateExpression(T) } {
	return updateAppender[T, A]{append: true, key: ue.key, val: val}
}

// Prepend element to list
//
//	name.Inc(x) ⟼ SET Field = list_append (:value, Field)
func (ue UpdateExpression[T, A]) Prepend(val A) interface{ UpdateExpression(T) } {
	return &updateAppender[T, A]{append: false, key: ue.key, val: val}
}

type updateAppender[T any, A any] struct {
	append bool
	key    string
	val    A
}

func (op updateAppender[T, A]) UpdateExpression(T) {}

func (op updateAppender[T, A]) Apply(req *dynamodb.UpdateItemInput) {
	val, err := attributevalue.Marshal(op.val)
	if err != nil {
		return
	}

	ekey := "#__" + op.key + "__"
	eval := ":__" + op.key + "__"

	req.ExpressionAttributeNames[ekey] = op.key
	req.ExpressionAttributeValues[eval] = val

	var cmd string
	if op.append {
		cmd = "list_append(" + ekey + "," + eval + ")"
	} else {
		cmd = "list_append(" + eval + "," + ekey + ")"
	}

	if req.UpdateExpression == nil {
		req.UpdateExpression = aws.String("SET " + ekey + " = " + cmd)
	} else {
		req.UpdateExpression = aws.String(*req.UpdateExpression + "," + ekey + " = " + cmd)
	}
}

// Remove attribute
//
//	name.Remove() ⟼ REMOVE Field
func (ue UpdateExpression[T, A]) Remove() interface{ UpdateExpression(T) } {
	return &updateRemover[T]{key: ue.key}
}

type updateRemover[T any] struct {
	key string
}

func (op updateRemover[T]) UpdateExpression(T) {}

func (op updateRemover[T]) Apply(req *dynamodb.UpdateItemInput) {
	ekey := "#__" + op.key + "__"

	req.ExpressionAttributeNames[ekey] = op.key

	if req.UpdateExpression == nil {
		req.UpdateExpression = aws.String("REMOVE " + ekey)
	} else {
		req.UpdateExpression = aws.String(*req.UpdateExpression + "," + ekey)
	}
}
