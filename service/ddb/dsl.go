package ddb

import (
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/fogfish/curie"
	"github.com/fogfish/dynamo/v2"
	"github.com/fogfish/golem/pure/hseq"
)

//
//
//

func Expression[T dynamo.Thing](entity T) Expr[T] {
	return Expr[T]{entity: entity}
}

type Expr[T dynamo.Thing] struct {
	entity T
	update *dynamodb.UpdateItemInput
}

func (expr Expr[T]) HashKey() curie.IRI { return expr.entity.HashKey() }
func (expr Expr[T]) SortKey() curie.IRI { return expr.entity.SortKey() }

func (expr Expr[T]) Update(opts ...interface{ UpdateExpression(T) }) Expr[T] {
	expr.update = &dynamodb.UpdateItemInput{
		ExpressionAttributeNames:  map[string]string{},
		ExpressionAttributeValues: map[string]types.AttributeValue{},
	}
	for _, opt := range opts {
		if ap, ok := opt.(interface {
			Apply(*dynamodb.UpdateItemInput)
		}); ok {
			ap.Apply(expr.update)
		}
	}

	if len(expr.update.ExpressionAttributeValues) == 0 {
		expr.update.ExpressionAttributeValues = nil
	}

	return expr
}

//
//
//

type UpdateExpression[T dynamo.Thing, A any] struct{ key string }

func newUpdateExpression[T dynamo.Thing, A any](t hseq.Type[T]) UpdateExpression[T, A] {
	tag := t.Tag.Get("dynamodbav")
	if tag == "" {
		// TODO: Panic
		return UpdateExpression[T, A]{""}
	}

	return UpdateExpression[T, A]{strings.Split(tag, ",")[0]}
}

func SchemaX[T dynamo.Thing, A any](a string) UpdateExpression[T, A] {
	return hseq.FMap1(
		generic[T](a),
		newUpdateExpression[T, A],
	)
}

// Set attribute
//
//	name.Inc(x) ⟼ SET Field = :value
func (ue UpdateExpression[T, A]) Set(val A) interface{ UpdateExpression(T) } {
	return updateSetter[T, A]{key: ue.key, val: val}
}

type updateSetter[T any, A any] struct {
	key string
	val A
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

	if req.UpdateExpression == nil {
		req.UpdateExpression = aws.String("SET " + ekey + " = " + eval)
	} else {
		req.UpdateExpression = aws.String(*req.UpdateExpression + "," + ekey + " = " + eval)
	}
}

// Increment attribute
//
//	name.Inc(x) ⟼ SET Field = Field + :value
func (ue UpdateExpression[T, A]) Inc(val A) interface{ UpdateExpression(T) } {
	return updateIncrement[T, A]{op: " + ", key: ue.key, val: val}
}

// Decrement attribute
//
//	name.Inc(x) ⟼ SET Field = Field - :value
func (ue UpdateExpression[T, A]) Dec(val A) interface{ UpdateExpression(T) } {
	return updateIncrement[T, A]{op: " - ", key: ue.key, val: val}
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
	return updateAppender[T, A]{append: false, key: ue.key, val: val}
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
	return updateRemover[T, A]{key: ue.key}
}

type updateRemover[T any, A any] struct {
	key string
}

func (op updateRemover[T, A]) UpdateExpression(T) {}

func (op updateRemover[T, A]) Apply(req *dynamodb.UpdateItemInput) {
	ekey := "#__" + op.key + "__"

	req.ExpressionAttributeNames[ekey] = op.key

	if req.UpdateExpression == nil {
		req.UpdateExpression = aws.String("REMOVE " + ekey)
	} else {
		req.UpdateExpression = aws.String(*req.UpdateExpression + "," + ekey)
	}
}
