package ddb

import (
	"context"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/fogfish/dynamo"
	"github.com/fogfish/dynamo/internal/common"
)

/*

ddb internal handler for dynamo I/O
*/
type ddb[T dynamo.Thing] struct {
	io     *session.Session
	dynamo dynamodbiface.DynamoDBAPI
	codec  Codec[T]
	table  *string
	index  *string
	schema *Schema[T]
}

func New[T dynamo.Thing](io *session.Session, spec *common.URL) dynamo.KeyVal[T] {
	db := &ddb[T]{io: io, dynamo: dynamodb.New(io)}

	// config table name and index name
	seq := spec.Segments(2)
	db.table = seq[0]
	db.index = seq[1]
	db.schema = NewSchema[T]()

	// config mapping of Indentity to table attributes
	db.codec = Codec[T]{
		pkPrefix: spec.Query("prefix", "prefix"),
		skSuffix: spec.Query("suffix", "suffix"),
	}

	return db
}

// Mock dynamoDB I/O channel
func (db *ddb[T]) Mock(dynamo dynamodbiface.DynamoDBAPI) {
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
func (db *ddb[T]) Get(ctx context.Context, key T) (*T, error) {
	gen, err := db.codec.EncodeKey(key)
	if err != nil {
		return nil, err
	}

	req := &dynamodb.GetItemInput{
		Key:                      gen,
		TableName:                db.table,
		ProjectionExpression:     db.schema.Projection,
		ExpressionAttributeNames: db.schema.ExpectedAttributeNames,
	}

	val, err := db.dynamo.GetItemWithContext(ctx, req)
	if err != nil {
		return nil, err
	}

	if val.Item == nil {
		return nil, dynamo.NotFound{key}
	}

	return db.codec.Decode(val.Item)
}

// Put writes entity
func (db *ddb[T]) Put(ctx context.Context, entity T, config ...dynamo.Constrain[T]) error {
	gen, err := db.codec.Encode(entity)
	if err != nil {
		return err
	}

	req := &dynamodb.PutItemInput{
		Item:      gen,
		TableName: db.table,
	}

	names, values := maybeConditionExpression(&req.ConditionExpression, config)
	req.ExpressionAttributeValues = values
	req.ExpressionAttributeNames = names

	_, err = db.dynamo.PutItemWithContext(ctx, req)
	if err != nil {
		switch v := err.(type) {
		case awserr.Error:
			if v.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
				return dynamo.PreConditionFailed{entity}
			}
			return err
		default:
			return err
		}
	}

	return nil
}

// Remove discards the entity from the table
func (db *ddb[T]) Remove(ctx context.Context, key T, config ...dynamo.Constrain[T]) error {
	gen, err := db.codec.EncodeKey(key)
	if err != nil {
		return err
	}

	req := &dynamodb.DeleteItemInput{
		Key:       gen,
		TableName: db.table,
	}
	names, values := maybeConditionExpression(&req.ConditionExpression, config)
	req.ExpressionAttributeValues = values
	req.ExpressionAttributeNames = names

	_, err = db.dynamo.DeleteItemWithContext(ctx, req)
	if err != nil {
		switch v := err.(type) {
		case awserr.Error:
			if v.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
				return dynamo.PreConditionFailed{key}
			}
			return err
		default:
			return err
		}
	}

	return nil
}

// Update applies a partial patch to entity and returns new values
func (db *ddb[T]) Update(ctx context.Context, entity T, config ...dynamo.Constrain[T]) (*T, error) {
	gen, err := db.codec.Encode(entity)
	if err != nil {
		return nil, err
	}

	names := map[string]*string{}
	values := map[string]*dynamodb.AttributeValue{}
	update := make([]string, 0)
	for k, v := range gen {
		if k != db.codec.pkPrefix && k != db.codec.skSuffix && k != "id" {
			names["#__"+k+"__"] = aws.String(k)
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
		ReturnValues:              aws.String("ALL_NEW"),
	}

	maybeUpdateConditionExpression(
		&req.ConditionExpression,
		req.ExpressionAttributeNames,
		req.ExpressionAttributeValues,
		config,
	)

	val, err := db.dynamo.UpdateItemWithContext(ctx, req)
	if err != nil {
		switch v := err.(type) {
		case awserr.Error:
			if v.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
				return nil, dynamo.PreConditionFailed{entity}
			}
			return nil, err
		default:
			return nil, err
		}
	}

	return db.codec.Decode(val.Attributes)
}

// Match applies a pattern matching to elements in the table
func (db *ddb[T]) Match(ctx context.Context, key T) dynamo.Seq[T] {
	gen, err := db.codec.EncodeKey(key)
	if err != nil {
		return newSeq[T](nil, nil, nil, err)
	}

	suffix, isSuffix := gen[db.codec.skSuffix]
	if suffix.S != nil && *suffix.S == "_" {
		delete(gen, db.codec.skSuffix)
		isSuffix = false
	}

	expr := db.codec.pkPrefix + " = :__" + db.codec.pkPrefix + "__"
	if isSuffix && suffix.S != nil {
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
func exprOf(gen map[string]*dynamodb.AttributeValue) (val map[string]*dynamodb.AttributeValue) {
	val = map[string]*dynamodb.AttributeValue{}
	for k, v := range gen {
		if v.NULL == nil || !*v.NULL {
			val[":__"+k+"__"] = v
		}
	}

	return
}
