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
)

type ddb[T dynamo.ThingV2] struct {
	io     *session.Session
	dynamo dynamodbiface.DynamoDBAPI
	codec  Codec[T]
	table  *string
	index  *string
	schema *Schema[T]
}

// TODO: projection expression for get

func New[T dynamo.ThingV2](
	io *session.Session,
	spec *dynamo.URL,
) dynamo.KeyValV2[T] {
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
		return nil, dynamo.NotFound{
			HashKey: key.HashKey(),
			SortKey: key.SortKey(),
		}
	}

	return db.codec.Decode(val.Item)
}

// Put writes entity
func (db *ddb[T]) Put(ctx context.Context, entity T, config ...dynamo.ConstrainV2[T]) error {
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
				return dynamo.PreConditionFailed{
					HashKey: entity.HashKey(),
					SortKey: entity.SortKey(),
				}
			}
			return err
		default:
			return err
		}
	}

	return nil
}

// Remove discards the entity from the table
func (db *ddb[T]) Remove(ctx context.Context, key T, config ...dynamo.ConstrainV2[T]) error {
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
				return dynamo.PreConditionFailed{
					HashKey: key.HashKey(),
					SortKey: key.SortKey(),
				}
			}
			return err
		default:
			return err
		}
	}

	return nil
}

// Update applies a partial patch to entity and returns new values
func (db *ddb[T]) Update(ctx context.Context, entity T, config ...dynamo.ConstrainV2[T]) (*T, error) {
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
				return nil, dynamo.PreConditionFailed{
					HashKey: entity.HashKey(),
					SortKey: entity.SortKey(),
				}
			}
			return nil, err
		default:
			return nil, err
		}
	}

	return db.codec.Decode(val.Attributes)
}
