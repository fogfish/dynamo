package ddb

import (
	"context"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Update applies a partial patch to entity and returns new values
func (db *Storage[T]) Update(ctx context.Context, entity T, config ...interface{ Constraint(T) }) (T, error) {
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
		config,
	)

	val, err := db.service.UpdateItem(ctx, req)
	if err != nil {
		if recoverConditionalCheckFailedException(err) {
			return db.undefined, errPreConditionFailed(entity,
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
