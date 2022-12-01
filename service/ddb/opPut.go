package ddb

import (
	"context"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// Put writes entity
func (db *Storage[T]) Put(ctx context.Context, entity T, config ...interface{ Constraint(T) }) error {
	gen, err := db.codec.Encode(entity)
	if err != nil {
		return errInvalidEntity.New(err)
	}

	req := &dynamodb.PutItemInput{
		Item:      gen,
		TableName: db.table,
	}

	names, values := maybeConditionExpression(&req.ConditionExpression, config)
	req.ExpressionAttributeValues = values
	req.ExpressionAttributeNames = names

	_, err = db.service.PutItem(ctx, req)
	if err != nil {
		if recoverConditionalCheckFailedException(err) {
			return errPreConditionFailed(entity,
				strings.Contains(*req.ConditionExpression, "attribute_not_exists") || strings.Contains(*req.ConditionExpression, "="),
				strings.Contains(*req.ConditionExpression, "attribute_exists") || strings.Contains(*req.ConditionExpression, "<>"),
			)
		}
		return errServiceIO.New(err)
	}

	return nil
}
