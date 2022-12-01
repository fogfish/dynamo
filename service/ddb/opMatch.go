package ddb

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/fogfish/dynamo/v2"
)

// Match applies a pattern matching to elements in the table
func (db *Storage[T]) Match(ctx context.Context, key T, opts ...interface{ MatchOpt() }) ([]T, error) {
	gen, err := db.codec.EncodeKey(key)
	if err != nil {
		return nil, errInvalidKey.New(err)
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

	q := db.reqQuery(gen, expr, opts...)
	val, err := db.service.Query(ctx, q)
	if err != nil {
		return nil, errServiceIO.New(err)
	}

	seq := make([]T, val.Count)
	for i := 0; i < int(val.Count); i++ {
		obj, err := db.codec.Decode(val.Items[i])
		if err != nil {
			return nil, errInvalidEntity.New(err)
		}
		seq[i] = obj
	}

	return seq, nil
}

func (db *Storage[T]) reqQuery(
	gen map[string]types.AttributeValue,
	expr string,
	opts ...interface{ MatchOpt() },
) *dynamodb.QueryInput {
	var (
		limit             *int32                          = nil
		exclusiveStartKey map[string]types.AttributeValue = nil
	)
	for _, opt := range opts {
		switch v := opt.(type) {
		case dynamo.Limit:
			limit = aws.Int32(int32(v))
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

	return &dynamodb.QueryInput{
		KeyConditionExpression:    aws.String(expr),
		ExpressionAttributeValues: exprOf(gen),
		ProjectionExpression:      db.schema.Projection,
		ExpressionAttributeNames:  db.schema.ExpectedAttributeNames,
		TableName:                 db.table,
		IndexName:                 db.index,
		Limit:                     limit,
		ExclusiveStartKey:         exclusiveStartKey,
	}
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
