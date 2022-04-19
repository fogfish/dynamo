package ddb

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/fogfish/dynamo"
	"github.com/fogfish/dynamo/internal/constrain"
)

/*

Internal implementation of conditional expressions for dynamo db
*/
func maybeConditionExpression[T dynamo.Thing](
	conditionExpression **string,
	config []dynamo.Constrain[T],
) (
	expressionAttributeNames map[string]*string,
	expressionAttributeValues map[string]*dynamodb.AttributeValue,
) {
	if len(config) > 0 {
		expressionAttributeNames = map[string]*string{}
		expressionAttributeValues = map[string]*dynamodb.AttributeValue{}

		switch op := config[0].(type) {
		case *constrain.Dyadic:
			dyadic(op,
				conditionExpression,
				expressionAttributeNames,
				expressionAttributeValues,
			)
		case *constrain.Unary:
			unary(op,
				conditionExpression,
				expressionAttributeNames,
				expressionAttributeValues,
			)
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
	expressionAttributeNames map[string]*string,
	expressionAttributeValues map[string]*dynamodb.AttributeValue,
	config []dynamo.Constrain[T],
) {
	if len(config) > 0 {
		switch op := config[0].(type) {
		case *constrain.Dyadic:
			dyadic(op,
				conditionExpression,
				expressionAttributeNames,
				expressionAttributeValues,
			)
		case *constrain.Unary:
			unary(op,
				conditionExpression,
				expressionAttributeNames,
				expressionAttributeValues,
			)
		}
	}
}

/*

dyadic translate expression to dynamo format
*/
func dyadic(
	op *constrain.Dyadic,
	conditionExpression **string,
	expressionAttributeNames map[string]*string,
	expressionAttributeValues map[string]*dynamodb.AttributeValue,
) {
	if op.Key == "" {
		return
	}

	lit, err := dynamodbattribute.Marshal(op.Val)
	if err != nil {
		return
	}

	key := "#__" + op.Key + "__"
	let := ":__" + op.Key + "__"
	expressionAttributeValues[let] = lit
	expressionAttributeNames[key] = &op.Key
	*conditionExpression = aws.String(key + " " + op.Op + " " + let)
	return
}

/*

unary translate expression to dynamo format
*/
func unary(
	op *constrain.Unary,
	conditionExpression **string,
	expressionAttributeNames map[string]*string,
	expressionAttributeValues map[string]*dynamodb.AttributeValue,
) {
	if op.Key == "" {
		return
	}

	key := "#__" + op.Key + "__"
	expressionAttributeNames[key] = aws.String(op.Key)

	*conditionExpression = aws.String(op.Op + "(" + key + ")")
	return
}
