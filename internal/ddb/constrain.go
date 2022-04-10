package ddb

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/fogfish/dynamo"
	"github.com/fogfish/dynamo/internal/constrain"
)

//
// Internal
//
func maybeConditionExpression[T dynamo.ThingV2](
	conditionExpression **string,
	config []dynamo.ConstrainV2[T],
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
