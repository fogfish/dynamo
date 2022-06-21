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
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
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
	expressionAttributeNames map[string]string,
	expressionAttributeValues map[string]types.AttributeValue,
) {
	if len(config) > 0 {
		expressionAttributeNames = map[string]string{}
		expressionAttributeValues = map[string]types.AttributeValue{}

		switch op := config[0].(type) {
		case *constrain.Dyadic[T]:
			dyadic(op,
				conditionExpression,
				expressionAttributeNames,
				expressionAttributeValues,
			)
		case *constrain.Unary[T]:
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
	expressionAttributeNames map[string]string,
	expressionAttributeValues map[string]types.AttributeValue,
	config []dynamo.Constrain[T],
) {
	if len(config) > 0 {
		switch op := config[0].(type) {
		case *constrain.Dyadic[T]:
			dyadic(op,
				conditionExpression,
				expressionAttributeNames,
				expressionAttributeValues,
			)
		case *constrain.Unary[T]:
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
func dyadic[T dynamo.Thing](
	op *constrain.Dyadic[T],
	conditionExpression **string,
	expressionAttributeNames map[string]string,
	expressionAttributeValues map[string]types.AttributeValue,
) {
	if op.Key == "" {
		return
	}

	lit, err := attributevalue.Marshal(op.Val)
	if err != nil {
		return
	}

	key := "#__" + op.Key + "__"
	let := ":__" + op.Key + "__"
	expressionAttributeValues[let] = lit
	expressionAttributeNames[key] = op.Key
	*conditionExpression = aws.String(key + " " + op.Op + " " + let)
	return
}

/*

unary translate expression to dynamo format
*/
func unary[T dynamo.Thing](
	op *constrain.Unary[T],
	conditionExpression **string,
	expressionAttributeNames map[string]string,
	expressionAttributeValues map[string]types.AttributeValue,
) {
	if op.Key == "" {
		return
	}

	key := "#__" + op.Key + "__"
	expressionAttributeNames[key] = op.Key

	*conditionExpression = aws.String(op.Op + "(" + key + ")")
	return
}
