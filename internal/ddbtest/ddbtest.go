//
// Copyright (C) 2022 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

//
// The file mocks AWS DynamoDB
//

package ddbtest

import (
	"context"
	"errors"
	"reflect"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/fogfish/dynamo/v2"
	ddbapi "github.com/fogfish/dynamo/v2/service/ddb"
)

/*
mock factory
*/
func mock[T dynamo.Thing](mock ddbapi.DynamoDB) dynamo.KeyVal[T] {
	return ddbapi.Must(
		ddbapi.New[T](ddbapi.WithTable("test"), ddbapi.WithService(mock)),
	)
}

/*
GetItem mocks
*/
func GetItem[T dynamo.Thing](
	expectKey *map[string]types.AttributeValue,
	returnVal *map[string]types.AttributeValue,
) dynamo.KeyVal[T] {
	return mock[T](&ddbGetItem{expectKey: expectKey, returnVal: returnVal})
}

type ddbGetItem struct {
	ddbapi.DynamoDB
	expectKey *map[string]types.AttributeValue
	returnVal *map[string]types.AttributeValue
}

func (mock *ddbGetItem) GetItem(ctx context.Context, input *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	if !reflect.DeepEqual(*mock.expectKey, input.Key) {
		return nil, errors.New("unexpected entity")
	}

	if mock.returnVal == nil {
		return &dynamodb.GetItemOutput{}, nil
	}

	return &dynamodb.GetItemOutput{Item: *mock.returnVal}, nil
}

/*
PutItem mock
*/
func PutItem[T dynamo.Thing](
	expectVal *map[string]types.AttributeValue,
) dynamo.KeyVal[T] {
	return mock[T](&ddbPutItem{
		expectVal: expectVal,
	})
}

type ddbPutItem struct {
	ddbapi.DynamoDB
	expectVal *map[string]types.AttributeValue
}

func (mock *ddbPutItem) PutItem(ctx context.Context, input *dynamodb.PutItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	if !reflect.DeepEqual(*mock.expectVal, input.Item) {
		return nil, errors.New("unexpected entity")
	}
	return &dynamodb.PutItemOutput{}, nil
}

/*
DeleteItem mock
*/
func DeleteItem[T dynamo.Thing](
	expectKey *map[string]types.AttributeValue,
	returnVal *map[string]types.AttributeValue,
) dynamo.KeyVal[T] {
	return mock[T](&ddbDeleteItem{expectKey: expectKey, returnVal: returnVal})
}

type ddbDeleteItem struct {
	ddbapi.DynamoDB
	expectKey *map[string]types.AttributeValue
	returnVal *map[string]types.AttributeValue
}

func (mock *ddbDeleteItem) DeleteItem(ctx context.Context, input *dynamodb.DeleteItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	if !reflect.DeepEqual(*mock.expectKey, input.Key) {
		return nil, errors.New("unexpected entity")
	}

	return &dynamodb.DeleteItemOutput{Attributes: *mock.returnVal}, nil
}

/*
UpdateItem mock
*/
func UpdateItem[T dynamo.Thing](
	expectKey *map[string]types.AttributeValue,
	expectVal *map[string]types.AttributeValue,
	returnVal *map[string]types.AttributeValue,
) dynamo.KeyVal[T] {
	return mock[T](&ddbUpdateItem{
		expectKey: expectKey,
		expectVal: expectVal,
		returnVal: returnVal,
	})
}

type ddbUpdateItem struct {
	ddbapi.DynamoDB
	expectKey *map[string]types.AttributeValue
	expectVal *map[string]types.AttributeValue
	returnVal *map[string]types.AttributeValue
}

func (mock *ddbUpdateItem) UpdateItem(ctx context.Context, input *dynamodb.UpdateItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	if !reflect.DeepEqual(*mock.expectKey, input.Key) {
		return nil, errors.New("unexpected entity key")
	}

	for k, v := range *mock.expectVal {
		if k != "prefix" && k != "suffix" {
			if !reflect.DeepEqual(v, input.ExpressionAttributeValues[":__"+k+"__"]) {
				return nil, errors.New("unexpected entity")
			}
		}
	}

	return &dynamodb.UpdateItemOutput{Attributes: *mock.returnVal}, nil
}

/*
Query mock
*/
func Query[T dynamo.Thing](
	expectKey *map[string]types.AttributeValue,
	returnLen int,
	returnVal *map[string]types.AttributeValue,
	returnLastKey *map[string]types.AttributeValue,
) dynamo.KeyVal[T] {
	return mock[T](&ddbQuery{
		expectKey:     expectKey,
		returnLen:     returnLen,
		returnVal:     returnVal,
		returnLastKey: returnLastKey,
	})
}

type ddbQuery struct {
	ddbapi.DynamoDB
	expectKey     *map[string]types.AttributeValue
	returnLen     int
	returnVal     *map[string]types.AttributeValue
	returnLastKey *map[string]types.AttributeValue
}

func (mock *ddbQuery) Query(ctx context.Context, input *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	for k, v := range *mock.expectKey {
		if !reflect.DeepEqual(v, input.ExpressionAttributeValues[":__"+k+"__"]) {
			return nil, errors.New("unexpected entity")
		}
	}

	seq := []map[string]types.AttributeValue{}
	for i := 0; i < mock.returnLen; i++ {
		seq = append(seq, *mock.returnVal)
	}

	var lastEvaluatedKey map[string]types.AttributeValue
	if mock.returnLastKey != nil {
		lastEvaluatedKey = *mock.returnLastKey
	}

	return &dynamodb.QueryOutput{
		ScannedCount:     int32(mock.returnLen),
		Count:            int32(mock.returnLen),
		Items:            seq,
		LastEvaluatedKey: lastEvaluatedKey,
	}, nil
}

func Constrains[T dynamo.Thing](
	returnVal map[string]types.AttributeValue,
) dynamo.KeyVal[T] {
	return mock[T](&ddbConstrains{
		returnVal: returnVal,
	})
}

type ddbConstrains struct {
	ddbapi.DynamoDB
	returnVal map[string]types.AttributeValue
}

func (ddbConstrains) assert(values map[string]types.AttributeValue) error {
	value, exists := values[":__c_name__"]
	if !exists {
		return &types.ConditionalCheckFailedException{}
	}

	switch v := value.(type) {
	case *types.AttributeValueMemberS:
		if v.Value != "xxx" {
			return &types.ConditionalCheckFailedException{}
		}
	default:
		return &types.ConditionalCheckFailedException{}
	}

	return nil
}

func (mock ddbConstrains) PutItem(ctx context.Context, input *dynamodb.PutItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	if err := mock.assert(input.ExpressionAttributeValues); err != nil {
		return nil, err
	}

	return &dynamodb.PutItemOutput{}, nil
}

func (mock ddbConstrains) DeleteItem(ctx context.Context, input *dynamodb.DeleteItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	if err := mock.assert(input.ExpressionAttributeValues); err != nil {
		return nil, err
	}

	return &dynamodb.DeleteItemOutput{
		Attributes: mock.returnVal,
	}, nil
}

func (mock ddbConstrains) UpdateItem(ctx context.Context, input *dynamodb.UpdateItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	if err := mock.assert(input.ExpressionAttributeValues); err != nil {
		return nil, err
	}

	return &dynamodb.UpdateItemOutput{
		Attributes: mock.returnVal,
	}, nil
}
