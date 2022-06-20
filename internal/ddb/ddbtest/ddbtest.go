//
// Copyright (C) 2022 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

package ddbtest

import (
	"errors"
	"reflect"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/fogfish/dynamo"
	"github.com/fogfish/dynamo/keyval"
)

/*

mock factory
*/
type MockDynamoDB interface {
	Mock(db dynamodbiface.DynamoDBAPI)
}

func mock[T dynamo.Thing](mock dynamodbiface.DynamoDBAPI) dynamo.KeyValNoContext[T] {
	client := keyval.Must(keyval.New[T](dynamo.WithURI("ddb:///test")))
	switch v := client.(type) {
	case MockDynamoDB:
		v.Mock(mock)
	default:
		panic("Invalid config")
	}

	return keyval.NewKeyValContextDefault(client)
}

/*

GetItem mocks
*/
func GetItem[T dynamo.Thing](
	expectKey *map[string]*dynamodb.AttributeValue,
	returnVal *map[string]*dynamodb.AttributeValue,
) dynamo.KeyValNoContext[T] {
	return mock[T](&ddbGetItem{expectKey: expectKey, returnVal: returnVal})
}

type ddbGetItem struct {
	dynamodbiface.DynamoDBAPI
	expectKey *map[string]*dynamodb.AttributeValue
	returnVal *map[string]*dynamodb.AttributeValue
}

func (mock *ddbGetItem) GetItemWithContext(ctx aws.Context, input *dynamodb.GetItemInput, opts ...request.Option) (*dynamodb.GetItemOutput, error) {
	if !reflect.DeepEqual(*mock.expectKey, input.Key) {
		return nil, errors.New("Unexpected entity.")
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
	expectVal *map[string]*dynamodb.AttributeValue,
) dynamo.KeyValNoContext[T] {
	return mock[T](&ddbPutItem{
		expectVal: expectVal,
	})
}

type ddbPutItem struct {
	dynamodbiface.DynamoDBAPI
	expectVal *map[string]*dynamodb.AttributeValue
}

func (mock *ddbPutItem) PutItemWithContext(ctx aws.Context, input *dynamodb.PutItemInput, opts ...request.Option) (*dynamodb.PutItemOutput, error) {
	if !reflect.DeepEqual(*mock.expectVal, input.Item) {
		return nil, errors.New("Unexpected entity.")
	}
	return &dynamodb.PutItemOutput{}, nil
}

/*

DeleteItem mock
*/
func DeleteItem[T dynamo.Thing](
	expectKey *map[string]*dynamodb.AttributeValue,
) dynamo.KeyValNoContext[T] {
	return mock[T](&ddbDeleteItem{expectKey: expectKey})
}

type ddbDeleteItem struct {
	dynamodbiface.DynamoDBAPI
	expectKey *map[string]*dynamodb.AttributeValue
}

func (mock *ddbDeleteItem) DeleteItemWithContext(ctx aws.Context, input *dynamodb.DeleteItemInput, opts ...request.Option) (*dynamodb.DeleteItemOutput, error) {
	if !reflect.DeepEqual(*mock.expectKey, input.Key) {
		return nil, errors.New("Unexpected entity.")
	}

	return &dynamodb.DeleteItemOutput{}, nil
}

/*

UpdateItem mock
*/
func UpdateItem[T dynamo.Thing](
	expectKey *map[string]*dynamodb.AttributeValue,
	expectVal *map[string]*dynamodb.AttributeValue,
	returnVal *map[string]*dynamodb.AttributeValue,
) dynamo.KeyValNoContext[T] {
	return mock[T](&ddbUpdateItem{
		expectKey: expectKey,
		expectVal: expectVal,
		retrunVal: returnVal,
	})
}

type ddbUpdateItem struct {
	dynamodbiface.DynamoDBAPI
	expectKey *map[string]*dynamodb.AttributeValue
	expectVal *map[string]*dynamodb.AttributeValue
	retrunVal *map[string]*dynamodb.AttributeValue
}

func (mock *ddbUpdateItem) UpdateItemWithContext(ctx aws.Context, input *dynamodb.UpdateItemInput, opts ...request.Option) (*dynamodb.UpdateItemOutput, error) {
	if !reflect.DeepEqual(*mock.expectKey, input.Key) {
		return nil, errors.New("Unexpected entity key.")
	}

	for k, v := range *mock.expectVal {
		if k != "prefix" && k != "suffix" {
			if !reflect.DeepEqual(v, input.ExpressionAttributeValues[":__"+k+"__"]) {
				return nil, errors.New("Unexpected entity.")
			}
		}
	}

	return &dynamodb.UpdateItemOutput{Attributes: *mock.retrunVal}, nil
}

/*

Query mock
*/
func Query[T dynamo.Thing](
	expectKey *map[string]*dynamodb.AttributeValue,
	returnLen int,
	returnVal *map[string]*dynamodb.AttributeValue,
	returnLastKey *map[string]*dynamodb.AttributeValue,
) dynamo.KeyValNoContext[T] {
	return mock[T](&ddbQuery{
		expectKey:     expectKey,
		returnLen:     returnLen,
		returnVal:     returnVal,
		returnLastKey: returnLastKey,
	})
}

type ddbQuery struct {
	dynamodbiface.DynamoDBAPI
	expectKey     *map[string]*dynamodb.AttributeValue
	returnLen     int
	returnVal     *map[string]*dynamodb.AttributeValue
	returnLastKey *map[string]*dynamodb.AttributeValue
}

func (mock *ddbQuery) QueryWithContext(ctx aws.Context, input *dynamodb.QueryInput, opts ...request.Option) (*dynamodb.QueryOutput, error) {
	for k, v := range *mock.expectKey {
		if !reflect.DeepEqual(v, input.ExpressionAttributeValues[":__"+k+"__"]) {
			return nil, errors.New("Unexpected entity.")
		}
	}

	seq := []map[string]*dynamodb.AttributeValue{}
	for i := 0; i < mock.returnLen; i++ {
		seq = append(seq, *mock.returnVal)
	}

	var lastEvaluatedKey map[string]*dynamodb.AttributeValue
	if mock.returnLastKey != nil {
		lastEvaluatedKey = *mock.returnLastKey
	}

	return &dynamodb.QueryOutput{
		ScannedCount:     aws.Int64(int64(mock.returnLen)),
		Count:            aws.Int64(int64(mock.returnLen)),
		Items:            seq,
		LastEvaluatedKey: lastEvaluatedKey,
	}, nil
}

func Constrains[T dynamo.Thing](
	returnVal map[string]*dynamodb.AttributeValue,
) dynamo.KeyValNoContext[T] {
	return mock[T](&ddbConstrains{
		returnVal: returnVal,
	})
}

//
//
type ddbConstrains struct {
	dynamodbiface.DynamoDBAPI
	returnVal map[string]*dynamodb.AttributeValue
}

func (ddbConstrains) PutItemWithContext(ctx aws.Context, input *dynamodb.PutItemInput, opts ...request.Option) (*dynamodb.PutItemOutput, error) {
	if *(input.ExpressionAttributeValues[":__name__"].S) != "xxx" {
		return nil, &dynamodb.ConditionalCheckFailedException{}
	}

	return &dynamodb.PutItemOutput{}, nil
}

func (ddbConstrains) DeleteItemWithContext(ctx aws.Context, input *dynamodb.DeleteItemInput, opts ...request.Option) (*dynamodb.DeleteItemOutput, error) {
	if *(input.ExpressionAttributeValues[":__name__"].S) != "xxx" {
		return nil, &dynamodb.ConditionalCheckFailedException{}
	}

	return &dynamodb.DeleteItemOutput{}, nil
}

func (mock ddbConstrains) UpdateItemWithContext(ctx aws.Context, input *dynamodb.UpdateItemInput, opts ...request.Option) (*dynamodb.UpdateItemOutput, error) {
	if *(input.ExpressionAttributeValues[":__name__"].S) != "xxx" {
		return nil, &dynamodb.ConditionalCheckFailedException{}
	}

	return &dynamodb.UpdateItemOutput{
		Attributes: mock.returnVal,
	}, nil
}
