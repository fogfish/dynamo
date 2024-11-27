//
// Copyright (C) 2022 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

package ddb_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/fogfish/curie/v2"
	"github.com/fogfish/dynamo/v3/internal/ddbtest"
	"github.com/fogfish/dynamo/v3/internal/dynamotest"
	"github.com/fogfish/dynamo/v3/service/ddb"
	"github.com/fogfish/it/v2"
)

type person struct {
	Prefix  curie.IRI `dynamodbav:"prefix,omitempty"`
	Suffix  curie.IRI `dynamodbav:"suffix,omitempty"`
	Name    string    `dynamodbav:"name,omitempty"`
	Age     int       `dynamodbav:"age,omitempty"`
	Address string    `dynamodbav:"address,omitempty"`
}

func (p person) HashKey() curie.IRI { return p.Prefix }
func (p person) SortKey() curie.IRI { return p.Suffix }

func entityStruct() person {
	return person{
		Prefix:  curie.IRI("dead:beef"),
		Suffix:  curie.IRI("1"),
		Name:    "Verner Pleishner",
		Age:     64,
		Address: "Blumenstrasse 14, Berne, 3013",
	}
}

func entityStructKey() person {
	return person{
		Prefix: curie.IRI("dead:beef"),
		Suffix: curie.IRI("1"),
	}
}

func entityDynamo() map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		"prefix":  &types.AttributeValueMemberS{Value: "dead:beef"},
		"suffix":  &types.AttributeValueMemberS{Value: "1"},
		"address": &types.AttributeValueMemberS{Value: "Blumenstrasse 14, Berne, 3013"},
		"name":    &types.AttributeValueMemberS{Value: "Verner Pleishner"},
		"age":     &types.AttributeValueMemberN{Value: "64"},
	}
}

func entityDynamoKey() map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		"prefix": &types.AttributeValueMemberS{Value: "dead:beef"},
		"suffix": &types.AttributeValueMemberS{Value: "1"},
	}
}

func codec(p dynamotest.Person) (map[string]types.AttributeValue, error) {
	return attributevalue.MarshalMap(p)
}

func TestNew(t *testing.T) {
	api, err := ddb.New[dynamotest.Person]("abc")
	it.Then(t).
		Should(it.Nil(err)).
		ShouldNot(it.Nil(api))
}
func TestDynamoDB(t *testing.T) {
	dynamotest.TestGet(t, codec, ddbtest.GetItem[dynamotest.Person])
	dynamotest.TestPut(t, codec, ddbtest.PutItem[dynamotest.Person])
	dynamotest.TestRemove(t, codec, ddbtest.DeleteItem[dynamotest.Person])
	dynamotest.TestUpdate(t, codec, ddbtest.UpdateItem[dynamotest.Person])
	dynamotest.TestMatch(t, codec, ddbtest.Query[dynamotest.Person])
}

func TestDdbPutWithConstrain(t *testing.T) {
	name := ddb.ClauseFor[person, string]("Name")
	ddb := ddbtest.Constrains[person](nil)

	success := ddb.Put(context.TODO(), entityStruct(), name.Eq("xxx"))
	failure := ddb.Put(context.TODO(), entityStruct(), name.Eq("yyy"))
	_, ispcf := failure.(interface{ PreConditionFailed() bool })

	it.Then(t).Should(
		it.Nil(success),
		it.True(ispcf),
	)
}

func TestDdbRemoveWithConstrain(t *testing.T) {
	name := ddb.ClauseFor[person, string]("Name")
	ddb := ddbtest.Constrains[person](entityDynamo())

	_, success := ddb.Remove(context.TODO(), entityStruct(), name.Eq("xxx"))
	_, failure := ddb.Remove(context.TODO(), entityStruct(), name.Eq("yyy"))
	_, ispcf := failure.(interface{ PreConditionFailed() bool })

	it.Then(t).Should(
		it.Nil(success),
		it.True(ispcf),
	)
}

func TestDdbUpdateWithConstrain(t *testing.T) {
	name := ddb.ClauseFor[person, string]("Name")
	ddb := ddbtest.Constrains[person](entityDynamo())
	patch := person{
		Prefix: curie.IRI("dead:beef"),
		Suffix: curie.IRI("1"),
		Age:    65,
	}

	_, success := ddb.Update(context.TODO(), patch, name.Eq("xxx"))
	_, failure := ddb.Update(context.TODO(), patch, name.Eq("yyy"))
	_, ispcf := failure.(interface{ PreConditionFailed() bool })

	it.Then(t).Should(
		it.Nil(success),
		it.True(ispcf),
	)
}

func TestDdbUpdateWithExpression(t *testing.T) {
	fixtureKey := map[string]types.AttributeValue{
		"prefix": &types.AttributeValueMemberS{Value: "dead:beef"},
		"suffix": &types.AttributeValueMemberS{Value: "1"},
	}

	fixtureVal := map[string]types.AttributeValue{
		"age": &types.AttributeValueMemberN{Value: "64"},
	}

	returnVal := map[string]types.AttributeValue{
		"prefix":  &types.AttributeValueMemberS{Value: "dead:beef"},
		"suffix":  &types.AttributeValueMemberS{Value: "1"},
		"address": &types.AttributeValueMemberS{Value: "Blumenstrasse 14, Berne, 3013"},
		"name":    &types.AttributeValueMemberS{Value: "Verner Pleishner"},
		"age":     &types.AttributeValueMemberN{Value: "64"},
	}

	key := person{
		Prefix: curie.IRI("dead:beef"),
		Suffix: curie.IRI("1"),
	}
	age := ddb.UpdateFor[person, int]("Age")
	db := ddbtest.UpdateItem[person](&fixtureKey, &fixtureVal, &returnVal).(*ddb.Storage[person])

	_, success := db.UpdateWith(context.Background(),
		ddb.Updater(key, age.Set(64)),
	)

	it.Then(t).Should(
		it.Nil(success),
	)
}

func TestDdbBatchPut(t *testing.T) {
	expectVal := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			"test": {
				{PutRequest: &types.PutRequest{Item: entityDynamo()}},
				{PutRequest: &types.PutRequest{Item: entityDynamo()}},
			},
		},
	}

	inputSeq := []person{
		entityStruct(),
		entityStruct(),
	}

	t.Run("Put", func(t *testing.T) {
		mock := ddbtest.BatchWriteItem{
			Mock: ddbtest.Mock[dynamodb.BatchWriteItemInput, dynamodb.BatchWriteItemOutput]{
				ExpectVal: expectVal,
				ReturnVal: &dynamodb.BatchWriteItemOutput{},
			},
		}

		api := ddb.Must(ddb.New[person]("test", ddb.WithDynamoDB(mock)))

		out, err := api.BatchPut(context.Background(), inputSeq)
		it.Then(t).Should(
			it.Nil(err),
			it.Seq(out).BeEmpty(),
		)
	})

	t.Run("PutPartial", func(t *testing.T) {
		mock := ddbtest.BatchWriteItem{
			Mock: ddbtest.Mock[dynamodb.BatchWriteItemInput, dynamodb.BatchWriteItemOutput]{
				ExpectVal: expectVal,
				ReturnVal: &dynamodb.BatchWriteItemOutput{
					UnprocessedItems: map[string][]types.WriteRequest{
						"test": {
							{PutRequest: &types.PutRequest{Item: entityDynamo()}},
						},
					},
				},
			},
		}

		api := ddb.Must(ddb.New[person]("test", ddb.WithDynamoDB(mock)))

		out, err := api.BatchPut(context.Background(), inputSeq)
		it.Then(t).Should(
			it.Seq(out).Equal(entityStruct()),
		).ShouldNot(
			it.Nil(err),
		)
	})
}

func TestDdbBatchRemove(t *testing.T) {
	expectVal := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			"test": {
				{DeleteRequest: &types.DeleteRequest{Key: entityDynamoKey()}},
				{DeleteRequest: &types.DeleteRequest{Key: entityDynamoKey()}},
			},
		},
	}

	inputSeq := []person{
		entityStructKey(),
		entityStructKey(),
	}

	t.Run("Remove", func(t *testing.T) {
		mock := ddbtest.BatchWriteItem{
			Mock: ddbtest.Mock[dynamodb.BatchWriteItemInput, dynamodb.BatchWriteItemOutput]{
				ExpectVal: expectVal,
				ReturnVal: &dynamodb.BatchWriteItemOutput{},
			},
		}

		api := ddb.Must(ddb.New[person]("test", ddb.WithDynamoDB(mock)))

		out, err := api.BatchRemove(context.Background(), inputSeq)
		it.Then(t).Should(
			it.Nil(err),
			it.Seq(out).BeEmpty(),
		)
	})

	t.Run("RemovePartial", func(t *testing.T) {
		mock := ddbtest.BatchWriteItem{
			Mock: ddbtest.Mock[dynamodb.BatchWriteItemInput, dynamodb.BatchWriteItemOutput]{
				ExpectVal: expectVal,
				ReturnVal: &dynamodb.BatchWriteItemOutput{
					UnprocessedItems: map[string][]types.WriteRequest{
						"test": {
							{PutRequest: &types.PutRequest{Item: entityDynamo()}},
						},
					},
				},
			},
		}

		api := ddb.Must(ddb.New[person]("test", ddb.WithDynamoDB(mock)))

		out, err := api.BatchRemove(context.Background(), inputSeq)
		it.Then(t).Should(
			it.Seq(out).Equal(entityStruct()),
		).ShouldNot(
			it.Nil(err),
		)
	})
}

func TestDdbBatchGet(t *testing.T) {
	expectVal := &dynamodb.BatchGetItemInput{
		RequestItems: map[string]types.KeysAndAttributes{
			"test": {
				Keys: []map[string]types.AttributeValue{
					entityDynamoKey(),
					entityDynamoKey(),
				},
			},
		},
	}

	inputSeq := []person{
		entityStructKey(),
		entityStructKey(),
	}

	t.Run("Get", func(t *testing.T) {
		mock := ddbtest.BatchGetItem{
			Mock: ddbtest.Mock[dynamodb.BatchGetItemInput, dynamodb.BatchGetItemOutput]{
				ExpectVal: expectVal,
				ReturnVal: &dynamodb.BatchGetItemOutput{
					Responses: map[string][]map[string]types.AttributeValue{
						"test": {
							entityDynamo(),
							entityDynamo(),
						},
					},
				},
			},
		}

		api := ddb.Must(ddb.New[person]("test", ddb.WithDynamoDB(mock)))

		seq, err := api.BatchGet(context.Background(), inputSeq)
		it.Then(t).Should(
			it.Nil(err),
			it.Seq(seq).Equal(entityStruct(), entityStruct()),
		)
	})
}
