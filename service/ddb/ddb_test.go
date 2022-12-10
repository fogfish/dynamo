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
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/fogfish/curie"
	"github.com/fogfish/dynamo/v2/internal/ddbtest"
	"github.com/fogfish/dynamo/v2/internal/dynamotest"
	"github.com/fogfish/dynamo/v2/service/ddb"
	"github.com/fogfish/it"
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
		Prefix:  curie.New("dead:beef"),
		Suffix:  curie.New("1"),
		Name:    "Verner Pleishner",
		Age:     64,
		Address: "Blumenstrasse 14, Berne, 3013",
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

func codec(p dynamotest.Person) (map[string]types.AttributeValue, error) {
	return attributevalue.MarshalMap(p)
}

func TestDynamoDB(t *testing.T) {
	dynamotest.TestGet(t, codec, ddbtest.GetItem[dynamotest.Person])
	dynamotest.TestPut(t, codec, ddbtest.PutItem[dynamotest.Person])
	dynamotest.TestRemove(t, codec, ddbtest.DeleteItem[dynamotest.Person])
	dynamotest.TestUpdate(t, codec, ddbtest.UpdateItem[dynamotest.Person])
	dynamotest.TestMatch(t, codec, ddbtest.Query[dynamotest.Person])
}

func TestDdbPutWithConstrain(t *testing.T) {
	name := ddb.Schema[person, string]("Name").Condition()
	ddb := ddbtest.Constrains[person](nil)

	success := ddb.Put(context.TODO(), entityStruct(), name.Eq("xxx"))
	failure := ddb.Put(context.TODO(), entityStruct(), name.Eq("yyy"))
	_, ispcf := failure.(interface{ PreConditionFailed() bool })

	it.Ok(t).
		If(success).Should().Equal(nil).
		IfTrue(ispcf)
}

func TestDdbRemoveWithConstrain(t *testing.T) {
	name := ddb.Schema[person, string]("Name").Condition()
	ddb := ddbtest.Constrains[person](entityDynamo())

	_, success := ddb.Remove(context.TODO(), entityStruct(), name.Eq("xxx"))
	_, failure := ddb.Remove(context.TODO(), entityStruct(), name.Eq("yyy"))
	_, ispcf := failure.(interface{ PreConditionFailed() bool })

	it.Ok(t).
		If(success).Should().Equal(nil).
		IfTrue(ispcf)
}

func TestDdbUpdateWithConstrain(t *testing.T) {
	name := ddb.Schema[person, string]("Name").Condition()
	ddb := ddbtest.Constrains[person](entityDynamo())
	patch := person{
		Prefix: curie.New("dead:beef"),
		Suffix: curie.New("1"),
		Age:    65,
	}

	_, success := ddb.Update(context.TODO(), patch, name.Eq("xxx"))
	_, failure := ddb.Update(context.TODO(), patch, name.Eq("yyy"))
	_, ispcf := failure.(interface{ PreConditionFailed() bool })

	it.Ok(t).
		If(success).Should().Equal(nil).
		IfTrue(ispcf)
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
		Prefix: curie.New("dead:beef"),
		Suffix: curie.New("1"),
	}
	age := ddb.Schema[person, int]("Age").Updater()
	db := ddbtest.UpdateItem[person](&fixtureKey, &fixtureVal, &returnVal).(*ddb.Storage[person])

	_, success := db.UpdateWith(context.Background(),
		ddb.Updater(key, age.Set(64)),
	)

	it.Ok(t).
		If(success).Should().Equal(nil)
}
