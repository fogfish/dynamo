//
// Copyright (C) 2022 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

package ddb

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/fogfish/curie"
	"github.com/fogfish/it/v2"
)

type tUpdatable struct {
	Name string   `dynamodbav:"anothername,omitempty"`
	None int      `dynamodbav:"anothernone,omitempty"`
	List []string `dynamodbav:"anotherlist,omitempty"`
}

func (tUpdatable) HashKey() curie.IRI { return "" }
func (tUpdatable) SortKey() curie.IRI { return "" }

var (
	dslName = Schema[tUpdatable, string]("Name").Updater()
	dslNone = Schema[tUpdatable, int]("None").Updater()
	dslList = Schema[tUpdatable, []string]("List").Updater()
)

func TestUpdateExpressionModifyingOne(t *testing.T) {
	val := tUpdatable{}
	dsl := Updater(val, dslName.Set("some"))
	n := dsl.request.ExpressionAttributeNames
	v := dsl.request.ExpressionAttributeValues
	e := *dsl.request.UpdateExpression

	it.Then(t).
		Should(it.Map(n).Have("#__anothername__", "anothername")).
		Should(it.Map(v).Have(":__anothername__", &types.AttributeValueMemberS{Value: "some"})).
		Should(it.Equal(e, "SET #__anothername__ = :__anothername__"))
}

func TestUpdateExpressionModifyingFew(t *testing.T) {
	val := tUpdatable{}
	dsl := Updater(val, dslName.Set("some"), dslNone.Set(1000))
	n := dsl.request.ExpressionAttributeNames
	v := dsl.request.ExpressionAttributeValues
	e := *dsl.request.UpdateExpression

	it.Then(t).
		Should(it.Map(n).Have("#__anothername__", "anothername")).
		Should(it.Map(n).Have("#__anothernone__", "anothernone")).
		Should(it.Map(v).Have(":__anothername__", &types.AttributeValueMemberS{Value: "some"})).
		Should(it.Map(v).Have(":__anothernone__", &types.AttributeValueMemberN{Value: "1000"})).
		Should(it.Equal(e, "SET #__anothername__ = :__anothername__,#__anothernone__ = :__anothernone__"))
}

func TestUpdateExpressionIncrement(t *testing.T) {
	val := tUpdatable{}
	dsl := Updater(val, dslNone.Inc(1))
	n := dsl.request.ExpressionAttributeNames
	v := dsl.request.ExpressionAttributeValues
	e := *dsl.request.UpdateExpression

	it.Then(t).
		Should(it.Map(n).Have("#__anothernone__", "anothernone")).
		Should(it.Map(v).Have(":__anothernone__", &types.AttributeValueMemberN{Value: "1"})).
		Should(it.Equal(e, "SET #__anothernone__ = #__anothernone__ + :__anothernone__"))
}

func TestUpdateExpressionDecrement(t *testing.T) {
	val := tUpdatable{}
	dsl := Updater(val, dslNone.Dec(1))
	n := dsl.request.ExpressionAttributeNames
	v := dsl.request.ExpressionAttributeValues
	e := *dsl.request.UpdateExpression

	it.Then(t).
		Should(it.Map(n).Have("#__anothernone__", "anothernone")).
		Should(it.Map(v).Have(":__anothernone__", &types.AttributeValueMemberN{Value: "1"})).
		Should(it.Equal(e, "SET #__anothernone__ = #__anothernone__ - :__anothernone__"))
}

func TestUpdateExpressionAppend(t *testing.T) {
	val := tUpdatable{}
	dsl := Updater(val, dslList.Append([]string{"a", "b", "c"}))
	n := dsl.request.ExpressionAttributeNames
	v := dsl.request.ExpressionAttributeValues
	e := *dsl.request.UpdateExpression

	it.Then(t).
		Should(it.Map(n).Have("#__anotherlist__", "anotherlist")).
		Should(it.Map(v).Have(":__anotherlist__", &types.AttributeValueMemberL{Value: []types.AttributeValue{&types.AttributeValueMemberS{Value: "a"}, &types.AttributeValueMemberS{Value: "b"}, &types.AttributeValueMemberS{Value: "c"}}})).
		Should(it.Equal(e, "SET #__anotherlist__ = list_append(#__anotherlist__,:__anotherlist__)"))
}

func TestUpdateExpressionPrepend(t *testing.T) {
	val := tUpdatable{}
	dsl := Updater(val, dslList.Prepend([]string{"a", "b", "c"}))
	n := dsl.request.ExpressionAttributeNames
	v := dsl.request.ExpressionAttributeValues
	e := *dsl.request.UpdateExpression

	it.Then(t).
		Should(it.Map(n).Have("#__anotherlist__", "anotherlist")).
		Should(it.Map(v).Have(":__anotherlist__", &types.AttributeValueMemberL{Value: []types.AttributeValue{&types.AttributeValueMemberS{Value: "a"}, &types.AttributeValueMemberS{Value: "b"}, &types.AttributeValueMemberS{Value: "c"}}})).
		Should(it.Equal(e, "SET #__anotherlist__ = list_append(:__anotherlist__,#__anotherlist__)"))
}

func TestUpdateExpressionRemoveAttributeOne(t *testing.T) {
	val := tUpdatable{}
	dsl := Updater(val, dslName.Remove())
	n := dsl.request.ExpressionAttributeNames
	e := *dsl.request.UpdateExpression

	it.Then(t).
		Should(it.Map(n).Have("#__anothername__", "anothername")).
		Should(it.Equal(e, "REMOVE #__anothername__"))
}

func TestUpdateExpressionRemoveAttributeFew(t *testing.T) {
	val := tUpdatable{}
	dsl := Updater(val, dslName.Remove(), dslNone.Remove())
	n := dsl.request.ExpressionAttributeNames
	e := *dsl.request.UpdateExpression

	it.Then(t).
		Should(it.Map(n).Have("#__anothername__", "anothername")).
		Should(it.Map(n).Have("#__anothernone__", "anothernone")).
		Should(it.Equal(e, "REMOVE #__anothername__,#__anothernone__"))
}
