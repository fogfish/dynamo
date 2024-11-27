//
// Copyright (C) 2022 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

package ddb

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/fogfish/curie/v2"
	"github.com/fogfish/it"
)

type tConstrain struct {
	Name string `dynamodbav:"anothername,omitempty"`
}

func (tConstrain) HashKey() curie.IRI { return "" }
func (tConstrain) SortKey() curie.IRI { return "" }

var (
	Name = ClauseFor[tConstrain, string]("Name")
	Type = ClauseFor[tConstrain, string]()
)

func TestConditionExpression(t *testing.T) {
	var (
		expr *string = nil
	)

	spec := map[string]func(string) interface{ WriterOpt(tConstrain) }{
		"=":  Name.Eq,
		"<>": Name.Ne,
		"<":  Name.Lt,
		"<=": Name.Le,
		">":  Name.Gt,
		">=": Name.Ge,
	}

	for op, fn := range spec {
		expr = nil
		opts := []interface{ WriterOpt(tConstrain) }{fn("abc")}
		name, vals := maybeConditionExpression(&expr, opts)

		expectExpr := fmt.Sprintf("(#__c_anothername__ %s :__c_anothername__)", op)
		expectName := "anothername"
		expectVals := &types.AttributeValueMemberS{Value: "abc"}

		it.Ok(t).
			If(*expr).Should().Equal(expectExpr).
			If(vals[":__c_anothername__"]).Should().Equal(expectVals).
			If(name["#__c_anothername__"]).Should().Equal(expectName)
	}
}

func TestExists(t *testing.T) {
	var (
		expr *string = nil
	)

	opts := []interface{ WriterOpt(tConstrain) }{Name.Exists()}
	name, vals := maybeConditionExpression(&expr, opts)

	expectExpr := "(attribute_exists(#__c_anothername__))"
	expectName := map[string]string{"#__c_anothername__": "anothername"}

	it.Ok(t).
		If(*expr).Should().Equal(expectExpr).
		If(len(vals)).Should().Equal(0).
		If(name).Should().Equal(expectName)
}

func TestNotExists(t *testing.T) {
	var (
		expr *string = nil
	)

	opts := []interface{ WriterOpt(tConstrain) }{Name.NotExists()}
	name, vals := maybeConditionExpression(&expr, opts)

	expectExpr := "(attribute_not_exists(#__c_anothername__))"
	expectName := map[string]string{"#__c_anothername__": "anothername"}

	it.Ok(t).
		If(*expr).Should().Equal(expectExpr).
		If(len(vals)).Should().Equal(0).
		If(name).Should().Equal(expectName)
}

func TestBetween(t *testing.T) {
	var (
		expr *string = nil
	)

	opts := []interface{ WriterOpt(tConstrain) }{Name.Between("abc", "def")}
	name, vals := maybeConditionExpression(&expr, opts)

	expectExpr := "(#__c_anothername__ BETWEEN :__c_anothername_a__ AND :__c_anothername_b__)"
	expectName := map[string]string{"#__c_anothername__": "anothername"}
	expectVals := map[string]types.AttributeValue{
		":__c_anothername_a__": &types.AttributeValueMemberS{Value: "abc"},
		":__c_anothername_b__": &types.AttributeValueMemberS{Value: "def"},
	}

	it.Ok(t).
		If(*expr).Should().Equal(expectExpr).
		If(vals).Should().Equal(expectVals).
		If(name).Should().Equal(expectName)
}

func TestIn(t *testing.T) {
	var (
		expr *string = nil
	)

	opts := []interface{ WriterOpt(tConstrain) }{Name.In("abc", "def", "foo")}
	name, vals := maybeConditionExpression(&expr, opts)

	expectExpr := "(#__c_anothername__ IN (:__c_anothername_0__,:__c_anothername_1__,:__c_anothername_2__))"
	expectName := map[string]string{"#__c_anothername__": "anothername"}
	expectVals := map[string]types.AttributeValue{
		":__c_anothername_0__": &types.AttributeValueMemberS{Value: "abc"},
		":__c_anothername_1__": &types.AttributeValueMemberS{Value: "def"},
		":__c_anothername_2__": &types.AttributeValueMemberS{Value: "foo"},
	}

	it.Ok(t).
		If(*expr).Should().Equal(expectExpr).
		If(vals).Should().Equal(expectVals).
		If(name).Should().Equal(expectName)
}

func TestHasPrefix(t *testing.T) {
	var (
		expr *string = nil
	)

	opts := []interface{ WriterOpt(tConstrain) }{Name.HasPrefix("abc")}
	name, vals := maybeConditionExpression(&expr, opts)

	expectExpr := "(begins_with(#__c_anothername__,:__c_anothername__))"
	expectName := map[string]string{"#__c_anothername__": "anothername"}
	expectVals := map[string]types.AttributeValue{
		":__c_anothername__": &types.AttributeValueMemberS{Value: "abc"},
	}

	it.Ok(t).
		If(*expr).Should().Equal(expectExpr).
		If(vals).Should().Equal(expectVals).
		If(name).Should().Equal(expectName)
}

func TestContains(t *testing.T) {
	var (
		expr *string = nil
	)

	opts := []interface{ WriterOpt(tConstrain) }{Name.Contains("abc")}
	name, vals := maybeConditionExpression(&expr, opts)

	expectExpr := "(contains(#__c_anothername__,:__c_anothername__))"
	expectName := map[string]string{"#__c_anothername__": "anothername"}
	expectVals := map[string]types.AttributeValue{
		":__c_anothername__": &types.AttributeValueMemberS{Value: "abc"},
	}

	it.Ok(t).
		If(*expr).Should().Equal(expectExpr).
		If(vals).Should().Equal(expectVals).
		If(name).Should().Equal(expectName)
}

func TestIs(t *testing.T) {
	var (
		expr *string = nil
	)

	opts := []interface{ WriterOpt(tConstrain) }{Name.Is("_")}
	name, vals := maybeConditionExpression(&expr, opts)

	expectExpr := "(attribute_not_exists(#__c_anothername__))"
	expectName := map[string]string{"#__c_anothername__": "anothername"}

	it.Ok(t).
		If(*expr).Should().Equal(expectExpr).
		If(len(vals)).Should().Equal(0).
		If(name).Should().Equal(expectName)

	//
	expr = nil
	opts = []interface{ WriterOpt(tConstrain) }{Name.Is("abc")}
	name, vals = maybeConditionExpression(&expr, opts)

	expectExpr = "(#__c_anothername__ = :__c_anothername__)"
	expectVals := map[string]types.AttributeValue{
		":__c_anothername__": &types.AttributeValueMemberS{Value: "abc"},
	}

	it.Ok(t).
		If(*expr).Should().Equal(expectExpr).
		If(vals).Should().Equal(expectVals).
		If(name).Should().Equal(expectName)
}

func TestOptimistic(t *testing.T) {
	var (
		expr *string = nil
	)

	opts := []interface{ WriterOpt(tConstrain) }{Name.Optimistic("abc")}
	name, vals := maybeConditionExpression(&expr, opts)

	expectExpr := "(attribute_not_exists(#__c_anothername__)) or (#__c_anothername__ = :__c_anothername__)"
	expectName := map[string]string{"#__c_anothername__": "anothername"}
	expectVals := map[string]types.AttributeValue{
		":__c_anothername__": &types.AttributeValueMemberS{Value: "abc"},
	}

	it.Ok(t).
		If(*expr).Should().Equal(expectExpr).
		If(vals).Should().Equal(expectVals).
		If(name).Should().Equal(expectName)
}

func TestOneOf(t *testing.T) {
	var (
		expr *string = nil
	)

	opts := []interface{ WriterOpt(tConstrain) }{
		OneOf(Name.NotExists(), Name.Eq("abc")),
	}
	name, vals := maybeConditionExpression(&expr, opts)

	expectExpr := "(attribute_not_exists(#__c_anothername__)) or (#__c_anothername__ = :__c_anothername__)"
	expectName := map[string]string{"#__c_anothername__": "anothername"}
	expectVals := map[string]types.AttributeValue{
		":__c_anothername__": &types.AttributeValueMemberS{Value: "abc"},
	}

	it.Ok(t).
		If(*expr).Should().Equal(expectExpr).
		If(vals).Should().Equal(expectVals).
		If(name).Should().Equal(expectName)
}

func TestAllOf(t *testing.T) {
	var (
		expr *string = nil
	)

	opts := []interface{ WriterOpt(tConstrain) }{
		AllOf(Name.NotExists(), Name.Eq("abc")),
	}
	name, vals := maybeConditionExpression(&expr, opts)

	expectExpr := "(attribute_not_exists(#__c_anothername__)) and (#__c_anothername__ = :__c_anothername__)"
	expectName := map[string]string{"#__c_anothername__": "anothername"}
	expectVals := map[string]types.AttributeValue{
		":__c_anothername__": &types.AttributeValueMemberS{Value: "abc"},
	}

	it.Ok(t).
		If(*expr).Should().Equal(expectExpr).
		If(vals).Should().Equal(expectVals).
		If(name).Should().Equal(expectName)
}
