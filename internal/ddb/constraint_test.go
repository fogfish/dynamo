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
	"github.com/fogfish/curie"
	"github.com/fogfish/dynamo/v2"
	"github.com/fogfish/it"
)

type tConstrain struct {
	Name string `dynamodbav:"anothername,omitempty"`
}

func (tConstrain) HashKey() curie.IRI { return "" }
func (tConstrain) SortKey() curie.IRI { return "" }

var Name = dynamo.Schema1[tConstrain, string]("Name")

func TestConditionExpression(t *testing.T) {
	var (
		expr *string = nil
	)

	spec := map[string]func(string) dynamo.Constraint[tConstrain]{
		"=":  Name.Eq,
		"<>": Name.Ne,
		"<":  Name.Lt,
		"<=": Name.Le,
		">":  Name.Gt,
		">=": Name.Ge,
	}

	for op, fn := range spec {
		config := []dynamo.Constraint[tConstrain]{fn("abc")}
		name, vals := maybeConditionExpression(&expr, config)

		expectExpr := fmt.Sprintf("#__anothername__ %s :__anothername__", op)
		expectName := "anothername"
		expectVals := &types.AttributeValueMemberS{Value: "abc"}

		it.Ok(t).
			If(*expr).Should().Equal(expectExpr).
			If(vals[":__anothername__"]).Should().Equal(expectVals).
			If(name["#__anothername__"]).Should().Equal(expectName)
	}
}

func TestExists(t *testing.T) {
	var (
		expr *string = nil
	)

	config := []dynamo.Constraint[tConstrain]{Name.Exists()}
	name, vals := maybeConditionExpression(&expr, config)

	expectExpr := "attribute_exists(#__anothername__)"
	expectName := map[string]string{"#__anothername__": "anothername"}

	it.Ok(t).
		If(*expr).Should().Equal(expectExpr).
		If(len(vals)).Should().Equal(0).
		If(name).Should().Equal(expectName)
}

func TestNotExists(t *testing.T) {
	var (
		expr *string = nil
	)

	config := []dynamo.Constraint[tConstrain]{Name.NotExists()}
	name, vals := maybeConditionExpression(&expr, config)

	expectExpr := "attribute_not_exists(#__anothername__)"
	expectName := map[string]string{"#__anothername__": "anothername"}

	it.Ok(t).
		If(*expr).Should().Equal(expectExpr).
		If(len(vals)).Should().Equal(0).
		If(name).Should().Equal(expectName)
}

func TestIs(t *testing.T) {
	var (
		expr *string = nil
	)

	config := []dynamo.Constraint[tConstrain]{Name.Is("_")}
	name, vals := maybeConditionExpression(&expr, config)

	expectExpr := "attribute_not_exists(#__anothername__)"
	expectName := map[string]string{"#__anothername__": "anothername"}

	it.Ok(t).
		If(*expr).Should().Equal(expectExpr).
		If(len(vals)).Should().Equal(0).
		If(name).Should().Equal(expectName)

	//
	config = []dynamo.Constraint[tConstrain]{Name.Is("abc")}
	name, vals = maybeConditionExpression(&expr, config)

	expectExpr = "#__anothername__ = :__anothername__"
	expectVals := map[string]types.AttributeValue{
		":__anothername__": &types.AttributeValueMemberS{Value: "abc"},
	}

	it.Ok(t).
		If(*expr).Should().Equal(expectExpr).
		If(vals).Should().Equal(expectVals).
		If(name).Should().Equal(expectName)
}
