package ddb

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/fogfish/dynamo"
	"github.com/fogfish/it"
)

type tConstrain struct {
	Name string `dynamodbav:"anothername,omitempty"`
}

func (tConstrain) HashKey() string { return "" }
func (tConstrain) SortKey() string { return "" }

var Name = dynamo.Schema1[tConstrain, string]("Name")

func TestConditionExpression(t *testing.T) {
	var (
		expr *string = nil
	)

	spec := map[string]func(string) dynamo.Constrain[tConstrain]{
		"=":  Name.Eq,
		"<>": Name.Ne,
		"<":  Name.Lt,
		"<=": Name.Le,
		">":  Name.Gt,
		">=": Name.Ge,
	}

	for op, fn := range spec {
		config := []dynamo.Constrain[tConstrain]{fn("abc")}
		name, vals := maybeConditionExpression(&expr, config)

		expectExpr := fmt.Sprintf("#__anothername__ %s :__anothername__", op)
		expectName := "anothername"
		expectVals := &dynamodb.AttributeValue{S: aws.String("abc")}

		it.Ok(t).
			If(*expr).Should().Equal(expectExpr).
			If(vals[":__anothername__"]).Should().Equal(expectVals).
			If(*name["#__anothername__"]).Should().Equal(expectName)
	}
}

func TestExists(t *testing.T) {
	var (
		expr *string = nil
	)

	config := []dynamo.Constrain[tConstrain]{Name.Exists()}
	name, vals := maybeConditionExpression(&expr, config)

	expectExpr := "attribute_exists(#__anothername__)"
	expectName := map[string]*string{"#__anothername__": aws.String("anothername")}

	it.Ok(t).
		If(*expr).Should().Equal(expectExpr).
		If(len(vals)).Should().Equal(0).
		If(name).Should().Equal(expectName)
}

func TestNotExists(t *testing.T) {
	var (
		expr *string = nil
	)

	config := []dynamo.Constrain[tConstrain]{Name.NotExists()}
	name, vals := maybeConditionExpression(&expr, config)

	expectExpr := "attribute_not_exists(#__anothername__)"
	expectName := map[string]*string{"#__anothername__": aws.String("anothername")}

	it.Ok(t).
		If(*expr).Should().Equal(expectExpr).
		If(len(vals)).Should().Equal(0).
		If(name).Should().Equal(expectName)
}

func TestIs(t *testing.T) {
	var (
		expr *string = nil
	)

	config := []dynamo.Constrain[tConstrain]{Name.Is("_")}
	name, vals := maybeConditionExpression(&expr, config)

	expectExpr := "attribute_not_exists(#__anothername__)"
	expectName := map[string]*string{"#__anothername__": aws.String("anothername")}

	it.Ok(t).
		If(*expr).Should().Equal(expectExpr).
		If(len(vals)).Should().Equal(0).
		If(name).Should().Equal(expectName)

	//
	config = []dynamo.Constrain[tConstrain]{Name.Is("abc")}
	name, vals = maybeConditionExpression(&expr, config)

	expectExpr = fmt.Sprintf("#__anothername__ = :__anothername__")
	expectVals := map[string]*dynamodb.AttributeValue{":__anothername__": {S: aws.String("abc")}}

	it.Ok(t).
		If(*expr).Should().Equal(expectExpr).
		If(vals).Should().Equal(expectVals).
		If(name).Should().Equal(expectName)
}
