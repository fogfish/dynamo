package dynamo_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/fogfish/dynamo"
	"github.com/fogfish/it"
)

type tConstrain struct {
	Name string `dynamodbav:"anothername,omitempty"`
}

var Name = dynamo.Kind(tConstrain{}).Field("Name")

func TestCompare(t *testing.T) {
	var (
		expr *string                             = nil
		name map[string]*string                  = map[string]*string{}
		vals map[string]*dynamodb.AttributeValue = map[string]*dynamodb.AttributeValue{}
	)

	spec := map[string]func(interface{}) dynamo.Constrain{
		"=":  Name.Eq,
		"<>": Name.Ne,
		"<":  Name.Lt,
		"<=": Name.Le,
		">":  Name.Gt,
		">=": Name.Ge,
	}

	for op, fn := range spec {
		fn("abc")(&expr, name, vals)

		expectExpr := fmt.Sprintf("anothername %s :__anothername__", op)
		expectVals := &dynamodb.AttributeValue{S: aws.String("abc")}

		it.Ok(t).
			If(*expr).Should().Equal(expectExpr).
			If(vals[":__anothername__"]).Should().Equal(expectVals)
	}
}

func TestExists(t *testing.T) {
	var (
		expr *string                             = nil
		name map[string]*string                  = map[string]*string{}
		vals map[string]*dynamodb.AttributeValue = map[string]*dynamodb.AttributeValue{}
	)

	Name.Exists()(&expr, name, vals)

	expectExpr := "attribute_exists(#__anothername__)"
	expectVals := map[string]*dynamodb.AttributeValue{}
	expectName := map[string]*string{"#__anothername__": aws.String("anothername")}

	it.Ok(t).
		If(*expr).Should().Equal(expectExpr).
		If(vals).Should().Equal(expectVals).
		If(name).Should().Equal(expectName)
}

func TestNotExists(t *testing.T) {
	var (
		expr *string                             = nil
		name map[string]*string                  = map[string]*string{}
		vals map[string]*dynamodb.AttributeValue = map[string]*dynamodb.AttributeValue{}
	)

	Name.NotExists()(&expr, name, vals)

	expectExpr := "attribute_not_exists(#__anothername__)"
	expectVals := map[string]*dynamodb.AttributeValue{}
	expectName := map[string]*string{"#__anothername__": aws.String("anothername")}

	it.Ok(t).
		If(*expr).Should().Equal(expectExpr).
		If(vals).Should().Equal(expectVals).
		If(name).Should().Equal(expectName)
}

func TestIs(t *testing.T) {
	var (
		expr *string                             = nil
		name map[string]*string                  = map[string]*string{}
		vals map[string]*dynamodb.AttributeValue = map[string]*dynamodb.AttributeValue{}
	)

	Name.Is("_")(&expr, name, vals)

	expectExpr := "attribute_not_exists(#__anothername__)"
	expectVals := map[string]*dynamodb.AttributeValue{}
	expectName := map[string]*string{"#__anothername__": aws.String("anothername")}

	it.Ok(t).
		If(*expr).Should().Equal(expectExpr).
		If(vals).Should().Equal(expectVals).
		If(name).Should().Equal(expectName)

	//
	Name.Is("abc")(&expr, name, vals)

	expectExpr = fmt.Sprintf("anothername = :__anothername__")
	expectVals = map[string]*dynamodb.AttributeValue{":__anothername__": {S: aws.String("abc")}}

	it.Ok(t).
		If(*expr).Should().Equal(expectExpr).
		If(vals).Should().Equal(expectVals).
		If(name).Should().Equal(expectName)
}

func TestDdbPutWithConstrain(t *testing.T) {
	ceq := dynamo.Kind(person{}).Field("Name")
	ddb := mockConstrains()

	success := ddb.Put(entity(), ceq.Eq("xxx"))
	failure := ddb.Put(entity(), ceq.Eq("yyy"))

	it.Ok(t).
		If(success).Should().Equal(nil).
		If(failure).Should().Be().Like(dynamo.PreConditionFailed{})
}

func TestDdbRemoveWithConstrain(t *testing.T) {
	ceq := dynamo.Kind(person{}).Field("Name")
	ddb := mockConstrains()

	success := ddb.Remove(entity(), ceq.Eq("xxx"))
	failure := ddb.Remove(entity(), ceq.Eq("yyy"))

	it.Ok(t).
		If(success).Should().Equal(nil).
		If(failure).Should().Be().Like(dynamo.PreConditionFailed{})
}

func TestDdbUpdateWithConstrain(t *testing.T) {
	ceq := dynamo.Kind(person{}).Field("Name")
	val := person{
		Prefix: dynamo.NewIRI("dead:beef"),
		Suffix: dynamo.NewIRI("1"),
		Age:    65,
	}
	ddb := mockConstrains()

	success := ddb.Update(&val, ceq.Eq("xxx"))
	failure := ddb.Update(&val, ceq.Eq("yyy"))

	it.Ok(t).
		If(success).Should().Equal(nil).
		If(failure).Should().Be().Like(dynamo.PreConditionFailed{})
}

//
//
type ddbConstrains struct {
	dynamodbiface.DynamoDBAPI
}

func mockConstrains() dynamo.KeyValNoContext {
	return mockDynamoDB(&ddbConstrains{})
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

func (ddbConstrains) UpdateItemWithContext(ctx aws.Context, input *dynamodb.UpdateItemInput, opts ...request.Option) (*dynamodb.UpdateItemOutput, error) {
	if *(input.ExpressionAttributeValues[":__name__"].S) != "xxx" {
		return nil, &dynamodb.ConditionalCheckFailedException{}
	}

	return &dynamodb.UpdateItemOutput{
		Attributes: map[string]*dynamodb.AttributeValue{
			"prefix":  {S: aws.String("dead")},
			"address": {S: aws.String("Blumenstrasse 14, Berne, 3013")},
			"name":    {S: aws.String("Verner Pleishner")},
			"suffix":  {S: aws.String("beef")},
			"age":     {N: aws.String("64")},
		},
	}, nil
}
