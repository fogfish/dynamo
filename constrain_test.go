package dynamo_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/fogfish/dynamo"
	"github.com/fogfish/iri"
	"github.com/fogfish/it"
)

type tConstrain struct {
	Name string `dynamodbav:"anothername,omitempty"`
}

var Name = dynamo.Thing(tConstrain{}).Field("Name")

func TestCompare(t *testing.T) {
	var (
		expr *string                             = nil
		vals map[string]*dynamodb.AttributeValue = map[string]*dynamodb.AttributeValue{}
	)

	spec := map[string]func(interface{}) dynamo.Config{
		"=":  Name.Eq,
		"<>": Name.Ne,
		"<":  Name.Lt,
		"<=": Name.Le,
		">":  Name.Gt,
		">=": Name.Ge,
	}

	for op, fn := range spec {
		fn("abc")(&expr, vals)

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
		vals map[string]*dynamodb.AttributeValue = map[string]*dynamodb.AttributeValue{}
	)

	Name.Exists()(&expr, vals)

	expectExpr := "attribute_exists(anothername)"
	expectVals := map[string]*dynamodb.AttributeValue{}

	it.Ok(t).
		If(*expr).Should().Equal(expectExpr).
		If(vals).Should().Equal(expectVals)
}

func TestNotExists(t *testing.T) {
	var (
		expr *string                             = nil
		vals map[string]*dynamodb.AttributeValue = map[string]*dynamodb.AttributeValue{}
	)

	Name.NotExists()(&expr, vals)

	expectExpr := "attribute_not_exists(anothername)"
	expectVals := map[string]*dynamodb.AttributeValue{}

	it.Ok(t).
		If(*expr).Should().Equal(expectExpr).
		If(vals).Should().Equal(expectVals)
}

func TestDdbPutWithConstrain(t *testing.T) {
	ceq := dynamo.Thing(person{}).Field("Name")

	success := apiDBWithConstrain().Put(entity(), ceq.Eq("xxx"))
	failure := apiDBWithConstrain().Put(entity(), ceq.Eq("yyy"))

	it.Ok(t).
		If(success).Should().Equal(nil).
		If(failure).Should().Be().Like(dynamo.PreConditionFailed{})
}

func TestDdbRemoveWithConstrain(t *testing.T) {
	ceq := dynamo.Thing(person{}).Field("Name")

	success := apiDBWithConstrain().Remove(entity(), ceq.Eq("xxx"))
	failure := apiDBWithConstrain().Remove(entity(), ceq.Eq("yyy"))

	it.Ok(t).
		If(success).Should().Equal(nil).
		If(failure).Should().Be().Like(dynamo.PreConditionFailed{})
}

func TestDdbUpdateWithConstrain(t *testing.T) {
	ceq := dynamo.Thing(person{}).Field("Name")
	val := person{
		ID:  iri.New("dead:beef"),
		Age: 65,
	}

	success := apiDBWithConstrain().Update(&val, ceq.Eq("xxx"))
	failure := apiDBWithConstrain().Update(&val, ceq.Eq("yyy"))

	it.Ok(t).
		If(success).Should().Equal(nil).
		If(failure).Should().Be().Like(dynamo.PreConditionFailed{})
}

func apiDBWithConstrain() *dynamo.DB {
	client := &dynamo.DB{}
	client.Mock(&mockDDBWithConstrain{})
	return client
}

type mockDDBWithConstrain struct {
	dynamodbiface.DynamoDBAPI
}

func (mockDDBWithConstrain) PutItem(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	if *(input.ExpressionAttributeValues[":__name__"].S) != "xxx" {
		return nil, &dynamodb.ConditionalCheckFailedException{}
	}

	return &dynamodb.PutItemOutput{}, nil
}

func (mockDDBWithConstrain) DeleteItem(input *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
	if *(input.ExpressionAttributeValues[":__name__"].S) != "xxx" {
		return nil, &dynamodb.ConditionalCheckFailedException{}
	}

	return &dynamodb.DeleteItemOutput{}, nil
}

func (mockDDBWithConstrain) UpdateItem(input *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
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
