package dynamo_test

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

		expectExpr := fmt.Sprintf("anothername %s :tecanothername", op)
		expectVals := &dynamodb.AttributeValue{S: aws.String("abc")}

		it.Ok(t).
			If(*expr).Should().Equal(expectExpr).
			If(vals[":tecanothername"]).Should().Equal(expectVals)
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
