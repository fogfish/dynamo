package dynamo_test

import (
	"testing"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/fogfish/curie"
	"github.com/fogfish/dynamo"
	"github.com/fogfish/it"
)

type MyType struct {
	ID   curie.IRI     `dynamodbav:"-"`
	Link *curie.String `dynamodbav:"link,omitempty"`
}

func (x MyType) MarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	type tStruct MyType
	return dynamo.Encode(av, x.ID, tStruct(x))
}

func (x *MyType) UnmarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	type tStruct *MyType
	return dynamo.Decode(av, &x.ID, tStruct(x))
}

func TestEncodeDecode(t *testing.T) {
	core := MyType{
		ID:   curie.New("test:a/b"),
		Link: curie.Safe(curie.New("test:a/b/c")),
	}

	av, err := dynamodbattribute.Marshal(core)
	it.Ok(t).IfNil(err)

	var some MyType
	err = dynamodbattribute.Unmarshal(av, &some)
	it.Ok(t).IfNil(err)

	it.Ok(t).
		IfTrue(curie.Eq(core.ID, some.ID)).
		IfTrue(*core.Link == *some.Link)
}
