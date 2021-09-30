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
	HKey curie.IRI     `dynamodbav:"-"`
	SKey curie.IRI     `dynamodbav:"-"`
	Link *curie.String `dynamodbav:"link,omitempty"`
}

func (x MyType) MarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	type tStruct MyType
	return dynamo.Encode(av, dynamo.IRI(x.HKey), dynamo.IRI(x.SKey), tStruct(x))
}

func (x *MyType) UnmarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	type tStruct *MyType
	return dynamo.Decode(av, (*dynamo.IRI)(&x.HKey), (*dynamo.IRI)(&x.SKey), tStruct(x))
}

func TestEncodeDecode(t *testing.T) {
	core := MyType{
		HKey: curie.New("test:a/b"),
		SKey: curie.New("c/d"),
		Link: curie.Safe(curie.New("test:a/b/c")),
	}

	av, err := dynamodbattribute.Marshal(core)
	it.Ok(t).IfNil(err)

	var some MyType
	err = dynamodbattribute.Unmarshal(av, &some)
	it.Ok(t).IfNil(err)

	it.Ok(t).
		IfTrue(curie.Eq(core.HKey, some.HKey)).
		IfTrue(curie.Eq(core.SKey, some.SKey)).
		IfTrue(*core.Link == *some.Link)
}

func TestEncodeDecodeKeyOnly(t *testing.T) {
	core := MyType{
		HKey: curie.New("test:a/b"),
		SKey: curie.New("c/d"),
	}

	av, err := dynamodbattribute.Marshal(core)
	it.Ok(t).IfNil(err)

	var some MyType
	err = dynamodbattribute.Unmarshal(av, &some)
	it.Ok(t).IfNil(err)

	it.Ok(t).
		IfTrue(curie.Eq(core.HKey, some.HKey)).
		IfTrue(curie.Eq(core.SKey, some.SKey))
}

func TestEncodeDecodeKeyOnlyHash(t *testing.T) {
	core := MyType{
		HKey: curie.New("test:a/b"),
	}

	av, err := dynamodbattribute.Marshal(core)
	it.Ok(t).IfNil(err)

	var some MyType
	err = dynamodbattribute.Unmarshal(av, &some)
	it.Ok(t).IfNil(err)

	it.Ok(t).
		IfTrue(curie.Eq(core.HKey, some.HKey)).
		IfTrue(curie.Eq(core.SKey, some.SKey))
}
