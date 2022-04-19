package keyval_test

/*

import (
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/fogfish/curie"
	"github.com/fogfish/dynamo"
	"github.com/fogfish/it"
)

type Item struct {
	Prefix dynamo.IRI    `json:"prefix,omitempty"  dynamodbav:"prefix,omitempty"`
	Suffix dynamo.IRI    `json:"suffix,omitempty"  dynamodbav:"suffix,omitempty"`
	Ref    *curie.String `json:"ref,omitempty"  dynamodbav:"ref,omitempty"`
	Tag    string        `json:"tag,omitempty"  dynamodbav:"tag,omitempty"`
}

var fixtureItem Item = Item{
	Prefix: dynamo.NewIRI("foo:prefix"),
	Suffix: dynamo.NewIRI("suffix"),
	Ref:    curie.Safe(curie.IRI(dynamo.NewIRI("foo:a/suffix"))),
	Tag:    "tag",
}
var fixtureEmptyItem Item = Item{
	Prefix: dynamo.NewIRI("foo:prefix"),
	Suffix: dynamo.NewIRI("suffix"),
}
var fixtureJson string = "{\"prefix\":\"[foo:prefix]\",\"suffix\":\"[suffix]\",\"ref\":\"[foo:a/suffix]\",\"tag\":\"tag\"}"

var fixtureEmptyJson string = "{\"prefix\":\"[foo:prefix]\",\"suffix\":\"[suffix]\"}"

var fixtureDdb map[string]*dynamodb.AttributeValue = map[string]*dynamodb.AttributeValue{
	"prefix": {S: aws.String("foo:prefix")},
	"suffix": {S: aws.String("suffix")},
	"ref":    {S: aws.String("[foo:a/suffix]")},
	"tag":    {S: aws.String("tag")},
}

func TestMarshalJSON(t *testing.T) {
	bytes, err := json.Marshal(fixtureItem)

	it.Ok(t).
		If(err).Should().Equal(nil).
		If(string(bytes)).Should().Equal(fixtureJson)
}

func TestMarshalEmptyJSON(t *testing.T) {
	bytes, err := json.Marshal(fixtureEmptyItem)

	it.Ok(t).
		If(err).Should().Equal(nil).
		If(string(bytes)).Should().Equal(fixtureEmptyJson)
}

func TestUnmarshalJSON(t *testing.T) {
	var item Item

	it.Ok(t).
		If(json.Unmarshal([]byte(fixtureJson), &item)).Should().Equal(nil).
		If(item).Should().Equal(fixtureItem)
}

func TestUnmarshalEmptyJSON(t *testing.T) {
	var item Item

	it.Ok(t).
		If(json.Unmarshal([]byte(fixtureEmptyJson), &item)).Should().Equal(nil).
		If(item).Should().Equal(fixtureEmptyItem)
}

func TestMarshalDynamo(t *testing.T) {
	gen, err := dynamodbattribute.MarshalMap(fixtureItem)

	it.Ok(t).
		If(err).Should().Equal(nil).
		If(gen).Should().Equal(fixtureDdb)
}

func TestUnmarshalDynamo(t *testing.T) {
	var item Item

	it.Ok(t).
		If(dynamodbattribute.UnmarshalMap(fixtureDdb, &item)).Should().Equal(nil).
		If(item).Should().Equal(fixtureItem)
}

func TestNew(t *testing.T) {
	it.Ok(t).
		If(dynamo.Must(dynamo.New("ddb:///a"))).ShouldNot().Equal(nil).
		If(dynamo.Must(dynamo.New("s3:///a"))).ShouldNot().Equal(nil)
}

func TestReadOnly(t *testing.T) {
	it.Ok(t).
		If(dynamo.MustReadOnly(dynamo.ReadOnly("ddb:///a"))).ShouldNot().Equal(nil).
		If(dynamo.MustReadOnly(dynamo.ReadOnly("s3:///a"))).ShouldNot().Equal(nil)
}

func TestStream(t *testing.T) {
	it.Ok(t).
		If(dynamo.MustStream(dynamo.NewStream("s3:///a"))).ShouldNot().Equal(nil).
		If(func() {
			dynamo.MustStream(dynamo.NewStream("ddb:///a"))
		}).Should().Fail()
}
*/
