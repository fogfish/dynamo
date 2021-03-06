package dynamo_test

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
	dynamo.ID
	Ref *dynamo.IRI `json:"ref,omitempty"  dynamodbav:"ref,omitempty"`
	Tag string      `json:"tag,omitempty"  dynamodbav:"tag,omitempty"`
}

var fixtureLink dynamo.ID = dynamo.ID{dynamo.IRI{curie.New("foo:a/suffix")}}

var fixtureItem Item = Item{
	ID:  dynamo.NewID("foo:prefix/suffix"),
	Ref: dynamo.NewID("foo:a/suffix").Unwrap(),
	Tag: "tag",
}
var fixtureJson string = "{\"@id\":\"[foo:prefix/suffix]\",\"ref\":\"[foo:a/suffix]\",\"tag\":\"tag\"}"

var fixtureEmptyItem Item = Item{
	ID: dynamo.NewID("foo:prefix/suffix"),
}
var fixtureEmptyJson string = "{\"@id\":\"[foo:prefix/suffix]\"}"

var fixtureDdb map[string]*dynamodb.AttributeValue = map[string]*dynamodb.AttributeValue{
	"id":  {S: aws.String("foo:prefix/suffix")},
	"ref": {S: aws.String("foo:a/suffix")},
	"tag": {S: aws.String("tag")},
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

func TestIDs(t *testing.T) {
	expect := curie.New("a:b/c")
	a := dynamo.NewID("a:b/c")
	b := dynamo.MkID(curie.New("a:b/c"))

	it.Ok(t).
		If(a.Identity()).Should().Equal(expect).
		If(b.Identity()).Should().Equal(expect)
}

func TestUnwrap(t *testing.T) {
	c := curie.New("a:b/c")
	a := dynamo.IRI{c}
	var b *dynamo.IRI

	it.Ok(t).
		If(*a.Unwrap()).Should().Equal(c).
		If(b.Unwrap()).Should().Equal(nil)
}
