package dynamo_test

import (
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/fogfish/dynamo"
	"github.com/fogfish/it"
)

func TestIRI(t *testing.T) {
	it.Ok(t).
		If(dynamo.IRI{}.Path()).Should().Equal("").
		If(dynamo.ParseIRI("")).Should().Equal(dynamo.IRI{}).
		//
		If(dynamo.IRI{Prefix: "a"}.Path()).Should().Equal("a").
		If(dynamo.ParseIRI("a")).Should().Equal(dynamo.IRI{Prefix: "a"}).
		//
		If(dynamo.IRI{"a", "b"}.Path()).Should().Equal("a/b").
		If(dynamo.ParseIRI("a/b")).Should().Equal(dynamo.IRI{"a", "b"}).
		//
		If(dynamo.IRI{"a/b", "c"}.Path()).Should().Equal("a/b/c").
		If(dynamo.ParseIRI("a/b/c")).Should().Equal(dynamo.IRI{"a/b", "c"}).
		//
		If(dynamo.IRI{"a/b/c", "d"}.Path()).Should().Equal("a/b/c/d").
		If(dynamo.ParseIRI("a/b/c/d")).Should().Equal(dynamo.IRI{"a/b/c", "d"})
}

func TestSubIRI(t *testing.T) {
	it.Ok(t).
		If(dynamo.IRI{}.Heir("a")).Should().Equal(dynamo.IRI{Prefix: "a"}).
		If(dynamo.IRI{Prefix: "a"}.Heir("b")).Should().Equal(dynamo.IRI{"a", "b"}).
		If(dynamo.IRI{"a", "b"}.Heir("c")).Should().Equal(dynamo.IRI{"a/b", "c"}).
		If(dynamo.IRI{"a/b", "c"}.Heir("d")).Should().Equal(dynamo.IRI{"a/b/c", "d"}).
		//
		If(dynamo.IRI{}.Parent()).Should().Equal(dynamo.IRI{}).
		If(dynamo.IRI{Prefix: "a"}.Parent()).Should().Equal(dynamo.IRI{}).
		If(dynamo.IRI{"a", "b"}.Parent()).Should().Equal(dynamo.IRI{}).
		If(dynamo.IRI{"a/b", "c"}.Parent()).Should().Equal(dynamo.IRI{"a", "b"}).
		If(dynamo.IRI{"a/b/c", "d"}.Parent()).Should().Equal(dynamo.IRI{"a/b", "c"})
}

type Item struct {
	dynamo.ID
	Ref dynamo.IRI `json:"ref"  dynamodbav:"ref,omitempty"`
	Tag string     `json:"tag"  dynamodbav:"tag,omitempty"`
}

var fixtureItem Item = Item{
	ID:  dynamo.ID{dynamo.IRI{"foo/prefix", "suffix"}},
	Ref: dynamo.IRI{"foo/a", "suffix"},
	Tag: "tag",
}
var fixtureJson string = "{\"id\":\"/foo/prefix/suffix\",\"ref\":\"/foo/a/suffix\",\"tag\":\"tag\"}"

var fixtureDdb map[string]*dynamodb.AttributeValue = map[string]*dynamodb.AttributeValue{
	"id":  &dynamodb.AttributeValue{S: aws.String("foo/prefix/suffix")},
	"ref": &dynamodb.AttributeValue{S: aws.String("foo/a/suffix")},
	"tag": &dynamodb.AttributeValue{S: aws.String("tag")},
}

func TestMarshalJSON(t *testing.T) {
	bytes, err := json.Marshal(fixtureItem)

	it.Ok(t).
		If(err).Should().Equal(nil).
		If(string(bytes)).Should().Equal(fixtureJson)
}

func TestUnmarshalJSON(t *testing.T) {
	var item Item

	it.Ok(t).
		If(json.Unmarshal([]byte(fixtureJson), &item)).Should().Equal(nil).
		If(item).Should().Equal(fixtureItem)
}

func TestUnmarshalInvalidJSON(t *testing.T) {
	var item Item

	badUid := []byte("{\"id\":\"foo/prefix/suffix\",\"ref\":\"/foo/a/suffix\",\"tag\":\"tag\"}")
	badRef := []byte("{\"id\":\"/foo/prefix/suffix\",\"ref\":\"foo/a/suffix\",\"tag\":\"tag\"}")

	it.Ok(t).
		If(json.Unmarshal(badUid, &item)).Should().Be().Like(dynamo.InvalidIRI{}).
		If(json.Unmarshal(badRef, &item)).Should().Be().Like(dynamo.InvalidIRI{})
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
