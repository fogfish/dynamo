package dynamo_test

import (
	"encoding/json"
	"testing"

	"github.com/fogfish/dynamo"
	"github.com/fogfish/it"
)

func TestIRI(t *testing.T) {
	it.Ok(t).
		If(dynamo.IRI{}.Path()).Should().Equal("").
		If(dynamo.IRI{Prefix: "a"}.Path()).Should().Equal("a").
		If(dynamo.IRI{"a", "b"}.Path()).Should().Equal("a/b").
		If(dynamo.IRI{"a/b", "c"}.Path()).Should().Equal("a/b/c").
		If(dynamo.IRI{"a/b/c", "d"}.Path()).Should().Equal("a/b/c/d").
		//
		If(dynamo.IRI{}.SubIRI("a")).Should().Equal(dynamo.IRI{"a", ""}).
		If(dynamo.IRI{Prefix: "a"}.SubIRI("b")).Should().Equal(dynamo.IRI{"a", "b"}).
		If(dynamo.IRI{"a", "b"}.SubIRI("c")).Should().Equal(dynamo.IRI{"a/b", "c"}).
		If(dynamo.IRI{"a/b", "c"}.SubIRI("d")).Should().Equal(dynamo.IRI{"a/b/c", "d"}).
		//
		If(dynamo.IRI{}.Parent()).Should().Equal(dynamo.IRI{}).
		If(dynamo.IRI{Prefix: "a"}.Parent()).Should().Equal(dynamo.IRI{"", ""}).
		If(dynamo.IRI{"a", "b"}.Parent()).Should().Equal(dynamo.IRI{"", ""}).
		If(dynamo.IRI{"a/b", "c"}.Parent()).Should().Equal(dynamo.IRI{"a", "b"}).
		If(dynamo.IRI{"a/b/c", "d"}.Parent()).Should().Equal(dynamo.IRI{"a/b", "c"})
}

type Item struct {
	dynamo.ID
	Ref dynamo.IRI `json:"ref"`
	Tag string     `json:"tag"`
}

var fixtureItem Item = Item{
	ID:  dynamo.ID{dynamo.IRI{"foo/prefix", "suffix"}},
	Ref: dynamo.IRI{"foo/a", "suffix"},
	Tag: "tag",
}
var fixtureJson string = "{\"id\":\"/foo/prefix/suffix\",\"ref\":\"/foo/a/suffix\",\"tag\":\"tag\"}"

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
