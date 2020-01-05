package dynamo_test

import (
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
