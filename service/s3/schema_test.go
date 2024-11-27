//
// Copyright (C) 2022 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

package s3

import (
	"testing"

	"github.com/fogfish/curie/v2"
	"github.com/fogfish/dynamo/v3/internal/dynamotest"
	"github.com/fogfish/it"
)

func TestMerge(t *testing.T) {
	a := dynamotest.Person{
		Prefix: curie.IRI("dead:beef"),
		Suffix: curie.IRI("1"),
		Name:   "Verner Pleishner",
	}

	b := dynamotest.Person{
		Age:     64,
		Address: "Blumenstrasse 14, Berne, 3013",
	}

	c := dynamotest.Person{
		Prefix:  curie.IRI("dead:beef"),
		Suffix:  curie.IRI("1"),
		Name:    "Verner Pleishner",
		Age:     64,
		Address: "Blumenstrasse 14, Berne, 3013",
	}

	t.Run("Values", func(t *testing.T) {
		schema := newSchema[dynamotest.Person]()
		it.Ok(t).
			If(schema.Merge(a, b)).Should().Equal(c)
	})

	t.Run("Pointers", func(t *testing.T) {
		schema := newSchema[*dynamotest.Person]()
		it.Ok(t).
			If(schema.Merge(&a, &b)).Should().Equal(&c)
	})
}
