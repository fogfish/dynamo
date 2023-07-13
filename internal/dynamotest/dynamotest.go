//
// Copyright (C) 2022 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

//
// The file declares test suite for Key/Value interfaces
// Each Key/Value interface MUST pass the defined test suite
//

package dynamotest

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/fogfish/curie"
	"github.com/fogfish/dynamo/v2"
	"github.com/fogfish/it"
)

/*
Person is a type used for testing
*/
type Person struct {
	Prefix  curie.IRI `dynamodbav:"prefix,omitempty"`
	Suffix  curie.IRI `dynamodbav:"suffix,omitempty"`
	Name    string    `dynamodbav:"name,omitempty"`
	Age     int       `dynamodbav:"age,omitempty"`
	Address string    `dynamodbav:"address,omitempty"`
}

func (p Person) HashKey() curie.IRI { return p.Prefix }
func (p Person) SortKey() curie.IRI { return p.Suffix }

// Use type aliases and methods to implement FMap
type Persons []Person

func (seq *Persons) Join(val Person) error {
	*seq = append(*seq, val)
	return nil
}

func fixtureKey() Person {
	return Person{
		Prefix: curie.New("dead:beef"),
		Suffix: curie.New("1"),
	}
}

func fixtureKeyHashOnly() Person {
	return Person{
		Prefix: curie.New("dead:beef"),
	}
}

func fixtureVal() Person {
	return Person{
		Prefix:  curie.New("dead:beef"),
		Suffix:  curie.New("1"),
		Name:    "Verner Pleishner",
		Age:     64,
		Address: "Blumenstrasse 14, Berne, 3013",
	}
}

func fixturePatch() Person {
	return Person{
		Prefix: curie.New("dead:beef"),
		Suffix: curie.New("1"),
		Age:    64,
	}
}

/*
Encoder of test type to storage internal format
*/
type Encoder[A any] func(Person) (A, error)

func TestGet[S any](
	t *testing.T,
	encoder Encoder[S],
	factory func(*S, *S) dynamo.KeyVal[Person],
) {
	t.Helper()

	expectKey, err := encoder(fixtureKey())
	it.Ok(t).IfNil(err)

	expectVal, err := encoder(fixtureVal())
	it.Ok(t).IfNil(err)

	//
	t.Run("GetSuccess", func(t *testing.T) {
		ddb := factory(&expectKey, &expectVal)

		val, err := ddb.Get(context.TODO(), fixtureKey())
		it.Ok(t).
			If(err).Should().Equal(nil).
			If(val).Should().Equal(fixtureVal())
	})

	//
	t.Run("GetNotFound", func(t *testing.T) {
		ddb := factory(&expectKey, nil)

		val, err := ddb.Get(context.TODO(), fixtureKey())
		_, isnfe := err.(interface{ NotFound() string })

		it.Ok(t).
			If(val).Should().Equal(Person{}).
			If(err).ShouldNot().Equal(nil).
			IfTrue(isnfe)
	})

	//
	t.Run("GetFailure", func(t *testing.T) {
		ddb := factory(new(S), nil)

		val, err := ddb.Get(context.TODO(), fixtureKey())
		it.Ok(t).
			If(val).Should().Equal(Person{}).
			If(err).ShouldNot().Equal(nil)
	})
}

func TestPut[S any](
	t *testing.T,
	encoder Encoder[S],
	factory func(*S) dynamo.KeyVal[Person],
) {
	t.Helper()

	expectVal, err := encoder(fixtureVal())
	it.Ok(t).IfNil(err)

	//
	t.Run("PutSuccess", func(t *testing.T) {
		ddb := factory(&expectVal)

		err := ddb.Put(context.TODO(), fixtureVal())
		it.Ok(t).
			If(err).Should().Equal(nil)
	})

	// Note: Constrains are not supported by each storage yet
	// t.Run("SuccessWithConstrain", func(t *testing.T) {
	// 	name := dynamo.Schema1[Person, string]("Name")
	// 	ddb := factory(&expectVal, aws.String("Test"))
	//
	// 	err := ddb.Put(fixtureVal(), name.Eq("Test"))
	// 	it.Ok(t).
	// 		If(err).Should().Equal(nil)
	// })

	// t.Run("FailureWithConstrain", func(t *testing.T) {
	// 	name := dynamo.Schema1[Person, string]("Name")
	// 	ddb := factory(&expectVal, aws.String("Test"))
	//
	// 	err := ddb.Put(fixtureVal(), name.Eq("Some"))
	// 	it.Ok(t).
	// 		If(err).Should().Be().Like(dynamo.PreConditionFailed{})
	// })
}

func TestRemove[S any](
	t *testing.T,
	encoder Encoder[S],
	factory func(*S, *S) dynamo.KeyVal[Person],
) {
	t.Helper()

	expectKey, err := encoder(fixtureKey())
	it.Ok(t).IfNil(err)

	returnVal, err := encoder(fixtureVal())
	it.Ok(t).IfNil(err)

	//
	t.Run("RemoveSuccess", func(t *testing.T) {
		ddb := factory(&expectKey, &returnVal)

		val, err := ddb.Remove(context.TODO(), fixtureVal())
		it.Ok(t).
			If(err).Should().Equal(nil).
			If(val).Should().Equal(fixtureVal())
	})
}

func TestUpdate[S any](
	t *testing.T,
	encoder Encoder[S],
	factory func(*S, *S, *S) dynamo.KeyVal[Person],
) {
	t.Helper()

	expectKey, err := encoder(fixtureKey())
	it.Ok(t).IfNil(err)

	expectVal, err := encoder(fixturePatch())
	it.Ok(t).IfNil(err)

	returnVal, err := encoder(fixtureVal())
	it.Ok(t).IfNil(err)

	//
	t.Run("UpdateSuccess", func(t *testing.T) {
		ddb := factory(&expectKey, &expectVal, &returnVal)

		val, err := ddb.Update(context.TODO(), fixturePatch())
		it.Ok(t).
			If(err).Should().Equal(nil).
			If(val).Should().Equal(fixtureVal())
	})
}

func TestMatch[S any](
	t *testing.T,
	encoder Encoder[S],
	factory func(*S, int, *S, *S) dynamo.KeyVal[Person],
) {
	t.Helper()

	expectKey, err := encoder(fixtureKeyHashOnly())
	it.Ok(t).IfNil(err)

	returnVal, err := encoder(fixtureVal())
	it.Ok(t).IfNil(err)

	//
	t.Run("MatchNone", func(t *testing.T) {
		ddb := factory(&expectKey, 0, &returnVal, nil)

		seq, _, err := ddb.Match(context.Background(), fixtureKeyHashOnly())

		it.Ok(t).
			IfNil(err).
			If(len(seq)).Equal(0)
	})

	//
	t.Run("MatchOne", func(t *testing.T) {
		ddb := factory(&expectKey, 1, &returnVal, nil)

		seq, _, err := ddb.Match(context.Background(), fixtureKeyHashOnly())

		it.Ok(t).
			If(err).Should().Equal(nil).
			If(len(seq)).ShouldNot().Equal(0).
			If(seq[0]).Should().Equal(fixtureVal())
	})

	//
	t.Run("MatchMany", func(t *testing.T) {
		ddb := factory(&expectKey, 5, &returnVal, nil)

		seq, _, err := ddb.Match(context.Background(), fixtureKeyHashOnly())

		it.Ok(t).
			If(err).Should().Equal(nil).
			If(len(seq)).Should().Equal(5)

		for _, val := range seq {
			it.Ok(t).If(val).Should().Equal(fixtureVal())
		}
	})

	//
	t.Run("MatchKey", func(t *testing.T) {
		ddb := factory(&expectKey, 5, &returnVal, nil)

		seq, _, err := ddb.MatchKey(context.Background(), fixtureKeyHashOnly())

		it.Ok(t).
			If(err).Should().Equal(nil).
			If(len(seq)).Should().Equal(5)

		for _, val := range seq {
			it.Ok(t).If(val).Should().Equal(fixtureVal())
		}
	})

	//
	t.Run("MatchWithCursor", func(t *testing.T) {
		expectKeyFull, err := encoder(fixtureKey())
		it.Ok(t).IfNil(err)

		ddb := factory(&expectKey, 2, &returnVal, &expectKeyFull)

		_, cursor0, err := ddb.Match(context.Background(), fixtureKeyHashOnly(), dynamo.Limit[Person](2))
		it.Ok(t).IfNil(err)

		thing0 := cursor0.(dynamo.Thing)
		keys0 := filepath.Join(string(thing0.HashKey()), string(thing0.SortKey()))

		_, cursor1, err := ddb.Match(context.Background(), fixtureKey(), cursor0)
		it.Ok(t).IfNil(err)

		thing1 := cursor1.(dynamo.Thing)
		keys1 := filepath.Join(string(thing1.HashKey()), string(thing1.SortKey()))

		it.Ok(t).
			If(string(keys0)).Equal("dead:beef/1").
			If(string(keys1)).Equal("dead:beef/1")
	})
}
