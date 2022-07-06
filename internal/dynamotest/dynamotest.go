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
	"path/filepath"
	"testing"

	"github.com/fogfish/curie"
	"github.com/fogfish/dynamo"
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

//
// Use type aliases and methods to implement FMap
type Persons []Person

func (seq *Persons) Join(val Person) error {
	*seq = append(*seq, val)
	return nil
}

//
//
func fixtureKey() Person {
	return Person{
		Prefix: curie.New("dead:beef"),
		Suffix: curie.New("1"),
	}
}

//
//
func fixtureKeyHashOnly() Person {
	return Person{
		Prefix: curie.New("dead:beef"),
	}
}

//
//
func fixtureVal() Person {
	return Person{
		Prefix:  curie.New("dead:beef"),
		Suffix:  curie.New("1"),
		Name:    "Verner Pleishner",
		Age:     64,
		Address: "Blumenstrasse 14, Berne, 3013",
	}
}

//
//
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

//
//
func TestGet[S any](
	t *testing.T,
	encoder Encoder[S],
	factory func(*S, *S) dynamo.KeyValNoContext[Person],
) {
	t.Helper()

	expectKey, err := encoder(fixtureKey())
	it.Ok(t).IfNil(err)

	expectVal, err := encoder(fixtureVal())
	it.Ok(t).IfNil(err)

	//
	t.Run("GetSuccess", func(t *testing.T) {
		ddb := factory(&expectKey, &expectVal)

		val, err := ddb.Get(fixtureKey())
		it.Ok(t).
			If(err).Should().Equal(nil).
			If(val).Should().Equal(fixtureVal())
	})

	//
	t.Run("GetNotFound", func(t *testing.T) {
		ddb := factory(&expectKey, nil)

		val, err := ddb.Get(fixtureKey())
		_, isnfe := err.(interface{ NotFound() string })

		it.Ok(t).
			If(val).Should().Equal(Person{}).
			If(err).ShouldNot().Equal(nil).
			IfTrue(isnfe)
	})

	//
	t.Run("GetFailure", func(t *testing.T) {
		ddb := factory(new(S), nil)

		val, err := ddb.Get(fixtureKey())
		it.Ok(t).
			If(val).Should().Equal(Person{}).
			If(err).ShouldNot().Equal(nil)
	})
}

//
//
func TestPut[S any](
	t *testing.T,
	encoder Encoder[S],
	factory func(*S) dynamo.KeyValNoContext[Person],
) {
	t.Helper()

	expectVal, err := encoder(fixtureVal())
	it.Ok(t).IfNil(err)

	//
	t.Run("PutSuccess", func(t *testing.T) {
		ddb := factory(&expectVal)

		err := ddb.Put(fixtureVal())
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

//
//
func TestRemove[S any](
	t *testing.T,
	encoder Encoder[S],
	factory func(*S) dynamo.KeyValNoContext[Person],
) {
	t.Helper()

	expectKey, err := encoder(fixtureKey())
	it.Ok(t).IfNil(err)

	//
	t.Run("RemoveSuccess", func(t *testing.T) {
		ddb := factory(&expectKey)

		err := ddb.Remove(fixtureVal())
		it.Ok(t).
			If(err).Should().Equal(nil)
	})
}

//
//
func TestUpdate[S any](
	t *testing.T,
	encoder Encoder[S],
	factory func(*S, *S, *S) dynamo.KeyValNoContext[Person],
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

		val, err := ddb.Update(fixturePatch())
		it.Ok(t).
			If(err).Should().Equal(nil).
			If(val).Should().Equal(fixtureVal())
	})
}

//
//
func TestMatch[S any](
	t *testing.T,
	encoder Encoder[S],
	factory func(*S, int, *S, *S) dynamo.KeyValNoContext[Person],
) {
	t.Helper()

	expectKey, err := encoder(fixtureKeyHashOnly())
	it.Ok(t).IfNil(err)

	returnVal, err := encoder(fixtureVal())
	it.Ok(t).IfNil(err)

	//
	t.Run("MatchNone", func(t *testing.T) {
		ddb := factory(&expectKey, 0, &returnVal, nil)

		seq := ddb.Match(fixtureKeyHashOnly())

		it.Ok(t).
			IfFalse(seq.Tail()).
			If(seq.Error()).Should().Equal(nil)
	})

	//
	t.Run("MatchOne", func(t *testing.T) {
		ddb := factory(&expectKey, 1, &returnVal, nil)

		seq := ddb.Match(fixtureKeyHashOnly())
		val, err := seq.Head()

		it.Ok(t).
			IfFalse(seq.Tail()).
			If(seq.Error()).Should().Equal(nil).
			If(err).Should().Equal(nil).
			If(val).Should().Equal(fixtureVal())
	})

	//
	t.Run("MatchMany", func(t *testing.T) {
		ddb := factory(&expectKey, 5, &returnVal, nil)

		cnt := 0
		seq := ddb.Match(fixtureKeyHashOnly())

		for seq.Tail() {
			cnt++

			val, err := seq.Head()
			it.Ok(t).
				If(err).Should().Equal(nil).
				If(val).Should().Equal(fixtureVal())
		}

		it.Ok(t).
			If(seq.Error()).Should().Equal(nil).
			If(cnt).Should().Equal(5)
	})

	//
	t.Run("FMapNone", func(t *testing.T) {
		ddb := factory(&expectKey, 0, &returnVal, nil)

		var seq Persons
		err := ddb.Match(fixtureKeyHashOnly()).FMap(seq.Join)

		it.Ok(t).
			If(err).Should().Equal(nil).
			If(len(seq)).Should().Equal(0)
	})

	//
	t.Run("FMapPrefix", func(t *testing.T) {
		ddb := factory(&expectKey, 2, &returnVal, nil)

		var seq Persons
		err := ddb.Match(fixtureKeyHashOnly()).FMap(seq.Join)

		it.Ok(t).
			If(err).Should().Equal(nil).
			If(seq).Should().Equal(Persons{fixtureVal(), fixtureVal()})
	})

	//
	t.Run("FMapPrefixSuffix", func(t *testing.T) {
		expectKeyFull, err := encoder(fixtureKey())
		it.Ok(t).IfNil(err)

		ddb := factory(&expectKeyFull, 2, &returnVal, nil)

		var seq Persons
		err = ddb.Match(fixtureKey()).FMap(seq.Join)

		it.Ok(t).
			If(err).Should().Equal(nil).
			If(seq).Should().Equal(Persons{fixtureVal(), fixtureVal()})
	})

	//
	t.Run("FMapThings", func(t *testing.T) {
		ddb := factory(&expectKey, 2, &returnVal, nil)

		var seq dynamo.Things[Person]
		err := ddb.Match(fixtureKeyHashOnly()).FMap(seq.Join)

		it.Ok(t).
			If(err).Should().Equal(nil).
			If(seq).Should().Equal(dynamo.Things[Person]{fixtureVal(), fixtureVal()})
	})

	//
	t.Run("FMapWithCursor", func(t *testing.T) {
		expectKeyFull, err := encoder(fixtureKey())
		it.Ok(t).IfNil(err)

		ddb := factory(&expectKey, 2, &returnVal, &expectKeyFull)

		dbseq := ddb.Match(fixtureKeyHashOnly())
		dbseq.Tail()
		cursor0 := dbseq.Cursor()
		keys0 := filepath.Join(string(cursor0.HashKey()), string(cursor0.SortKey()))

		dbseq = ddb.Match(fixtureKey()).Continue(cursor0)
		dbseq.Tail()
		cursor1 := dbseq.Cursor()
		keys1 := filepath.Join(string(cursor1.HashKey()), string(cursor1.SortKey()))

		it.Ok(t).
			If(string(keys0)).Equal("dead:beef/1").
			If(string(keys1)).Equal("dead:beef/1")
	})
}
