//
// Copyright (C) 2019 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

package main

import (
	"fmt"
	"os"

	"github.com/fogfish/curie"
	"github.com/fogfish/dynamo"
	"github.com/fogfish/dynamo/keyval"
)

//
// Person type demonstrates composition of core type with db one
type Person struct {
	Org     curie.IRI `dynamodbav:"prefix,omitempty"`
	ID      curie.IRI `dynamodbav:"suffix,omitempty"`
	Name    string    `dynamodbav:"name,omitempty"`
	Age     int       `dynamodbav:"age,omitempty"`
	Address string    `dynamodbav:"address,omitempty"`
}

func (p Person) HashKey() curie.IRI { return p.Org }
func (p Person) SortKey() curie.IRI { return p.ID }

// KeyVal is type synonym
type KeyVal dynamo.KeyValNoContext[Person]

//
//
func main() {
	db := keyval.NewKeyValContextDefault(
		keyval.Must(keyval.New[Person](
			dynamo.WithURI(os.Args[1]),
			dynamo.WithPrefixes(
				curie.Namespaces{
					"test":   "t/kv",
					"person": "person/",
				},
			),
		)),
	)

	examplePut(db)
	exampleGet(db)
	exampleUpdate(db)
	exampleMatch(db)
	exampleMatchWithCursor(db)
	exampleRemove(db)
}

const n = 5

func examplePut(db KeyVal) {
	for i := 0; i < n; i++ {
		val := Person{
			Org:     curie.New("test:"),
			ID:      curie.New("person:%d", i),
			Name:    "Verner Pleishner",
			Age:     64,
			Address: "Blumenstrasse 14, Berne, 3013",
		}
		err := db.Put(val)

		fmt.Println("=[ put ]=> ", either(err, val))
	}
}

func exampleGet(db KeyVal) {
	for i := 0; i < n; i++ {
		val, err := db.Get(Person{
			Org: curie.New("test:"),
			ID:  curie.New("person:%d", i),
		})

		switch v := err.(type) {
		case nil:
			fmt.Printf("=[ get ]=> %+v\n", val)
		case dynamo.NotFound:
			fmt.Printf("=[ get ]=> Not found: (%v, %v)\n", val.Org, val.ID)
		default:
			fmt.Printf("=[ get ]=> Fail: %v\n", v)
		}
	}
}

func exampleUpdate(db KeyVal) {
	for i := 0; i < n; i++ {
		val, err := db.Update(Person{
			Org:     curie.New("test:"),
			ID:      curie.New("person:%d", i),
			Address: "Viktoriastrasse 37, Berne, 3013",
		})

		fmt.Printf("=[ update ]=> %+v\n", either(err, val))
	}
}

func exampleMatch(db KeyVal) {
	seq := dynamo.Things[Person]{}
	err := db.Match(Person{Org: curie.New("test:")}).FMap(seq.Join)

	if err == nil {
		fmt.Printf("=[ match ]=> %+v\n", seq)
	} else {
		fmt.Printf("=[ match ]=> %v\n", err)
	}
}

func exampleMatchWithCursor(db KeyVal) {
	// first batch
	persons := dynamo.Things[Person]{}
	seq := db.Match(Person{Org: curie.New("test:")}).Limit(2)
	err := seq.FMap(persons.Join)
	cur := seq.Cursor()

	if err != nil {
		fmt.Printf("=[ match 1st ]=> %v\n", err)
		return
	}
	fmt.Printf("=[ match 1st ]=> %+v\n", persons)

	// second batch
	persons = dynamo.Things[Person]{}
	seq = db.Match(Person{Org: curie.New("test:")}).Continue(cur)
	err = seq.FMap(persons.Join)

	if err != nil {
		fmt.Printf("=[ match 2nd ]=> %v\n", err)
		return
	}
	fmt.Printf("=[ match 2nd ]=> %+v\n", persons)
}

func exampleRemove(db KeyVal) {
	for i := 0; i < n; i++ {
		err := db.Remove(Person{
			Org: curie.New("test:"),
			ID:  curie.New("person:%d", i),
		})

		fmt.Println("=[ remove ]=> ", err)
	}
}

func either(e error, x interface{}) interface{} {
	if e != nil {
		return e
	}
	return x
}
