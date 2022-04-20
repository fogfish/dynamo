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

	"github.com/aws/aws-sdk-go/service/dynamodb"
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

func (p Person) HashKey() string { return p.Org.String() }
func (p Person) SortKey() string { return p.ID.String() }

var codecHKey, codecSKey = dynamo.Codec2[Person, dynamo.IRI, dynamo.IRI]("Org", "ID")

//
func (p Person) Identity() (string, string) { return p.Org.String(), p.ID.String() }

//
func (p Person) MarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	type tStruct Person
	return dynamo.Encode(av, tStruct(p),
		codecHKey.Encode(dynamo.IRI(p.Org)),
		codecSKey.Encode(dynamo.IRI(p.ID)),
	)
}

//
func (p *Person) UnmarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	type tStruct *Person
	return dynamo.Decode(av, tStruct(p),
		codecHKey.Decode((*dynamo.IRI)(&p.Org)),
		codecSKey.Decode((*dynamo.IRI)(&p.ID)),
	)
}

// KeyVal is type synonym
type KeyVal dynamo.KeyValNoContext[Person]

//
//
func main() {
	db := dynamo.NewKeyValContextDefault(
		keyval.Must(keyval.New[Person](os.Args[1])),
	)

	examplePut(db)
	exampleGet(db)
	exampleUpdate(db)
	exampleMatch(db)
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
