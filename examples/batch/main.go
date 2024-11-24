//
// Copyright (C) 2019 - 2024 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

package main

import (
	"context"
	"fmt"
	"os"

	"github.com/fogfish/curie"
	"github.com/fogfish/dynamo/v3/service/ddb"
)

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
type KeyVal = *ddb.Storage[*Person]

func main() {
	db := ddb.Must(
		ddb.New[*Person](
			ddb.WithTable(os.Args[1]),
			ddb.WithPrefixes(
				curie.Namespaces{
					"test":   "t/kv",
					"person": "person/",
				},
			),
		),
	)

	examplePut(db)
	exampleGet(db)
	exampleRemove(db)
}

const n = 5

func examplePut(db KeyVal) {
	seq := make([]*Person, n)
	for i := 0; i < n; i++ {
		seq[i] = &Person{
			Org:     curie.New("test:"),
			ID:      curie.New("person:%d", i),
			Name:    "Verner Pleishner",
			Age:     64,
			Address: "Blumenstrasse 14, Berne, 3013",
		}
	}

	out, err := db.BatchPut(context.Background(), seq)
	switch {
	case err == nil:
		fmt.Println("=[ batch put ]=>")
		for _, x := range seq {
			fmt.Printf("\t%+v\n", x)
		}
	default:
		fmt.Printf("=[ put ]=> Fail: %v, %+v\n", err, out)
	}
}

func exampleGet(db KeyVal) {
	keys := make([]*Person, n)
	for i := 0; i < n; i++ {
		keys[i] = &Person{
			Org: curie.New("test:"),
			ID:  curie.New("person:%d", i),
		}
	}

	seq, err := db.BatchGet(context.Background(), keys)
	if err != nil {
		fmt.Printf("=[ batch get ]=> failed %v\n", err)
		return
	}

	fmt.Println("=[ batch get ]=>")
	for _, x := range seq {
		fmt.Printf("\t%+v\n", x)
	}
}

func exampleRemove(db KeyVal) {
	keys := make([]*Person, n)
	for i := 0; i < n; i++ {
		keys[i] = &Person{
			Org: curie.New("test:"),
			ID:  curie.New("person:%d", i),
		}
	}

	out, err := db.BatchRemove(context.Background(), keys)
	if err != nil {
		fmt.Printf("=[ batch remove ]=> failed %v, %+v\n", err, out)
		return
	}

	fmt.Println("=[ batch remove ]=>")
	for _, x := range keys {
		fmt.Printf("\t%+v\n", x)
	}
}
