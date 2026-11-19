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
	"strconv"

	"github.com/fogfish/dynamo/v4/service/ddb"
	"github.com/fogfish/gold"
)

// Person type demonstrates composition of core type with db one
// University and Person are classes of the identities. gold.IRI[T] is a type-safe
// identity of the class T, it is stored in DynamoDB as a string "university:kiel".
type University struct{}

type Person struct {
	Org     gold.IRI[University] `dynamodbav:"prefix,omitempty"`
	ID      gold.IRI[Person]     `dynamodbav:"suffix,omitempty"`
	Name    string               `dynamodbav:"name,omitempty"`
	Age     int                  `dynamodbav:"age,omitempty"`
	Address string               `dynamodbav:"address,omitempty"`
}

func (p Person) HashKey() string { return string(p.Org) }
func (p Person) SortKey() string { return string(p.ID) }

// KeyVal is type synonym
type KeyVal = *ddb.Storage[*Person]

func main() {
	db := ddb.Must(
		ddb.New[*Person](os.Args[1]),
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
			Org:     gold.ToIRI[University]("kiel"),
			ID:      gold.ToIRI[Person](strconv.Itoa(i)),
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
			Org: gold.ToIRI[University]("kiel"),
			ID:  gold.ToIRI[Person](strconv.Itoa(i)),
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
			Org: gold.ToIRI[University]("kiel"),
			ID:  gold.ToIRI[Person](strconv.Itoa(i)),
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
