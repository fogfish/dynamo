//
// Copyright (C) 2019 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/fogfish/dynamo/v4"
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
	exampleUpdate(db)
	exampleMatch(db)
	exampleMatchWithCursor(db)
	exampleRemove(db)
}

const n = 5

func examplePut(db KeyVal) {
	for i := 0; i < n; i++ {
		val := Person{
			Org:     gold.ToIRI[University]("kiel"),
			ID:      gold.ToIRI[Person](strconv.Itoa(i)),
			Name:    "Verner Pleishner",
			Age:     64,
			Address: "Blumenstrasse 14, Berne, 3013",
		}
		err := db.Put(context.Background(), &val)
		switch {
		case err == nil:
			fmt.Printf("=[ put ]=> %+v\n", val)
		default:
			fmt.Printf("=[ put ]=> Fail: %v\n", err)
		}
	}
}

func exampleGet(db KeyVal) {
	for i := 0; i < n; i++ {
		key := Person{
			Org: gold.ToIRI[University]("kiel"),
			ID:  gold.ToIRI[Person](strconv.Itoa(i)),
		}

		val, err := db.Get(context.Background(), &key)
		switch {
		case err == nil:
			fmt.Printf("=[ get ]=> %+v\n", val)
		case recoverNotFound(err):
			fmt.Printf("=[ get ]=> Not found: (%v, %v)\n", val.Org, val.ID)
		default:
			fmt.Printf("=[ get ]=> Fail: %v\n", err)
		}
	}
}

func exampleUpdate(db KeyVal) {
	for i := 0; i < n; i++ {
		patch := Person{
			Org:     gold.ToIRI[University]("kiel"),
			ID:      gold.ToIRI[Person](strconv.Itoa(i)),
			Address: "Viktoriastrasse 37, Berne, 3013",
		}
		val, err := db.Update(context.Background(), &patch)
		switch {
		case err == nil:
			fmt.Printf("=[ update ]=> %+v\n", val)
		default:
			fmt.Printf("=[ update ]=> Fail: %v\n", err)
		}
	}
}

func exampleMatch(db KeyVal) {
	key := Person{Org: gold.ToIRI[University]("kiel")}
	seq, _, err := db.Match(context.Background(), &key)
	if err != nil {
		fmt.Printf("=[ match ]=> %v\n", err)
		return
	}

	fmt.Println("=[ match ]=>")
	for _, x := range seq {
		fmt.Printf("\t%+v\n", x)
	}
}

func exampleMatchWithCursor(db KeyVal) {
	// first batch
	key := Person{Org: gold.ToIRI[University]("kiel")}
	seq, cur, err := db.Match(context.Background(), &key, dynamo.Limit[*Person](2))
	if err != nil {
		fmt.Printf("=[ match 1st ]=> %v\n", err)
		return
	}

	fmt.Println("=[ match 1st ]=>")
	for _, x := range seq {
		fmt.Printf("\t%+v\n", x)
	}

	// second batch
	seq, _, err = db.Match(context.Background(), &key, cur)
	if err != nil {
		fmt.Printf("=[ match 2nd ]=> %v\n", err)
		return
	}

	fmt.Println("=[ match 2nd ]=>")
	for _, x := range seq {
		fmt.Printf("\t%+v\n", x)
	}
}

func exampleRemove(db KeyVal) {
	for i := 0; i < n; i++ {
		key := Person{
			Org: gold.ToIRI[University]("kiel"),
			ID:  gold.ToIRI[Person](strconv.Itoa(i)),
		}
		val, err := db.Remove(context.Background(), &key)
		switch {
		case err == nil:
			fmt.Printf("=[ remove ]=> %+v\n", val)
		default:
			fmt.Printf("=[ remove ]=> Fail: %v\n", err)
		}
	}
}

func recoverNotFound(err error) bool {
	var e interface{ NotFound() string }

	ok := errors.As(err, &e)
	return ok && e.NotFound() != ""
}
