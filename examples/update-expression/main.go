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
	"fmt"
	"os"

	"github.com/fogfish/curie"
	"github.com/fogfish/dynamo/v3/service/ddb"
)

type Person struct {
	Org     curie.IRI `dynamodbav:"prefix,omitempty"`
	ID      curie.IRI `dynamodbav:"suffix,omitempty"`
	Name    string    `dynamodbav:"name,omitempty"`
	Age     int       `dynamodbav:"age,omitempty"`
	Address string    `dynamodbav:"address,omitempty"`
	Degrees []string  `dynamodbav:"degrees,omitempty"`
}

func (p Person) HashKey() curie.IRI { return p.Org }
func (p Person) SortKey() curie.IRI { return p.ID }

var (
	Name    = ddb.UpdateFor[*Person, string]("Name")
	Age     = ddb.UpdateFor[*Person, int]("Age")
	Address = ddb.UpdateFor[*Person, string]("Address")
	Degrees = ddb.UpdateFor[*Person, []string]("Degrees")
	ifName  = ddb.ClauseFor[*Person, string]("Name")
)

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
	exampleUpdateExpressionModifyingOne(db)
	exampleUpdateExpressionModifyingFew(db)
	exampleUpdateExpressionIncrement(db)
	exampleUpdateExpressionIncrementConditional(db)
	exampleUpdateExpressionAppendToList(db)
	exampleUpdateExpressionRemoveAttribute(db)
}

func examplePut(db *ddb.Storage[*Person]) {
	val := Person{
		Org:     curie.New("test:"),
		ID:      curie.New("person:%d", 1),
		Name:    "Verner Pleishner",
		Degrees: []string{},
	}
	err := db.Put(context.Background(), &val)
	if err != nil {
		fmt.Printf("=[ put ]=> Failed: %v\n", err)
		return
	}

	fmt.Printf("=[ put ]=> %+v\n", val)
}

func exampleUpdateExpressionModifyingOne(db *ddb.Storage[*Person]) {
	key := Person{
		Org: curie.New("test:"),
		ID:  curie.New("person:%d", 1),
	}

	val, err := db.UpdateWith(context.Background(),
		ddb.Updater(&key,
			Address.Set("Blumenstrasse 14, Berne, 3013"),
		),
	)
	if err != nil {
		fmt.Printf("=[ update one ]=> Failed: %v\n", err)
		return
	}

	fmt.Printf("=[ update one ]=> %+v\n", val)
}

func exampleUpdateExpressionModifyingFew(db *ddb.Storage[*Person]) {
	key := Person{
		Org: curie.New("test:"),
		ID:  curie.New("person:%d", 1),
	}

	val, err := db.UpdateWith(context.Background(),
		ddb.Updater(&key,
			Address.Set("Viktoriastrasse 37, Berne, 3013"),
			Age.Set(64),
		),
	)
	if err != nil {
		fmt.Printf("=[ update few ]=> Failed: %v\n", err)
		return
	}

	fmt.Printf("=[ update few ]=> %+v\n", val)
}

func exampleUpdateExpressionIncrement(db *ddb.Storage[*Person]) {
	key := Person{
		Org: curie.New("test:"),
		ID:  curie.New("person:%d", 1),
	}

	val, err := db.UpdateWith(context.Background(),
		ddb.Updater(&key,
			Age.Inc(1),
		),
	)
	if err != nil {
		fmt.Printf("=[ update inc ]=> Failed: %v\n", err)
		return
	}

	fmt.Printf("=[ update inc ]=> %+v\n", val)
}

func exampleUpdateExpressionIncrementConditional(db *ddb.Storage[*Person]) {
	key := Person{
		Org: curie.New("test:"),
		ID:  curie.New("person:%d", 1),
	}

	val, err := db.UpdateWith(context.Background(),
		ddb.Updater(&key,
			Age.Inc(1),
		),
		ifName.Eq("Verner Pleishner"),
	)
	if err != nil {
		fmt.Printf("=[ update inc ]=> Failed: %v\n", err)
		return
	}

	fmt.Printf("=[ update inc ]=> %+v\n", val)
}

func exampleUpdateExpressionAppendToList(db *ddb.Storage[*Person]) {
	key := Person{
		Org: curie.New("test:"),
		ID:  curie.New("person:%d", 1),
	}

	val, err := db.UpdateWith(context.Background(),
		ddb.Updater(&key,
			Degrees.Append([]string{"prof"}),
		),
	)
	if err != nil {
		fmt.Printf("=[ append to ]=> Failed: %v\n", err)
		return
	}

	fmt.Printf("=[ append to ]=> %+v\n", val)
}

func exampleUpdateExpressionRemoveAttribute(db *ddb.Storage[*Person]) {
	key := Person{
		Org: curie.New("test:"),
		ID:  curie.New("person:%d", 1),
	}

	val, err := db.UpdateWith(context.Background(),
		ddb.Updater(&key,
			Degrees.Remove(),
		),
	)
	if err != nil {
		fmt.Printf("=[ remove ]=> Failed: %v\n", err)
		return
	}

	fmt.Printf("=[ remove ]=> %+v\n", val)
}
