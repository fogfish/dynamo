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

	"github.com/fogfish/dynamo/v4/service/ddb"
	"github.com/fogfish/gold"
)

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

var (
	ifName    = ddb.ClauseFor[*Person, string]("Name")
	ifAge     = ddb.ClauseFor[*Person, int]("Age")
	ifAddress = ddb.ClauseFor[*Person, string]("Address")
)

func main() {
	db := ddb.Must(
		ddb.New[*Person](os.Args[1]),
	)

	examplePutOptimisticLock(db)
	exampleRemoveWithCondition(db)

	examplePutWithCondition(db)
	exampleUpdateWithConditionOne(db)
	exampleUpdateWithConditionFew(db)
	exampleRemoveWithCondition(db)
}

func examplePutOptimisticLock(db *ddb.Storage[*Person]) {
	val := Person{
		Org:  gold.ToIRI[University]("kiel"),
		ID:   gold.ToIRI[Person]("1"),
		Name: "Verner Pleishner",
	}
	err := db.Put(context.Background(), &val, ifName.Optimistic("Verner Pleishner"))
	if err != nil {
		fmt.Printf("=[ put ]=> Failed: %v\n", err)
		return
	}

	fmt.Printf("=[ put ]=> %+v\n", val)

	val = Person{
		Org:  gold.ToIRI[University]("kiel"),
		ID:   gold.ToIRI[Person]("1"),
		Name: "Prof. Verner Pleishner",
	}
	err = db.Put(context.Background(), &val, ifName.Optimistic("Verner Pleishner"))
	if err != nil {
		fmt.Printf("=[ put ]=> Failed: %v\n", err)
		return
	}

	fmt.Printf("=[ put ]=> %+v\n", val)
}

func examplePutWithCondition(db *ddb.Storage[*Person]) {
	val := Person{
		Org:  gold.ToIRI[University]("kiel"),
		ID:   gold.ToIRI[Person]("1"),
		Name: "Verner Pleishner",
	}
	err := db.Put(context.Background(), &val, ifName.NotExists())
	if err != nil {
		fmt.Printf("=[ put ]=> Failed: %v\n", err)
		return
	}

	fmt.Printf("=[ put ]=> %+v\n", val)
}

func exampleUpdateWithConditionOne(db *ddb.Storage[*Person]) {
	patch := &Person{
		Org:     gold.ToIRI[University]("kiel"),
		ID:      gold.ToIRI[Person]("1"),
		Age:     64,
		Address: "Blumenstrasse 14, Berne, 3013",
	}
	val, err := db.Update(context.Background(), patch, ifName.HasPrefix("Verner"))
	if err != nil {
		fmt.Printf("=[ update ]=> Failed: %v\n", err)
		return
	}

	fmt.Printf("=[ update ]=> %+v\n", val)
}

func exampleUpdateWithConditionFew(db *ddb.Storage[*Person]) {
	patch := &Person{
		Org:     gold.ToIRI[University]("kiel"),
		ID:      gold.ToIRI[Person]("1"),
		Age:     66,
		Address: "Viktoriastrasse 37, Berne, 3013",
	}
	val, err := db.Update(context.Background(), patch,
		ifName.HasPrefix("Verner"),
		ifAge.In(60, 62, 64, 66, 68),
		ifAddress.Contains("strasse 14"),
	)
	if err != nil {
		fmt.Printf("=[ update ]=> Failed: %v\n", err)
		return
	}

	fmt.Printf("=[ update ]=> %+v\n", val)
}

func exampleRemoveWithCondition(db *ddb.Storage[*Person]) {
	key := &Person{
		Org: gold.ToIRI[University]("kiel"),
		ID:  gold.ToIRI[Person]("1"),
	}
	val, err := db.Remove(context.Background(), key, ifName.Exists())
	if err != nil {
		fmt.Printf("=[ remove ]=> Failed: %v\n", err)
		return
	}

	fmt.Printf("=[ remove ]=> %+v\n", val)
}
