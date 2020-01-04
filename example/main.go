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
	"strconv"

	"github.com/fogfish/dynamo"
)

type person struct {
	dynamo.IRI
	Name    string `dynamodbav:"name,omitempty"`
	Age     int    `dynamodbav:"age,omitempty"`
	Address string `dynamodbav:"address,omitempty"`
}

func main() {
	db, err := dynamo.New(os.Args[1])
	if err != nil {
		panic(err)
	}

	examplePut(db)
	exampleGet(db)
	exampleUpdate(db)
	exampleMatch(db)
	exampleRemove(db)
}

const n = 5

func examplePut(db dynamo.KeyVal) {
	for i := 0; i < n; i++ {
		val := folk(i)
		err := db.Put(val)

		fmt.Println("=[ put ]=> ", either(err, val))
	}
}

func exampleGet(db dynamo.KeyVal) {
	for i := 0; i < n; i++ {
		val := &person{IRI: id(i)}
		err := db.Get(val)

		fmt.Println("=[ get ]=> ", either(err, val))
	}
}

func exampleUpdate(db dynamo.KeyVal) {
	for i := 0; i < n; i++ {
		val := &person{IRI: id(i), Address: "Viktoriastrasse 37, Berne, 3013"}
		err := db.Update(val)

		fmt.Println("=[ update ]=> ", either(err, val))
	}
}

func exampleMatch(db dynamo.KeyVal) {
	seq := db.Match(dynamo.IRI{Prefix: "test"})

	for seq.Tail() {
		val := &person{}
		err := seq.Head(val)
		fmt.Println("=[ match ]=> ", either(err, val))
	}

	if err := seq.Error(); err != nil {
		fmt.Println("=[ match ]=> ", err)
	}

}

func exampleRemove(db dynamo.KeyVal) {
	for i := 0; i < n; i++ {
		val := &person{IRI: id(i)}
		err := db.Remove(val)

		fmt.Println("=[ remove ]=> ", either(err, val))
	}
}

func folk(x int) *person {
	return &person{id(x), "Verner Pleishner", 64, "Blumenstrasse 14, Berne, 3013"}
}

func id(x int) dynamo.IRI {
	return dynamo.IRI{"test", strconv.Itoa(x)}
}

func either(e error, x interface{}) interface{} {
	if e != nil {
		return e
	}
	return x
}
