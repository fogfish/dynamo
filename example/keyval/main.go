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

	"github.com/fogfish/dynamo"
	"github.com/fogfish/iri"
)

type person struct {
	iri.ID
	Name    string `dynamodbav:"name,omitempty" json:"name,omitempty"`
	Age     int    `dynamodbav:"age,omitempty" json:"age,omitempty"`
	Address string `dynamodbav:"address,omitempty" json:"address,omitempty"`
}

type persons []person

func (seq *persons) Join(gen dynamo.Gen) (iri.Thing, error) {
	val := person{}
	if fail := gen.To(&val); fail != nil {
		return nil, fail
	}
	*seq = append(*seq, val)
	return &val, nil
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
		val := &person{ID: id(i)}
		switch err := db.Get(val).(type) {
		case nil:
			fmt.Println("=[ get ]=> ", val)
		case dynamo.NotFound:
			fmt.Println("=[ get ]=> Not found: ", val.ID)
		default:
			fmt.Println("=[ get ]=> Fail: ", err)
		}
	}
}

func exampleUpdate(db dynamo.KeyVal) {
	for i := 0; i < n; i++ {
		val := &person{ID: id(i), Address: "Viktoriastrasse 37, Berne, 3013"}
		err := db.Update(val)

		fmt.Println("=[ update ]=> ", either(err, val))
	}
}

func exampleMatch(db dynamo.KeyVal) {
	seq := persons{}
	_, err := db.Match(iri.New("test")).FMap(seq.Join)

	if err == nil {
		fmt.Println("=[ match ]=> ", seq)
	} else {
		fmt.Println("=[ match ]=> ", err)
	}
}

func exampleRemove(db dynamo.KeyVal) {
	for i := 0; i < n; i++ {
		val := &person{ID: id(i)}
		err := db.Remove(val)

		fmt.Println("=[ remove ]=> ", either(err, val))
	}
}

func folk(x int) *person {
	return &person{
		ID:      id(x),
		Name:    "Verner Pleishner",
		Age:     64,
		Address: "Blumenstrasse 14, Berne, 3013",
	}
}

func id(x int) iri.ID {
	return iri.New("test:%v", x)
}

func either(e error, x interface{}) interface{} {
	if e != nil {
		return e
	}
	return x
}
