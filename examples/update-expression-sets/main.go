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
	"github.com/fogfish/dynamo/v2/service/ddb"
)

type Tag struct {
	HKey curie.IRI `dynamodbav:"prefix,omitempty"`
	SKey curie.IRI `dynamodbav:"suffix,omitempty"`
	Tags []string  `dynamodbav:"tags,omitempty,stringset"`
}

func (tag Tag) HashKey() curie.IRI { return tag.HKey }
func (tag Tag) SortKey() curie.IRI { return tag.SKey }

var (
	Tags = ddb.UpdateFor[Tag, []string]("Tags")
)

func main() {
	db := ddb.Must(
		ddb.New[Tag](
			ddb.WithTable(os.Args[1]),
		),
	)

	exampleCreateTags(db)
	exampleUpdateTags(db)
	exampleRemoveTags(db)
}

func exampleCreateTags(db *ddb.Storage[Tag]) {
	key := Tag{HKey: "example:", SKey: "tags:1"}

	val, err := db.UpdateWith(context.Background(),
		ddb.Updater(key, Tags.Union([]string{"test", "example"})),
	)
	if err != nil {
		fmt.Printf("=[ create ]=> Failed: %v\n", err)
		return
	}

	fmt.Printf("=[ create ]=> %+v\n", val)
}

func exampleUpdateTags(db *ddb.Storage[Tag]) {
	key := Tag{HKey: "example:", SKey: "tags:1"}

	val, err := db.UpdateWith(context.Background(),
		ddb.Updater(key, Tags.Union([]string{"dynamo"})),
	)
	if err != nil {
		fmt.Printf("=[ update ]=> Failed: %v\n", err)
		return
	}

	fmt.Printf("=[ update ]=> %+v\n", val)
}

func exampleRemoveTags(db *ddb.Storage[Tag]) {
	key := Tag{HKey: "example:", SKey: "tags:1"}

	val, err := db.UpdateWith(context.Background(),
		ddb.Updater(key, Tags.Minus([]string{"dynamo", "test"})),
	)
	if err != nil {
		fmt.Printf("=[ remove ]=> Failed: %v\n", err)
		return
	}

	fmt.Printf("=[ remove ]=> %+v\n", val)

	db.Remove(context.Background(), key)
}
