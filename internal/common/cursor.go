//
// Copyright (C) 2022 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

package common

import (
	"github.com/fogfish/curie"
	"github.com/fogfish/dynamo"
)

func Cursor(hashKey, sortKey string) dynamo.Thing {
	return &cursor{
		hashKey: hashKey,
		sortKey: sortKey,
	}
}

type cursor struct{ hashKey, sortKey string }

func (c cursor) HashKey() curie.IRI { return curie.IRI(c.hashKey) }
func (c cursor) SortKey() curie.IRI { return curie.IRI(c.sortKey) }
