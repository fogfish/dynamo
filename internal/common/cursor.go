//
// Copyright (C) 2022 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

package common

import (
	"github.com/fogfish/dynamo"
)

func Cursor(hashKey, sortKey string) dynamo.Thing {
	return &cursor{
		hashKey: hashKey,
		sortKey: sortKey,
	}
}

type cursor struct{ hashKey, sortKey string }

func (c cursor) HashKey() string { return c.hashKey }
func (c cursor) SortKey() string { return c.sortKey }
