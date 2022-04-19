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
