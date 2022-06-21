//
// Copyright (C) 2019 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

//
// The file declares NoContext wrappers for Key/Value interfaces
//

package keyval

import (
	"context"

	"github.com/fogfish/dynamo"
)

//-----------------------------------------------------------------------------
//
// KeyVal
//
//-----------------------------------------------------------------------------

type keyvalNoContext[T dynamo.Thing] struct{ dynamo.KeyVal[T] }

// NewKeyValContextDefault wraps
func NewKeyValContextDefault[T dynamo.Thing](keyval dynamo.KeyVal[T]) dynamo.KeyValNoContext[T] {
	return &keyvalNoContext[T]{keyval}
}

func (db *keyvalNoContext[T]) Get(key T) (*T, error) {
	return db.KeyVal.Get(context.Background(), key)
}

func (db *keyvalNoContext[T]) Put(entity T, config ...dynamo.Constrain[T]) error {
	return db.KeyVal.Put(context.Background(), entity, config...)
}

func (db *keyvalNoContext[T]) Remove(key T, config ...dynamo.Constrain[T]) error {
	return db.KeyVal.Remove(context.Background(), key, config...)
}

func (db *keyvalNoContext[T]) Update(entity T, config ...dynamo.Constrain[T]) (*T, error) {
	return db.KeyVal.Update(context.Background(), entity, config...)
}

func (db *keyvalNoContext[T]) Match(key T) dynamo.Seq[T] {
	return db.KeyVal.Match(context.Background(), key)
}

//
// NewReadOnlyContextDefault wraps
func NewReadOnlyContextDefault[T dynamo.Thing](keyval dynamo.KeyVal[T]) dynamo.KeyValReaderNoContext[T] {
	return &keyvalNoContext[T]{keyval}
}
