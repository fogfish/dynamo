//
// Copyright (C) 2019 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

//
// The file declares NoContext wrappers
//

package dynamo

import (
	"context"
)

//-----------------------------------------------------------------------------
//
// KeyVal
//
//-----------------------------------------------------------------------------

type keyvalNoContext[T Thing] struct{ KeyVal[T] }

// NewKeyValContextDefault wraps
func NewKeyValContextDefault[T Thing](keyval KeyVal[T]) KeyValNoContext[T] {
	return &keyvalNoContext[T]{keyval}
}

func (db *keyvalNoContext[T]) Get(key T) (*T, error) {
	return db.KeyVal.Get(context.Background(), key)
}

func (db *keyvalNoContext[T]) Put(entity T, config ...Constrain[T]) error {
	return db.KeyVal.Put(context.Background(), entity, config...)
}

func (db *keyvalNoContext[T]) Remove(key T, config ...Constrain[T]) error {
	return db.KeyVal.Remove(context.Background(), key, config...)
}

func (db *keyvalNoContext[T]) Update(entity T, config ...Constrain[T]) (*T, error) {
	return db.KeyVal.Update(context.Background(), entity, config...)
}

func (db *keyvalNoContext[T]) Match(key T) Seq[T] {
	return db.KeyVal.Match(context.Background(), key)
}

//-----------------------------------------------------------------------------
//
// Stream
//
//-----------------------------------------------------------------------------

// type streamNoContext struct{ Stream }

// // NewStreamContextDefault warps Stream interface with default context
// func NewStreamContextDefault(stream Stream) StreamNoContext {
// 	return &streamNoContext{stream}
// }

// func (db *streamNoContext) Get(entity Thing) (err error) {
// 	return db.Stream.Get(context.Background(), entity)
// }

// func (db *streamNoContext) Put(entity Thing, config ...Constrain) (err error) {
// 	return db.Stream.Put(context.Background(), entity, config...)
// }

// func (db *streamNoContext) Remove(entity Thing, config ...Constrain) (err error) {
// 	return db.Stream.Remove(context.Background(), entity, config...)
// }

// func (db *streamNoContext) Update(entity Thing, config ...Constrain) (err error) {
// 	return db.Stream.Update(context.Background(), entity, config...)
// }

// func (db *streamNoContext) Match(key Thing) Seq {
// 	return db.Stream.Match(context.Background(), key)
// }

// func (db *streamNoContext) SourceURL(key Thing, ttl time.Duration) (string, error) {
// 	return db.Stream.SourceURL(context.Background(), key, ttl)
// }

// func (db *streamNoContext) Read(key Thing) (io.ReadCloser, error) {
// 	return db.Stream.Read(context.Background(), key)
// }

// func (db *streamNoContext) Write(thing ThingStream, opts ...Content) error {
// 	return db.Stream.Write(context.Background(), thing, opts...)
// }
