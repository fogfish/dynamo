//
// Copyright (C) 2019 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

//
// The file declares public types of the library
//

package dynamo

import (
	"context"

	"github.com/fogfish/curie/v2"
)

//-----------------------------------------------------------------------------
//
// Thing
//
//-----------------------------------------------------------------------------

// Thing is the most generic item type used by the library to
// abstract writable/readable items into storage services.
//
// The interfaces declares anything that have a unique identifier.
// The unique identity is exposed by pair of string: HashKey and SortKey.
type Thing interface {
	HashKey() curie.IRI
	SortKey() curie.IRI
}

//-----------------------------------------------------------------------------
//
// Storage Getter
//
//-----------------------------------------------------------------------------

// Getter defines read by key notation
type Getter[T Thing] interface {
	Get(context.Context, T, ...interface{ GetterOpt(T) }) (T, error)
}

// -----------------------------------------------------------------------------
//
// # Storage Pattern Matcher
//
// -----------------------------------------------------------------------------

// KeyValPattern defines simple pattern matching lookup I/O
type Matcher[T Thing] interface {
	MatchKey(context.Context, Thing, ...interface{ MatcherOpt(T) }) ([]T, interface{ MatcherOpt(T) }, error)
	Match(context.Context, T, ...interface{ MatcherOpt(T) }) ([]T, interface{ MatcherOpt(T) }, error)
}

//-----------------------------------------------------------------------------
//
// Storage Reader
//
//-----------------------------------------------------------------------------

// KeyValReader a generic key-value trait to read domain objects
type Reader[T Thing] interface {
	Getter[T]
	Matcher[T]
}

//-----------------------------------------------------------------------------
//
// Storage Writer
//
//-----------------------------------------------------------------------------

// Writer defines a generic key-value writer
type Writer[T Thing] interface {
	Put(context.Context, T, ...interface{ WriterOpt(T) }) error
	Remove(context.Context, T, ...interface{ WriterOpt(T) }) (T, error)
	Update(context.Context, T, ...interface{ WriterOpt(T) }) (T, error)
}

//-----------------------------------------------------------------------------
//
// Storage interface
//
//-----------------------------------------------------------------------------

// KeyVal is a generic key-value trait to access domain objects.
type KeyVal[T Thing] interface {
	Reader[T]
	Writer[T]
}

//-----------------------------------------------------------------------------
//
// Options
//
//-----------------------------------------------------------------------------

// Limit option for Match
func Limit[T Thing](v int32) interface{ MatcherOpt(T) } { return limit[T](v) }

type limit[T Thing] int32

func (limit[T]) MatcherOpt(T) {}

func (limit limit[T]) Limit() int32 { return int32(limit) }

// Cursor option for Match
func Cursor[T Thing](c Thing) interface{ MatcherOpt(T) } { return cursor[T]{c} }

type cursor[T Thing] struct{ Thing }

func (cursor[T]) MatcherOpt(T) {}
