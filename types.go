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

	"github.com/fogfish/curie"
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

// KeyValGetter defines read by key notation
type KeyValGetter[T Thing] interface {
	Get(context.Context, T) (T, error)
}

// -----------------------------------------------------------------------------
//
// # Storage Pattern Matcher
//
// -----------------------------------------------------------------------------

// type alias for interface{ MatchOpt() }
type MatchOpt = interface{ MatchOpt() }

// KeyValPattern defines simple pattern matching lookup I/O
type KeyValPattern[T Thing] interface {
	MatchKey(context.Context, Thing, ...MatchOpt) ([]T, MatchOpt, error)
	Match(context.Context, T, ...MatchOpt) ([]T, MatchOpt, error)
}

// Limit option for Match
func Limit(v int32) MatchOpt { return limit(v) }

type limit int32

func (limit) MatchOpt() {}

func (limit limit) Limit() int32 { return int32(limit) }

// Cursor option for Match
func Cursor(c Thing) MatchOpt { return cursor{c} }

type cursor struct{ Thing }

func (cursor) MatchOpt() {}

//-----------------------------------------------------------------------------
//
// Storage Reader
//
//-----------------------------------------------------------------------------

// KeyValReader a generic key-value trait to read domain objects
type KeyValReader[T Thing] interface {
	KeyValGetter[T]
	KeyValPattern[T]
}

//-----------------------------------------------------------------------------
//
// Storage Writer
//
//-----------------------------------------------------------------------------

// KeyValWriter defines a generic key-value writer
type KeyValWriter[T Thing] interface {
	Put(context.Context, T, ...interface{ ConditionExpression(T) }) error
	Remove(context.Context, T, ...interface{ ConditionExpression(T) }) (T, error)
	Update(context.Context, T, ...interface{ ConditionExpression(T) }) (T, error)
}

//-----------------------------------------------------------------------------
//
// Storage interface
//
//-----------------------------------------------------------------------------

// KeyVal is a generic key-value trait to access domain objects.
type KeyVal[T Thing] interface {
	KeyValReader[T]
	KeyValWriter[T]
}
