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
	"net/url"
	"strings"

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

//-----------------------------------------------------------------------------
//
// Storage Pattern Matcher
//
//-----------------------------------------------------------------------------

// KeyValPattern defines simple pattern matching lookup I/O
type KeyValPattern[T Thing] interface {
	Match(context.Context, T, ...interface{ MatchOpt() }) ([]T, error)
}

// Limit option for Match
type Limit int32

func (Limit) MatchOpt() {}

// Cursor option for Match
func Cursor(c Thing) interface{ MatchOpt() } { return cursor{c} }

type cursor struct{ Thing }

func (cursor) MatchOpt() {}

//-----------------------------------------------------------------------------
//
// Storage Reader
//
//-----------------------------------------------------------------------------

/*
KeyValReader a generic key-value trait to read domain objects
*/
type KeyValReader[T Thing] interface {
	KeyValGetter[T]
	KeyValPattern[T]
}

//-----------------------------------------------------------------------------
//
// Storage Writer
//
//-----------------------------------------------------------------------------

/*
KeyValWriter defines a generic key-value writer
*/
type KeyValWriter[T Thing] interface {
	Put(context.Context, T, ...interface{ Constraint(T) }) error
	Remove(context.Context, T, ...interface{ Constraint(T) }) error
	Update(context.Context, T, ...interface{ Constraint(T) }) (T, error)
}

//-----------------------------------------------------------------------------
//
// Storage interface
//
//-----------------------------------------------------------------------------

/*
KeyVal is a generic key-value trait to access domain objects.
*/
type KeyVal[T Thing] interface {
	KeyValReader[T]
	KeyValWriter[T]
}

//-----------------------------------------------------------------------------
//
// External Services
//
//-----------------------------------------------------------------------------

/*

 */

/*
URL custom type with helper functions
*/
type URL url.URL

func (uri *URL) String() string {
	return (*url.URL)(uri).String()
}

// query parameters
func (uri *URL) Query(key, def string) string {
	val := (*url.URL)(uri).Query().Get(key)

	if val == "" {
		return def
	}

	return val
}

// path segments of length
func (uri *URL) Segments() []string {
	return strings.Split((*url.URL)(uri).Path, "/")[1:]
}
