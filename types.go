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
	"fmt"

	"github.com/fogfish/curie"
)

//-----------------------------------------------------------------------------
//
// Thing
//
//-----------------------------------------------------------------------------

/*

Thing is the most generic item type used by the library to
abstract writable/readable items into storage services.

The interfaces declares anything that have a unique identifier.
The unique identity is exposed by pair of string: HashKey and SortKey.
*/
type Thing interface {
	HashKey() curie.IRI
	SortKey() curie.IRI
}

/*

Things is sequence of Thing
*/
type Things[T Thing] []T

/*

Join lifts sequence of matched objects to seq of IDs
	seq := dynamo.Things{}
	dynamo.Match(...).FMap(seq.Join)
*/
func (seq *Things[T]) Join(t T) error {
	*seq = append(*seq, t)
	return nil
}

//-----------------------------------------------------------------------------
//
// Storage Lazy Sequence
//
//-----------------------------------------------------------------------------

/*

SeqLazy is an interface to iterate through collection of objects at storage
*/
type SeqLazy[T Thing] interface {
	// Head lifts first element of sequence
	Head() (T, error)
	// Tail evaluates tail of sequence
	Tail() bool
	// Error returns error of stream evaluation
	Error() error
	// Cursor is the global position in the sequence
	Cursor() Thing
}

/*

SeqConfig configures optional sequence behavior
*/
type SeqConfig[T Thing] interface {
	// Limit sequence size to N elements (pagination)
	Limit(int) Seq[T]
	// Continue limited sequence from the cursor
	Continue(Thing) Seq[T]
	// Reverse order of sequence
	Reverse() Seq[T]
}

/*

Seq is an interface to transform collection of objects

  db.Match(dynamo.NewID("users")).FMap(func(x *T) error { ... })
*/
type Seq[T Thing] interface {
	SeqLazy[T]
	SeqConfig[T]

	// Sequence transformer
	FMap(func(T) error) error
}

//-----------------------------------------------------------------------------
//
// Storage Getter
//
//-----------------------------------------------------------------------------

/*

KeyValGetter defines read by key notation
*/
type KeyValGetter[T Thing] interface {
	Get(context.Context, T) (T, error)
}

/*

KeyValGetterNoContext defines read by key notation
*/
type KeyValGetterNoContext[T Thing] interface {
	Get(T) (T, error)
}

//-----------------------------------------------------------------------------
//
// Storage Pattern Matcher
//
//-----------------------------------------------------------------------------

/*

KeyValPattern defines simple pattern matching lookup I/O
*/
type KeyValPattern[T Thing] interface {
	Match(context.Context, T) Seq[T]
}

/*

KeyValPatternNoContext defines simple pattern matching lookup I/O
*/
type KeyValPatternNoContext[T Thing] interface {
	Match(T) Seq[T]
}

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

/*

KeyValReaderNoContext a generic key-value trait to read domain objects
*/
type KeyValReaderNoContext[T Thing] interface {
	KeyValGetterNoContext[T]
	KeyValPatternNoContext[T]
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
	Put(context.Context, T, ...Constraint[T]) error
	Remove(context.Context, T, ...Constraint[T]) error
	Update(context.Context, T, ...Constraint[T]) (T, error)
}

/*

KeyValWriterNoContext defines a generic key-value writer
*/
type KeyValWriterNoContext[T Thing] interface {
	Put(T, ...Constraint[T]) error
	Remove(T, ...Constraint[T]) error
	Update(T, ...Constraint[T]) (T, error)
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

/*

KeyValNoContext is a generic key-value trait to access domain objects.
*/
type KeyValNoContext[T Thing] interface {
	KeyValReaderNoContext[T]
	KeyValWriterNoContext[T]
}

//-----------------------------------------------------------------------------
//
// Errors
//
//-----------------------------------------------------------------------------

/*

NotFound is an error to handle unknown elements
*/
type NotFound struct{ Thing }

func (e NotFound) Error() string {
	return fmt.Sprintf("Not Found (%s, %s) ", e.Thing.HashKey(), e.Thing.SortKey())
}

/*

PreConditionFailed is an error to handler aborted I/O on
requests with conditional expressions
*/
type PreConditionFailed struct{ Thing }

func (e PreConditionFailed) Error() string {
	return fmt.Sprintf("Pre Condition Failed (%s, %s) ", e.Thing.HashKey(), e.Thing.SortKey())
}

/*

EOS error indicates End Of Stream
*/
type EOS struct{}

func (e EOS) Error() string {
	return "End of Stream"
}
