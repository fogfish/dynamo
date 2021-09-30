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
	"io"
	"time"
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
*/
type Thing interface {
	Identity() (string, string)
}

/*

ThingStream is an extension to Thing that provides a stream
*/
type ThingStream interface {
	Thing
	// reader of thing content
	Reader() (io.Reader, error)
}

/*

Gen is a generic representation of Thing at the storage
*/
type Gen interface {
	// return unique id of Thing
	ID() (string, string)
	// To decodes generic representation to structure
	To(Thing) error
}

//-----------------------------------------------------------------------------
//
// Storage Lazy Sequence
//
//-----------------------------------------------------------------------------

/*

SeqLazy is an interface to iterate through collection of objects at storage
*/
type SeqLazy interface {
	// Head lifts first element of sequence
	Head(Thing) error
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
type SeqConfig interface {
	// Limit sequence size to N elements (pagination)
	Limit(int64) Seq
	// Continue limited sequence from the cursor
	Continue(cursor Thing) Seq
	// Reverse order of sequence
	Reverse() Seq
}

/*

Seq is an interface to transform collection of objects

  db.Match(dynamo.NewID("users")).FMap(func(gen Gen) error {
     val = &Person{}
     return gen.To(val)
  })
*/
type Seq interface {
	SeqLazy
	SeqConfig

	// Sequence transformer
	FMap(func(Gen) error) error
}

//-----------------------------------------------------------------------------
//
// Storage Getter
//
//-----------------------------------------------------------------------------

/*

KeyValGetter defines read by key notation
*/
type KeyValGetter interface {
	Get(context.Context, Thing) error
}

/*

KeyValGetterNoContext defines read by key notation
*/
type KeyValGetterNoContext interface {
	Get(Thing) error
}

//-----------------------------------------------------------------------------
//
// Storage Pattern Matcher
//
//-----------------------------------------------------------------------------

/*

KeyValPattern defines simple pattern matching lookup I/O
*/
type KeyValPattern interface {
	Match(context.Context, Thing) Seq
}

/*

KeyValPatternNoContext defines simple pattern matching lookup I/O
*/
type KeyValPatternNoContext interface {
	Match(Thing) Seq
}

//-----------------------------------------------------------------------------
//
// Storage Reader
//
//-----------------------------------------------------------------------------

/*

KeyValReader a generic key-value trait to read domain objects
*/
type KeyValReader interface {
	KeyValGetter
	KeyValPattern
}

/*

KeyValReaderNoContext a generic key-value trait to read domain objects
*/
type KeyValReaderNoContext interface {
	KeyValGetterNoContext
	KeyValPatternNoContext
}

//-----------------------------------------------------------------------------
//
// Storage Writer
//
//-----------------------------------------------------------------------------

/*

KeyValWriter defines a generic key-value writer
*/
type KeyValWriter interface {
	Put(context.Context, Thing, ...Constrain) error
	Remove(context.Context, Thing, ...Constrain) error
	Update(context.Context, Thing, ...Constrain) error
}

/*

KeyValWriterNoContext defines a generic key-value writer
*/
type KeyValWriterNoContext interface {
	Put(Thing, ...Constrain) error
	Remove(Thing, ...Constrain) error
	Update(Thing, ...Constrain) error
}

//-----------------------------------------------------------------------------
//
// Stream Reader
//
//-----------------------------------------------------------------------------

/*

StreamReader is a generic reader of byte streams
*/
type StreamReader interface {
	SourceURL(context.Context, Thing, time.Duration) (string, error)
	Read(context.Context, Thing) (io.ReadCloser, error)
}

/*

StreamReaderNoContext is a generic reader of byte streams
*/
type StreamReaderNoContext interface {
	SourceURL(Thing, time.Duration) (string, error)
	Read(Thing) (io.ReadCloser, error)
}

//-----------------------------------------------------------------------------
//
// Stream Writer
//
//-----------------------------------------------------------------------------

/*

StreamWriter is a generic writer of byte streams
*/
type StreamWriter interface {
	Write(context.Context, ThingStream, ...Content) error
}

/*

StreamWriterNoContext is a generic writer of byte streams
*/
type StreamWriterNoContext interface {
	Write(ThingStream, ...Content) error
}

//-----------------------------------------------------------------------------
//
// Storage interface
//
//-----------------------------------------------------------------------------

/*

KeyVal is a generic key-value trait to access domain objects.
*/
type KeyVal interface {
	KeyValReader
	KeyValWriter
}

/*

KeyValNoContext is a generic key-value trait to access domain objects.
*/
type KeyValNoContext interface {
	KeyValReaderNoContext
	KeyValWriterNoContext
}

/*

Stream is a generic byte stream trait to access large binary data
*/
type Stream interface {
	KeyVal
	StreamReader
	StreamWriter
}

/*

StreamNoContext is a generic byte stream trait to access large binary data
*/
type StreamNoContext interface {
	KeyValNoContext
	StreamReaderNoContext
	StreamWriterNoContext
}

//-----------------------------------------------------------------------------
//
// Errors
//
//-----------------------------------------------------------------------------

/*

NotFound is an error to handle unknown elements
*/
type NotFound struct {
	HashKey, SortKey string
}

func (e NotFound) Error() string {
	return fmt.Sprintf("Not Found (%s, %s) ", e.HashKey, e.SortKey)
}

/*

PreConditionFailed is an error to handler aborted I/O on
requests with conditional expressions
*/
type PreConditionFailed struct {
	HashKey, SortKey string
}

func (e PreConditionFailed) Error() string {
	return fmt.Sprintf("Pre Condition Failed (%s, %s) ", e.HashKey, e.SortKey)
}
