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

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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
	Put(context.Context, T, ...Constraint[T]) error
	Remove(context.Context, T, ...Constraint[T]) error
	Update(context.Context, T, ...Constraint[T]) (T, error)
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

DynamoDB declares interface of original AWS DynamoDB API used by the library
*/
type DynamoDB interface {
	GetItem(context.Context, *dynamodb.GetItemInput, ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	PutItem(context.Context, *dynamodb.PutItemInput, ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	DeleteItem(context.Context, *dynamodb.DeleteItemInput, ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
	UpdateItem(context.Context, *dynamodb.UpdateItemInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
	Query(context.Context, *dynamodb.QueryInput, ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
}

/*

S3 declares AWS API used by the library
*/
type S3 interface {
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	DeleteObject(context.Context, *s3.DeleteObjectInput, ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

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
