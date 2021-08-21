//
// Copyright (C) 2019 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

// Package dynamo implements a simple key-value abstraction to store
// algebraic data types with AWS services:
//
// ↣ AWS DynamoDB
//
// ↣ AWS S3
//
// Inspiration
//
// The library encourages developers to use Golang struct to define domain
// models, write correct, maintainable code. The library uses generic
// programming style to implement actual storage I/O, while expose external
// domain object as interface{} with implicit conversion back and forth
// between a concrete struct(s).
//
// Essentially, it implement a following generic key-value trait to access
// domain objects. The library AWS Go SDK under the hood
//
//   trait KeyVal[T] {
//     def put(entity: T): T
//     def get(pattern: T): T
//     def remove(pattern: T): T
//     def update(entity: T): T
//     def match(pattern: T): Seq[T]
//   }
//
// Getting started
//
// Define an application domain model using product types, which are
// strongly expressed by struct in Go.
//
//   type Person struct {
//     dynamo.ID
//     Name    string `dynamodbav:"name,omitempty"`
//     Age     int    `dynamodbav:"age,omitempty"`
//     Address string `dynamodbav:"address,omitempty"`
//   }
//
// Use DynamoDB attributes from AWS Go SDK to specify marshalling rules
// https://docs.aws.amazon.com/sdk-for-go/api/service/dynamodb/dynamodbattribute.
//
// The library demands usage of compact format of Internationalized Resource Identifier
// as object identity schema.
//
// Create an implicit I/O endpoint to Dynamo DB table
//   db := dynamo.New("ddb:///my-table")
//
// Creates a new entity, or replaces an old entity with a new value.
//   err := db.Put(
//     Person{
//       dynamo.NewID("8980789222"),
//       "Verner Pleishner",
//       64,
//       "Blumenstrasse 14, Berne, 3013",
//     }
//   )
//
// Lookup the struct using Get. This function takes "empty" structure as
// a placeholder and fill it with a data upon the completion. The only
// requirement - ID has to be defined.
//
//   person := Person{ID: dynamo.NewID("8980789222")}
//   switch err := db.Get(&person).(type) {
//   case nil:
//     // success
//   case dynamo.NotFound:
//     // not found
//   default:
//     // other i/o error
//   }
//
// Remove the entity
//   err := db.Remove(dynamo.NewID("8980789222"))
//
// Apply a partial update using Update function. This function takes
// a partially defined structure, patches the instance at storage and
// returns remaining attributes.
//   person := Person{
//     ID:      dynamo.NewID("8980789222"),
//     Address: "Viktoriastrasse 37, Berne, 3013",
//   }
//   if err := db.Update(&person); err != nil { ... }
//
// Use following DynamoDB schema:
//
//   const Schema = (): ddb.TableProps => ({
// 	   partitionKey: {type: ddb.AttributeType.STRING, name: 'prefix'},
// 	   sortKey: {type: ddb.AttributeType.STRING, name: 'suffix'},
// 	   tableName: 'my-table',
// 	   readCapacity: 1,
// 	   writeCapacity: 1,
//   })
//
// Linked data
//
// Interlinking of structured data is essential part of data design.
// Use `dynamo.IRI` type to model relations between data instances
//
//   type Person struct {
//     dynamo.ID
//     Account *curie.IRI `dynamodbav:"name,omitempty"`
//   }
//
// `dynamo.ID` and `curie.IRI` are equivalent data types. The first one
// is used as primary key, the latter one is a linked identity.
//
// Use with AWS DynamoDB
//
// ↣ create I/O handler using ddb schema `dynamo.New("ddb:///my-table")`
//
// ↣ provision DynamoDB table with few mandatory attributes
// primary key `prefix` and sort key `suffix`.
//
// ↣ storage persists struct fields at table columns, use `dynamodbav` field
// tags to specify serialization rules
//
// Use with AWS S3
//
// ↣ create I/O handler using s3 schema `dynamo.New("s3:///my-bucket")`
//
// ↣ primary key `dynamo.ID` is serialized to S3 bucket path `prefix/suffix`
//
// ↣ storage persists struct to JSON, use `json` field tags to specify
// serialization rules
package dynamo

import (
	"fmt"
	"io"
	"log"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/fogfish/curie"
)

//
// KeyVal is a generic key-value trait to access domain objects.
type KeyVal interface {
	KeyValReader
	KeyValWriter
}

//
// KeyValWriter defines a generic key-value I/O
type KeyValWriter interface {
	Put(Thing, ...Constrain) error
	Remove(Thing, ...Constrain) error
	Update(Thing, ...Constrain) error
}

//
// KeyValReader a generic key-value trait to read domain objects
type KeyValReader interface {
	KeyValLookup
	KeyValPattern
}

// KeyValLookup defines read by key notation
type KeyValLookup interface {
	Get(Thing) error
}

// KeyValPattern defines simple pattern matching lookup I/O
type KeyValPattern interface {
	Match(Thing) Seq
}

//
// FMap is a transformer of generic representation to concrete type
type FMap func(Gen) (Thing, error)

//
// Gen is a generic representation of storage type
type Gen interface {
	ID() (*ID, error)
	To(Thing) error
}

//
// Seq is an interface to transform collection of objects
//
//   db.Match(dynamo.NewID("users")).FMap(func(gen Gen) (dynamo.Thing, error) {
//      val = &Person{}
//      return gen.To(val)
//   })
type Seq interface {
	SeqLazy
	SeqConfig

	// Sequence transformer
	FMap(FMap) ([]Thing, error)
}

// SeqLazy is an interface to iterate through collection of objects
type SeqLazy interface {
	// Head lifts first element of sequence
	Head(Thing) error
	// Tail evaluates tail of sequence
	Tail() bool
	// Error returns error of stream evaluation
	Error() error
	// Cursor is the global position in the sequence
	Cursor() *curie.IRI
}

// SeqConfig define sequence behavior
type SeqConfig interface {
	// Limit sequence size to N elements, fetch a page of sequence
	Limit(int64) Seq
	// Continue limited sequence from the cursor
	Continue(cursor *curie.IRI) Seq
	// Reverse order of sequence
	Reverse() Seq
}

// Blob is a generic byte stream trait to access large binary data
type Blob interface {
	KeyVal
	URL(Thing, time.Duration) (string, error)
	Recv(Thing) (io.ReadCloser, error)
	Send(Thing, io.Reader, ...Content) error
}

//
// NotFound is an error to handle unknown elements
type NotFound struct {
	Key string
}

func (e NotFound) Error() string {
	return fmt.Sprintf("Not Found %v", e.Key)
}

//
// PreConditionFailed is an error to handler aborted I/O on
// requests with conditional expressions
type PreConditionFailed struct {
	Key string
}

func (e PreConditionFailed) Error() string {
	return fmt.Sprintf("Pre Condition Failed %v", e.Key)
}

//
// New establishes connection with AWS Storage service,
// use URI to specify service and name of the bucket.
// Supported scheme:
//   s3:///my-bucket
//   ddb:///my-table
func New(uri string, defSession ...*session.Session) (KeyVal, error) {
	awsSession, err := maybeNewSession(defSession)
	if err != nil {
		return nil, err
	}

	creator, spec, err := factory(uri, defSession...)
	if err != nil {
		return nil, err
	}

	return creator(awsSession, spec), nil
}

// Must is a helper function to ensure KeyVal interface is valid and there was no
// error when calling a New function.
//
// This helper is intended to be used in variable initialization to load the
// interface and configuration at startup. Such as:
//
//    var io = dynamo.Must(dynamo.New())
func Must(kv KeyVal, err error) KeyVal {
	if err != nil {
		log.Panicln(err)
	}
	return kv
}

// Stream establishes bytes stream connection with AWS Storage service,
// use URI to specify service and name of the bucket.
// Supported scheme:
//   s3:///my-bucket
func Stream(uri string, defSession ...*session.Session) (Blob, error) {
	awsSession, err := maybeNewSession(defSession)
	if err != nil {
		return nil, err
	}

	creator, spec, err := factory(uri, defSession...)
	if err != nil {
		return nil, err
	}

	keyval := creator(awsSession, spec)
	stream, ok := keyval.(Blob)
	if !ok {
		return nil, fmt.Errorf("Streaming is not supported by %s", uri)
	}

	return stream, nil
}

//
type creator func(io *session.Session, spec *dbURL) KeyVal

func factory(uri string, defSession ...*session.Session) (creator, *dbURL, error) {
	spec, _ := url.Parse(uri)
	switch {
	case spec == nil:
		return nil, nil, fmt.Errorf("Invalid url: %s", uri)
	case len(spec.Path) < 2:
		return nil, nil, fmt.Errorf("Invalid url, path to data storage is not defined: %s", uri)
	case spec.Scheme == "s3":
		return newS3, (*dbURL)(spec), nil
	case spec.Scheme == "ddb":
		return newDB, (*dbURL)(spec), nil
	default:
		return nil, nil, fmt.Errorf("Unsupported schema: %s", uri)
	}
}

//
func maybeNewSession(defSession []*session.Session) (*session.Session, error) {
	if len(defSession) != 0 {
		return defSession[0], nil
	}

	awsSession, err := session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	})

	if err != nil {
		return nil, err
	}

	return awsSession, nil
}

/*

dbURL custom type with helper functions
*/
type dbURL url.URL

// query parameters
func (uri *dbURL) query(key, def string) string {
	val := (*url.URL)(uri).Query().Get(key)

	if val == "" {
		return def
	}

	return val
}

// path segments of length
func (uri *dbURL) segments(n int) []*string {
	seq := make([]*string, n)

	seg := strings.Split((*url.URL)(uri).Path, "/")[1:]
	for i, x := range seg {
		val := x
		seq[i] = &val
	}

	return seq
}
