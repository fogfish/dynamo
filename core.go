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
//       dynamo.UID("dead", "beef"),
//       "Verner Pleishner",
//       64,
//       "Blumenstrasse 14, Berne, 3013",
//     }
//   )
//
// Lookup the entity
//   person := Person{ID: dynamo.UID("dead", "beef")}
//   err := db.Get(&person)
//
// Remove the entity
//   err := db.Remove(dynamo.UID("dead", "beef"))
//
// Partial update of the entity
//   err := db.Update(Person{ID: dynamo.UID("dead", "beef"), Age: 65})
//
// Lookup sequence of items
//   seq := db.Match(dynamo.Prefix("dead"))
//   for seq.Tail() {
//	   val := &Person{}
//     err := seq.Head(val)
//     ...
//   }
//
//
// Linked data
//
// Interlinking of structured data is essential part of data design.
// Use `dynamo.IRI` type to model relations between data instances
//
//   type Person struct {
//     dynamo.ID
//     Account dynamo.IRI `dynamodbav:"name,omitempty"`
//   }
//
// `dynamo.ID` and `dynamo.IRI` are equivalent data types. The first one
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
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/url"
	"path"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

//
// Entity is a "type tag". It ensures type-safe property of KeyVal interface
type Entity interface {
	Key() IRI
}

//
// KeyVal is a generic key-value trait to access domain objects.
type KeyVal interface {
	KeyValPure
	KeyValPattern
}

// KeyValPure defines a generic key-value I/O
type KeyValPure interface {
	Put(Entity) error
	Get(Entity) error
	Remove(Entity) error
	Update(Entity) error
}

// KeyValPattern defines simples pattern matching lookup I/O
type KeyValPattern interface {
	Match(Entity) Seq
}

//
// Seq is an interface iterate through collection of objects
//   for seq.Tail() {
//	   val := &Person{}
//     err := seq.Head(val)
//     ...
//   }
//
//   if err := seq.Error(); err != nil { ... }
type Seq interface {
	Head(Entity) error
	Tail() bool
	Error() error
}

// Blob is a generic byte stream trait to access large binary data
type Blob interface {
	Recv(Entity) (io.ReadCloser, error)
}

//
// IRI is a compact Internationalized Resource Identifier.
//
// Use following DynamoDB schema:
//   const Schema = (): ddb.TableProps => ({
// 	   partitionKey: {type: ddb.AttributeType.STRING, name: 'prefix'},
// 	   sortKey: {type: ddb.AttributeType.STRING, name: 'suffix'},
// 	   tableName: 'my-table',
// 	   readCapacity: 1,
// 	   writeCapacity: 1,
//   })
//
// Use following S3 keys
//   prefix/suffix
type IRI struct {
	Prefix string
	Suffix string
}

// ParseIRI parses string to IRI type
func ParseIRI(s string) IRI {
	seq := strings.Split(s, "/")
	if len(seq) == 1 {
		return IRI{Prefix: s}
	}
	return IRI{path.Join(seq[0 : len(seq)-1]...), seq[len(seq)-1]}
}

// NewIRI creates new IRI type
func NewIRI(prefix string, suffix ...string) IRI {
	if len(suffix) == 0 {
		return IRI{Prefix: prefix}
	}

	return IRI{Prefix: prefix, Suffix: suffix[0]}
}

// ID converts IRI to ID type
func (iri IRI) ID() ID {
	return ID{iri}
}

// Path converts IRI to absolute path
func (iri IRI) Path() string {
	if iri.Prefix == "" && iri.Suffix == "" {
		return ""
	}
	return path.Join(iri.Prefix, iri.Suffix)
}

// Parent returns IRI that is a prefix of this one.
func (iri IRI) Parent() IRI {
	seq := strings.Split(iri.Prefix, "/")
	if len(seq) == 1 {
		return IRI{}
	}
	return IRI{path.Join(seq[0 : len(seq)-1]...), seq[len(seq)-1]}
}

// Heir returns a IRI that descendant of this one.
func (iri IRI) Heir(suffix string) IRI {
	if iri.Prefix == "" && iri.Suffix == "" {
		return IRI{Prefix: suffix}
	}
	return IRI{path.Join(iri.Prefix, iri.Suffix), suffix}
}

// MarshalJSON `IRI ⟼ "/prefix/suffix"`
func (iri IRI) MarshalJSON() ([]byte, error) {
	if iri.Prefix == "" && iri.Suffix == "" {
		return nil, nil
	}
	return json.Marshal("/" + iri.Path())
}

// UnmarshalJSON `"/prefix/suffix" ⟼ IRI`
func (iri *IRI) UnmarshalJSON(b []byte) error {
	path := ""
	err := json.Unmarshal(b, &path)
	if err != nil {
		return err
	}

	if path[0] != '/' {
		return InvalidIRI{string(b)}
	}

	*iri = ParseIRI(path[1:])
	return nil
}

// MarshalDynamoDBAttributeValue `IRI ⟼ "prefix/suffix"`
func (iri IRI) MarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	if iri.Prefix == "" && iri.Suffix == "" {
		av.NULL = aws.Bool(true)
		return nil
	}

	val, err := dynamodbattribute.Marshal(iri.Path())
	if err != nil {
		return err
	}

	av.S = val.S
	return nil
}

// UnmarshalDynamoDBAttributeValue `"prefix/suffix" ⟼ IRI`
func (iri *IRI) UnmarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	path := aws.StringValue(av.S)

	*iri = ParseIRI(path)
	return nil
}

// ID is a primary key for Entities
type ID struct {
	IRI IRI `dynamodbav:"id" json:"id"`
}

// ParseID parses string to ID type
func ParseID(s string) ID {
	return ID{ParseIRI(s)}
}

// NewID creates unique
func NewID(prefix string, suffix ...string) ID {
	if len(suffix) == 0 {
		return ID{IRI{Prefix: prefix}}
	}

	return ID{IRI{Prefix: prefix, Suffix: suffix[0]}}
}

// Key return reference to primary key.
// The function adopts any type which embeds dynamo.ID into Entity interface
func (id ID) Key() IRI {
	return id.IRI
}

// Path converts IRI to absolute path
func (id ID) Path() string {
	return id.IRI.Path()
}

// Prefix returns IRI prefix
func (id ID) Prefix() string {
	return id.IRI.Prefix
}

// Suffix returns IRI suffix
func (id ID) Suffix() string {
	return id.IRI.Suffix
}

// Parent returns ID that is a prefix of this one.
func (id ID) Parent() ID {
	return ID{id.IRI.Parent()}
}

// Heir returns a ID that descendant of this one.
func (id ID) Heir(suffix string) ID {
	return ID{id.IRI.Heir(suffix)}
}

//
// NotFound is an error to handle unknown elements
type NotFound struct {
	Key IRI
}

func (e NotFound) Error() string {
	return fmt.Sprintf("Not Found %v", e.Key)
}

//
// InvalidIRI is caused by JSON unmarshal if text representation of
// IRI type is not valid
type InvalidIRI struct {
	IRI string
}

func (e InvalidIRI) Error() string {
	return fmt.Sprintf("Invalid IRI %v", e.IRI)
}

//
// New establishes connection with AWS Storage service,
// use URI to specify service and name of the bucket.
// Supported scheme:
//   s3:///my-bucket
//   ddb:///my-table
func New(uri string) (KeyVal, error) {
	spec, _ := url.Parse(uri)
	switch {
	case spec == nil:
		return nil, fmt.Errorf("Invalid url: %s", uri)
	case spec.Path == "":
		return nil, fmt.Errorf("Invalid url, path is missing: %s", uri)
	case spec.Scheme == "s3":
		return newS3(bucket(spec.Path)), nil
	case spec.Scheme == "ddb":
		return newDB(bucket(spec.Path)), nil
	default:
		return nil, fmt.Errorf("Unsupported schema: %s", uri)
	}
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
func Stream(uri string) (Blob, error) {
	spec, _ := url.Parse(uri)
	switch {
	case spec == nil:
		return nil, fmt.Errorf("Invalid url: %s", uri)
	case spec.Path == "":
		return nil, fmt.Errorf("Invalid url, path is missing: %s", uri)
	case spec.Scheme == "s3":
		return newS3(bucket(spec.Path)), nil
	default:
		return nil, fmt.Errorf("Unsupported schema: %s", uri)
	}
}

func bucket(s string) string {
	return strings.Split(s[1:], "/")[0]
}
