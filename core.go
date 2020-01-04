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
//     dynamo.IRI
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
//       dynamo.IRI{"dead", "beef"},
//       "Verner Pleishner",
//       64,
//       "Blumenstrasse 14, Berne, 3013",
//     }
//   )
//
// Lookup the entity
//   person := Person{IRI: dynamo.IRI{"dead", "beef"}}
//   err := db.Get(&person)
//
// Remove the entity
//   err := db.Remove(dynamo.IRI{"dead", "beef"})
//
// Partial update of the entity
//   err := db.Update(Person{IRI: dynamo.IRI{"dead", "beef"}, Age: 65})
//
// Lookup sequence of items
//   seq := db.Match(dynamo.IRI{Prefix: "dead"})
//   for seq.Tail() {
//	   val := &Person{}
//     err := seq.Head(val)
//     ...
//   }
package dynamo

import (
	"errors"
	"fmt"
	"net/url"
)

//
// KeyVal is a generic key-value trait to access domain objects
type KeyVal interface {
	Put(interface{}) error
	Get(interface{}) error
	Remove(interface{}) error
	Update(interface{}) error
	Match(interface{}) Seq
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
	Head(interface{}) error
	Tail() bool
	Error() error
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
	Prefix string `dynamodbav:"prefix" json:"prefix,omitempty"`
	Suffix string `dynamodbav:"suffix,omitempty" json:"suffix,omitempty"`
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
// New establishes connection with AWS Storage service,
// use URI to specify service and name of the bucket.
// Supported scheme:
//   s3:///my-bucket
//   ddb:///my-table
func New(uri string) (KeyVal, error) {
	spec, _ := url.Parse(uri)
	switch {
	case spec == nil:
		return nil, errors.New("Invalid url: " + uri)
	case spec.Path == "":
		return nil, errors.New("Invalid url, path is missing: " + uri)
	case spec.Scheme == "s3":
		return newS3(spec.Path[1:]), nil
	case spec.Scheme == "ddb":
		return newDB(spec.Path[1:]), nil
	default:
		return nil, errors.New("Unsupported schema: " + uri)
	}
}
