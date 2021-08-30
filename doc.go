//
// Copyright (C) 2019 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

/*
Package dynamo implements a simple key-value abstraction to store
algebraic data types with AWS services:

↣ AWS DynamoDB

↣ AWS S3

Inspiration

The library encourages developers to use Golang struct to define domain
models, write correct, maintainable code. Using the library, the application
can achieve the ideal data model that would require a single request to
DynamoDB and model one-to-one, one-to-many and even many-to-many relations.
The library uses generic programming style to implement actual storage I/O,
while expose external domain object as interface{} with implicit conversion
back and forth between a concrete struct(s).

Essentially, it implement a following generic key-value trait to access
domain objects. The library AWS Go SDK under the hood

  trait KeyVal[T] {
    def put(entity: T): T
    def get(pattern: T): T
    def remove(pattern: T): T
    def update(entity: T): T
    def match(pattern: T): Seq[T]
  }

Getting started

Define an application domain model using product types, which are
strongly expressed by struct in Go.

  type Person struct {
    dynamo.ID
    Name    string `dynamodbav:"name,omitempty"`
    Age     int    `dynamodbav:"age,omitempty"`
    Address string `dynamodbav:"address,omitempty"`
  }

Use DynamoDB attributes from AWS Go SDK to specify marshalling rules
https://docs.aws.amazon.com/sdk-for-go/api/service/dynamodb/dynamodbattribute.

The library demands usage of compact format of Internationalized Resource Identifier
as object identity schema.

Create an implicit I/O endpoint to Dynamo DB table
  db := dynamo.New("ddb:///my-table")

Creates a new entity, or replaces an old entity with a new value.
  err := db.Put(
    Person{
      dynamo.NewID("8980789222"),
      "Verner Pleishner",
      64,
      "Blumenstrasse 14, Berne, 3013",
    }
  )

Lookup the struct using Get. This function takes "empty" structure as
a placeholder and fill it with a data upon the completion. The only
requirement - ID has to be defined.

  person := Person{ID: dynamo.NewID("8980789222")}
  switch err := db.Get(&person).(type) {
  case nil:
    // success
  case dynamo.NotFound:
    // not found
  default:
    // other i/o error
  }

Remove the entity
  err := db.Remove(dynamo.NewID("8980789222"))

Apply a partial update using Update function. This function takes
a partially defined structure, patches the instance at storage and
returns remaining attributes.
  person := Person{
    ID:      dynamo.NewID("8980789222"),
    Address: "Viktoriastrasse 37, Berne, 3013",
  }
  if err := db.Update(&person); err != nil { ... }

Use following DynamoDB schema:

  const Schema = (): ddb.TableProps => ({
	   partitionKey: {type: ddb.AttributeType.STRING, name: 'prefix'},
	   sortKey: {type: ddb.AttributeType.STRING, name: 'suffix'},
	   tableName: 'my-table',
	   readCapacity: 1,
	   writeCapacity: 1,
  })

Linked data

Interlinking of structured data is essential part of data design.
Use `dynamo.IRI` type to model relations between data instances

  type Person struct {
    dynamo.ID
    Account *curie.IRI `dynamodbav:"name,omitempty"`
  }

`dynamo.ID` and `curie.IRI` are equivalent data types. The first one
is used as primary key, the latter one is a linked identity.

Use with AWS DynamoDB

↣ create I/O handler using ddb schema `dynamo.New("ddb:///my-table")`

↣ provision DynamoDB table with few mandatory attributes
primary key `prefix` and sort key `suffix`.

↣ storage persists struct fields at table columns, use `dynamodbav` field
tags to specify serialization rules

Use with AWS S3

↣ create I/O handler using s3 schema `dynamo.New("s3:///my-bucket")`

↣ primary key `dynamo.ID` is serialized to S3 bucket path `prefix/suffix`

↣ storage persists struct to JSON, use `json` field tags to specify
serialization rules
*/
package dynamo
