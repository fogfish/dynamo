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

# Inspiration

The library encourages developers to use Golang struct to define domain
models, write correct, maintainable code. Using the library, the application
can achieve the ideal data model that would require a single request to
DynamoDB and model one-to-one, one-to-many and even many-to-many relations.
The library uses generic programming style to implement actual storage I/O,
while expose external domain object as `[T dynamo.Thing]` with implicit
conversion back and forth between a concrete struct(s).

Essentially, it implement a following generic key-value trait to access
domain objects. The library AWS Go SDK under the hood

	type KeyVal[T any] interface {
	  Put(T) error
	  Get(T) (T, error)
	  Remove(T) error
	  Update(T): (T, error)
	  Match(T): []T
	}

# Getting started

Define an application domain model using product types, which are
strongly expressed by struct in Go.

	type Person struct {
	  Org     curie.IRI `dynamodbav:"prefix,omitempty"`
	  ID      curie.IRI `dynamodbav:"suffix,omitempty"`
	  Name    string    `dynamodbav:"name,omitempty"`
	  Age     int       `dynamodbav:"age,omitempty"`
	  Address string    `dynamodbav:"address,omitempty"`
	}

Make sure that defined type implements dynamo.Thing interface for identity

	func (p Person) HashKey() curie.IRI { return p.Org }
	func (p Person) SortKey() curie.IRI { return p.ID }

Use DynamoDB attributes from AWS Go SDK to specify marshalling rules
https://docs.aws.amazon.com/sdk-for-go/api/service/dynamodb/dynamodbattribute.

Create an implicit I/O endpoint to Dynamo DB table

	db := keyval.New[Person](dynamo.WithURI("ddb:///my-table"))

Creates a new entity, or replaces an old entity with a new value.

	err := db.Put(
	  Person{
	    Org:      curie.IRI("test"),
	    ID:       curie.IRI("8980789222"),
	    Name:     "Verner Pleishner",
	    Age:      64,
	    Address:  "Blumenstrasse 14, Berne, 3013",
	  }
	)

Lookup the struct using Get. This function takes input structure as key
and return a new copy upon the completion. The only requirement - ID has to
be defined.

	val, err := db.Get(Person{Org: curie.IRI("test"), ID: curie.IRI("8980789222")})
	switch err.(type) {
	case nil:
	  // success
	case dynamo.NotFound:
	  // not found
	default:
	  // other i/o error
	}

Remove the entity

	err := db.Remove(Person{Org: curie.IRI("test"), ID: curie.IRI("8980789222")})

Apply a partial update using Update function. This function takes
a partially defined structure, patches the instance at storage and
returns remaining attributes.

	person := Person{
	  Org:     "test",
	  ID:      "8980789222"
	  Address: "Viktoriastrasse 37, Berne, 3013",
	}
	val, err := db.Update(person)
	if err != nil { ... }

Use following DynamoDB schema:

	  const Schema = (): ddb.TableProps => ({
		   partitionKey: {type: ddb.AttributeType.STRING, name: 'prefix'},
		   sortKey: {type: ddb.AttributeType.STRING, name: 'suffix'},
		   tableName: 'my-table',
		   readCapacity: 1,
		   writeCapacity: 1,
	  })

See README at https://github.com/fogfish/dynamo
*/
package dynamo
