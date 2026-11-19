//
// Copyright (C) 2019 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

/*
Package dynamo implements a simple key-value abstraction to store
algebraic data types at AWS DynamoDB.

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
strongly expressed by struct in Go. Keys are modelled with type-safe
identities of https://github.com/fogfish/gold library.

	type University struct{}
	type Person struct{}

	type Professor struct {
	  Org     gold.IRI[University] `dynamodbav:"prefix,omitempty"`
	  ID      gold.IRI[Person]     `dynamodbav:"suffix,omitempty"`
	  Name    string               `dynamodbav:"name,omitempty"`
	  Age     int                  `dynamodbav:"age,omitempty"`
	  Address string               `dynamodbav:"address,omitempty"`
	}

Make sure that defined type implements dynamo.Thing interface for identity,
the keys are plain strings

	func (p Professor) HashKey() string { return string(p.Org) }
	func (p Professor) SortKey() string { return string(p.ID) }

Gold identities are stored as DynamoDB strings and validated when read.

Use DynamoDB attributes from AWS Go SDK to specify marshalling rules
https://docs.aws.amazon.com/sdk-for-go/api/service/dynamodb/dynamodbattribute.

Create an I/O endpoint to Dynamo DB table

	db := ddb.Must(ddb.New[Professor]("my-table"))

Creates a new entity, or replaces an old entity with a new value.

	err := db.Put(ctx,
	  Professor{
	    Org:      gold.ToIRI[University]("kiel"),
	    ID:       gold.ToIRI[Person]("8980789222"),
	    Name:     "Verner Pleishner",
	    Age:      64,
	    Address:  "Blumenstrasse 14, Berne, 3013",
	  },
	)

Lookup the struct using Get. This function takes input structure as key
and return a new copy upon the completion. The only requirement - ID has to
be defined.

	val, err := db.Get(ctx,
	  Professor{
	    Org: gold.ToIRI[University]("kiel"),
	    ID:  gold.ToIRI[Person]("8980789222"),
	  },
	)
	switch {
	case err == nil:
	  // success
	case recoverNotFound(err):
	  // not found
	default:
	  // other i/o error
	}

Remove the entity

	_, err := db.Remove(ctx,
	  Professor{
	    Org: gold.ToIRI[University]("kiel"),
	    ID:  gold.ToIRI[Person]("8980789222"),
	  },
	)

Apply a partial update using Update function. This function takes
a partially defined structure, patches the instance at storage and
returns remaining attributes.

	person := Professor{
	  Org:     gold.ToIRI[University]("kiel"),
	  ID:      gold.ToIRI[Person]("8980789222"),
	  Address: "Viktoriastrasse 37, Berne, 3013",
	}
	val, err := db.Update(ctx, person)
	if err != nil { ... }

Match the entities using the partition key and the prefix of the sort key.
gold.Any selects every identity of the class.

	seq, cursor, err := db.Match(ctx,
	  Professor{
	    Org: gold.ToIRI[University]("kiel"),
	    ID:  gold.Any[Person](),
	  },
	)

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
