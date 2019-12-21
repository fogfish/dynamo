//
// Copyright (C) 2019 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

// Package dynamo implements a driver to AWS DynamoDB that operates
// with generic representation of algebraic data types.
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
// The struct dynamo.IRI is an Internationalized Resource Identifier used as
// object identity.
//
// Create an implicit I/O endpoint to Dynamo DB table
//   db := dynamo.New("my-table")
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
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

// IRI is a compact Internationalized Resource Identifier. It is designed
// to support following DynamoDB schema
//
//   MyTable:
//     Type: AWS::DynamoDB::Table
//     Properties:
//       TableName: !Sub ${AWS::StackName}
//       AttributeDefinitions:
//         - AttributeName: prefix
//           AttributeType: S
//         - AttributeName: suffix
//           AttributeType: S
//
//       KeySchema:
//         - AttributeName: prefix
//           KeyType: HASH
//         - AttributeName: suffix
//           KeyType: RANGE
type IRI struct {
	Prefix string `dynamodbav:"prefix"`
	Suffix string `dynamodbav:"suffix,omitempty"`
}

// NotFound is an error to handle unknown elements
type NotFound struct {
	Key IRI
}

func (e NotFound) Error() string {
	return fmt.Sprintf("Not Found %v", e.Key)
}

// DB is a connection handle
type DB struct {
	io    *session.Session
	db    *dynamodb.DynamoDB
	table *string
}

// New establishes connection to DynamoDB table
func New(table string) DB {
	io := session.Must(session.NewSession())
	db := dynamodb.New(io)
	return DB{io, db, aws.String(table)}
}

//-----------------------------------------------------------------------------
//
// Key Value
//
//-----------------------------------------------------------------------------

// Get fetches the entity identified by the key
func (dynamo DB) Get(key interface{}) (err error) {
	gen, err := dynamodbattribute.MarshalMap(key)
	if err != nil {
		return
	}

	req := &dynamodb.GetItemInput{
		Key:       keyOf(gen),
		TableName: dynamo.table,
	}
	val, err := dynamo.db.GetItem(req)
	if err != nil {
		return
	}

	if val.Item == nil {
		iri := IRI{}
		dynamodbattribute.UnmarshalMap(keyOf(gen), &iri)
		err = NotFound{iri}
		return
	}

	dynamodbattribute.UnmarshalMap(val.Item, &key)
	return
}

// Put writes entity
func (dynamo DB) Put(val interface{}) (err error) {
	gen, err := dynamodbattribute.MarshalMap(val)
	if err != nil {
		return
	}

	req := &dynamodb.PutItemInput{
		Item:      gen,
		TableName: dynamo.table,
	}
	_, err = dynamo.db.PutItem(req)

	return
}

// Remove discards the entity from the table
func (dynamo DB) Remove(key interface{}) (err error) {
	gen, err := dynamodbattribute.MarshalMap(key)
	if err != nil {
		return
	}

	req := &dynamodb.DeleteItemInput{
		Key:       keyOf(gen),
		TableName: dynamo.table,
	}
	_, err = dynamo.db.DeleteItem(req)

	return
}

// Update applies a partial patch to entity
func (dynamo DB) Update(val interface{}) (err error) {
	gen, err := dynamodbattribute.MarshalMap(val)
	if err != nil {
		return
	}

	values := map[string]*dynamodb.AttributeValue{}
	update := make([]string, 0)
	for k, v := range gen {
		if k != "prefix" && k != "suffix" && k != "id" {
			values[":"+k] = v
			update = append(update, k+"="+":"+k)
		}
	}
	expresion := aws.String("SET " + strings.Join(update, ","))

	req := &dynamodb.UpdateItemInput{
		Key:                       keyOf(gen),
		ExpressionAttributeValues: values,
		UpdateExpression:          expresion,
		TableName:                 dynamo.table,
	}
	_, err = dynamo.db.UpdateItem(req)

	return
}

//-----------------------------------------------------------------------------
//
// Pattern Match
//
//-----------------------------------------------------------------------------

// Seq is an iterator over match results
type Seq struct {
	at    int
	items []map[string]*dynamodb.AttributeValue
	Fail  error
}

// Head selects the first element of matched collection
func (seq *Seq) Head(v interface{}) error {
	if seq.at == -1 {
		seq.at++
	}
	return dynamodbattribute.UnmarshalMap(seq.items[seq.at], v)
}

// Tail selects the all elements except the first one
func (seq *Seq) Tail() bool {
	seq.at++
	return seq.Fail == nil && seq.at < len(seq.items)
}

// Match applies a pattern matching to elements in the table
func (dynamo DB) Match(key interface{}) *Seq {
	gen, err := dynamodbattribute.MarshalMap(key)
	if err != nil {
		return &Seq{-1, nil, err}
	}

	req := &dynamodb.QueryInput{
		KeyConditionExpression:    aws.String("prefix = :prefix"),
		ExpressionAttributeValues: exprOf(gen),
		TableName:                 dynamo.table,
	}
	val, err := dynamo.db.Query(req)
	if err != nil {
		return &Seq{-1, nil, err}
	}

	return &Seq{-1, val.Items, nil}
}

//-----------------------------------------------------------------------------
//
// internal helpers
//
//-----------------------------------------------------------------------------

func keyOf(gen map[string]*dynamodb.AttributeValue) (key map[string]*dynamodb.AttributeValue) {
	key = map[string]*dynamodb.AttributeValue{}
	key["prefix"] = gen["prefix"]
	key["suffix"] = gen["suffix"]

	return
}

func exprOf(gen map[string]*dynamodb.AttributeValue) (val map[string]*dynamodb.AttributeValue) {
	val = map[string]*dynamodb.AttributeValue{}
	for k, v := range gen {
		val[":"+k] = v
	}

	return
}
