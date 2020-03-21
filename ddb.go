//
// Copyright (C) 2019 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

package dynamo

import (
	"errors"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

// DB is a service connection handle
type DB struct {
	io    *session.Session
	db    dynamodbiface.DynamoDBAPI
	table *string
}

func newDB(table string) *DB {
	io := session.Must(session.NewSession())
	db := dynamodb.New(io)
	return &DB{io, db, aws.String(table)}
}

// Mock dynamoDB I/O channel
func (dynamo *DB) Mock(db dynamodbiface.DynamoDBAPI) {
	dynamo.db = db
}

//-----------------------------------------------------------------------------
//
// Key Value
//
//-----------------------------------------------------------------------------

// Get fetches the entity identified by the key.
func (dynamo DB) Get(entity Entity) (err error) {
	gen, err := marshal(dynamodbattribute.MarshalMap(entity))
	if err != nil {
		return
	}

	req := &dynamodb.GetItemInput{
		Key:       keyOnly(gen),
		TableName: dynamo.table,
	}
	val, err := dynamo.db.GetItem(req)
	if err != nil {
		return
	}

	if val.Item == nil {
		prefix, _ := gen["prefix"]
		suffix, _ := gen["suffix"]
		err = NotFound{IRI{aws.StringValue(prefix.S), aws.StringValue(suffix.S)}}
		return
	}

	item, err := unmarshal(val.Item)
	if err != nil {
		return
	}

	err = dynamodbattribute.UnmarshalMap(item, &entity)
	return
}

// Put writes entity
func (dynamo DB) Put(entity Entity) (err error) {
	gen, err := marshal(dynamodbattribute.MarshalMap(entity))
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
func (dynamo DB) Remove(entity Entity) (err error) {

	gen, err := marshal(dynamodbattribute.MarshalMap(entity))
	if err != nil {
		return
	}

	req := &dynamodb.DeleteItemInput{
		Key:       keyOnly(gen),
		TableName: dynamo.table,
	}
	_, err = dynamo.db.DeleteItem(req)
	return
}

// Update applies a partial patch to entity and returns new values
func (dynamo DB) Update(entity Entity) (err error) {
	gen, err := marshal(dynamodbattribute.MarshalMap(entity))
	if err != nil {
		return
	}

	names := map[string]*string{}
	values := map[string]*dynamodb.AttributeValue{}
	update := make([]string, 0)
	for k, v := range gen {
		if k != "prefix" && k != "suffix" && k != "id" {
			names["#"+k] = aws.String(k)
			values[":"+k] = v
			update = append(update, "#"+k+"="+":"+k)
		}
	}
	expresion := aws.String("SET " + strings.Join(update, ","))

	req := &dynamodb.UpdateItemInput{
		Key:                       keyOnly(gen),
		ExpressionAttributeNames:  names,
		ExpressionAttributeValues: values,
		UpdateExpression:          expresion,
		TableName:                 dynamo.table,
		ReturnValues:              aws.String("ALL_NEW"),
	}
	val, err := dynamo.db.UpdateItem(req)
	if err != nil {
		return
	}

	dynamodbattribute.UnmarshalMap(val.Attributes, &entity)
	return
}

//-----------------------------------------------------------------------------
//
// Pattern Match
//
//-----------------------------------------------------------------------------

// SeqDB is an iterator over match results
type SeqDB struct {
	at    int
	items []map[string]*dynamodb.AttributeValue
	err   error
}

// Head selects the first element of matched collection.
func (seq *SeqDB) Head(v Entity) error {
	if seq.at == -1 {
		seq.at++
	}
	item, err := unmarshal(seq.items[seq.at])
	if err != nil {
		return err
	}
	return dynamodbattribute.UnmarshalMap(item, v)
}

// Tail selects the all elements except the first one
func (seq *SeqDB) Tail() bool {
	seq.at++
	return seq.err == nil && seq.at < len(seq.items)
}

// Error indicates if any error appears during I/O
func (seq *SeqDB) Error() error {
	return seq.err
}

// Match applies a pattern matching to elements in the table
func (dynamo DB) Match(key Entity) Seq {
	gen, err := marshal(dynamodbattribute.MarshalMap(key))
	if err != nil {
		return &SeqDB{-1, nil, err}
	}

	req := &dynamodb.QueryInput{
		KeyConditionExpression:    aws.String("prefix = :prefix"),
		ExpressionAttributeValues: exprOf(gen),
		TableName:                 dynamo.table,
	}
	val, err := dynamo.db.Query(req)
	if err != nil {
		return &SeqDB{-1, nil, err}
	}

	return &SeqDB{-1, val.Items, nil}
}

//-----------------------------------------------------------------------------
//
// internal helpers
//
//-----------------------------------------------------------------------------

//
func marshal(gen map[string]*dynamodb.AttributeValue, err error) (map[string]*dynamodb.AttributeValue, error) {
	if err != nil {
		return nil, err
	}

	iri := ParseIRI(aws.StringValue(gen["id"].S))
	gen["prefix"] = &dynamodb.AttributeValue{S: aws.String(iri.Prefix)}

	if iri.Suffix != "" {
		gen["suffix"] = &dynamodb.AttributeValue{S: aws.String(iri.Suffix)}
	}

	delete(gen, "id")
	return gen, nil
}

//
func unmarshal(ddb map[string]*dynamodb.AttributeValue) (map[string]*dynamodb.AttributeValue, error) {
	prefix, isPrefix := ddb["prefix"]
	suffix, isSuffix := ddb["suffix"]
	if !isPrefix || !isSuffix {
		return nil, errors.New("Invalid DDB schema")
	}

	iri := IRI{aws.StringValue(prefix.S), aws.StringValue(suffix.S)}
	ddb["id"] = &dynamodb.AttributeValue{S: aws.String(iri.Path())}

	delete(ddb, "prefix")
	delete(ddb, "suffix")
	return ddb, nil
}

//
func keyOnly(gen map[string]*dynamodb.AttributeValue) map[string]*dynamodb.AttributeValue {
	key := map[string]*dynamodb.AttributeValue{}
	key["prefix"] = gen["prefix"]
	key["suffix"] = gen["suffix"]

	return key
}

//
func exprOf(gen map[string]*dynamodb.AttributeValue) (val map[string]*dynamodb.AttributeValue) {
	val = map[string]*dynamodb.AttributeValue{}
	for k, v := range gen {
		val[":"+k] = v
	}

	return
}
