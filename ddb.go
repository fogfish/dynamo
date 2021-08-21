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
	"fmt"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/fogfish/curie"
)

// ddbConfig of DynamoDB
type ddbConfig struct {
	pkPrefix string
	skSuffix string
}

// DB is a service connection handle
type DB struct {
	ddbConfig
	io    *session.Session
	db    dynamodbiface.DynamoDBAPI
	table *string
	index *string
}

func newDB(io *session.Session, spec *url.URL) *DB {
	db := &DB{io: io, db: dynamodb.New(io)}

	// config table name and index name
	seq := strings.Split(spec.Path, "/")

	if len(seq) > 1 {
		db.table = aws.String(seq[1])
	}

	if len(seq) > 2 {
		db.index = aws.String(seq[2])
	}

	// config mapping of Indentity to table attributes
	prefix := spec.Query().Get("prefix")
	if prefix == "" {
		prefix = "prefix"
	}

	suffix := spec.Query().Get("suffix")
	if suffix == "" {
		suffix = "suffix"
	}

	db.ddbConfig = ddbConfig{pkPrefix: prefix, skSuffix: suffix}

	return db
}

// Mock dynamoDB I/O channel
func (dynamo *DB) Mock(db dynamodbiface.DynamoDBAPI) {
	dynamo.db = db
	dynamo.ddbConfig = ddbConfig{
		pkPrefix: "prefix",
		skSuffix: "suffix",
	}
}

//-----------------------------------------------------------------------------
//
// Key Value
//
//-----------------------------------------------------------------------------

// Get fetches the entity identified by the key.
func (dynamo DB) Get(entity Thing) (err error) {
	gen, err := marshal(dynamo.ddbConfig, entity)
	if err != nil {
		return
	}

	req := &dynamodb.GetItemInput{
		Key:       keyOnly(dynamo.ddbConfig, gen),
		TableName: dynamo.table,
	}
	val, err := dynamo.db.GetItem(req)
	if err != nil {
		return
	}

	if val.Item == nil {
		err = NotFound{entity.Identity().String()}
		return
	}

	item, err := unmarshal(dynamo.ddbConfig, val.Item)
	if err != nil {
		return
	}

	err = dynamodbattribute.UnmarshalMap(item, &entity)
	return
}

// Put writes entity
func (dynamo DB) Put(entity Thing, config ...Constrain) (err error) {
	gen, err := marshal(dynamo.ddbConfig, entity)
	if err != nil {
		return
	}

	req := &dynamodb.PutItemInput{
		Item:      gen,
		TableName: dynamo.table,
	}
	names, values := maybeConditionExpression(&req.ConditionExpression, config)
	req.ExpressionAttributeValues = values
	req.ExpressionAttributeNames = names

	_, err = dynamo.db.PutItem(req)
	if err != nil {
		switch v := err.(type) {
		case awserr.Error:
			if v.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
				return PreConditionFailed{entity.Identity().String()}
			}
			return
		default:
			return
		}
	}

	return
}

// Remove discards the entity from the table
func (dynamo DB) Remove(entity Thing, config ...Constrain) (err error) {
	gen, err := marshal(dynamo.ddbConfig, entity)
	if err != nil {
		return
	}

	req := &dynamodb.DeleteItemInput{
		Key:       keyOnly(dynamo.ddbConfig, gen),
		TableName: dynamo.table,
	}
	names, values := maybeConditionExpression(&req.ConditionExpression, config)
	req.ExpressionAttributeValues = values
	req.ExpressionAttributeNames = names

	_, err = dynamo.db.DeleteItem(req)
	if err != nil {
		switch v := err.(type) {
		case awserr.Error:
			if v.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
				return PreConditionFailed{entity.Identity().String()}
			}
			return
		default:
			return
		}
	}

	return
}

func maybeConditionExpression(
	conditionExpression **string,
	config []Constrain,
) (
	expressionAttributeNames map[string]*string,
	expressionAttributeValues map[string]*dynamodb.AttributeValue,
) {
	if len(config) > 0 {
		expressionAttributeNames = map[string]*string{}
		expressionAttributeValues = map[string]*dynamodb.AttributeValue{}
		config[0](
			conditionExpression,
			expressionAttributeNames,
			expressionAttributeValues,
		)
		// Unfortunately empty maps are not accepted by DynamoDB
		if len(expressionAttributeNames) == 0 {
			expressionAttributeNames = nil
		}
		if len(expressionAttributeValues) == 0 {
			expressionAttributeValues = nil
		}
	}
	return
}

// Update applies a partial patch to entity and returns new values
func (dynamo DB) Update(entity Thing, config ...Constrain) (err error) {
	gen, err := marshal(dynamo.ddbConfig, entity)
	if err != nil {
		return
	}

	names := map[string]*string{}
	values := map[string]*dynamodb.AttributeValue{}
	update := make([]string, 0)
	for k, v := range gen {
		if k != dynamo.pkPrefix && k != dynamo.skSuffix && k != "id" {
			names["#"+k] = aws.String(k)
			values[":"+k] = v
			update = append(update, "#"+k+"="+":"+k)
		}
	}
	expression := aws.String("SET " + strings.Join(update, ","))

	req := &dynamodb.UpdateItemInput{
		Key:                       keyOnly(dynamo.ddbConfig, gen),
		ExpressionAttributeNames:  names,
		ExpressionAttributeValues: values,
		UpdateExpression:          expression,
		TableName:                 dynamo.table,
		ReturnValues:              aws.String("ALL_NEW"),
	}

	if len(config) > 0 {
		config[0](
			&req.ConditionExpression,
			req.ExpressionAttributeNames,
			req.ExpressionAttributeValues,
		)
	}

	val, err := dynamo.db.UpdateItem(req)
	if err != nil {
		switch v := err.(type) {
		case awserr.Error:
			if v.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
				return PreConditionFailed{entity.Identity().String()}
			}
			return
		default:
			return
		}
	}

	dynamodbattribute.UnmarshalMap(val.Attributes, &entity)
	return
}

//-----------------------------------------------------------------------------
//
// Pattern Match
//
//-----------------------------------------------------------------------------

// dbGen is type alias for generic representation
type dbGen struct {
	dynamo *DB
	val    map[string]*dynamodb.AttributeValue
}

// ID lifts generic representation to its Identity
func (gen *dbGen) ID() (*ID, error) {
	prefix, isPrefix := gen.val[gen.dynamo.pkPrefix]
	suffix, isSuffix := gen.val[gen.dynamo.skSuffix]
	if !isPrefix || !isSuffix {
		return nil, errors.New("Invalid DDB schema")
	}

	pf := aws.StringValue(prefix.S)
	sf := aws.StringValue(suffix.S)
	id := MkID(curie.New(pf).Join(sf))

	return &id, nil
}

// To lifts generic representation to Thing
func (gen *dbGen) To(thing Thing) error {
	item, err := unmarshal(gen.dynamo.ddbConfig, gen.val)
	if err != nil {
		return err
	}
	return dynamodbattribute.UnmarshalMap(item, thing)
}

// dbSlice active page
type dbSlice struct {
	dynamo *DB
	head   int
	heap   []map[string]*dynamodb.AttributeValue
}

func mkDbSlice(dynamo *DB, heap []map[string]*dynamodb.AttributeValue) *dbSlice {
	return &dbSlice{
		dynamo: dynamo,
		head:   0,
		heap:   heap,
	}
}

func (slice *dbSlice) Head() *dbGen {
	if slice.head < len(slice.heap) {
		return &dbGen{dynamo: slice.dynamo, val: slice.heap[slice.head]}
	}
	return nil
}

func (slice *dbSlice) Tail() bool {
	slice.head++
	return slice.head < len(slice.heap)
}

// dbSeq is an iterator over matched results
type dbSeq struct {
	dynamo *DB
	q      *dynamodb.QueryInput
	slice  *dbSlice
	stream bool
	err    error
}

func mkDbSeq(dynamo *DB, q *dynamodb.QueryInput, err error) *dbSeq {
	return &dbSeq{
		dynamo: dynamo,
		q:      q,
		slice:  nil,
		stream: true,
		err:    err,
	}
}

func (seq *dbSeq) maybeSeed() error {
	if !seq.stream {
		return fmt.Errorf("End of Stream")
	}

	return seq.seed()
}

func (seq *dbSeq) seed() error {
	if seq.slice != nil && seq.q.ExclusiveStartKey == nil {
		return fmt.Errorf("End of Stream")
	}

	val, err := seq.dynamo.db.Query(seq.q)
	if err != nil {
		seq.err = err
		return err
	}

	if *val.Count == 0 {
		return fmt.Errorf("End of Stream")
	}

	seq.slice = mkDbSlice(seq.dynamo, val.Items)
	seq.q.ExclusiveStartKey = val.LastEvaluatedKey

	return nil
}

// FMap transforms sequence
func (seq *dbSeq) FMap(f FMap) ([]Thing, error) {
	things := []Thing{}
	for seq.Tail() {
		thing, err := f(seq.slice.Head())
		if err != nil {
			return nil, err
		}
		things = append(things, thing)
	}
	return things, nil
}

// Head selects the first element of matched collection.
func (seq *dbSeq) Head(thing Thing) error {
	if seq.slice == nil {
		if err := seq.seed(); err != nil {
			return err
		}
	}

	return seq.slice.Head().To(thing)
}

// Tail selects the all elements except the first one
func (seq *dbSeq) Tail() bool {
	switch {
	case seq.err != nil:
		return false
	case seq.slice == nil:
		err := seq.seed()
		return err == nil
	case seq.err == nil && !seq.slice.Tail():
		err := seq.maybeSeed()
		return err == nil
	default:
		return true
	}
}

// Cursor is the global position in the sequence
func (seq *dbSeq) Cursor() *curie.IRI {
	if seq.q.ExclusiveStartKey != nil {
		val := seq.q.ExclusiveStartKey
		prefix, _ := val[seq.dynamo.pkPrefix]
		suffix, _ := val[seq.dynamo.skSuffix]
		iri := curie.New(aws.StringValue(prefix.S)).Join(aws.StringValue(suffix.S))
		return &iri
	}

	return nil
}

// Error indicates if any error appears during I/O
func (seq *dbSeq) Error() error {
	return seq.err
}

// Limit sequence size to N elements, fetch a page of sequence
func (seq *dbSeq) Limit(n int64) Seq {
	seq.q.Limit = aws.Int64(n)
	seq.stream = false
	return seq
}

// Continue limited sequence from the cursor
func (seq *dbSeq) Continue(cursor *curie.IRI) Seq {
	if cursor != nil {
		key := map[string]*dynamodb.AttributeValue{}
		key[seq.dynamo.pkPrefix] = &dynamodb.AttributeValue{S: aws.String(cursor.Prefix())}
		if cursor.Suffix() != "" {
			key[seq.dynamo.skSuffix] = &dynamodb.AttributeValue{S: aws.String(cursor.Suffix())}
		}
		seq.q.ExclusiveStartKey = key
	}
	return seq
}

// Reverse order of sequence
func (seq *dbSeq) Reverse() Seq {
	seq.q.ScanIndexForward = aws.Bool(false)
	return seq
}

// Match applies a pattern matching to elements in the table
func (dynamo DB) Match(key Thing) Seq {
	gen, err := pattern(dynamo.ddbConfig, key)
	if err != nil {
		return mkDbSeq(nil, nil, err)
	}

	q := &dynamodb.QueryInput{
		KeyConditionExpression:    aws.String(dynamo.pkPrefix + " = :" + dynamo.pkPrefix),
		ExpressionAttributeValues: exprOf(gen),
		TableName:                 dynamo.table,
		IndexName:                 dynamo.index,
	}

	fmt.Println(q)

	return mkDbSeq(&dynamo, q, err)
}

//-----------------------------------------------------------------------------
//
// internal helpers
//
//-----------------------------------------------------------------------------

//
func marshal(cfg ddbConfig, entity Thing) (map[string]*dynamodb.AttributeValue, error) {
	gen, err := dynamodbattribute.MarshalMap(entity)
	if err != nil {
		return nil, err
	}

	iri := curie.New(aws.StringValue(gen["id"].S))
	gen[cfg.pkPrefix] = &dynamodb.AttributeValue{S: aws.String(iri.Prefix())}
	if iri.Suffix() != "" {
		gen[cfg.skSuffix] = &dynamodb.AttributeValue{S: aws.String(iri.Suffix())}
	}

	delete(gen, "id")
	return gen, nil
}

//
func pattern(cfg ddbConfig, key Thing) (map[string]*dynamodb.AttributeValue, error) {
	gen, err := dynamodbattribute.MarshalMap(key)
	if err != nil {
		return nil, err
	}

	gen[cfg.pkPrefix] = &dynamodb.AttributeValue{S: gen["id"].S}
	delete(gen, "id")
	return gen, nil
}

//
func unmarshal(cfg ddbConfig, ddb map[string]*dynamodb.AttributeValue) (map[string]*dynamodb.AttributeValue, error) {
	prefix, isPrefix := ddb[cfg.pkPrefix]
	suffix, isSuffix := ddb[cfg.skSuffix]
	if !isPrefix || !isSuffix {
		return nil, errors.New("Invalid DDB schema")
	}

	iri := curie.New(aws.StringValue(prefix.S)).Join(aws.StringValue(suffix.S))
	ddb["id"] = &dynamodb.AttributeValue{S: aws.String(iri.String())}

	delete(ddb, cfg.pkPrefix)
	delete(ddb, cfg.skSuffix)
	return ddb, nil
}

//
func keyOnly(cfg ddbConfig, gen map[string]*dynamodb.AttributeValue) map[string]*dynamodb.AttributeValue {
	key := map[string]*dynamodb.AttributeValue{}
	key[cfg.pkPrefix] = gen[cfg.pkPrefix]
	key[cfg.skSuffix] = gen[cfg.skSuffix]

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
