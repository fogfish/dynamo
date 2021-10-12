//
// Copyright (C) 2019 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

//
// Implementation of interfaces to DynamoDB
//

package dynamo

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

// ddbConfig of DynamoDB
type ddbConfig struct {
	pkPrefix string
	skSuffix string
}

// ddb is a DynamoDB client
type ddb struct {
	ddbConfig
	io    *session.Session
	db    dynamodbiface.DynamoDBAPI
	table *string
	index *string
}

func newDynamo(io *session.Session, spec *dbURL) KeyVal {
	db := &ddb{io: io, db: dynamodb.New(io)}

	// config table name and index name
	seq := spec.segments(2)
	db.table = seq[0]
	db.index = seq[1]

	// config mapping of Indentity to table attributes
	db.ddbConfig = ddbConfig{
		pkPrefix: spec.query("prefix", "prefix"),
		skSuffix: spec.query("suffix", "suffix"),
	}

	return db
}

// Mock dynamoDB I/O channel
func (dynamo *ddb) Mock(db dynamodbiface.DynamoDBAPI) {
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
func (dynamo *ddb) Get(ctx context.Context, entity Thing) (err error) {
	gen, err := marshal(dynamo.ddbConfig, entity)
	if err != nil {
		return
	}

	req := &dynamodb.GetItemInput{
		Key:       keyOnly(dynamo.ddbConfig, gen),
		TableName: dynamo.table,
	}
	val, err := dynamo.db.GetItemWithContext(ctx, req)
	if err != nil {
		return
	}

	if val.Item == nil {
		hkey, skey := entity.Identity()
		err = NotFound{HashKey: hkey, SortKey: skey}
		return
	}

	err = dynamo.unmarshalThing(val.Item, entity)
	return
}

// Put writes entity
func (dynamo *ddb) Put(ctx context.Context, entity Thing, config ...Constrain) (err error) {
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

	_, err = dynamo.db.PutItemWithContext(ctx, req)
	if err != nil {
		switch v := err.(type) {
		case awserr.Error:
			if v.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
				hkey, skey := entity.Identity()
				err = PreConditionFailed{HashKey: hkey, SortKey: skey}
			}
			return
		default:
			return
		}
	}

	return
}

// Remove discards the entity from the table
func (dynamo *ddb) Remove(ctx context.Context, entity Thing, config ...Constrain) (err error) {
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

	_, err = dynamo.db.DeleteItemWithContext(ctx, req)
	if err != nil {
		switch v := err.(type) {
		case awserr.Error:
			if v.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
				hkey, skey := entity.Identity()
				err = PreConditionFailed{HashKey: hkey, SortKey: skey}
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
func (dynamo *ddb) Update(ctx context.Context, entity Thing, config ...Constrain) (err error) {
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

	val, err := dynamo.db.UpdateItemWithContext(ctx, req)
	if err != nil {
		switch v := err.(type) {
		case awserr.Error:
			if v.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
				hkey, skey := entity.Identity()
				err = PreConditionFailed{HashKey: hkey, SortKey: skey}
			}
			return
		default:
			return
		}
	}

	err = dynamo.unmarshalThing(val.Attributes, entity)
	return
}

func (dynamo *ddb) unmarshalThing(gen map[string]*dynamodb.AttributeValue, entity Thing) error {
	item, err := unmarshal(dynamo.ddbConfig, gen)
	if err != nil {
		return err
	}

	return dynamodbattribute.UnmarshalMap(item, &entity)
}

//-----------------------------------------------------------------------------
//
// Pattern Match
//
//-----------------------------------------------------------------------------

// Match applies a pattern matching to elements in the table
func (dynamo *ddb) Match(ctx context.Context, key Thing) Seq {
	gen, err := marshalEntity(dynamo.ddbConfig, key)
	if err != nil {
		return mkDbSeq(nil, nil, nil, err)
	}

	expr := dynamo.pkPrefix + " = :" + dynamo.pkPrefix
	suffix, isSuffix := gen[dynamo.skSuffix]
	if isSuffix && suffix.S != nil {
		expr = expr + " and begins_with(" + dynamo.skSuffix + ", :" + dynamo.skSuffix + ")"
	}

	q := &dynamodb.QueryInput{
		KeyConditionExpression:    aws.String(expr),
		ExpressionAttributeValues: exprOf(gen),
		TableName:                 dynamo.table,
		IndexName:                 dynamo.index,
	}

	return mkDbSeq(ctx, dynamo, q, err)
}

// dbGen is type alias for generic representation
type dbGen struct {
	ddb *ddb
	val map[string]*dynamodb.AttributeValue
}

// ID lifts generic representation to its Identity
func (gen *dbGen) ID() (hkey string, skey string) {
	prefix, isPrefix := gen.val[gen.ddb.pkPrefix]
	if isPrefix && prefix.S != nil {
		hkey = aws.StringValue(prefix.S)
	}

	suffix, isSuffix := gen.val[gen.ddb.skSuffix]
	if isSuffix && suffix.S != nil {
		skey = aws.StringValue(suffix.S)
	}

	return
}

// To lifts generic representation to Thing
func (gen *dbGen) To(thing Thing) error {
	return gen.ddb.unmarshalThing(gen.val, thing)
}

// dbSlice active page
type dbSlice struct {
	ddb  *ddb
	head int
	heap []map[string]*dynamodb.AttributeValue
}

func mkDbSlice(ddb *ddb, heap []map[string]*dynamodb.AttributeValue) *dbSlice {
	return &dbSlice{
		ddb:  ddb,
		head: 0,
		heap: heap,
	}
}

func (slice *dbSlice) Head() *dbGen {
	if slice.head < len(slice.heap) {
		return &dbGen{ddb: slice.ddb, val: slice.heap[slice.head]}
	}
	return nil
}

func (slice *dbSlice) Tail() bool {
	slice.head++
	return slice.head < len(slice.heap)
}

// dbSeq is an iterator over matched results
type dbSeq struct {
	ctx    context.Context
	ddb    *ddb
	q      *dynamodb.QueryInput
	slice  *dbSlice
	stream bool
	err    error
}

func mkDbSeq(ctx context.Context, ddb *ddb, q *dynamodb.QueryInput, err error) *dbSeq {
	return &dbSeq{
		ctx:    ctx,
		ddb:    ddb,
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

	val, err := seq.ddb.db.QueryWithContext(seq.ctx, seq.q)
	if err != nil {
		seq.err = err
		return err
	}

	if *val.Count == 0 {
		return fmt.Errorf("End of Stream")
	}

	seq.slice = mkDbSlice(seq.ddb, val.Items)
	seq.q.ExclusiveStartKey = val.LastEvaluatedKey

	return nil
}

// FMap transforms sequence
func (seq *dbSeq) FMap(f func(Gen) error) error {
	for seq.Tail() {
		if err := f(seq.slice.Head()); err != nil {
			return err
		}
	}
	return seq.err
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
func (seq *dbSeq) Cursor() (string, string) {
	// Note: q.ExclusiveStartKey is set by sequence seeding
	if seq.q.ExclusiveStartKey != nil {
		var hkey, skey string
		val := seq.q.ExclusiveStartKey
		prefix, isPrefix := val[seq.ddb.pkPrefix]
		if isPrefix && prefix.S != nil {
			hkey = aws.StringValue(prefix.S)
		}

		suffix, isSuffix := val[seq.ddb.skSuffix]
		if isSuffix && suffix.S != nil {
			skey = aws.StringValue(suffix.S)
		}

		return hkey, skey
	}

	return "", ""
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
func (seq *dbSeq) Continue(prefix, suffix string) Seq {
	if prefix != "" {
		key := map[string]*dynamodb.AttributeValue{}

		key[seq.ddb.pkPrefix] = &dynamodb.AttributeValue{S: aws.String(prefix)}
		if suffix != "" {
			key[seq.ddb.skSuffix] = &dynamodb.AttributeValue{S: aws.String(suffix)}
		} else {
			key[seq.ddb.skSuffix] = &dynamodb.AttributeValue{S: aws.String("_")}
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

//-----------------------------------------------------------------------------
//
// internal helpers
//
//-----------------------------------------------------------------------------

//
func marshal(cfg ddbConfig, entity Thing) (map[string]*dynamodb.AttributeValue, error) {
	gen, err := marshalEntity(cfg, entity)
	if err != nil {
		return nil, err
	}

	suffix, isSuffix := gen[cfg.skSuffix]
	if !isSuffix || suffix.S == nil {
		gen[cfg.skSuffix] = &dynamodb.AttributeValue{S: aws.String("_")}
	}

	return gen, nil
}

//
func marshalEntity(cfg ddbConfig, entity Thing) (map[string]*dynamodb.AttributeValue, error) {
	gen, err := dynamodbattribute.MarshalMap(entity)
	if err != nil {
		return nil, err
	}

	return gen, nil
}

//
func unmarshal(cfg ddbConfig, ddb map[string]*dynamodb.AttributeValue) (map[string]*dynamodb.AttributeValue, error) {
	_, isPrefix := ddb[cfg.pkPrefix]
	_, isSuffix := ddb[cfg.skSuffix]
	if !isPrefix || !isSuffix {
		return nil, errors.New("Invalid DDB schema")
	}

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
