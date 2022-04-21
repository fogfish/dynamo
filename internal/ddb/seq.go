//
// Copyright (C) 2022 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

package ddb

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/fogfish/dynamo"
	"github.com/fogfish/dynamo/internal/common"
)

// slice active page, loaded into memory
type slice[T dynamo.Thing] struct {
	db   *ddb[T]
	head int
	heap []map[string]*dynamodb.AttributeValue
}

func newSlice[T dynamo.Thing](db *ddb[T], heap []map[string]*dynamodb.AttributeValue) *slice[T] {
	return &slice[T]{
		db:   db,
		head: 0,
		heap: heap,
	}
}

// TODO: error fmt.Errorf("End of Stream")

func (slice *slice[T]) Head() (*T, error) {
	if slice.head < len(slice.heap) {
		return slice.db.codec.Decode(slice.heap[slice.head])
	}
	return nil, dynamo.EOS{}
}

func (slice *slice[T]) Tail() bool {
	slice.head++
	return slice.head < len(slice.heap)
}

//
// seq is an iterator over matched results
type seq[T dynamo.Thing] struct {
	ctx    context.Context
	db     *ddb[T]
	q      *dynamodb.QueryInput
	slice  *slice[T]
	stream bool
	err    error
}

func newSeq[T dynamo.Thing](
	ctx context.Context,
	ddb *ddb[T],
	q *dynamodb.QueryInput,
	err error,
) *seq[T] {
	return &seq[T]{
		ctx:    ctx,
		db:     ddb,
		q:      q,
		slice:  nil,
		stream: true,
		err:    err,
	}
}

func (seq *seq[T]) maybeSeed() error {
	if !seq.stream {
		return dynamo.EOS{}
	}

	return seq.seed()
}

func (seq *seq[T]) seed() error {
	if seq.slice != nil && seq.q.ExclusiveStartKey == nil {
		return dynamo.EOS{}
	}

	val, err := seq.db.dynamo.QueryWithContext(seq.ctx, seq.q)
	if err != nil {
		seq.err = err
		return err
	}

	if *val.Count == 0 {
		return dynamo.EOS{}
	}

	seq.slice = newSlice(seq.db, val.Items)
	seq.q.ExclusiveStartKey = val.LastEvaluatedKey

	return nil
}

// FMap transforms sequence
func (seq *seq[T]) FMap(f func(*T) error) error {
	for seq.Tail() {
		head, err := seq.slice.Head()
		if err != nil {
			return err
		}

		if err := f(head); err != nil {
			return err
		}
	}
	return seq.err
}

// Head selects the first element of matched collection.
func (seq *seq[T]) Head() (*T, error) {
	if seq.slice == nil {
		if err := seq.seed(); err != nil {
			return nil, err
		}
	}

	return seq.slice.Head()
}

// Tail selects the all elements except the first one
func (seq *seq[T]) Tail() bool {
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
func (seq *seq[T]) Cursor() dynamo.Thing {
	// Note: q.ExclusiveStartKey is set by sequence seeding
	if seq.q.ExclusiveStartKey != nil {
		var hkey, skey string
		val := seq.q.ExclusiveStartKey
		prefix, isPrefix := val[seq.db.codec.pkPrefix]
		if isPrefix && prefix.S != nil {
			hkey = aws.StringValue(prefix.S)
		}

		suffix, isSuffix := val[seq.db.codec.skSuffix]
		if isSuffix && suffix.S != nil {
			skey = aws.StringValue(suffix.S)
		}

		return common.Cursor(hkey, skey)
	}

	return common.Cursor("", "")
}

// Error indicates if any error appears during I/O
func (seq *seq[T]) Error() error {
	return seq.err
}

// Limit sequence size to N elements, fetch a page of sequence
func (seq *seq[T]) Limit(n int64) dynamo.Seq[T] {
	seq.q.Limit = aws.Int64(n)
	seq.stream = false
	return seq
}

// Continue limited sequence from the cursor
func (seq *seq[T]) Continue(key dynamo.Thing) dynamo.Seq[T] {
	prefix := key.HashKey()
	suffix := key.SortKey()

	if prefix != "" {
		key := map[string]*dynamodb.AttributeValue{}

		key[seq.db.codec.pkPrefix] = &dynamodb.AttributeValue{S: aws.String(prefix)}
		if suffix != "" {
			key[seq.db.codec.skSuffix] = &dynamodb.AttributeValue{S: aws.String(suffix)}
		} else {
			key[seq.db.codec.skSuffix] = &dynamodb.AttributeValue{S: aws.String("_")}
		}
		seq.q.ExclusiveStartKey = key
	}
	return seq
}

// Reverse order of sequence
func (seq *seq[T]) Reverse() dynamo.Seq[T] {
	seq.q.ScanIndexForward = aws.Bool(false)
	return seq
}
