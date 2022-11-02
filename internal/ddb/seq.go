//
// Copyright (C) 2022 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

//
// The file declares sequence type (traversal) for dynamodb
//

package ddb

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/fogfish/curie"
	"github.com/fogfish/dynamo"
)

//
type cursor struct{ hashKey, sortKey string }

func (c cursor) HashKey() curie.IRI { return curie.IRI(c.hashKey) }
func (c cursor) SortKey() curie.IRI { return curie.IRI(c.sortKey) }

// slice active page, loaded into memory
type slice[T dynamo.Thing] struct {
	db   *Storage[T]
	head int
	heap []map[string]types.AttributeValue
}

func newSlice[T dynamo.Thing](db *Storage[T], heap []map[string]types.AttributeValue) *slice[T] {
	return &slice[T]{
		db:   db,
		head: 0,
		heap: heap,
	}
}

func (slice *slice[T]) Head() (T, error) {
	if slice.head < len(slice.heap) {
		head, err := slice.db.Codec.Decode(slice.heap[slice.head])
		if err != nil {
			return slice.db.undefined, errInvalidEntity(err)
		}

		return head, nil
	}
	return slice.db.undefined, errEndOfStream()
}

func (slice *slice[T]) Tail() bool {
	slice.head++
	return slice.head < len(slice.heap)
}

//
// seq is an iterator over matched results
type seq[T dynamo.Thing] struct {
	ctx    context.Context
	db     *Storage[T]
	q      *dynamodb.QueryInput
	slice  *slice[T]
	stream bool
	err    error
}

func newSeq[T dynamo.Thing](
	ctx context.Context,
	ddb *Storage[T],
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
		return errEndOfStream()
	}

	return seq.seed()
}

func (seq *seq[T]) seed() error {
	if seq.slice != nil && seq.q.ExclusiveStartKey == nil {
		return errEndOfStream()
	}

	val, err := seq.db.Service.Query(seq.ctx, seq.q)
	if err != nil {
		seq.err = err
		return errServiceIO(err)
	}

	if val.Count == 0 {
		return errEndOfStream()
	}

	seq.slice = newSlice(seq.db, val.Items)
	seq.q.ExclusiveStartKey = val.LastEvaluatedKey

	return nil
}

// FMap transforms sequence
func (seq *seq[T]) FMap(f func(T) error) error {
	for seq.Tail() {
		head, err := seq.slice.Head()
		if err != nil {
			return err
		}

		if err := f(head); err != nil {
			return errProcessEntity(err, head)
		}
	}
	return seq.err
}

// Head selects the first element of matched collection.
func (seq *seq[T]) Head() (T, error) {
	if seq.slice == nil {
		if err := seq.seed(); err != nil {
			return seq.db.undefined,
				fmt.Errorf("can't seed head of stream: %w", err)
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
		prefix, isPrefix := val[seq.db.Codec.pkPrefix]
		if isPrefix {
			switch v := prefix.(type) {
			case *types.AttributeValueMemberS:
				hkey = v.Value
			}
		}

		suffix, isSuffix := val[seq.db.Codec.skSuffix]
		if isSuffix {
			switch v := suffix.(type) {
			case *types.AttributeValueMemberS:
				skey = v.Value
			}
		}

		return &cursor{hashKey: hkey, sortKey: skey}
	}

	return &cursor{}
}

// Error indicates if any error appears during I/O
func (seq *seq[T]) Error() error {
	return seq.err
}

// Limit sequence size to N elements, fetch a page of sequence
func (seq *seq[T]) Limit(n int) dynamo.Seq[T] {
	seq.q.Limit = aws.Int32(int32(n))
	seq.stream = false
	return seq
}

// Continue limited sequence from the cursor
func (seq *seq[T]) Continue(key dynamo.Thing) dynamo.Seq[T] {
	prefix := key.HashKey()
	suffix := key.SortKey()

	if prefix != "" {
		key := map[string]types.AttributeValue{}

		key[seq.db.Codec.pkPrefix] = &types.AttributeValueMemberS{Value: string(prefix)}
		if suffix != "" {
			key[seq.db.Codec.skSuffix] = &types.AttributeValueMemberS{Value: string(suffix)}
		} else {
			key[seq.db.Codec.skSuffix] = &types.AttributeValueMemberS{Value: "_"}
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
