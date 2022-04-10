package ddb

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/fogfish/dynamo"
)

//
type cursor struct{ hashKey, sortKey string }

func (c cursor) HashKey() string { return c.hashKey }
func (c cursor) SortKey() string { return c.sortKey }

// slice active page, loaded into memory
type slice[T dynamo.ThingV2] struct {
	db   *ddb[T]
	head int
	heap []map[string]*dynamodb.AttributeValue
}

func newSlice[T dynamo.ThingV2](db *ddb[T], heap []map[string]*dynamodb.AttributeValue) *slice[T] {
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
	return nil, fmt.Errorf("End of Stream")
}

func (slice *slice[T]) Tail() bool {
	slice.head++
	return slice.head < len(slice.heap)
}

//
// seq is an iterator over matched results
type seq[T dynamo.ThingV2] struct {
	ctx    context.Context
	db     *ddb[T]
	q      *dynamodb.QueryInput
	slice  *slice[T]
	stream bool
	err    error
}

func newSeq[T dynamo.ThingV2](
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
		return fmt.Errorf("End of Stream")
	}

	return seq.seed()
}

func (seq *seq[T]) seed() error {
	if seq.slice != nil && seq.q.ExclusiveStartKey == nil {
		return fmt.Errorf("End of Stream")
	}

	val, err := seq.db.dynamo.QueryWithContext(seq.ctx, seq.q)
	if err != nil {
		seq.err = err
		return err
	}

	if *val.Count == 0 {
		return fmt.Errorf("End of Stream")
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
func (seq *seq[T]) Cursor() dynamo.ThingV2 {
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

		return &cursor{hashKey: hkey, sortKey: skey}
	}

	return &cursor{}
}

// Error indicates if any error appears during I/O
func (seq *seq[T]) Error() error {
	return seq.err
}

// Limit sequence size to N elements, fetch a page of sequence
func (seq *seq[T]) Limit(n int64) dynamo.SeqV2[T] {
	seq.q.Limit = aws.Int64(n)
	seq.stream = false
	return seq
}

// Continue limited sequence from the cursor
func (seq *seq[T]) Continue(key dynamo.ThingV2) dynamo.SeqV2[T] {
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
func (seq *seq[T]) Reverse() dynamo.SeqV2[T] {
	seq.q.ScanIndexForward = aws.Bool(false)
	return seq
}
