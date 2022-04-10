package s3

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/fogfish/dynamo"
)

//
type cursor struct{ hashKey, sortKey string }

func (c cursor) HashKey() string { return c.hashKey }
func (c cursor) SortKey() string { return c.sortKey }

// // s3Gen is type alias for generic representation
// type s3Gen struct {
// 	ctx context.Context
// 	s3  *ds3
// 	key *string
// }

// // ID lifts generic representation to its Identity
// func (gen s3Gen) ID() (string, string) {
// 	if gen.key == nil {
// 		return "", ""
// 	}

// 	seq := strings.Split(*gen.key, "/_/")
// 	if len(seq) == 1 {
// 		return seq[0], ""
// 	}

// 	return seq[0], seq[1]
// }

// // Lifts generic representation to Thing
// func (gen s3Gen) To(thing Thing) error {
// 	req := &s3.GetObjectInput{
// 		Bucket: gen.s3.bucket,
// 		Key:    gen.key,
// 	}
// 	val, err := gen.s3.db.GetObjectWithContext(gen.ctx, req)
// 	if err != nil {
// 		return err
// 	}

// 	return json.NewDecoder(val.Body).Decode(thing)
// }

// seq is an iterator over matched results
type seq[T dynamo.ThingV2] struct {
	ctx    context.Context
	db     *ds3[T]
	q      *s3.ListObjectsV2Input
	at     int
	items  []*string
	stream bool
	err    error
}

func newSeq[T dynamo.ThingV2](
	ctx context.Context,
	db *ds3[T],
	q *s3.ListObjectsV2Input,
	err error,
) *seq[T] {
	return &seq[T]{
		ctx:    ctx,
		db:     db,
		q:      q,
		at:     0,
		items:  nil,
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
	if seq.items != nil && seq.q.StartAfter == nil {
		return fmt.Errorf("End of Stream")
	}

	val, err := seq.db.s3.ListObjectsV2WithContext(seq.ctx, seq.q)
	if err != nil {
		seq.err = err
		return err
	}

	if *val.KeyCount == 0 {
		return fmt.Errorf("End of Stream")
	}

	items := make([]*string, 0)
	for _, x := range val.Contents {
		items = append(items, x.Key)
	}

	seq.at = 0
	seq.items = items
	if len(items) > 0 && val.NextContinuationToken != nil {
		seq.q.StartAfter = items[len(items)-1]
	}
	return nil
}

// FMap transforms sequence
func (seq *seq[T]) FMap(f func(*T) error) error {
	for seq.Tail() {
		head, err := seq.Head()
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
	if seq.items == nil {
		if err := seq.seed(); err != nil {
			return nil, err
		}
	}

	req := &s3.GetObjectInput{
		Bucket: seq.db.bucket,
		Key:    seq.items[seq.at],
	}
	val, err := seq.db.s3.GetObjectWithContext(seq.ctx, req)
	if err != nil {
		return nil, err
	}

	var head T
	err = json.NewDecoder(val.Body).Decode(&head)
	if err != nil {
		return nil, err
	}

	return &head, nil
}

// Tail selects the all elements except the first one
func (seq *seq[T]) Tail() bool {
	seq.at++

	switch {
	case seq.err != nil:
		return false
	case seq.items == nil:
		err := seq.seed()
		return err == nil
	case seq.err == nil && seq.at >= len(seq.items):
		err := seq.maybeSeed()
		return err == nil
	default:
		return true
	}
}

// Cursor is the global position in the sequence
func (seq *seq[T]) Cursor() dynamo.ThingV2 {
	if seq.q.StartAfter != nil {
		seq := strings.Split(*seq.q.StartAfter, "/_/")
		if len(seq) == 1 {
			return &cursor{hashKey: seq[0]}
		}
		return &cursor{hashKey: seq[0], sortKey: seq[1]}
	}
	return &cursor{}
}

// Error indicates if any error appears during I/O
func (seq *seq[T]) Error() error {
	return seq.err
}

// Limit sequence to N elements
func (seq *seq[T]) Limit(n int64) dynamo.SeqV2[T] {
	seq.q.MaxKeys = aws.Int64(n)
	seq.stream = false
	return seq
}

// Continue limited sequence from the cursor
func (seq *seq[T]) Continue(key dynamo.ThingV2) dynamo.SeqV2[T] {
	prefix := key.HashKey()
	suffix := key.SortKey()

	if prefix != "" {
		if suffix == "" {
			seq.q.StartAfter = aws.String(prefix)
		} else {
			seq.q.StartAfter = aws.String(prefix + "/_/" + suffix)
		}
	}
	return seq
}

// Reverse order of sequence
func (seq *seq[T]) Reverse() dynamo.SeqV2[T] {
	return seq
}
