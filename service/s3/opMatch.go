//
// Copyright (C) 2022 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

package s3

import (
	"context"
	"encoding/json"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/fogfish/curie/v2"
	"github.com/fogfish/dynamo/v3"
)

func (db *Storage[T]) MatchKey(ctx context.Context, key dynamo.Thing, opts ...interface{ MatcherOpt(T) }) ([]T, interface{ MatcherOpt(T) }, error) {
	req := db.reqListObjects(key, opts)
	return db.match(ctx, req)
}

func (db *Storage[T]) Match(ctx context.Context, key T, opts ...interface{ MatcherOpt(T) }) ([]T, interface{ MatcherOpt(T) }, error) {
	req := db.reqListObjects(key, opts)
	return db.match(ctx, req)
}

func (db *Storage[T]) match(ctx context.Context, req *s3.ListObjectsV2Input) ([]T, interface{ MatcherOpt(T) }, error) {
	val, err := db.service.ListObjectsV2(context.Background(), req)
	if err != nil {
		return nil, nil, errServiceIO.New(err)
	}

	seq := make([]T, aws.ToInt32(val.KeyCount))
	for i := 0; i < int(aws.ToInt32(val.KeyCount)); i++ {
		req := &s3.GetObjectInput{
			Bucket: aws.String(db.bucket),
			Key:    val.Contents[i].Key,
		}
		val, err := db.service.GetObject(ctx, req)
		if err != nil {
			return nil, nil, errServiceIO.New(err)
		}

		var head T
		err = json.NewDecoder(val.Body).Decode(&head)
		if err != nil {
			return nil, nil, errInvalidEntity.New(err)
		}

		seq[i] = head
	}

	return seq, lastKeyToCursor[T](val), nil
}

func (db *Storage[T]) reqListObjects(key dynamo.Thing, opts []interface{ MatcherOpt(T) }) *s3.ListObjectsV2Input {
	var (
		limit  int32   = 1000
		cursor *string = nil
	)
	for _, opt := range opts {
		switch v := opt.(type) {
		case interface{ Limit() int32 }:
			limit = v.Limit()
		case dynamo.Thing:
			cursor = aws.String(db.codec.EncodeKey(v))
		}
	}

	return &s3.ListObjectsV2Input{
		Bucket:     aws.String(db.bucket),
		MaxKeys:    aws.Int32(limit),
		Prefix:     aws.String(db.codec.EncodeKey(key)),
		StartAfter: cursor,
	}
}

type cursor struct{ hashKey, sortKey string }

func (c cursor) HashKey() curie.IRI { return curie.IRI(c.hashKey) }
func (c cursor) SortKey() curie.IRI { return curie.IRI(c.sortKey) }

func lastKeyToCursor[T dynamo.Thing](val *s3.ListObjectsV2Output) interface{ MatcherOpt(T) } {
	count := aws.ToInt32(val.KeyCount)

	if count == 0 || val.NextContinuationToken == nil {
		return nil
	}

	return dynamo.Cursor[T](&cursor{hashKey: *val.Contents[count-1].Key})
}
