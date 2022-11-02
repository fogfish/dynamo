//
// Copyright (C) 2022 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

//
// The file declares key/value interface for s3
//

package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fogfish/dynamo"
)

// ds3 is a S3 client
type Storage[T dynamo.Thing] struct {
	Service   dynamo.S3
	Bucket    *string
	Codec     *Codec[T]
	Schema    *Schema[T]
	undefined T
}

//-----------------------------------------------------------------------------
//
// Key Value
//
//-----------------------------------------------------------------------------

// Get item from storage
func (db *Storage[T]) Get(ctx context.Context, key T) (T, error) {
	req := &s3.GetObjectInput{
		Bucket: db.Bucket,
		Key:    aws.String(db.Codec.EncodeKey(key)),
	}

	val, err := db.Service.GetObject(ctx, req)
	if err != nil {
		switch {
		case recoverNoSuchKey(err):
			return db.undefined, errNotFound(err, key)
		default:
			return db.undefined, errServiceIO(err)
		}
	}

	var entity T
	err = json.NewDecoder(val.Body).Decode(&entity)
	if err != nil {
		return db.undefined, errInvalidEntity(err)
	}

	return entity, nil
}

// Put writes entity
func (db *Storage[T]) Put(ctx context.Context, entity T, config ...dynamo.Constraint[T]) error {
	gen, err := json.Marshal(entity)
	if err != nil {
		return errInvalidEntity(err)
	}

	req := &s3.PutObjectInput{
		Bucket: db.Bucket,
		Key:    aws.String(db.Codec.EncodeKey(entity)),
		Body:   bytes.NewReader(gen),
	}

	_, err = db.Service.PutObject(ctx, req)
	if err != nil {
		return errServiceIO(err)
	}

	return nil
}

// Remove discards the entity from the table
func (db *Storage[T]) Remove(ctx context.Context, key T, config ...dynamo.Constraint[T]) error {
	req := &s3.DeleteObjectInput{
		Bucket: db.Bucket,
		Key:    aws.String(db.Codec.EncodeKey(key)),
	}

	_, err := db.Service.DeleteObject(ctx, req)
	if err != nil {
		return errServiceIO(err)
	}

	return nil
}

// Update applies a partial patch to entity and returns new values
func (db *Storage[T]) Update(ctx context.Context, entity T, config ...dynamo.Constraint[T]) (T, error) {
	req := &s3.GetObjectInput{
		Bucket: db.Bucket,
		Key:    aws.String(db.Codec.EncodeKey(entity)),
	}

	val, err := db.Service.GetObject(ctx, req)
	if err != nil {
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			err := db.Put(ctx, entity)
			if err != nil {
				return db.undefined, err
			}
			return entity, nil
		}

		return db.undefined, errServiceIO(err)
	}

	var existing T
	err = json.NewDecoder(val.Body).Decode(&existing)
	if err != nil {
		return db.undefined, errInvalidEntity(err)
	}

	updated := db.Schema.Merge(entity, existing)

	err = db.Put(ctx, updated)
	if err != nil {
		return db.undefined, err
	}

	return updated, nil
}

// Match applies a pattern matching to elements in the bucket
func (db *Storage[T]) Match(ctx context.Context, key T) dynamo.Seq[T] {
	req := &s3.ListObjectsV2Input{
		Bucket:  db.Bucket,
		MaxKeys: 1000,
		Prefix:  aws.String(db.Codec.EncodeKey(key)),
	}

	return newSeq(ctx, db, req, nil)
}
