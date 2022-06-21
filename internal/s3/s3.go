//
// Copyright (C) 2022 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fogfish/dynamo"
	"github.com/fogfish/dynamo/internal/common"
)

/*

S3 declares AWS API used by the library
*/
type S3 interface {
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	DeleteObject(context.Context, *s3.DeleteObjectInput, ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

// ds3 is a S3 client
type ds3[T dynamo.Thing] struct {
	// io     *session.Session
	s3     S3
	bucket *string
	codec  *Codec[T]
	schema *Schema[T]
}

func New[T dynamo.Thing](cfg *dynamo.Config) dynamo.KeyVal[T] {
	db := &ds3[T]{
		// io: cfg.Session,
		s3: s3.NewFromConfig(cfg.AWS),
	}

	seq := (*common.URL)(cfg.URI).Segments()
	db.bucket = &seq[0]
	db.schema = NewSchema[T]()

	//
	rootPath := filepath.Join(seq[1:]...)
	if rootPath != "" {
		rootPath = rootPath + "/"
	}
	db.codec = NewCodec[T](cfg.Prefixes)
	return db
}

// Mock S3 I/O channel
func (db *ds3[T]) Mock(s3 S3) {
	db.s3 = s3
}

//-----------------------------------------------------------------------------
//
// Key Value
//
//-----------------------------------------------------------------------------

// Get item from storage
func (db *ds3[T]) Get(ctx context.Context, key T) (*T, error) {
	req := &s3.GetObjectInput{
		Bucket: db.bucket,
		Key:    aws.String(db.codec.EncodeKey(key)),
	}
	val, err := db.s3.GetObject(ctx, req)
	if err != nil {
		switch err.(type) {
		case *types.NoSuchKey:
			return nil, dynamo.NotFound{Thing: key}
		default:
			return nil, err
		}
	}

	var entity T
	err = json.NewDecoder(val.Body).Decode(&entity)

	return &entity, err
}

// Put writes entity
func (db *ds3[T]) Put(ctx context.Context, entity T, config ...dynamo.Constrain[T]) error {
	gen, err := json.Marshal(entity)
	if err != nil {
		return err
	}

	req := &s3.PutObjectInput{
		Bucket: db.bucket,
		Key:    aws.String(db.codec.EncodeKey(entity)),
		Body:   bytes.NewReader(gen),
	}

	_, err = db.s3.PutObject(ctx, req)

	return err
}

// Remove discards the entity from the table
func (db *ds3[T]) Remove(ctx context.Context, key T, config ...dynamo.Constrain[T]) error {
	req := &s3.DeleteObjectInput{
		Bucket: db.bucket,
		Key:    aws.String(db.codec.EncodeKey(key)),
	}

	_, err := db.s3.DeleteObject(ctx, req)

	return err
}

// Update applies a partial patch to entity and returns new values
func (db *ds3[T]) Update(ctx context.Context, entity T, config ...dynamo.Constrain[T]) (*T, error) {
	req := &s3.GetObjectInput{
		Bucket: db.bucket,
		Key:    aws.String(db.codec.EncodeKey(entity)),
	}

	val, err := db.s3.GetObject(ctx, req)
	if err != nil {
		return nil, err
	}

	var existing T
	err = json.NewDecoder(val.Body).Decode(&existing)
	if err != nil {
		return nil, err
	}

	updated := db.schema.Merge(entity, existing)

	err = db.Put(ctx, updated)
	if err != nil {
		return nil, err
	}

	return &updated, nil
}

// Match applies a pattern matching to elements in the bucket
func (db *ds3[T]) Match(ctx context.Context, key T) dynamo.Seq[T] {
	req := &s3.ListObjectsV2Input{
		Bucket:  db.bucket,
		MaxKeys: 1000,
		Prefix:  aws.String(db.codec.EncodeKey(key)),
	}

	return newSeq(ctx, db, req, nil)
}
