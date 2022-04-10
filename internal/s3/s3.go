package s3

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/fogfish/dynamo"
)

// ds3 is a S3 client
type ds3[T dynamo.ThingV2] struct {
	io     *session.Session
	s3     s3iface.S3API
	codec  Codec[T]
	bucket *string
	schema *Schema[T]
}

func New[T dynamo.ThingV2](
	io *session.Session,
	spec *dynamo.URL,
) dynamo.KeyValV2[T] {
	db := &ds3[T]{io: io, s3: s3.New(io)}

	// config bucket name
	seq := spec.Segments(2)
	db.bucket = seq[0]
	db.schema = NewSchema[T]()

	//
	db.codec = Codec[T]{}

	return db
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
	val, err := db.s3.GetObjectWithContext(ctx, req)
	if err != nil {
		switch v := err.(type) {
		case awserr.Error:
			if v.Code() == s3.ErrCodeNoSuchKey {
				return nil, dynamo.NotFound{
					HashKey: key.HashKey(),
					SortKey: key.SortKey(),
				}
			}
			return nil, err
		default:
			return nil, err
		}
	}

	var entity T
	err = json.NewDecoder(val.Body).Decode(&entity)

	return &entity, err
}

// Put writes entity
func (db *ds3[T]) Put(ctx context.Context, entity T, config ...dynamo.ConstrainV2[T]) error {
	gen, err := json.Marshal(entity)
	if err != nil {
		return err
	}

	req := &s3.PutObjectInput{
		Bucket: db.bucket,
		Key:    aws.String(db.codec.EncodeKey(entity)),
		Body:   aws.ReadSeekCloser(bytes.NewReader(gen)),
	}

	_, err = db.s3.PutObjectWithContext(ctx, req)

	return err
}

// Remove discards the entity from the table
func (db *ds3[T]) Remove(ctx context.Context, key T, config ...dynamo.ConstrainV2[T]) error {
	req := &s3.DeleteObjectInput{
		Bucket: db.bucket,
		Key:    aws.String(db.codec.EncodeKey(key)),
	}

	_, err := db.s3.DeleteObjectWithContext(ctx, req)

	return err
}

// Update applies a partial patch to entity and returns new values
func (db *ds3[T]) Update(ctx context.Context, entity T, config ...dynamo.ConstrainV2[T]) (*T, error) {
	req := &s3.GetObjectInput{
		Bucket: db.bucket,
		Key:    aws.String(db.codec.EncodeKey(entity)),
	}

	val, err := db.s3.GetObjectWithContext(ctx, req)
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
func (db *ds3[T]) Match(ctx context.Context, key T) dynamo.SeqV2[T] {
	req := &s3.ListObjectsV2Input{
		Bucket:  db.bucket,
		MaxKeys: aws.Int64(1000),
		Prefix:  aws.String(db.codec.EncodeKey(key)),
	}

	return newSeq(ctx, db, req, nil)
}
