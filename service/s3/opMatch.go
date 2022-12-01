package s3

import (
	"context"
	"encoding/json"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/fogfish/dynamo/v2"
)

func (db *Storage[T]) Match(ctx context.Context, key T, opts ...interface{ MatchOpt() }) ([]T, error) {
	req := db.reqListObjects(key, opts...)
	val, err := db.service.ListObjectsV2(context.Background(), req)
	if err != nil {
		return nil, errServiceIO.New(err)
	}

	seq := make([]T, val.KeyCount)
	for i := 0; i < int(val.KeyCount); i++ {
		req := &s3.GetObjectInput{
			Bucket: db.bucket,
			Key:    val.Contents[i].Key,
		}
		val, err := db.service.GetObject(ctx, req)
		if err != nil {
			return nil, errServiceIO.New(err)
		}

		var head T
		err = json.NewDecoder(val.Body).Decode(&head)
		if err != nil {
			return nil, errInvalidEntity.New(err)
		}

		seq[i] = head
	}

	return seq, nil
}

func (db *Storage[T]) reqListObjects(key T, opts ...interface{ MatchOpt() }) *s3.ListObjectsV2Input {
	var (
		limit  int32   = 1000
		cursor *string = nil
	)
	for _, opt := range opts {
		switch v := opt.(type) {
		case dynamo.Limit:
			limit = int32(v)
		case dynamo.Thing:
			cursor = aws.String(db.codec.EncodeKey(v))
		}
	}

	return &s3.ListObjectsV2Input{
		Bucket:     db.bucket,
		MaxKeys:    limit,
		Prefix:     aws.String(db.codec.EncodeKey(key)),
		StartAfter: cursor,
	}
}
