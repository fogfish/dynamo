package s3

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Remove discards the entity from the table
func (db *Storage[T]) Remove(ctx context.Context, key T, config ...interface{ Constraint(T) }) error {
	req := &s3.DeleteObjectInput{
		Bucket: db.bucket,
		Key:    aws.String(db.codec.EncodeKey(key)),
	}

	_, err := db.service.DeleteObject(ctx, req)
	if err != nil {
		return errServiceIO.New(err)
	}

	return nil
}
