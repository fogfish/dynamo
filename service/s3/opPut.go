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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Put writes entity
func (db *Storage[T]) Put(ctx context.Context, entity T, config ...interface{ Constraint(T) }) error {
	gen, err := json.Marshal(entity)
	if err != nil {
		return errInvalidEntity.New(err)
	}

	req := &s3.PutObjectInput{
		Bucket: db.bucket,
		Key:    aws.String(db.codec.EncodeKey(entity)),
		Body:   bytes.NewReader(gen),
	}

	_, err = db.service.PutObject(ctx, req)
	if err != nil {
		return errServiceIO.New(err)
	}

	return nil
}
