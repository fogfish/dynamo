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
	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// Update applies a partial patch to entity and returns new values
func (db *Storage[T]) Update(ctx context.Context, entity T, opts ...interface{ WriterOpt(T) }) (T, error) {
	req := &s3.GetObjectInput{
		Bucket: db.bucket,
		Key:    aws.String(db.codec.EncodeKey(entity)),
	}

	val, err := db.service.GetObject(ctx, req)
	if err != nil {
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			err := db.Put(ctx, entity)
			if err != nil {
				return db.undefined, err
			}
			return entity, nil
		}

		return db.undefined, errServiceIO.New(err)
	}

	var existing T
	err = json.NewDecoder(val.Body).Decode(&existing)
	if err != nil {
		return db.undefined, errInvalidEntity.New(err)
	}

	updated := db.schema.Merge(entity, existing)

	err = db.Put(ctx, updated)
	if err != nil {
		return db.undefined, err
	}

	return updated, nil
}
