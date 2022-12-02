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
)

// Get item from storage
func (db *Storage[T]) Get(ctx context.Context, key T) (T, error) {
	req := &s3.GetObjectInput{
		Bucket: db.bucket,
		Key:    aws.String(db.codec.EncodeKey(key)),
	}

	val, err := db.service.GetObject(ctx, req)
	if err != nil {
		switch {
		case recoverNoSuchKey(err):
			return db.undefined, errNotFound(err, key)
		default:
			return db.undefined, errServiceIO.New(err)
		}
	}

	var entity T
	err = json.NewDecoder(val.Body).Decode(&entity)
	if err != nil {
		return db.undefined, errInvalidEntity.New(err)
	}

	return entity, nil
}
