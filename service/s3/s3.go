//
// Copyright (C) 2019 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

package s3

import (
	"github.com/fogfish/dynamo/v3"
	"github.com/fogfish/opts"
)

type Storage[T dynamo.Thing] struct {
	Options
	bucket    string
	codec     *codec[T]
	schema    *schema[T]
	undefined T
}

// Must constraint for api factory
func Must[T dynamo.Thing](keyval *Storage[T], err error) *Storage[T] {
	if err != nil {
		panic(err)
	}

	return keyval
}

// New creates instance of S3 api
func New[T dynamo.Thing](bucket string, opt ...Option) (*Storage[T], error) {
	conf := optsDefault()
	if err := opts.Apply(&conf, opt); err != nil {
		return nil, err
	}

	if conf.service == nil {
		if err := optsDefaultS3(&conf); err != nil {
			return nil, err
		}
	}

	return &Storage[T]{
		Options: conf,
		bucket:  bucket,
		codec:   newCodec[T](conf.prefixes),
		schema:  newSchema[T](),
	}, conf.checkRequired()
}
