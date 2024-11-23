//
// Copyright (C) 2019 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

package ddb

import (
	"github.com/fogfish/dynamo/v3"
	"github.com/fogfish/opts"
)

// Storage type
type Storage[T dynamo.Thing] struct {
	Options
	codec     *codec[T]
	schema    *schema[T]
	undefined T
}

func Must[T dynamo.Thing](keyval *Storage[T], err error) *Storage[T] {
	if err != nil {
		panic(err)
	}

	return keyval
}

// New creates instance of DynamoDB api
func New[T dynamo.Thing](opt ...Option) (*Storage[T], error) {
	conf := optsDefault()
	if err := opts.Apply(&conf, opt); err != nil {
		return nil, err
	}

	if conf.service == nil {
		if err := optsDefaultDDB(&conf); err != nil {
			return nil, err
		}
	}

	return &Storage[T]{
		Options: conf,
		codec:   newCodec[T](&conf),
		schema:  newSchema[T](conf.useStrictType),
	}, conf.checkRequired()
}
