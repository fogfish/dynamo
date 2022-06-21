//
// Copyright (C) 2022 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

package keyval

import (
	"fmt"

	"github.com/fogfish/dynamo"
	"github.com/fogfish/dynamo/internal/ddb"
	"github.com/fogfish/dynamo/internal/s3"
)

/*

New creates a key/val client to access Things at AWS storage service.
The connection URI controls the access parameters.

Supported scheme:
  s3:///my-bucket
  ddb:///my-table/my-index?prefix=hashkey&suffix=sortkey
*/
func New[T dynamo.Thing](opts ...dynamo.Option) (dynamo.KeyVal[T], error) {
	cfg, err := dynamo.NewConfig(opts...)
	if err != nil {
		return nil, err
	}

	creator, err := factory[T](cfg)
	if err != nil {
		return nil, err
	}

	return creator(cfg), nil
}

/*

Must is a helper function to ensure KeyVal interface is valid and there was no
error when calling a New function.

This helper is intended to be used in variable initialization to load the
interface and configuration at startup. Such as:

  var io = dynamo.Must(dynamo.New())
*/
func Must[T dynamo.Thing](kv dynamo.KeyVal[T], err error) dynamo.KeyVal[T] {
	if err != nil {
		panic(err)
	}
	return kv
}

/*

ReadOnly establishes read-only connection with AWS Storage service.
*/
func ReadOnly[T dynamo.Thing](opts ...dynamo.Option) (dynamo.KeyValReader[T], error) {
	return New[T](opts...)
}

/*

creator is a factory function
*/
type creator[T dynamo.Thing] func(*dynamo.Config) dynamo.KeyVal[T]

/*

parses connector url
*/
func factory[T dynamo.Thing](cfg *dynamo.Config) (creator[T], error) {
	switch {
	case cfg.URI == nil:
		return nil, fmt.Errorf("undefined storage endpoint")
	case len(cfg.URI.Path) < 2:
		return nil, fmt.Errorf("invalid storage endpoint, missing storage name: %s", cfg.URI.String())
	case cfg.URI.Scheme == "s3":
		return s3.New[T], nil
	case cfg.URI.Scheme == "ddb":
		return ddb.New[T], nil
	default:
		return nil, fmt.Errorf("unsupported storage schema: %s", cfg.URI.String())
	}
}
