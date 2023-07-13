//
// Copyright (C) 2022 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

package s3

import (
	"errors"
	"fmt"

	"github.com/fogfish/dynamo/v2"
	"github.com/fogfish/faults"
)

const (
	errUndefinedBucket = faults.Type("undefined S3 bucket")
	errServiceIO       = faults.Type("service i/o failed")
	errInvalidEntity   = faults.Type("invalid entity")
)

// NotFound is an error to handle unknown elements
func errNotFound(err error, thing dynamo.Thing) error {
	return &notFound{Thing: thing, err: err}
}

type notFound struct {
	dynamo.Thing
	err error
}

func (e *notFound) Error() string {
	return fmt.Sprintf("Not Found (%s, %s): %v", e.HashKey(), e.SortKey(), e.err)
}

func (e *notFound) Unwrap() error { return e.err }

func (e *notFound) NotFound() string {
	return e.HashKey().Safe() + " " + e.SortKey().Safe()
}

// recover
func recoverNoSuchKey(err error) bool {
	var e interface{ ErrorCode() string }

	ok := errors.As(err, &e)
	return ok && e.ErrorCode() == "NoSuchKey"
}
