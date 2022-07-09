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
	"runtime"

	"github.com/fogfish/dynamo"
)

//
func errServiceIO(err error) error {
	var name string

	if pc, _, _, ok := runtime.Caller(1); ok {
		name = runtime.FuncForPC(pc).Name()
	}

	return fmt.Errorf("[%s] service i/o failed: %w", name, err)
}

//
func errInvalidEntity(err error) error {
	var name string

	if pc, _, _, ok := runtime.Caller(1); ok {
		name = runtime.FuncForPC(pc).Name()
	}

	return fmt.Errorf("[%s] invalid entity: %w", name, err)
}

//
func errProcessEntity(err error, thing dynamo.Thing) error {
	var name string

	if pc, _, _, ok := runtime.Caller(1); ok {
		name = runtime.FuncForPC(pc).Name()
	}

	return fmt.Errorf("[%s] can't process (%s, %s) : %w", name, thing.HashKey(), thing.SortKey(), err)
}

// NotFound is an error to handle unknown elements
func errNotFound(err error, thing dynamo.Thing) error {
	var name string

	if pc, _, _, ok := runtime.Caller(1); ok {
		name = runtime.FuncForPC(pc).Name()
	}

	return &notFound{Thing: thing, ctx: name, err: err}
}

type notFound struct {
	dynamo.Thing

	ctx string
	err error
}

func (e *notFound) Error() string {
	return fmt.Sprintf("[%s] Not Found (%s, %s): %v", e.ctx, e.HashKey(), e.SortKey(), e.err)
}

func (e *notFound) Unwrap() error { return e.err }

func (e *notFound) NotFound() string {
	return e.HashKey().Safe() + " " + e.SortKey().Safe()
}

//
func errEndOfStream() error {
	var name string

	if pc, _, _, ok := runtime.Caller(1); ok {
		name = runtime.FuncForPC(pc).Name()
	}

	return fmt.Errorf("[%s] end of stream", name)
}

//
func recoverNoSuchKey(err error) bool {
	var e interface{ ErrorCode() string }

	ok := errors.As(err, &e)
	return ok && e.ErrorCode() == "NoSuchKey"
}
