//
// Copyright (C) 2022 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

package ddb

import (
	"errors"
	"fmt"

	"github.com/fogfish/dynamo/v3"
	"github.com/fogfish/faults"
)

const (
	errServiceIO     = faults.Type("service i/o failed")
	errInvalidKey    = faults.Type("invalid key")
	errInvalidEntity = faults.Type("invalid entity")
)

// NotFound is an error to handle unknown elements
func errNotFound(err error, key dynamo.Thing) error {
	return &notFound{err: err, Thing: key}
}

type notFound struct {
	dynamo.Thing
	err error
}

func (e *notFound) Error() string {
	return fmt.Sprintf("Not Found (%s, %s)", e.HashKey(), e.SortKey())
}

func (e *notFound) Unwrap() error { return e.err }

func (e *notFound) NotFound() string { return e.HashKey().Safe() + " " + e.SortKey().Safe() }

// errPreConditionFailed
func errPreConditionFailed(err error, thing dynamo.Thing, conflict bool, gone bool) error {
	return &preConditionFailed{Thing: thing, conflict: conflict, gone: gone, err: err}
}

type preConditionFailed struct {
	dynamo.Thing
	conflict bool
	gone     bool
	err      error
}

func (e *preConditionFailed) Error() string {
	return fmt.Sprintf("Pre Condition Failed (%s, %s)", e.HashKey(), e.SortKey())
}

func (e *preConditionFailed) PreConditionFailed() bool { return true }

func (e *preConditionFailed) Conflict() bool { return e.conflict }

func (e *preConditionFailed) Gone() bool { return e.gone }

func (e *preConditionFailed) Unwrap() error { return e.err }

// recover AWS ErrorCode
func recoverConditionalCheckFailedException(err error) bool {
	var e interface{ ErrorCode() string }

	ok := errors.As(err, &e)
	return ok && e.ErrorCode() == "ConditionalCheckFailedException"
}
