package ddb

import (
	"errors"
	"fmt"

	"github.com/fogfish/dynamo/v2"
	xerrors "github.com/fogfish/errors"
)

const (
	errInvalidConnectorURL = xerrors.Safe1[string]("invalid connector url %s")
	errServiceIO           = xerrors.Type("service i/o failed")
	errInvalidKey          = xerrors.Type("invalid key")
	errInvalidEntity       = xerrors.Type("invalid entity")
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
func errPreConditionFailed(thing dynamo.Thing, conflict bool, gone bool) error {
	return &preConditionFailed{Thing: thing, conflict: conflict, gone: gone}
}

type preConditionFailed struct {
	dynamo.Thing
	conflict bool
	gone     bool
}

func (e *preConditionFailed) Error() string {
	return fmt.Sprintf("Pre Condition Failed (%s, %s)", e.HashKey(), e.SortKey())
}

func (e *preConditionFailed) PreConditionFailed() bool { return true }

func (e *preConditionFailed) Conflict() bool { return e.conflict }

func (e *preConditionFailed) Gone() bool { return e.gone }

// recover AWS ErrorCode
func recoverConditionalCheckFailedException(err error) bool {
	var e interface{ ErrorCode() string }

	ok := errors.As(err, &e)
	return ok && e.ErrorCode() == "ConditionalCheckFailedException"
}
