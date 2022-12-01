package s3

import (
	"errors"
	"fmt"

	"github.com/fogfish/dynamo/v2"
	xerrors "github.com/fogfish/errors"
)

const (
	errInvalidConnectorURL = xerrors.Safe1[string]("invalid connector url %s")
	errServiceIO           = xerrors.Type("service i/o failed")
	errInvalidEntity       = xerrors.Type("invalid entity")
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
