//
// Copyright (C) 2022 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

package keyval_test

import (
	"testing"

	"github.com/fogfish/dynamo"
	"github.com/fogfish/dynamo/internal/dynamotest"
	"github.com/fogfish/dynamo/keyval"
	"github.com/fogfish/it"
)

func TestNew(t *testing.T) {
	for _, opt := range []dynamo.Option{
		dynamo.WithURI("ddb:///a"),
		dynamo.WithURI("s3:///a"),
	} {
		val, err := keyval.New[dynamotest.Person](opt)
		it.Ok(t).
			IfNil(err).
			IfNotNil(val)
	}
}

func TestNewWithError(t *testing.T) {
	t.Run("NoURI", func(t *testing.T) {
		val, err := keyval.New[dynamotest.Person]()
		it.Ok(t).
			IfNotNil(err).
			IfNil(val)
	})

	t.Run("NoStorage", func(t *testing.T) {
		val, err := keyval.New[dynamotest.Person](dynamo.WithURI("ddb:///"))
		it.Ok(t).
			IfNotNil(err).
			IfNil(val)
	})

	t.Run("Unsupported", func(t *testing.T) {
		val, err := keyval.New[dynamotest.Person](dynamo.WithURI("xxx:///"))
		it.Ok(t).
			IfNotNil(err).
			IfNil(val)
	})
}

func TestReadOnly(t *testing.T) {
	for _, opt := range []dynamo.Option{
		dynamo.WithURI("ddb:///a"),
		dynamo.WithURI("s3:///a"),
	} {
		val, err := keyval.ReadOnly[dynamotest.Person](opt)
		it.Ok(t).
			IfNil(err).
			IfNotNil(val)
	}
}
