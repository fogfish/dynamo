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

	"github.com/fogfish/dynamo/internal/dynamotest"
	"github.com/fogfish/dynamo/keyval"
	"github.com/fogfish/it"
)

func TestNew(t *testing.T) {
	it.Ok(t).
		If(keyval.Must(keyval.New[dynamotest.Person]("ddb:///a"))).ShouldNot().Equal(nil).
		If(keyval.Must(keyval.New[dynamotest.Person]("s3:///a"))).ShouldNot().Equal(nil)
}

func TestReadOnly(t *testing.T) {
	it.Ok(t).
		If(keyval.NewReadOnly(keyval.Must(keyval.New[dynamotest.Person]("ddb:///a")))).ShouldNot().Equal(nil).
		If(keyval.NewReadOnly(keyval.Must(keyval.New[dynamotest.Person]("s3:///a")))).ShouldNot().Equal(nil)
}
