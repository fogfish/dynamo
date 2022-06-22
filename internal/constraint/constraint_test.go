//
// Copyright (C) 2022 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

package constraint_test

import (
	"testing"

	"github.com/fogfish/dynamo/internal/constraint"
	"github.com/fogfish/it"
)

func TestDyadic(t *testing.T) {
	for op, f := range map[string]func(string, string) *constraint.Dyadic[any]{
		"=":  constraint.Eq[any, string],
		"<>": constraint.Ne[any, string],
		"<":  constraint.Lt[any, string],
		"<=": constraint.Le[any, string],
		">":  constraint.Gt[any, string],
		">=": constraint.Ge[any, string],
	} {
		d := f("key", "val")
		d.TypeOf("x")
		it.Ok(t).
			If(d.Op).Equal(op).
			If(d.Key).Equal("key").
			If(d.Val).Equal("val")
	}
}

func TestUnary(t *testing.T) {
	for op, f := range map[string]func(string) *constraint.Unary[any]{
		"attribute_exists":     constraint.Exists[any],
		"attribute_not_exists": constraint.NotExists[any],
	} {
		d := f("key")
		d.TypeOf("x")
		it.Ok(t).
			If(d.Op).Equal(op).
			If(d.Key).Equal("key")
	}
}

func TestDyadicHttp(t *testing.T) {
	for key, f := range map[string]func(string) *constraint.Dyadic[any]{
		"CacheControl":    constraint.CacheControl[any],
		"ContentEncoding": constraint.ContentEncoding[any],
		"ContentLanguage": constraint.ContentLanguage[any],
		"ContentType":     constraint.ContentType[any],
	} {
		d := f("val")
		it.Ok(t).
			If(d.Op).Equal("http").
			If(d.Key).Equal(key).
			If(d.Val).Equal("val")
	}
}
