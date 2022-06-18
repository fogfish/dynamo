//
// Copyright (C) 2022 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

package constrain_test

import (
	"testing"

	"github.com/fogfish/dynamo/internal/constrain"
	"github.com/fogfish/it"
)

func TestDyadic(t *testing.T) {
	for op, f := range map[string]func(string, string) *constrain.Dyadic[any]{
		"=":  constrain.Eq[any, string],
		"<>": constrain.Ne[any, string],
		"<":  constrain.Lt[any, string],
		"<=": constrain.Le[any, string],
		">":  constrain.Gt[any, string],
		">=": constrain.Ge[any, string],
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
	for op, f := range map[string]func(string) *constrain.Unary[any]{
		"attribute_exists":     constrain.Exists[any],
		"attribute_not_exists": constrain.NotExists[any],
	} {
		d := f("key")
		d.TypeOf("x")
		it.Ok(t).
			If(d.Op).Equal(op).
			If(d.Key).Equal("key")
	}
}

func TestDyadicHttp(t *testing.T) {
	for key, f := range map[string]func(string) *constrain.Dyadic[any]{
		"CacheControl":    constrain.CacheControl[any],
		"ContentEncoding": constrain.ContentEncoding[any],
		"ContentLanguage": constrain.ContentLanguage[any],
		"ContentType":     constrain.ContentType[any],
	} {
		d := f("val")
		it.Ok(t).
			If(d.Op).Equal("http").
			If(d.Key).Equal(key).
			If(d.Val).Equal("val")
	}
}
