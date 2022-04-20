package constrain_test

import (
	"testing"

	"github.com/fogfish/dynamo/internal/constrain"
	"github.com/fogfish/it"
)

func TestDyadic(t *testing.T) {
	for op, f := range map[string]func(string, string) *constrain.Dyadic{
		"=":  constrain.Eq[string],
		"<>": constrain.Ne[string],
		"<":  constrain.Lt[string],
		"<=": constrain.Le[string],
		">":  constrain.Gt[string],
		">=": constrain.Ge[string],
	} {
		d := f("key", "val")
		it.Ok(t).
			If(d.Op).Equal(op).
			If(d.Key).Equal("key").
			If(d.Val).Equal("val")
	}
}

func TestUnary(t *testing.T) {
	for op, f := range map[string]func(string) *constrain.Unary{
		"attribute_exists":     constrain.Exists,
		"attribute_not_exists": constrain.NotExists,
	} {
		d := f("key")
		it.Ok(t).
			If(d.Op).Equal(op).
			If(d.Key).Equal("key")
	}
}

func TestDyadicHttp(t *testing.T) {
	for key, f := range map[string]func(string) *constrain.Dyadic{
		"CacheControl":    constrain.CacheControl,
		"ContentEncoding": constrain.ContentEncoding,
		"ContentLanguage": constrain.ContentLanguage,
		"ContentType":     constrain.ContentType,
	} {
		d := f("val")
		it.Ok(t).
			If(d.Op).Equal("http").
			If(d.Key).Equal(key).
			If(d.Val).Equal("val")
	}
}