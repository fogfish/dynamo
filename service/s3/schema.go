//
// Copyright (C) 2022 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

package s3

import (
	"reflect"

	"github.com/fogfish/dynamo/v2"
	"github.com/fogfish/golem/pure/hseq"
)

/*
Schema is utility that merges two struct
*/
type schema[T dynamo.Thing] struct{ hseq.Seq[T] }

func newSchema[T dynamo.Thing]() *schema[T] {
	return &schema[T]{hseq.New[T]()}
}

func (schema schema[T]) Merge(a, b T) (c T) {
	va := reflect.ValueOf(a)
	if va.Kind() == reflect.Pointer {
		va = va.Elem()
	}

	vb := reflect.ValueOf(b)
	if vb.Kind() == reflect.Pointer {
		vb = vb.Elem()
	}

	// pointer to c makes reflect.ValueOf settable
	// see The third law of reflection
	// https://go.dev/blog/laws-of-reflection
	vc := reflect.ValueOf(&c).Elem()
	if vc.Kind() == reflect.Pointer {
		// T is a pointer type, therefore c is nil
		// it has to be filled with empty value before merging
		empty := reflect.New(vc.Type().Elem())
		vc.Set(empty)
		vc = vc.Elem()
	}

	for _, f := range schema.Seq {
		fa := va.FieldByName(f.Name)
		fb := vb.FieldByName(f.Name)
		fc := vc.FieldByName(f.Name)

		switch {
		case !fa.IsZero():
			fc.Set(fa)
		case !fb.IsZero():
			fc.Set(fb)
		}
	}

	return
}
