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

	"github.com/fogfish/dynamo"
	"github.com/fogfish/golem/pure/hseq"
)

/*

Schema is utility that merges two struct
*/
type Schema[T dynamo.Thing] struct{ hseq.Seq[T] }

func NewSchema[T dynamo.Thing]() *Schema[T] {
	return &Schema[T]{hseq.Generic[T]()}
}

func (schema Schema[T]) Merge(a, b T) (c T) {
	va := reflect.ValueOf(a)
	vb := reflect.ValueOf(b)
	vc := reflect.ValueOf(&c).Elem()

	for _, f := range schema.Seq {
		fa := va.FieldByIndex(f.Index)
		fb := vb.FieldByIndex(f.Index)
		fc := vc.FieldByIndex(f.Index)

		switch {
		case !fa.IsZero():
			fc.Set(fa)
		case !fb.IsZero():
			fc.Set(fb)
		}
	}

	return
}
