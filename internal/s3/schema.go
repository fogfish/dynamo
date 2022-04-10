package s3

import (
	"reflect"

	"github.com/fogfish/dynamo"
	"github.com/fogfish/golem/pure/hseq"
)

type Schema[T dynamo.ThingV2] struct{ hseq.Seq[T] }

func NewSchema[T dynamo.ThingV2]() *Schema[T] {
	return &Schema[T]{hseq.Generic[T]()}

	// keys := hseq.FMap(
	// 	hseq.Generic[T](),
	// 	func(t hseq.Type[T]) string {
	// 		return t.StructField.Name
	// 	},
	// )

	// return &Schema{Keys: keys}
}

func (schema Schema[T]) Merge(a, b T) (c T) {
	va := reflect.ValueOf(a)
	vb := reflect.ValueOf(b)
	vc := reflect.ValueOf(c)

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
