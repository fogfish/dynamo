package dynamo

import (
	"strings"

	"github.com/fogfish/dynamo/internal/constrain"
	"github.com/fogfish/golem/pure/hseq"
)

type ConstrainV2[T ThingV2] interface{}

type TypeV2[T ThingV2, A any] interface {
	Eq(A) ConstrainV2[T]
	Ne(A) ConstrainV2[T]
	Lt(A) ConstrainV2[T]
	Le(A) ConstrainV2[T]
	Gt(A) ConstrainV2[T]
	Ge(A) ConstrainV2[T]
	Is(string) ConstrainV2[T]
	Exists() ConstrainV2[T]
	NotExists() ConstrainV2[T]
}

func Schema3[T ThingV2, A, B, C any]() (
	TypeV2[T, A],
	TypeV2[T, B],
	TypeV2[T, C],
) {
	return hseq.FMap3(
		hseq.Generic[T](),
		mkType[T, A],
		mkType[T, B],
		mkType[T, C],
	)
}

func mkType[T ThingV2, A any](t hseq.Type[T]) TypeV2[T, A] {
	tag := t.Tag.Get("dynamodbav")
	if tag == "" {
		return effect[T, A]{""}
	}

	return effect[T, A]{strings.Split(tag, ",")[0]}
}

/*

Internal implementation of Constrain effects
*/
type effect[T ThingV2, A any] struct{ Key string }

/*

Eq is equal constrain
  name.Eq(x) ⟼ Field = :value
*/
func (eff effect[T, A]) Eq(val A) ConstrainV2[T] {
	return constrain.Eq(eff.Key, val)
}

/*

Ne is non equal constrain
  name.Ne(x) ⟼ Field <> :value
*/
func (eff effect[T, A]) Ne(val A) ConstrainV2[T] {
	return constrain.Ne(eff.Key, val)
}

/*

Lt is less than constain
  name.Lt(x) ⟼ Field < :value
*/
func (eff effect[T, A]) Lt(val A) ConstrainV2[T] {
	return constrain.Lt(eff.Key, val)
}

/*

Le is less or equal constain
  name.Le(x) ⟼ Field <= :value
*/
func (eff effect[T, A]) Le(val A) ConstrainV2[T] {
	return constrain.Le(eff.Key, val)
}

/*

Gt is greater than constrain
  name.Le(x) ⟼ Field > :value
*/
func (eff effect[T, A]) Gt(val A) ConstrainV2[T] {
	return constrain.Gt(eff.Key, val)
}

/*

Ge is greater or equal constrain
  name.Le(x) ⟼ Field >= :value
*/
func (eff effect[T, A]) Ge(val A) ConstrainV2[T] {
	return constrain.Ge(eff.Key, val)
}

/*

Is matches either Eq or NotExists if value is not defined
*/
func (eff effect[T, A]) Is(val string) ConstrainV2[T] {
	return constrain.Is(eff.Key, val)
}

/*

Exists attribute constrain
  name.Exists(x) ⟼ attribute_exists(name)
*/
func (eff effect[T, A]) Exists() ConstrainV2[T] {
	return constrain.Exists(eff.Key)
}

/*

NotExists attribute constrain
	name.NotExists(x) ⟼ attribute_not_exists(name)
*/
func (eff effect[T, A]) NotExists() ConstrainV2[T] {
	return constrain.NotExists(eff.Key)
}
