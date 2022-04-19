package dynamo

import (
	"strings"
	"time"

	"github.com/fogfish/dynamo/internal/constrain"
	"github.com/fogfish/golem/pure/hseq"
)

/*

Constrain is a function that applies conditional expression to storage request.
Each storage implements own constrains protocols. The module here defines a few
constrain protocol. The structure of the constrain is abstracted away from the client.

See internal/constrain package to see details about its implementation
*/
type Constrain[T Thing] interface{}

/*

TypeOf declares type descriptor to express Storage I/O Constrains.

Let's consider a following example:

  type Person struct {
    curie.ID
    Name    string `dynamodbav:"anothername,omitempty"`
  }

How to define a condition expression on the field Name? Golang struct defines
and refers the field by `Name` but DynamoDB stores it under the attribute
`anothername`. Struct field dynamodbav tag specifies serialization rules.
Golang does not support a typesafe approach to build a correspondence between
`Name` ⟷ `anothername`. Developers have to utilize dynamodb attribute
name(s) in conditional expression and Golang struct name in rest of the code.
It becomes confusing and hard to maintain.

The types TypeOf and SchemaN are helpers to declare builders for conditional
expressions. Just declare a global variables next to type definition and
use them across the application.

  var name = dynamo.Schema1[Person, string]("Name")

	name.Eq("Joe Doe")
  name.NotExists()

*/
type TypeOf[T Thing, A any] interface {
	Eq(A) Constrain[T]
	Ne(A) Constrain[T]
	Lt(A) Constrain[T]
	Le(A) Constrain[T]
	Gt(A) Constrain[T]
	Ge(A) Constrain[T]
	Is(string) Constrain[T]
	Exists() Constrain[T]
	NotExists() Constrain[T]
}

/*

Schema1 builds Constrain builder for product type of arity 1
*/
func Schema1[T Thing, A any](a string) TypeOf[T, A] {
	return hseq.FMap1(
		generic[T](a),
		mkTypeOf[T, A],
	)
}

/*

Schema2 builds Constrain builder for product type of arity 2
*/
func Schema2[T Thing, A, B any](a, b string) (
	TypeOf[T, A],
	TypeOf[T, B],
) {
	return hseq.FMap2(
		generic[T](a, b),
		mkTypeOf[T, A],
		mkTypeOf[T, B],
	)
}

/*

Schema3 builds Constrain builder for product type of arity 3
*/
func Schema3[T Thing, A, B, C any](a, b, c string) (
	TypeOf[T, A],
	TypeOf[T, B],
	TypeOf[T, C],
) {
	return hseq.FMap3(
		generic[T](a, b, c),
		mkTypeOf[T, A],
		mkTypeOf[T, B],
		mkTypeOf[T, C],
	)
}

/*

Schema4 builds Constrain builder for product type of arity 4
*/
func Schema4[T Thing, A, B, C, D any](a, b, c, d string) (
	TypeOf[T, A],
	TypeOf[T, B],
	TypeOf[T, C],
	TypeOf[T, D],
) {
	return hseq.FMap4(
		generic[T](a, b, c, d),
		mkTypeOf[T, A],
		mkTypeOf[T, B],
		mkTypeOf[T, C],
		mkTypeOf[T, D],
	)
}

/*

Schema5 builds Constrain builder for product type of arity 5
*/
func Schema5[T Thing, A, B, C, D, E any](a, b, c, d, e string) (
	TypeOf[T, A],
	TypeOf[T, B],
	TypeOf[T, C],
	TypeOf[T, D],
	TypeOf[T, E],
) {
	return hseq.FMap5(
		generic[T](a, b, c, d, e),
		mkTypeOf[T, A],
		mkTypeOf[T, B],
		mkTypeOf[T, C],
		mkTypeOf[T, D],
		mkTypeOf[T, E],
	)
}

/*

Schema6 builds Constrain builder for product type of arity 6
*/
func Schema6[T Thing, A, B, C, D, E, F any](a, b, c, d, e, f string) (
	TypeOf[T, A],
	TypeOf[T, B],
	TypeOf[T, C],
	TypeOf[T, D],
	TypeOf[T, E],
	TypeOf[T, F],
) {
	return hseq.FMap6(
		generic[T](a, b, c, d, e, f),
		mkTypeOf[T, A],
		mkTypeOf[T, B],
		mkTypeOf[T, C],
		mkTypeOf[T, D],
		mkTypeOf[T, E],
		mkTypeOf[T, F],
	)
}

/*

Schema7 builds Constrain builder for product type of arity 7
*/
func Schema7[T Thing, A, B, C, D, E, F, G any](a, b, c, d, e, f, g string) (
	TypeOf[T, A],
	TypeOf[T, B],
	TypeOf[T, C],
	TypeOf[T, D],
	TypeOf[T, E],
	TypeOf[T, F],
	TypeOf[T, G],
) {
	return hseq.FMap7(
		generic[T](a, b, c, d, e, f, g),
		mkTypeOf[T, A],
		mkTypeOf[T, B],
		mkTypeOf[T, C],
		mkTypeOf[T, D],
		mkTypeOf[T, E],
		mkTypeOf[T, F],
		mkTypeOf[T, G],
	)
}

/*

Schema8 builds Constrain builder for product type of arity 8
*/
func Schema8[T Thing, A, B, C, D, E, F, G, H any](a, b, c, d, e, f, g, h string) (
	TypeOf[T, A],
	TypeOf[T, B],
	TypeOf[T, C],
	TypeOf[T, D],
	TypeOf[T, E],
	TypeOf[T, F],
	TypeOf[T, G],
	TypeOf[T, H],
) {
	return hseq.FMap8(
		generic[T](a, b, c, d, e, f, g, h),
		mkTypeOf[T, A],
		mkTypeOf[T, B],
		mkTypeOf[T, C],
		mkTypeOf[T, D],
		mkTypeOf[T, E],
		mkTypeOf[T, F],
		mkTypeOf[T, G],
		mkTypeOf[T, H],
	)
}

/*

Schema9 builds Constrain builder for product type of arity 9
*/
func Schema9[T Thing, A, B, C, D, E, F, G, H, I any](a, b, c, d, e, f, g, h, i string) (
	TypeOf[T, A],
	TypeOf[T, B],
	TypeOf[T, C],
	TypeOf[T, D],
	TypeOf[T, E],
	TypeOf[T, F],
	TypeOf[T, G],
	TypeOf[T, H],
	TypeOf[T, I],
) {
	return hseq.FMap9(
		generic[T](a, b, c, d, e, f, g, h, i),
		mkTypeOf[T, A],
		mkTypeOf[T, B],
		mkTypeOf[T, C],
		mkTypeOf[T, D],
		mkTypeOf[T, E],
		mkTypeOf[T, F],
		mkTypeOf[T, G],
		mkTypeOf[T, H],
		mkTypeOf[T, I],
	)
}

/*

Schema10 builds Constrain builder for product type of arity 10
*/
func Schema10[T Thing, A, B, C, D, E, F, G, H, I, J any](a, b, c, d, e, f, g, h, i, j string) (
	TypeOf[T, A],
	TypeOf[T, B],
	TypeOf[T, C],
	TypeOf[T, D],
	TypeOf[T, E],
	TypeOf[T, F],
	TypeOf[T, G],
	TypeOf[T, H],
	TypeOf[T, I],
	TypeOf[T, J],
) {
	return hseq.FMap10(
		generic[T](a, b, c, d, e, f, g, h, i, j),
		mkTypeOf[T, A],
		mkTypeOf[T, B],
		mkTypeOf[T, C],
		mkTypeOf[T, D],
		mkTypeOf[T, E],
		mkTypeOf[T, F],
		mkTypeOf[T, G],
		mkTypeOf[T, H],
		mkTypeOf[T, I],
		mkTypeOf[T, J],
	)
}

// generic[T] filters hseq.Generic[T] list with defined fields
func generic[T any](fs ...string) hseq.Seq[T] {
	seq := make(hseq.Seq[T], 0)
	for _, t := range hseq.Generic[T]() {
		for _, f := range fs {
			if t.Name == f {
				seq = append(seq, t)
			}
		}
	}
	return seq
}

// Builds TypeOf constrain
func mkTypeOf[T Thing, A any](t hseq.Type[T]) TypeOf[T, A] {
	tag := t.Tag.Get("dynamodbav")
	if tag == "" {
		return effect[T, A]{""}
	}

	return effect[T, A]{strings.Split(tag, ",")[0]}
}

/*

Internal implementation of Constrain effects for storage
*/
type effect[T Thing, A any] struct{ Key string }

/*

Eq is equal constrain
  name.Eq(x) ⟼ Field = :value
*/
func (eff effect[T, A]) Eq(val A) Constrain[T] {
	return constrain.Eq(eff.Key, val)
}

/*

Ne is non equal constrain
  name.Ne(x) ⟼ Field <> :value
*/
func (eff effect[T, A]) Ne(val A) Constrain[T] {
	return constrain.Ne(eff.Key, val)
}

/*

Lt is less than constain
  name.Lt(x) ⟼ Field < :value
*/
func (eff effect[T, A]) Lt(val A) Constrain[T] {
	return constrain.Lt(eff.Key, val)
}

/*

Le is less or equal constain
  name.Le(x) ⟼ Field <= :value
*/
func (eff effect[T, A]) Le(val A) Constrain[T] {
	return constrain.Le(eff.Key, val)
}

/*

Gt is greater than constrain
  name.Le(x) ⟼ Field > :value
*/
func (eff effect[T, A]) Gt(val A) Constrain[T] {
	return constrain.Gt(eff.Key, val)
}

/*

Ge is greater or equal constrain
  name.Le(x) ⟼ Field >= :value
*/
func (eff effect[T, A]) Ge(val A) Constrain[T] {
	return constrain.Ge(eff.Key, val)
}

/*

Is matches either Eq or NotExists if value is not defined
*/
func (eff effect[T, A]) Is(val string) Constrain[T] {
	return constrain.Is(eff.Key, val)
}

/*

Exists attribute constrain
  name.Exists(x) ⟼ attribute_exists(name)
*/
func (eff effect[T, A]) Exists() Constrain[T] {
	return constrain.Exists(eff.Key)
}

/*

NotExists attribute constrain
	name.NotExists(x) ⟼ attribute_not_exists(name)
*/
func (eff effect[T, A]) NotExists() Constrain[T] {
	return constrain.NotExists(eff.Key)
}

/*

StreamOf declares type descriptor to express streaming I/O Constrains.
*/
type StreamOf[T Thing] interface {
	CacheControl(val string) Constrain[T]
	ContentEncoding(val string) Constrain[T]
	ContentLanguage(val string) Constrain[T]
	ContentType(val string) Constrain[T]
	Expires(val time.Time) Constrain[T]
}

/*

NewStreamOf builds Constrain builder for Streams
*/
func NewStreamOf[T Thing]() StreamOf[T] {
	return stream[T]("StreamOf")
}

/*

Internal implementation of StreamOf constrains
*/
type stream[T Thing] string

// CacheControl header
func (stream[T]) CacheControl(val string) Constrain[T] {
	return constrain.CacheControl(val)
}

// ContentEncoding header
func (stream[T]) ContentEncoding(val string) Constrain[T] {
	return constrain.ContentEncoding(val)
}

// ContentLanguage header
func (stream[T]) ContentLanguage(val string) Constrain[T] {
	return constrain.ContentLanguage(val)
}

// ContentType header
func (stream[T]) ContentType(val string) Constrain[T] {
	return constrain.ContentType(val)
}

// Expires header
func (stream[T]) Expires(val time.Time) Constrain[T] {
	return constrain.Expires(val)
}
