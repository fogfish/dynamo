//
// Copyright (C) 2022 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

//
// The file declares public types to perform I/O with conditional expression
//

package dynamo

import (
	"strings"

	"github.com/fogfish/dynamo/v2/internal/constraint"
	"github.com/fogfish/golem/pure/hseq"
)

/*
Constraint is a function that applies conditional expression to storage request.
Each storage implements own constrains protocols. The module here defines a few
constrain protocol. The structure of the constrain is abstracted away from the client.

See internal/constrain package to see details about its implementation
*/
type Constraint[T Thing] interface{ TypeOf(T) }

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
	Eq(A) Constraint[T]
	Ne(A) Constraint[T]
	Lt(A) Constraint[T]
	Le(A) Constraint[T]
	Gt(A) Constraint[T]
	Ge(A) Constraint[T]
	Is(string) Constraint[T]
	Exists() Constraint[T]
	NotExists() Constraint[T]
}

/*
Schema builds Constrain builder for product type of arity 1
*/
func Schema[T Thing, A any](a string) TypeOf[T, A] {
	return hseq.FMap1(
		generic[T](a),
		mkTypeOf[T, A],
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
func (eff effect[T, A]) Eq(val A) Constraint[T] {
	return constraint.Eq[T](eff.Key, val)
}

/*
Ne is non equal constrain

	name.Ne(x) ⟼ Field <> :value
*/
func (eff effect[T, A]) Ne(val A) Constraint[T] {
	return constraint.Ne[T](eff.Key, val)
}

/*
Lt is less than constain

	name.Lt(x) ⟼ Field < :value
*/
func (eff effect[T, A]) Lt(val A) Constraint[T] {
	return constraint.Lt[T](eff.Key, val)
}

/*
Le is less or equal constain

	name.Le(x) ⟼ Field <= :value
*/
func (eff effect[T, A]) Le(val A) Constraint[T] {
	return constraint.Le[T](eff.Key, val)
}

/*
Gt is greater than constrain

	name.Le(x) ⟼ Field > :value
*/
func (eff effect[T, A]) Gt(val A) Constraint[T] {
	return constraint.Gt[T](eff.Key, val)
}

/*
Ge is greater or equal constrain

	name.Le(x) ⟼ Field >= :value
*/
func (eff effect[T, A]) Ge(val A) Constraint[T] {
	return constraint.Ge[T](eff.Key, val)
}

/*
Is matches either Eq or NotExists if value is not defined
*/
func (eff effect[T, A]) Is(val string) Constraint[T] {
	return constraint.Is[T](eff.Key, val)
}

/*
Exists attribute constrain

	name.Exists(x) ⟼ attribute_exists(name)
*/
func (eff effect[T, A]) Exists() Constraint[T] {
	return constraint.Exists[T](eff.Key)
}

/*
NotExists attribute constrain

	name.NotExists(x) ⟼ attribute_not_exists(name)
*/
func (eff effect[T, A]) NotExists() Constraint[T] {
	return constraint.NotExists[T](eff.Key)
}
