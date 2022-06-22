//
// Copyright (C) 2022 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

//
// The file implements type-safe constraint library
//

package constraint

import (
	"time"
)

/*

Unary operation, applies over the key
*/
type Unary[T any] struct {
	Op  string
	Key string
}

func (Unary[T]) TypeOf(T) {}

/*

Dyadic operation, applied over the key * value
*/
type Dyadic[T any] struct {
	Op  string
	Key string
	Val interface{}
}

func (Dyadic[T]) TypeOf(T) {}

//
// Constraints for storage
//

/*

Eq is equal constrain
  name.Eq(x) ⟼ Field = :value
*/
func Eq[T, A any](key string, val A) *Dyadic[T] {
	return &Dyadic[T]{Op: "=", Key: key, Val: val}
}

/*

Ne is non equal constrain
  name.Ne(x) ⟼ Field <> :value
*/
func Ne[T, A any](key string, val A) *Dyadic[T] {
	return &Dyadic[T]{Op: "<>", Key: key, Val: val}
}

/*

Lt is less than constain
  name.Lt(x) ⟼ Field < :value
*/
func Lt[T, A any](key string, val A) *Dyadic[T] {
	return &Dyadic[T]{Op: "<", Key: key, Val: val}
}

/*

Le is less or equal constain
  name.Le(x) ⟼ Field <= :value
*/
func Le[T, A any](key string, val A) *Dyadic[T] {
	return &Dyadic[T]{Op: "<=", Key: key, Val: val}
}

/*

Gt is greater than constrain
  name.Le(x) ⟼ Field > :value
*/
func Gt[T, A any](key string, val A) *Dyadic[T] {
	return &Dyadic[T]{Op: ">", Key: key, Val: val}
}

/*

Ge is greater or equal constrain
  name.Le(x) ⟼ Field >= :value
*/
func Ge[T, A any](key string, val A) *Dyadic[T] {
	return &Dyadic[T]{Op: ">=", Key: key, Val: val}
}

/*

Is matches either Eq or NotExists if value is not defined
*/
func Is[T any](key string, val string) interface{ TypeOf(T) } {
	if val == "_" {
		return NotExists[T](key)
	}

	return Eq[T](key, val)
}

/*

Exists attribute constrain
  name.Exists(x) ⟼ attribute_exists(name)
*/
func Exists[T any](key string) *Unary[T] {
	return &Unary[T]{Op: "attribute_exists", Key: key}
}

/*

NotExists attribute constrain
	name.NotExists(x) ⟼ attribute_not_exists(name)
*/
func NotExists[T any](key string) *Unary[T] {
	return &Unary[T]{Op: "attribute_not_exists", Key: key}
}

//
// Constraints for protocol
//

// CacheControl header
func CacheControl[T any](val string) *Dyadic[T] {
	return &Dyadic[T]{Op: "http", Key: "CacheControl", Val: val}
}

// ContentEncoding header
func ContentEncoding[T any](val string) *Dyadic[T] {
	return &Dyadic[T]{Op: "http", Key: "ContentEncoding", Val: val}
}

// ContentLanguage header
func ContentLanguage[T any](val string) *Dyadic[T] {
	return &Dyadic[T]{Op: "http", Key: "ContentLanguage", Val: val}
}

// ContentType header
func ContentType[T any](val string) *Dyadic[T] {
	return &Dyadic[T]{Op: "http", Key: "ContentType", Val: val}
}

// Expires header
func Expires[T any](val time.Time) *Dyadic[T] {
	return &Dyadic[T]{Op: "http", Key: "Expires", Val: val}
}
