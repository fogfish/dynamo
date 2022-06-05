//
// Copyright (C) 2022 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

package s3

import (
	"strings"

	"github.com/fogfish/dynamo"
)

/*

Codec is utility to encode/decode objects to s3 representation
*/
type Codec[T dynamo.Thing] struct{}

func (codec Codec[T]) EncodeIRI(key string) string {
	return strings.ReplaceAll(key, ":", ":/")
}

func (codec Codec[T]) DecodeIRI(key string) string {
	return strings.ReplaceAll(key, ":/", ":")
}

//
func (codec Codec[T]) EncodeKey(key T) string {
	hkey := codec.EncodeIRI(key.HashKey())
	skey := codec.EncodeIRI(key.SortKey())

	if skey == "" {
		return hkey
	}

	return hkey + "/_/" + skey
}
