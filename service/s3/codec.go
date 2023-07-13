//
// Copyright (C) 2022 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

package s3

import (
	"github.com/fogfish/curie"
	"github.com/fogfish/dynamo/v3"
)

/*
Codec is utility to encode/decode objects to s3 representation
*/
type codec[T dynamo.Thing] struct {
	prefixes curie.Prefixes
}

func newCodec[T dynamo.Thing](prefixes curie.Prefixes) *codec[T] {
	if prefixes == nil {
		return &codec[T]{prefixes: curie.Namespaces{}}
	}

	return &codec[T]{prefixes: prefixes}
}

func (codec codec[T]) EncodeKey(key dynamo.Thing) string {
	hkey := curie.URI(codec.prefixes, key.HashKey())
	skey := curie.URI(codec.prefixes, key.SortKey())

	if skey == "" {
		return hkey
	}

	return hkey + "/" + skey
}
