//
// Copyright (C) 2022 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

package s3

import (
	"github.com/fogfish/dynamo"
)

/*

Codec is utility to encode/decode objects to s3 representation
*/
type Codec[T dynamo.Thing] struct {
	rootPath string
}

//
func (codec Codec[T]) EncodeKey(key T) string {
	hkey := key.HashKey()
	skey := key.SortKey()

	if skey == "" {
		return hkey
	}

	return codec.rootPath + hkey + "/_/" + skey
}
