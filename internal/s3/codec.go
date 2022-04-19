package s3

import "github.com/fogfish/dynamo"

/*

Codec is utility to encode/decode objects to s3 representation
*/
type Codec[T dynamo.Thing] struct{}

//
func (codec Codec[T]) EncodeKey(key T) string {
	hkey := key.HashKey()
	skey := key.SortKey()

	if skey == "" {
		return hkey
	}

	return hkey + "/_/" + skey
}
