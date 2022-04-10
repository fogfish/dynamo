package s3

import "github.com/fogfish/dynamo"

type Codec[T dynamo.ThingV2] struct{}

//
func (codec Codec[T]) EncodeKey(key T) string {
	hkey := key.HashKey()
	skey := key.SortKey()

	if skey == "" {
		return hkey
	}

	return hkey + "/_/" + skey
}
