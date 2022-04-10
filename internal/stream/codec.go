package stream

import "github.com/fogfish/dynamo"

type Codec struct{}

//
func (codec Codec) EncodeKey(key dynamo.StreamV2) string {
	hkey := key.HashKey()
	skey := key.SortKey()

	if skey == "" {
		return hkey
	}

	return hkey + "/_/" + skey
}
