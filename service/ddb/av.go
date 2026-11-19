//
// Copyright (C) 2025 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

package ddb

import (
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

//
// The file is the only place of the library that calls attributevalue
// (un)marshalers. Types implementing encoding.TextMarshaler (e.g. gold.IRI,
// gold.Imply) are stored as a string (S) and validated when read. Every
// marshaling must use the same configuration, otherwise the value used in
// update or condition expression is encoded differently from the stored one.
//

func avEncoderOptions(o *attributevalue.EncoderOptions) { o.UseEncodingMarshalers = true }
func avDecoderOptions(o *attributevalue.DecoderOptions) { o.UseEncodingUnmarshalers = true }

func avMarshal(v any) (types.AttributeValue, error) {
	return attributevalue.MarshalWithOptions(v, avEncoderOptions)
}

func avMarshalMap(v any) (map[string]types.AttributeValue, error) {
	return attributevalue.MarshalMapWithOptions(v, avEncoderOptions)
}

func avUnmarshal(av types.AttributeValue, v any) error {
	return attributevalue.UnmarshalWithOptions(av, v, avDecoderOptions)
}

func avUnmarshalMap(m map[string]types.AttributeValue, v any) error {
	return attributevalue.UnmarshalMapWithOptions(m, v, avDecoderOptions)
}
