//
// Copyright (C) 2019 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

//
// The file declares public types to implement custom codecs
//

package dynamo

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/fogfish/golem/pure/hseq"
)

/*

CodecOf for struct fields, the type implement Encode/Decode primitives.
Codec helps to implement semi-automated encoding/decoding algebraic data type
into the format compatible with storage.

Let's consider scenario were application uses complex types that skips
implementation of marshal/unmarshal protocols. Here the type MyComplexType
needs to be casted to MyDynamoType that knows how to marshal/unmarshal the type.

  type MyType struct {
    ID   MyComplexType
    Name MyComplexType
  }
  var ID, Name = dynamo.Codec2[MyType, MyDynamoType, MyDynamoType]("ID", "Name")

  func (t MyType) MarshalDynamoDBAttributeValue() (*dynamodb.AttributeValue, error) {
    type tStruct MyType
    return dynamo.Encode(tStruct(p),
      ID.Encode(MyDynamoType(t.ID)),
      Name.Encode(MyDynamoType(t.Name)),
    )
  }

*/
type CodecOf[T Thing, A any] interface {
	Decode(*A) Coder
	Encode(A) Coder
}

/*

Codec1 builds Codec for 1 attributes
*/
func Codec1[T Thing, A any](a string) CodecOf[T, A] {
	return hseq.FMap1(
		generic[T](a),
		mkCodecOf[T, A],
	)
}

/*

Codec2 builds Codec for 2 attributes
*/
func Codec2[T Thing, A, B any](a, b string) (
	CodecOf[T, A],
	CodecOf[T, B],
) {
	return hseq.FMap2(
		generic[T](a, b),
		mkCodecOf[T, A],
		mkCodecOf[T, B],
	)
}

/*

Codec4 builds Codec for 4 attributes
*/
func Codec3[T Thing, A, B, C any](a, b, c string) (
	CodecOf[T, A],
	CodecOf[T, B],
	CodecOf[T, C],
) {
	return hseq.FMap3(
		generic[T](a, b, c),
		mkCodecOf[T, A],
		mkCodecOf[T, B],
		mkCodecOf[T, C],
	)
}

/*

Codec4 builds Codec for 4 attributes
*/
func Codec4[T Thing, A, B, C, D any](a, b, c, d string) (
	CodecOf[T, A],
	CodecOf[T, B],
	CodecOf[T, C],
	CodecOf[T, D],
) {
	return hseq.FMap4(
		generic[T](a, b, c, d),
		mkCodecOf[T, A],
		mkCodecOf[T, B],
		mkCodecOf[T, C],
		mkCodecOf[T, D],
	)
}

/*

Codec5 builds Codec for 5 attributes
*/
func Codec5[T Thing, A, B, C, D, E any](a, b, c, d, e string) (
	CodecOf[T, A],
	CodecOf[T, B],
	CodecOf[T, C],
	CodecOf[T, D],
	CodecOf[T, E],
) {
	return hseq.FMap5(
		generic[T](a, b, c, d, e),
		mkCodecOf[T, A],
		mkCodecOf[T, B],
		mkCodecOf[T, C],
		mkCodecOf[T, D],
		mkCodecOf[T, E],
	)
}

/*

Codec6 builds Codec for 6 attributes
*/
func Codec6[T Thing, A, B, C, D, E, F any](a, b, c, d, e, f string) (
	CodecOf[T, A],
	CodecOf[T, B],
	CodecOf[T, C],
	CodecOf[T, D],
	CodecOf[T, E],
	CodecOf[T, F],
) {
	return hseq.FMap6(
		generic[T](a, b, c, d, e, f),
		mkCodecOf[T, A],
		mkCodecOf[T, B],
		mkCodecOf[T, C],
		mkCodecOf[T, D],
		mkCodecOf[T, E],
		mkCodecOf[T, F],
	)
}

/*

Codec7 builds Codec for 7 attributes
*/
func Codec7[T Thing, A, B, C, D, E, F, G any](a, b, c, d, e, f, g string) (
	CodecOf[T, A],
	CodecOf[T, B],
	CodecOf[T, C],
	CodecOf[T, D],
	CodecOf[T, E],
	CodecOf[T, F],
	CodecOf[T, G],
) {
	return hseq.FMap7(
		generic[T](a, b, c, d, e, f, g),
		mkCodecOf[T, A],
		mkCodecOf[T, B],
		mkCodecOf[T, C],
		mkCodecOf[T, D],
		mkCodecOf[T, E],
		mkCodecOf[T, F],
		mkCodecOf[T, G],
	)
}

/*

Codec8 builds Codec for 8 attributes
*/
func Codec8[T Thing, A, B, C, D, E, F, G, H any](a, b, c, d, e, f, g, h string) (
	CodecOf[T, A],
	CodecOf[T, B],
	CodecOf[T, C],
	CodecOf[T, D],
	CodecOf[T, E],
	CodecOf[T, F],
	CodecOf[T, G],
	CodecOf[T, H],
) {
	return hseq.FMap8(
		generic[T](a, b, c, d, e, f, g, h),
		mkCodecOf[T, A],
		mkCodecOf[T, B],
		mkCodecOf[T, C],
		mkCodecOf[T, D],
		mkCodecOf[T, E],
		mkCodecOf[T, F],
		mkCodecOf[T, G],
		mkCodecOf[T, H],
	)
}

/*

Codec9 builds Codec for 9 attributes
*/
func Codec9[T Thing, A, B, C, D, E, F, G, H, I any](a, b, c, d, e, f, g, h, i string) (
	CodecOf[T, A],
	CodecOf[T, B],
	CodecOf[T, C],
	CodecOf[T, D],
	CodecOf[T, E],
	CodecOf[T, F],
	CodecOf[T, G],
	CodecOf[T, H],
	CodecOf[T, I],
) {
	return hseq.FMap9(
		generic[T](a, b, c, d, e, f, g, h, i),
		mkCodecOf[T, A],
		mkCodecOf[T, B],
		mkCodecOf[T, C],
		mkCodecOf[T, D],
		mkCodecOf[T, E],
		mkCodecOf[T, F],
		mkCodecOf[T, G],
		mkCodecOf[T, H],
		mkCodecOf[T, I],
	)
}

/*

Codec10 builds Codec for 10 attributes
*/
func Codec10[T Thing, A, B, C, D, E, F, G, H, I, J any](a, b, c, d, e, f, g, h, i, j string) (
	CodecOf[T, A],
	CodecOf[T, B],
	CodecOf[T, C],
	CodecOf[T, D],
	CodecOf[T, E],
	CodecOf[T, F],
	CodecOf[T, G],
	CodecOf[T, H],
	CodecOf[T, I],
	CodecOf[T, J],
) {
	return hseq.FMap10(
		generic[T](a, b, c, d, e, f, g, h, i, j),
		mkCodecOf[T, A],
		mkCodecOf[T, B],
		mkCodecOf[T, C],
		mkCodecOf[T, D],
		mkCodecOf[T, E],
		mkCodecOf[T, F],
		mkCodecOf[T, G],
		mkCodecOf[T, H],
		mkCodecOf[T, I],
		mkCodecOf[T, J],
	)
}

// Builds CodecOf
func mkCodecOf[T Thing, A any](t hseq.Type[T]) CodecOf[T, A] {
	tag := t.Tag.Get("dynamodbav")
	if tag == "" {
		return codec[T, A]("")
	}

	return codec[T, A](strings.Split(tag, ",")[0])
}

// internal implementation of codec
type codec[T Thing, A any] string

/*

Decode generic DynamoDB attribute values into struct fields behind pointers
*/
func (key codec[T, A]) Decode(val *A) Coder {
	return func(gen map[string]types.AttributeValue) (map[string]types.AttributeValue, error) {
		if gval, exists := gen[string(key)]; exists {
			if err := attributevalue.Unmarshal(gval, val); err != nil {
				return nil, err
			}
			delete(gen, string(key))
		}
		return gen, nil
	}
}

/*

Encode encode struct field into DynamoDB attribute values
*/
func (key codec[T, A]) Encode(val A) Coder {
	return func(gen map[string]types.AttributeValue) (map[string]types.AttributeValue, error) {
		gval, err := attributevalue.Marshal(val)
		if err != nil {
			return nil, err
		}

		gen[string(key)] = gval
		return gen, nil
	}
}

/*

Coder is a function, applies transformation of generic dynamodb AttributeValue
*/
type Coder func(map[string]types.AttributeValue) (map[string]types.AttributeValue, error)

/*

Decode is a helper function to decode core domain types from Dynamo DB format.
The helper ensures compact URI de-serialization from DynamoDB schema.

  type MyType struct {
    ID   MyComplexType
    Name MyComplexType
  }
  var ID, Name = dynamo.Codec2[MyType, MyDynamoType, MyDynamoType]("ID", "Name")

  func (x *MyType) UnmarshalDynamoDBAttributeValue(av types.AttributeValue) error {
    type tStruct *MyType
    return dynamo.Decode(av, tStruct(x),
      ID.Decode((*MyDynamoType)(&x.ID)),
			Name.Decode((*MyDynamoType)(&x.Name)),
    )
  }

*/
func Decode(av types.AttributeValue, val interface{}, coder ...Coder) (err error) {
	tv, ok := av.(*types.AttributeValueMemberM)
	if !ok {
		return &attributevalue.UnmarshalTypeError{
			Value: fmt.Sprintf("%T", av),
			Err:   fmt.Errorf("Only struct type is supported"),
		}
	}

	for _, fcoder := range coder {
		tv.Value, err = fcoder(tv.Value)
		if err != nil {
			return err
		}
	}

	return attributevalue.Unmarshal(tv, val)
}

/*

Encode is a helper function to encode core domain types into struct.
The helper ensures compact URI serialization into DynamoDB schema.

  type MyType struct {
    ID   MyComplexType
    Name MyComplexType
  }
  var ID, Name = dynamo.Codec2[MyType, MyDynamoType, MyDynamoType]("ID", "Name")

  func (x MyType) MarshalDynamoDBAttributeValue() (types.AttributeValue, error) {
    type tStruct MyType
    return dynamo.Encode(av, tStruct(x),
      ID.Encode(MyDynamoType(x.ID)),
			Name.Encode(MyDynamoType(x.Name)),
    )
  }

*/
func Encode(val interface{}, coder ...Coder) (types.AttributeValue, error) {
	gen, err := attributevalue.Marshal(val)
	if err != nil {
		return nil, err
	}

	var gem *types.AttributeValueMemberM

	switch v := gen.(type) {
	case *types.AttributeValueMemberM:
		gem = v
	case *types.AttributeValueMemberNULL:
		gem = &types.AttributeValueMemberM{
			Value: make(map[string]types.AttributeValue),
		}
	}

	for _, fcoder := range coder {
		gem.Value, err = fcoder(gem.Value)
		if err != nil {
			return nil, err
		}
	}

	return gem, nil
}
