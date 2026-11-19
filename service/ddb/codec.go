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

package ddb

import (
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/fogfish/dynamo/v4"
	"github.com/fogfish/golem/hseq"
)

// CodecOf for struct fields, the type implement Encode/Decode primitives.
// Codec helps to implement semi-automated encoding/decoding algebraic data type
// into the format compatible with storage.
//
// Let's consider scenario were application uses complex types that skips
// implementation of marshal/unmarshal protocols. Here the type MyComplexType
// needs to be casted to MyDynamoType that knows how to marshal/unmarshal the type.
//
//	type MyType struct {
//	  ID   MyComplexType
//	  Name MyComplexType
//	}
//	var (
//	  ID   = dynamo.Codec[MyType, MyDynamoType]("ID")
//	  Name = dynamo.Codec[MyType, MyDynamoType]("Name")
//	)
//
//	func (t MyType) MarshalDynamoDBAttributeValue() (*dynamodb.AttributeValue, error) {
//	  type tStruct MyType
//	  return dynamo.Encode(tStruct(p),
//	    ID.Encode(MyDynamoType(t.ID)),
//	    Name.Encode(MyDynamoType(t.Name)),
//	  )
//	}
type CodecOf[T dynamo.Thing, A any] interface {
	Decode(*A) Coder
	Encode(A) Coder
}

// Build field codec for attribute
func Codec[T dynamo.Thing, A any](a string) CodecOf[T, A] {
	return hseq.FMap1(
		genCodec[T](a),
		mkCodecOf[T, A],
	)
}

// generic[T] filters hseq.Generic[T] list with defined fields
func genCodec[T any](fs ...string) hseq.Seq[T] {
	seq := make(hseq.Seq[T], 0)
	for _, t := range hseq.New[T]() {
		for _, f := range fs {
			if t.Name == f {
				seq = append(seq, t)
			}
		}
	}
	return seq
}

// Builds CodecOf
func mkCodecOf[T dynamo.Thing, A any](t hseq.Type[T]) CodecOf[T, A] {
	tag := t.Tag.Get("dynamodbav")
	if tag == "" {
		return codecOf[T, A]("")
	}

	return codecOf[T, A](strings.Split(tag, ",")[0])
}

// internal implementation of codec
type codecOf[T dynamo.Thing, A any] string

// Decode generic DynamoDB attribute values into struct fields behind pointers
func (key codecOf[T, A]) Decode(val *A) Coder {
	return func(gen map[string]types.AttributeValue) (map[string]types.AttributeValue, error) {
		if gval, exists := gen[string(key)]; exists {
			if err := avUnmarshal(gval, val); err != nil {
				return nil, err
			}
			delete(gen, string(key))
		}
		return gen, nil
	}
}

// Encode encode struct field into DynamoDB attribute values
func (key codecOf[T, A]) Encode(val A) Coder {
	return func(gen map[string]types.AttributeValue) (map[string]types.AttributeValue, error) {
		gval, err := avMarshal(val)
		if err != nil {
			return nil, err
		}

		gen[string(key)] = gval
		return gen, nil
	}
}

// Coder is a function, applies transformation of generic dynamodb AttributeValue
type Coder func(map[string]types.AttributeValue) (map[string]types.AttributeValue, error)

// Decode is a helper function to decode core domain types from Dynamo DB format.
// The helper ensures de-serialization of custom types from DynamoDB schema.
//
//	  type MyType struct {
//	    ID   MyComplexType
//	    Name MyComplexType
//	  }
//	  var ID, Name = dynamo.Codec2[MyType, MyDynamoType, MyDynamoType]("ID", "Name")
//
//	  func (x *MyType) UnmarshalDynamoDBAttributeValue(av types.AttributeValue) error {
//	    type tStruct *MyType
//	    return dynamo.Decode(av, tStruct(x),
//	      ID.Decode((*MyDynamoType)(&x.ID)),
//				Name.Decode((*MyDynamoType)(&x.Name)),
//	    )
//	  }
func Decode(av types.AttributeValue, val interface{}, coder ...Coder) (err error) {
	tv, ok := av.(*types.AttributeValueMemberM)
	if !ok {
		return &attributevalue.UnmarshalTypeError{
			Value: fmt.Sprintf("%T", av),
			Err:   fmt.Errorf("only struct type is supported"),
		}
	}

	for _, fcoder := range coder {
		tv.Value, err = fcoder(tv.Value)
		if err != nil {
			return err
		}
	}

	return avUnmarshal(tv, val)
}

// Encode is a helper function to encode core domain types into struct.
// The helper ensures serialization of custom types into DynamoDB schema.
//
//	  type MyType struct {
//	    ID   MyComplexType
//	    Name MyComplexType
//	  }
//	  var ID, Name = dynamo.Codec2[MyType, MyDynamoType, MyDynamoType]("ID", "Name")
//
//	  func (x MyType) MarshalDynamoDBAttributeValue() (types.AttributeValue, error) {
//	    type tStruct MyType
//	    return dynamo.Encode(av, tStruct(x),
//	      ID.Encode(MyDynamoType(x.ID)),
//				Name.Encode(MyDynamoType(x.Name)),
//	    )
//	  }
func Encode(val interface{}, coder ...Coder) (types.AttributeValue, error) {
	gen, err := avMarshal(val)
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

// Codec is utility to encode/decode objects to dynamo representation
type codec[T dynamo.Thing] struct {
	pkPrefix  string
	skSuffix  string
	undefined T
}

func newCodec[T dynamo.Thing](conf *Options) *codec[T] {
	return &codec[T]{
		pkPrefix: conf.hashKey,
		skSuffix: conf.sortKey,
	}
}

// EncodeKey to dynamo representation
func (codec codec[T]) EncodeKey(key dynamo.Thing) (map[string]types.AttributeValue, error) {
	hashkey := key.HashKey()
	if hashkey == "" {
		return nil, fmt.Errorf("invalid key of %T, hashkey cannot be empty", key)
	}

	sortkey := key.SortKey()
	if sortkey == "" {
		sortkey = keyPlaceholder
	}

	gen := map[string]types.AttributeValue{}
	gen[codec.pkPrefix] = &types.AttributeValueMemberS{Value: hashkey}
	gen[codec.skSuffix] = &types.AttributeValueMemberS{Value: sortkey}

	return gen, nil
}

// KeyOnly extracts key value from generic representation
func (codec codec[T]) KeyOnly(gen map[string]types.AttributeValue) map[string]types.AttributeValue {
	key := map[string]types.AttributeValue{}
	key[codec.pkPrefix] = gen[codec.pkPrefix]
	key[codec.skSuffix] = gen[codec.skSuffix]

	return key
}

// Encode object to dynamo representation
func (codec codec[T]) Encode(entity T) (map[string]types.AttributeValue, error) {
	gen, err := avMarshalMap(entity)
	if err != nil {
		return nil, err
	}

	// The hash key is mandatory. Absent or empty key is rejected, otherwise
	// DynamoDB fails the request later with a less useful error.
	if isEmptyKey(gen[codec.pkPrefix]) {
		return nil, errInvalidKey.New(
			fmt.Errorf("invalid key of %T, hashkey cannot be empty", entity),
		)
	}

	// The sort key is optional, empty value is stored as the placeholder.
	if isEmptyKey(gen[codec.skSuffix]) {
		gen[codec.skSuffix] = &types.AttributeValueMemberS{Value: keyPlaceholder}
	}

	return gen, nil
}

// keyPlaceholder is a value of the sort key attribute for undefined sort key.
const keyPlaceholder = "_"

// isEmptyKey checks if the key attribute is missing, NULL or an empty string.
func isEmptyKey(av types.AttributeValue) bool {
	switch v := av.(type) {
	case nil:
		return true
	case *types.AttributeValueMemberNULL:
		return true
	case *types.AttributeValueMemberS:
		return v.Value == ""
	default:
		return false
	}
}

// Decode dynamo representation to object
func (codec codec[T]) Decode(gen map[string]types.AttributeValue) (T, error) {
	_, isPrefix := gen[codec.pkPrefix]
	_, isSuffix := gen[codec.skSuffix]
	if !isPrefix || !isSuffix {
		return codec.undefined, errors.New("invalid DDB schema")
	}

	// The placeholder of undefined sort key must never reach the application
	// type, the key field keeps its zero value. Caller's map is not mutated
	// (batch I/O reuses it), the shallow copy is used instead.
	if v, ok := gen[codec.skSuffix].(*types.AttributeValueMemberS); ok && v.Value == keyPlaceholder {
		cpy := make(map[string]types.AttributeValue, len(gen))
		for k, a := range gen {
			if k != codec.skSuffix {
				cpy[k] = a
			}
		}
		gen = cpy
	}

	var entity T
	if err := avUnmarshalMap(gen, &entity); err != nil {
		return codec.undefined, err
	}

	return entity, nil
}
