//
// Copyright (C) 2022 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

package ddb

import (
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/fogfish/dynamo"
)

/*

Codec is utility to encode/decode objects to dynamo representation
*/
type Codec[T dynamo.Thing] struct {
	pkPrefix  string
	skSuffix  string
	undefined T
}

// EncodeKey to dynamo representation
func (codec Codec[T]) EncodeKey(key T) (map[string]types.AttributeValue, error) {
	hashkey := key.HashKey()
	if hashkey == "" {
		return nil, fmt.Errorf("invalid key of %T, hashkey cannot be empty", key)
	}

	sortkey := key.SortKey()
	if sortkey == "" {
		sortkey = "_"
	}

	gen := map[string]types.AttributeValue{}
	gen[codec.pkPrefix] = &types.AttributeValueMemberS{Value: string(hashkey)}
	gen[codec.skSuffix] = &types.AttributeValueMemberS{Value: string(sortkey)}

	return gen, nil
}

// KeyOnly extracts key value from generic representation
func (codec Codec[T]) KeyOnly(gen map[string]types.AttributeValue) map[string]types.AttributeValue {
	key := map[string]types.AttributeValue{}
	key[codec.pkPrefix] = gen[codec.pkPrefix]
	key[codec.skSuffix] = gen[codec.skSuffix]

	return key
}

// Encode object to dynamo representation
func (codec Codec[T]) Encode(entity T) (map[string]types.AttributeValue, error) {
	gen, err := attributevalue.MarshalMap(entity)
	if err != nil {
		return nil, err
	}

	_ /*suffix*/, isSuffix := gen[codec.skSuffix]
	if !isSuffix /*|| suffix.Value == nil*/ {
		gen[codec.skSuffix] = &types.AttributeValueMemberS{Value: "_"}
	}

	return gen, nil
}

// Decode dynamo representation to object
func (codec Codec[T]) Decode(gen map[string]types.AttributeValue) (T, error) {
	_, isPrefix := gen[codec.pkPrefix]
	_, isSuffix := gen[codec.skSuffix]
	if !isPrefix || !isSuffix {
		return codec.undefined, errors.New("Invalid DDB schema")
	}

	var entity T
	if err := attributevalue.UnmarshalMap(gen, &entity); err != nil {
		return codec.undefined, err
	}

	return entity, nil
}
