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

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/fogfish/dynamo"
)

/*

Codec is utility to encode/decode objects to dynamo representation
*/
type Codec[T dynamo.Thing] struct {
	pkPrefix string
	skSuffix string
}

// EncodeKey to dynamo representation
func (codec Codec[T]) EncodeKey(key T) (map[string]*dynamodb.AttributeValue, error) {
	hashkey := key.HashKey()
	if hashkey == "" {
		return nil, fmt.Errorf("invalid key of %T, hashkey cannot be empty", key)
	}

	sortkey := key.SortKey()
	if sortkey == "" {
		sortkey = "_"
	}

	gen := map[string]*dynamodb.AttributeValue{}
	gen[codec.pkPrefix] = &dynamodb.AttributeValue{S: aws.String(string(hashkey))}
	gen[codec.skSuffix] = &dynamodb.AttributeValue{S: aws.String(string(sortkey))}

	return gen, nil
}

// KeyOnly extracts key value from generic representation
func (codec Codec[T]) KeyOnly(gen map[string]*dynamodb.AttributeValue) map[string]*dynamodb.AttributeValue {
	key := map[string]*dynamodb.AttributeValue{}
	key[codec.pkPrefix] = gen[codec.pkPrefix]
	key[codec.skSuffix] = gen[codec.skSuffix]

	return key
}

// Encode object to dynamo representation
func (codec Codec[T]) Encode(entity T) (map[string]*dynamodb.AttributeValue, error) {
	gen, err := dynamodbattribute.MarshalMap(entity)
	if err != nil {
		return nil, err
	}

	suffix, isSuffix := gen[codec.skSuffix]
	if !isSuffix || suffix.S == nil {
		gen[codec.skSuffix] = &dynamodb.AttributeValue{S: aws.String("_")}
	}

	return gen, nil
}

// Decode dynamo representation to object
func (codec Codec[T]) Decode(gen map[string]*dynamodb.AttributeValue) (*T, error) {
	_, isPrefix := gen[codec.pkPrefix]
	_, isSuffix := gen[codec.skSuffix]
	if !isPrefix || !isSuffix {
		return nil, errors.New("Invalid DDB schema")
	}

	var entity T
	if err := dynamodbattribute.UnmarshalMap(gen, &entity); err != nil {
		return nil, err
	}

	return &entity, nil
}
