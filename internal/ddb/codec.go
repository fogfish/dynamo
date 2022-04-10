package ddb

import (
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/fogfish/dynamo"
)

type Codec[T dynamo.ThingV2] struct {
	pkPrefix string
	skSuffix string
}

//
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
	gen[codec.pkPrefix] = &dynamodb.AttributeValue{S: aws.String(hashkey)}
	gen[codec.skSuffix] = &dynamodb.AttributeValue{S: aws.String(sortkey)}

	return gen, nil
}

//
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

//
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
