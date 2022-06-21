//
// Copyright (C) 2022 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

package s3test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"reflect"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	// "github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/fogfish/curie"
	"github.com/fogfish/dynamo"
	ds3 "github.com/fogfish/dynamo/internal/s3"
	"github.com/fogfish/dynamo/keyval"
)

//
//
type MockS3 interface {
	Mock(ds3.S3)
}

func mock[T dynamo.Thing](mock ds3.S3) dynamo.KeyValNoContext[T] {
	client := keyval.Must(keyval.New[T](dynamo.WithURI("s3:///test")))
	switch v := client.(type) {
	case MockS3:
		v.Mock(mock)
	default:
		panic("Invalid config")
	}

	return keyval.NewKeyValContextDefault(client)
}

func encodeKey(key dynamo.Thing) string {
	hkey := curie.URI(curie.Namespaces{}, key.HashKey())
	skey := curie.URI(curie.Namespaces{}, key.SortKey())

	if skey == "" {
		return hkey
	}

	return hkey + "/" + skey
}

/*

GetObject mock
*/
func GetObject[T dynamo.Thing](
	expectKey *T,
	returnVal *T,
) dynamo.KeyValNoContext[T] {
	return mock[T](&s3GetObject[T]{
		expectKey: expectKey,
		returnVal: returnVal,
	})
}

type s3GetObject[T dynamo.Thing] struct {
	ds3.S3
	expectKey *T
	returnVal *T
}

func (mock *s3GetObject[T]) GetObject(ctx context.Context, input *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	fmt.Printf("==> %s  %s\n", *input.Key, encodeKey(*mock.expectKey))
	if *input.Key != encodeKey(*mock.expectKey) {
		return nil, errors.New("Unexpected request.")
	}

	if mock.returnVal == nil {
		return nil, &types.NoSuchKey{}
	}

	val, _ := json.Marshal(mock.returnVal)
	return &s3.GetObjectOutput{
		Body: ioutil.NopCloser(bytes.NewReader(val)),
	}, nil
}

/*

PutObject mock
*/
func PutObject[T dynamo.Thing](
	expectVal *T,
) dynamo.KeyValNoContext[T] {
	return mock[T](&s3PutObject[T]{
		expectVal: expectVal,
	})
}

type s3PutObject[T dynamo.Thing] struct {
	ds3.S3
	expectVal *T
}

func (mock *s3PutObject[T]) PutObject(ctx context.Context, input *s3.PutObjectInput, opts ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if *input.Key != encodeKey(*mock.expectVal) {
		return nil, errors.New("Unexpected request.")
	}

	var val T
	err := json.NewDecoder(input.Body).Decode(&val)

	if err != nil && !reflect.DeepEqual(val, mock.expectVal) {
		return nil, errors.New("Unexpected request.")
	}

	return &s3.PutObjectOutput{}, nil
}

/*

DeleteObject mock
*/
func DeleteObject[T dynamo.Thing](
	expectKey *T,
) dynamo.KeyValNoContext[T] {
	return mock[T](&s3DeleteObject[T]{expectKey: expectKey})
}

type s3DeleteObject[T dynamo.Thing] struct {
	ds3.S3
	expectKey *T
}

func (mock *s3DeleteObject[T]) DeleteObject(ctx context.Context, input *s3.DeleteObjectInput, opts ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	if *input.Key != encodeKey(*mock.expectKey) {
		return nil, errors.New("Unexpected entity. ")
	}

	return &s3.DeleteObjectOutput{}, nil
}

/*

GetPutObject mock (used by the Update)
*/
func GetPutObject[T dynamo.Thing](
	expectKey *T,
	expectVal *T,
	returnVal *T,
) dynamo.KeyValNoContext[T] {
	return mock[T](&s3GetPutObject[T]{
		put: &s3PutObject[T]{expectVal: expectVal},
		get: &s3GetObject[T]{expectKey: expectKey, returnVal: returnVal},
	})
}

type s3GetPutObject[T dynamo.Thing] struct {
	ds3.S3
	get *s3GetObject[T]
	put *s3PutObject[T]
}

func (mock *s3GetPutObject[T]) GetObject(ctx context.Context, input *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return mock.get.GetObject(ctx, input, opts...)
}

func (mock *s3GetPutObject[T]) PutObject(ctx context.Context, input *s3.PutObjectInput, opts ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	return mock.put.PutObject(ctx, input, opts...)
}

/*

GetListObjects mock
*/
func GetListObjects[T dynamo.Thing](
	expectKey *T,
	returnLen int,
	returnVal *T,
	returnLastKey *T,
) dynamo.KeyValNoContext[T] {
	return mock[T](&s3GetListObjects[T]{
		expectKey:     expectKey,
		returnLen:     returnLen,
		returnVal:     returnVal,
		returnLastKey: returnLastKey,
	})
}

type s3GetListObjects[T dynamo.Thing] struct {
	ds3.S3
	expectKey     *T
	returnLen     int
	returnVal     *T
	returnLastKey *T
}

func (mock *s3GetListObjects[T]) GetObject(ctx context.Context, input *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if *input.Key != encodeKey(*mock.returnVal) {
		return nil, errors.New("Unexpected request.")
	}

	val, _ := json.Marshal(mock.returnVal)
	return &s3.GetObjectOutput{
		Body: ioutil.NopCloser(bytes.NewReader(val)),
	}, nil
}

func (mock *s3GetListObjects[T]) ListObjectsV2(ctx context.Context, input *s3.ListObjectsV2Input, opts ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	seq := []types.Object{}
	for i := 0; i < mock.returnLen; i++ {
		seq = append(seq, types.Object{Key: aws.String(encodeKey(*mock.returnVal))})
	}

	var lastEvaluatedKey *string
	if mock.returnLastKey != nil {
		lastEvaluatedKey = aws.String(encodeKey(*mock.returnLastKey))
	}

	return &s3.ListObjectsV2Output{
		KeyCount:              int32(mock.returnLen),
		Contents:              seq,
		NextContinuationToken: lastEvaluatedKey,
	}, nil
}
