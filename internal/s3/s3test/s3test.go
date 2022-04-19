package s3test

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/fogfish/dynamo"
	"github.com/fogfish/dynamo/keyval"
)

//
//
type MockS3 interface {
	Mock(s3iface.S3API)
}

func mock[T dynamo.Thing](mock s3iface.S3API) dynamo.KeyValNoContext[T] {
	client := keyval.Must(keyval.New[T]("s3:///test"))
	switch v := client.(type) {
	case MockS3:
		v.Mock(mock)
	default:
		panic("Invalid config")
	}

	return dynamo.NewKeyValContextDefault(client)
}

func encodeKey(key dynamo.Thing) string {
	hkey := key.HashKey()
	skey := key.SortKey()

	if skey == "" {
		return hkey
	}

	return hkey + "/_/" + skey
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
	s3iface.S3API
	expectKey *T
	returnVal *T
}

func (mock *s3GetObject[T]) GetObjectWithContext(ctx aws.Context, input *s3.GetObjectInput, opts ...request.Option) (*s3.GetObjectOutput, error) {
	if aws.StringValue(input.Key) != encodeKey(*mock.expectKey) {
		return nil, errors.New("Unexpected request.")
	}

	if mock.returnVal == nil {
		return nil, awserr.New(s3.ErrCodeNoSuchKey, "", nil)
	}

	val, _ := json.Marshal(mock.returnVal)
	return &s3.GetObjectOutput{
		Body: aws.ReadSeekCloser(bytes.NewReader(val)),
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
	s3iface.S3API
	expectVal *T
}

func (mock *s3PutObject[T]) PutObjectWithContext(ctx aws.Context, input *s3.PutObjectInput, opts ...request.Option) (*s3.PutObjectOutput, error) {
	if aws.StringValue(input.Key) != encodeKey(*mock.expectVal) {
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
	s3iface.S3API
	expectKey *T
}

func (mock *s3DeleteObject[T]) DeleteObjectWithContext(ctx aws.Context, input *s3.DeleteObjectInput, opts ...request.Option) (*s3.DeleteObjectOutput, error) {
	if aws.StringValue(input.Key) != encodeKey(*mock.expectKey) {
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
	s3iface.S3API
	get *s3GetObject[T]
	put *s3PutObject[T]
}

func (mock *s3GetPutObject[T]) GetObjectWithContext(ctx aws.Context, input *s3.GetObjectInput, opts ...request.Option) (*s3.GetObjectOutput, error) {
	return mock.get.GetObjectWithContext(ctx, input, opts...)
}

func (mock *s3GetPutObject[T]) PutObjectWithContext(ctx aws.Context, input *s3.PutObjectInput, opts ...request.Option) (*s3.PutObjectOutput, error) {
	return mock.put.PutObjectWithContext(ctx, input, opts...)
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
	s3iface.S3API
	expectKey     *T
	returnLen     int
	returnVal     *T
	returnLastKey *T
}

func (mock *s3GetListObjects[T]) GetObjectWithContext(ctx aws.Context, input *s3.GetObjectInput, opts ...request.Option) (*s3.GetObjectOutput, error) {
	if aws.StringValue(input.Key) != encodeKey(*mock.returnVal) {
		return nil, errors.New("Unexpected request.")
	}

	val, _ := json.Marshal(mock.returnVal)
	return &s3.GetObjectOutput{
		Body: aws.ReadSeekCloser(bytes.NewReader(val)),
	}, nil
}

func (mock *s3GetListObjects[T]) ListObjectsV2WithContext(aws.Context, *s3.ListObjectsV2Input, ...request.Option) (*s3.ListObjectsV2Output, error) {
	seq := []*s3.Object{}
	for i := 0; i < mock.returnLen; i++ {
		seq = append(seq, &s3.Object{Key: aws.String(encodeKey(*mock.returnVal))})
	}

	var lastEvaluatedKey *string
	if mock.returnLastKey != nil {
		lastEvaluatedKey = aws.String(encodeKey(*mock.returnLastKey))
	}

	return &s3.ListObjectsV2Output{
		KeyCount:              aws.Int64(int64(mock.returnLen)),
		Contents:              seq,
		NextContinuationToken: lastEvaluatedKey,
	}, nil
}
