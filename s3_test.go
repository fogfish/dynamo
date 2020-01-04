package dynamo_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/fogfish/dynamo"
	"github.com/fogfish/it"
)

func TestS3Get(t *testing.T) {
	val := person{IRI: dynamo.IRI{"dead", "beef"}}
	err := apiS3().Get(&val)

	it.Ok(t).
		If(err).Should().Equal(nil).
		If(val).Should().Equal(entity())
}

func TestS3Put(t *testing.T) {
	it.Ok(t).If(apiS3().Put(entity())).Should().Equal(nil)
}

//-----------------------------------------------------------------------------
//
// Mock S3
//
//-----------------------------------------------------------------------------

func apiS3() *dynamo.S3 {
	client := &dynamo.S3{}
	client.Mock(&mockS3{})
	return client
}

type mockS3 struct {
	s3iface.S3API
}

func (mockS3) GetObject(*s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	val, _ := json.Marshal(entity())

	return &s3.GetObjectOutput{
		Body: aws.ReadSeekCloser(bytes.NewReader(val)),
	}, nil
}

func (mockS3) PutObject(input *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	if !reflect.DeepEqual(input.Key, aws.String("dead/beef")) {
		return nil, errors.New("Unexpected entity.")
	}

	val := person{}
	err := json.NewDecoder(input.Body).Decode(&val)

	if err != nil && !reflect.DeepEqual(val, entity()) {
		return nil, errors.New("Unexpected entity.")
	}

	return &s3.PutObjectOutput{}, nil
}
