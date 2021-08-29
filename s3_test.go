package dynamo_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/fogfish/dynamo"
	"github.com/fogfish/it"
)

func TestS3Get(t *testing.T) {
	val := person{ID: dynamo.NewID("dead:beef")}
	err := apiS3().Get(&val)

	it.Ok(t).
		If(err).Should().Equal(nil).
		If(val).Should().Equal(entity())
}

func TestS3Put(t *testing.T) {
	it.Ok(t).If(apiS3().Put(entity())).Should().Equal(nil)
}

func TestS3Remove(t *testing.T) {
	it.Ok(t).If(apiS3().Remove(entity())).Should().Equal(nil)
}

func TestS3Update(t *testing.T) {
	val := person{
		ID:  dynamo.NewID("dead:beef"),
		Age: 64,
	}
	err := apiS3().Update(&val)

	it.Ok(t).
		If(err).Should().Equal(nil).
		If(val).Should().Equal(entity())
}

func TestS3Match(t *testing.T) {
	cnt := 0
	seq := apiS3().Match(dynamo.NewID("dead:"))

	for seq.Tail() {
		cnt++
		val := person{}
		err := seq.Head(&val)

		it.Ok(t).
			If(err).Should().Equal(nil).
			If(val).Should().Equal(entity())
	}

	it.Ok(t).
		If(seq.Error()).Should().Equal(nil).
		If(cnt).Should().Equal(2)
}

func TestS3MatchHead(t *testing.T) {
	seq := apiS3().Match(dynamo.NewID("dead:"))

	val := person{}
	err := seq.Head(&val)

	it.Ok(t).
		If(err).Should().Equal(nil).
		If(val).Should().Equal(entity())
}

func TestS3MatchWithFMap(t *testing.T) {
	pseq := persons{}
	tseq, err := apiS3().Match(dynamo.NewID("dead:")).FMap(pseq.Join)

	thing := entity()
	it.Ok(t).
		If(err).Should().Equal(nil).
		If(tseq).Should().Equal([]dynamo.Thing{&thing, &thing}).
		If(pseq).Should().Equal(persons{thing, thing})
}

func TestS3MatchIDsWithFMap(t *testing.T) {
	seq := dynamo.IDs{}
	_, err := apiS3().Match(dynamo.NewID("dead:")).FMap(seq.Join)

	thing := entity().ID
	it.Ok(t).
		If(err).Should().Equal(nil).
		If(seq).Should().Equal(dynamo.IDs{thing, thing})
}

func TestBlobSendContent(t *testing.T) {
	req := &s3manager.UploadInput{}
	dynamo.HTTP.CacheControl("max-age=20")(req)
	dynamo.HTTP.ContentEncoding("identity")(req)
	dynamo.HTTP.ContentLanguage("en")(req)
	dynamo.HTTP.ContentType("text/plain")(req)
	dynamo.HTTP.Expires(time.Now())(req)

	it.Ok(t).
		If(*req.CacheControl).Equal("max-age=20").
		If(*req.ContentEncoding).Equal("identity").
		If(*req.ContentLanguage).Equal("en").
		If(*req.ContentType).Equal("text/plain").
		IfNotNil(req.Expires)
}

//-----------------------------------------------------------------------------
//
// Mock S3
//
//-----------------------------------------------------------------------------

type MockS3 interface {
	Mock(s3iface.S3API)
}

func apiS3() dynamo.KeyVal {
	client := dynamo.Must(dynamo.New("s3:///test"))
	switch v := client.(type) {
	case MockS3:
		v.Mock(&mockS3{})
	default:
		panic("Invalid config")
	}

	return client
}

type mockS3 struct {
	s3iface.S3API
}

func (mockS3) GetObjectWithContext(ctx aws.Context, input *s3.GetObjectInput, opts ...request.Option) (*s3.GetObjectOutput, error) {
	if aws.StringValue(input.Key) != "dead/beef" {
		return nil, errors.New("Unexpected request.")
	}

	val, _ := json.Marshal(entity())
	return &s3.GetObjectOutput{
		Body: aws.ReadSeekCloser(bytes.NewReader(val)),
	}, nil
}

func (mockS3) PutObjectWithContext(ctx aws.Context, input *s3.PutObjectInput, opts ...request.Option) (*s3.PutObjectOutput, error) {
	if aws.StringValue(input.Key) != "dead/beef" {
		return nil, errors.New("Unexpected request.")
	}

	val := person{}
	err := json.NewDecoder(input.Body).Decode(&val)

	if err != nil && !reflect.DeepEqual(val, entity()) {
		return nil, errors.New("Unexpected request.")
	}

	return &s3.PutObjectOutput{}, nil
}

func (mockS3) DeleteObjectWithContext(ctx aws.Context, input *s3.DeleteObjectInput, opts ...request.Option) (*s3.DeleteObjectOutput, error) {
	if aws.StringValue(input.Key) != "dead/beef" {
		return nil, errors.New("Unexpected entity. ")
	}

	return &s3.DeleteObjectOutput{}, nil
}

func (mockS3) ListObjectsV2WithContext(aws.Context, *s3.ListObjectsV2Input, ...request.Option) (*s3.ListObjectsV2Output, error) {
	return &s3.ListObjectsV2Output{
		KeyCount: aws.Int64(2),
		Contents: []*s3.Object{
			{Key: aws.String("dead/beef")},
			{Key: aws.String("dead/beef")},
		},
	}, nil
}

//-----------------------------------------------------------------------------
//
// Corrupted Update
//
//-----------------------------------------------------------------------------

//
// dynamodbattribute.MarshalMap / dynamodbattribute.UnmarshalMap corrupts struct(s)
// it do not resets the slice to zero when decoding generic structure back to the interface
// as the result old values might leakout while doing s3 update
// this test case ensures correctness of update function
type seqItem struct {
	dynamo.ID
	Flag  bool   `json:"flag,omitempty"`
	Label string `json:"label,omitempty"`
}

type seqType struct {
	dynamo.ID
	List []seqItem `json:"list,omitempty"`
}

func seqLong() seqType {
	return seqType{
		ID: dynamo.NewID("seq"),
		List: []seqItem{
			{ID: dynamo.NewID("1"), Flag: true, Label: "a"},
			{ID: dynamo.NewID("2"), Flag: true, Label: "b"},
			{ID: dynamo.NewID("3"), Label: "c"},
			{ID: dynamo.NewID("4"), Label: "d"},
		},
	}
}

func seqShort() seqType {
	return seqType{
		ID: dynamo.NewID("seq"),
		List: []seqItem{
			{ID: dynamo.NewID("5"), Label: "e"},
			{ID: dynamo.NewID("6"), Label: "f"},
		},
	}
}

func TestSeqS3Update(t *testing.T) {
	val := seqShort()
	err := apiSeqS3().Update(&val)

	it.Ok(t).
		If(err).Should().Equal(nil).
		If(val).Should().Equal(seqShort())
}

func apiSeqS3() dynamo.KeyVal {
	client := dynamo.Must(dynamo.New("s3:///test"))
	switch v := client.(type) {
	case MockS3:
		v.Mock(&mockSeqS3{})
	default:
		panic("Invalid config")
	}

	return client
}

type mockSeqS3 struct{ s3iface.S3API }

func (mockSeqS3) GetObjectWithContext(ctx aws.Context, input *s3.GetObjectInput, opts ...request.Option) (*s3.GetObjectOutput, error) {
	if aws.StringValue(input.Key) != "seq" {
		return nil, errors.New("Unexpected request.")
	}

	val, _ := json.Marshal(seqLong())
	return &s3.GetObjectOutput{
		Body: aws.ReadSeekCloser(bytes.NewReader(val)),
	}, nil
}

func (mockSeqS3) PutObjectWithContext(ctx aws.Context, input *s3.PutObjectInput, opts ...request.Option) (*s3.PutObjectOutput, error) {
	if aws.StringValue(input.Key) != "seq" {
		return nil, errors.New("Unexpected request.")
	}

	return &s3.PutObjectOutput{}, nil
}
