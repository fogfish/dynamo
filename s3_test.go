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
	t.Run("Using Key/Val", func(t *testing.T) {
		val := person{
			Prefix: dynamo.NewIRI("dead:beef"),
			Suffix: dynamo.NewIRI("1"),
		}
		api, _ := mockGetObject(entity())

		err := api.Get(&val)
		it.Ok(t).
			If(err).Should().Equal(nil).
			If(val).Should().Equal(entity())
	})

	t.Run("Using Stream", func(t *testing.T) {
		val := person{
			Prefix: dynamo.NewIRI("dead:beef"),
			Suffix: dynamo.NewIRI("1"),
		}
		_, api := mockGetObject(entity())

		err := api.Get(&val)
		it.Ok(t).
			If(err).Should().Equal(nil).
			If(val).Should().Equal(entity())
	})

	t.Run("I/O Error", func(t *testing.T) {
		val := person{
			Prefix: dynamo.NewIRI("some:key"),
			Suffix: dynamo.NewIRI("1"),
		}
		api, _ := mockGetObject(entity())

		err := api.Get(&val)
		it.Ok(t).
			If(err).ShouldNot().Equal(nil)
	})
}

func TestS3Put(t *testing.T) {
	t.Run("Using Key/Val", func(t *testing.T) {
		api, sio := mockPutObject("dead:beef/_/1", entity())

		it.Ok(t).
			If(api.Put(entity())).Should().Equal(nil).
			If(sio.Put(entity())).Should().Equal(nil)
	})
}

func TestS3Remove(t *testing.T) {
	t.Run("Using Key/Val", func(t *testing.T) {
		api, sio := mockDeleteObject("dead:beef/_/1")

		it.Ok(t).
			If(api.Remove(entity())).Should().Equal(nil).
			If(sio.Remove(entity())).Should().Equal(nil)
	})
}

func TestS3Update(t *testing.T) {
	t.Run("Using Key/Val", func(t *testing.T) {
		val := person{
			Prefix: dynamo.NewIRI("dead:beef"),
			Suffix: dynamo.NewIRI("1"),
			Age:    64,
		}
		api, _ := mockGetPutObject("dead:beef/_/1", entity())

		err := api.Update(&val)
		it.Ok(t).
			If(err).Should().Equal(nil).
			If(val).Should().Equal(entity())
	})

	t.Run("Using Stream", func(t *testing.T) {
		val := person{
			Prefix: dynamo.NewIRI("dead:beef"),
			Suffix: dynamo.NewIRI("1"),
			Age:    64,
		}
		_, sio := mockGetPutObject("dead:beef/_/1", entity())

		err := sio.Update(&val)
		it.Ok(t).
			If(err).Should().Equal(nil).
			If(val).Should().Equal(entity())
	})
}

func TestS3MatchNone(t *testing.T) {
	api, _ := mockGetListObjects("dead:beef", 0)

	seq := api.Match(person{Prefix: dynamo.NewIRI("dead:beef")})

	it.Ok(t).
		IfFalse(seq.Tail()).
		If(seq.Error()).Should().Equal(nil)
}

func TestS3MatchOne(t *testing.T) {
	api, _ := mockGetListObjects("dead:beef/_/1", 1)

	seq := api.Match(person{Prefix: dynamo.NewIRI("dead:beef")})

	val := person{}
	err := seq.Head(&val)

	it.Ok(t).
		IfFalse(seq.Tail()).
		If(seq.Error()).Should().Equal(nil).
		If(err).Should().Equal(nil).
		If(val).Should().Equal(entity())
}

func TestS3MatchMany(t *testing.T) {
	api, _ := mockGetListObjects("dead:beef/_/1", 5)

	cnt := 0
	seq := api.Match(person{Prefix: dynamo.NewIRI("dead:beef")})

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
		If(cnt).Should().Equal(5)
}

func TestS3FMapNone(t *testing.T) {
	seq := persons{}
	api, _ := mockGetListObjects("dead:beef/_/1", 0)

	err := api.Match(person{Prefix: dynamo.NewIRI("dead:beef")}).FMap(seq.Join)
	it.Ok(t).
		If(err).Should().Equal(nil).
		If(seq).Should().Equal(persons{})

}

func TestS3FMapPrefixOnly(t *testing.T) {
	seq := persons{}
	api, _ := mockGetListObjects("dead:beef/_/1", 2)
	thing := entity()

	err := api.Match(person{Prefix: dynamo.NewIRI("dead:beef")}).FMap(seq.Join)
	it.Ok(t).
		If(err).Should().Equal(nil).
		If(seq).Should().Equal(persons{thing, thing})
}

func TestS3FMapPrefixAndSuffix(t *testing.T) {
	seq := persons{}
	api, _ := mockGetListObjects("dead:beef/_/a/b/c", 2)
	thing := entity()

	err := api.Match(person{
		Prefix: dynamo.NewIRI("dead:beef"),
		Suffix: dynamo.NewIRI(""),
	}).FMap(seq.Join)
	it.Ok(t).
		If(err).Should().Equal(nil).
		If(seq).Should().Equal(persons{thing, thing})
}

func TestS3FMapIDs(t *testing.T) {
	seq := dynamo.Identities{}
	api, _ := mockGetListObjects("dead:beef", 2)
	prefix, _ := entity().Identity()
	thing := []string{prefix, ""}

	err := api.Match(person{Prefix: dynamo.NewIRI("dead:beef")}).FMap(seq.Join)
	it.Ok(t).
		If(err).Should().Equal(nil).
		If(seq).Should().Equal(dynamo.Identities{thing, thing})
}

func TestStreamSendContent(t *testing.T) {
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

//
//
type s3GetObject struct {
	s3iface.S3API
	returnVal interface{}
}

func mockGetObject(returnVal interface{}) (dynamo.KeyValNoContext, dynamo.StreamNoContext) {
	return mockS3(&s3GetObject{returnVal: returnVal})
}

func (mock *s3GetObject) GetObjectWithContext(ctx aws.Context, input *s3.GetObjectInput, opts ...request.Option) (*s3.GetObjectOutput, error) {
	if aws.StringValue(input.Key) != "dead:beef/_/1" {
		return nil, errors.New("Unexpected request.")
	}

	val, _ := json.Marshal(mock.returnVal)
	return &s3.GetObjectOutput{
		Body: aws.ReadSeekCloser(bytes.NewReader(val)),
	}, nil
}

//
//
type s3PutObject struct {
	s3iface.S3API
	expectKey string
	expectVal interface{}
}

func mockPutObject(expectKey string, expectVal interface{}) (dynamo.KeyValNoContext, dynamo.StreamNoContext) {
	return mockS3(&s3PutObject{expectKey: expectKey, expectVal: expectVal})
}

func (mock *s3PutObject) PutObjectWithContext(ctx aws.Context, input *s3.PutObjectInput, opts ...request.Option) (*s3.PutObjectOutput, error) {
	if aws.StringValue(input.Key) != mock.expectKey {
		return nil, errors.New("Unexpected request.")
	}

	val := person{}
	err := json.NewDecoder(input.Body).Decode(&val)

	if err != nil && !reflect.DeepEqual(val, mock.expectVal) {
		return nil, errors.New("Unexpected request.")
	}

	return &s3.PutObjectOutput{}, nil
}

//
//
type s3DeleteObject struct {
	s3iface.S3API
	expectKey string
}

func mockDeleteObject(expectKey string) (dynamo.KeyValNoContext, dynamo.StreamNoContext) {
	return mockS3(&s3DeleteObject{expectKey: expectKey})
}

func (mock *s3DeleteObject) DeleteObjectWithContext(ctx aws.Context, input *s3.DeleteObjectInput, opts ...request.Option) (*s3.DeleteObjectOutput, error) {
	if aws.StringValue(input.Key) != mock.expectKey {
		return nil, errors.New("Unexpected entity. ")
	}

	return &s3.DeleteObjectOutput{}, nil
}

//
//
type s3GetPutObject struct {
	s3iface.S3API
	get *s3GetObject
	put *s3PutObject
}

func mockGetPutObject(expectKey string, expectVal interface{}) (dynamo.KeyValNoContext, dynamo.StreamNoContext) {
	return mockS3(&s3GetPutObject{
		put: &s3PutObject{expectKey: expectKey, expectVal: expectVal},
		get: &s3GetObject{returnVal: expectVal},
	})
}

func (mock *s3GetPutObject) GetObjectWithContext(ctx aws.Context, input *s3.GetObjectInput, opts ...request.Option) (*s3.GetObjectOutput, error) {
	return mock.get.GetObjectWithContext(ctx, input, opts...)
}

func (mock *s3GetPutObject) PutObjectWithContext(ctx aws.Context, input *s3.PutObjectInput, opts ...request.Option) (*s3.PutObjectOutput, error) {
	return mock.put.PutObjectWithContext(ctx, input, opts...)
}

//
//
type s3GetListObjects struct {
	s3iface.S3API
	expectKey string
	returnLen int
}

func mockGetListObjects(expectKey string, returnLen int) (dynamo.KeyValNoContext, dynamo.StreamNoContext) {
	return mockS3(&s3GetListObjects{expectKey: expectKey, returnLen: returnLen})
}

func (mock *s3GetListObjects) GetObjectWithContext(ctx aws.Context, input *s3.GetObjectInput, opts ...request.Option) (*s3.GetObjectOutput, error) {
	if aws.StringValue(input.Key) != mock.expectKey {
		return nil, errors.New("Unexpected request.")
	}

	val, _ := json.Marshal(entity())
	return &s3.GetObjectOutput{
		Body: aws.ReadSeekCloser(bytes.NewReader(val)),
	}, nil
}

func (mock *s3GetListObjects) ListObjectsV2WithContext(aws.Context, *s3.ListObjectsV2Input, ...request.Option) (*s3.ListObjectsV2Output, error) {
	seq := []*s3.Object{}
	for i := 0; i < mock.returnLen; i++ {
		// prefix, suffix := entity().Identity()
		// aws.String(prefix + "/_/" + suffix)
		seq = append(seq, &s3.Object{Key: aws.String(mock.expectKey)})
	}

	return &s3.ListObjectsV2Output{
		KeyCount: aws.Int64(int64(mock.returnLen)),
		Contents: seq,
	}, nil
}

//
//
type MockS3 interface {
	Mock(s3iface.S3API)
}

func mockS3(mock s3iface.S3API) (dynamo.KeyValNoContext, dynamo.StreamNoContext) {
	return mockS3KeyVal(mock), mockS3Stream(mock)
}

func mockS3KeyVal(mock s3iface.S3API) dynamo.KeyValNoContext {
	client := dynamo.Must(dynamo.New("s3:///test"))
	switch v := client.(type) {
	case MockS3:
		v.Mock(mock)
	default:
		panic("Invalid config")
	}

	return dynamo.NewKeyValContextDefault(client)
}

func mockS3Stream(mock s3iface.S3API) dynamo.StreamNoContext {
	client := dynamo.MustStream(dynamo.NewStream("s3:///test"))
	switch v := client.(type) {
	case MockS3:
		v.Mock(mock)
	default:
		panic(mock)
	}

	return dynamo.NewStreamContextDefault(client)
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
	ID    string `json:"id,omitempty"`
	Flag  bool   `json:"flag,omitempty"`
	Label string `json:"label,omitempty"`
}

type seqType struct {
	ID   string    `json:"id,omitempty"`
	List []seqItem `json:"list,omitempty"`
}

func (seq seqType) Identity() (string, string) {
	return seq.ID, ""
}

func seqLong() seqType {
	return seqType{
		ID: "seq",
		List: []seqItem{
			{ID: "1", Flag: true, Label: "a"},
			{ID: "2", Flag: true, Label: "b"},
			{ID: "3", Label: "c"},
			{ID: "4", Label: "d"},
		},
	}
}

func seqShort() seqType {
	return seqType{
		ID: "seq",
		List: []seqItem{
			{ID: "5", Label: "e"},
			{ID: "6", Label: "f"},
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

func apiSeqS3() dynamo.KeyValNoContext {
	client := dynamo.Must(dynamo.New("s3:///test"))
	switch v := client.(type) {
	case MockS3:
		v.Mock(&mockSeqS3{})
	default:
		panic("Invalid config")
	}

	return dynamo.NewKeyValContextDefault(client)
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
