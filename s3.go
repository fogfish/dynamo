package dynamo

import (
	"bytes"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"reflect"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/fogfish/iri"
)

// S3 is a service connection handle
type S3 struct {
	io     *session.Session
	db     s3iface.S3API
	bucket *string
}

func newS3(bucket string) *S3 {
	io := session.Must(session.NewSession())
	db := s3.New(io)
	return &S3{io, db, aws.String(bucket)}
}

// Mock S3 I/O channel
func (dynamo *S3) Mock(db s3iface.S3API) {
	dynamo.db = db
}

//-----------------------------------------------------------------------------
//
// Key Value
//
//-----------------------------------------------------------------------------

// Get fetches the entity identified by the key.
func (dynamo S3) Get(entity iri.Thing) (err error) {
	req := &s3.GetObjectInput{
		Bucket: dynamo.bucket,
		Key:    aws.String(entity.Identity().Path()),
	}
	val, err := dynamo.db.GetObject(req)
	if err != nil {
		switch v := err.(type) {
		case awserr.Error:
			if v.Code() == s3.ErrCodeNoSuchKey {
				return NotFound{entity.Identity().Path()}
			}
			return err
		default:
			return err
		}
	}

	err = json.NewDecoder(val.Body).Decode(entity)
	return
}

// Put writes entity
func (dynamo S3) Put(entity iri.Thing, _ ...Config) (err error) {
	gen, err := json.Marshal(entity)
	if err != nil {
		return
	}

	req := &s3.PutObjectInput{
		Bucket: dynamo.bucket,
		Key:    aws.String(entity.Identity().Path()),
		Body:   aws.ReadSeekCloser(bytes.NewReader(gen)),
	}

	_, err = dynamo.db.PutObject(req)
	return
}

// Remove discards the entity from the bucket
func (dynamo S3) Remove(entity iri.Thing, _ ...Config) (err error) {
	req := &s3.DeleteObjectInput{
		Bucket: dynamo.bucket,
		Key:    aws.String(entity.Identity().Path()),
	}

	_, err = dynamo.db.DeleteObject(req)
	return

}

type tGen map[string]interface{}

func (z tGen) Identity() iri.ID { return z["id"].(iri.ID) }

// Update applies a partial patch to entity and returns new values
func (dynamo S3) Update(entity iri.Thing, _ ...Config) (err error) {
	gen := tGen{"id": entity.Identity()}
	dynamo.Get(&gen)

	var par tGen
	parbin, _ := json.Marshal(entity)
	json.Unmarshal(parbin, &par)

	for keyA, valA := range par {
		if !reflect.ValueOf(valA).IsZero() {
			gen[keyA] = valA
		}
	}

	genbin, _ := json.Marshal(gen)
	json.Unmarshal(genbin, &entity)

	err = json.Unmarshal(genbin, &entity)
	if err != nil {
		return
	}

	err = dynamo.Put(entity)
	return

	//
	// Corrupts target structure
	//
	// par, err := dynamodbattribute.MarshalMap(entity)
	// if err != nil {
	// 	return
	// }

	// dynamo.Get(entity)
	// gen, err := dynamodbattribute.MarshalMap(entity)
	// if err != nil {
	// 	return
	// }

	// for keyA, valA := range par {
	// 	gen[keyA] = valA
	// }

	// err = dynamodbattribute.UnmarshalMap(gen, entity)
	// if err != nil {
	// 	return
	// }

	// err = dynamo.Put(entity)
	// return
}

//-----------------------------------------------------------------------------
//
// Pattern Match
//
//-----------------------------------------------------------------------------

// s3Seq is an iterator over matched results
type s3Seq struct {
	at    int
	items []*string
	err   error
	s3    *S3
}

// s3Gen is type alias for generic representation
type s3Gen struct {
	s3  *S3
	key *string
}

// Lifts generic representation to Thing
func (gen s3Gen) To(thing iri.Thing) error {
	req := &s3.GetObjectInput{
		Bucket: gen.s3.bucket,
		Key:    gen.key,
	}
	val, err := gen.s3.db.GetObject(req)
	if err != nil {
		return err
	}

	return json.NewDecoder(val.Body).Decode(thing)
}

// FMap transforms sequence
func (seq *s3Seq) FMap(f FMap) ([]iri.Thing, error) {
	things := []iri.Thing{}
	for _, entity := range seq.items {
		thing, err := f(s3Gen{s3: seq.s3, key: entity})
		if err != nil {
			return nil, err
		}
		things = append(things, thing)
	}
	return things, nil
}

// Head selects the first element of matched collection.
func (seq *s3Seq) Head(thing iri.Thing) error {
	if seq.at == -1 {
		seq.at++
	}

	return s3Gen{s3: seq.s3, key: seq.items[seq.at]}.To(thing)
}

// Tail selects the all elements except the first one
func (seq *s3Seq) Tail() bool {
	seq.at++
	return seq.err == nil && seq.at < len(seq.items)
}

// Error indicates if any error appears during I/O
func (seq *s3Seq) Error() error {
	return seq.err
}

// Match applies a pattern matching to elements in the bucket
func (dynamo S3) Match(key iri.Thing) Seq {
	req := &s3.ListObjectsV2Input{
		Bucket:  dynamo.bucket,
		MaxKeys: aws.Int64(1000),
		Prefix:  aws.String(key.Identity().Path()),
	}

	val, err := dynamo.db.ListObjectsV2(req)
	if err != nil {
		return &s3Seq{-1, nil, err, nil}
	}

	items := make([]*string, 0)
	for _, x := range val.Contents {
		items = append(items, x.Key)
	}

	return &s3Seq{-1, items, nil, &dynamo}
}

//-----------------------------------------------------------------------------
//
// Streaming
//
//-----------------------------------------------------------------------------

// Recv establishes bytes stream to S3 object
func (dynamo S3) Recv(entity iri.Thing) (io.ReadCloser, error) {
	req := &s3.GetObjectInput{
		Bucket: dynamo.bucket,
		Key:    aws.String(entity.Identity().Path()),
	}

	item, _ := dynamo.db.GetObjectRequest(req)
	url, err := item.Presign(20 * time.Minute)
	if err != nil {
		return nil, err
	}

	api := &http.Client{
		Transport: &http.Transport{
			DisableKeepAlives: true,
			ReadBufferSize:    1024 * 1024,
			Dial: (&net.Dialer{
				Timeout: 10 * time.Second,
			}).Dial,
		},
	}
	eg, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	eg.Header.Add("Connection", "close")
	eg.Header.Add("Transfer-Encoding", "chunked")

	in, err := api.Do(eg)
	if err != nil {
		return nil, err
	}

	return in.Body, nil
}
