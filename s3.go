package dynamo

import (
	"bytes"
	"encoding/json"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/s3"
)

// S3 is a service connection handle
type S3 struct {
	io     *session.Session
	db     *s3.S3
	bucket *string
}

func newS3(bucket string) *S3 {
	io := session.Must(session.NewSession())
	db := s3.New(io)
	return &S3{io, db, aws.String(bucket)}
}

//-----------------------------------------------------------------------------
//
// Key Value
//
//-----------------------------------------------------------------------------

// Get fetches the entity identified by the key.
func (dynamo S3) Get(entity interface{}) (err error) {
	iri, err := toIRI(entity)
	if err != nil {
		return
	}

	req := &s3.GetObjectInput{
		Bucket: dynamo.bucket,
		Key:    aws.String(iri.Prefix + "/" + iri.Suffix),
	}
	val, err := dynamo.db.GetObject(req)
	if err != nil {
		switch v := err.(type) {
		case awserr.Error:
			if v.Code() == s3.ErrCodeNoSuchKey {
				return NotFound{iri}
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
func (dynamo S3) Put(entity interface{}) (err error) {
	iri, err := toIRI(entity)
	if err != nil {
		return
	}

	gen, err := json.Marshal(entity)
	if err != nil {
		return
	}

	req := &s3.PutObjectInput{
		Bucket: dynamo.bucket,
		Key:    aws.String(iri.Prefix + "/" + iri.Suffix),
		Body:   aws.ReadSeekCloser(bytes.NewReader(gen)),
	}

	_, err = dynamo.db.PutObject(req)
	return
}

// Remove discards the entity from the bucket
func (dynamo S3) Remove(entity interface{}) (err error) {
	iri, err := toIRI(entity)
	if err != nil {
		return
	}

	req := &s3.DeleteObjectInput{
		Bucket: dynamo.bucket,
		Key:    aws.String(iri.Prefix + "/" + iri.Suffix),
	}

	_, err = dynamo.db.DeleteObject(req)
	return

}

// Update applies a partial patch to entity and returns new values
func (dynamo S3) Update(entity interface{}) (err error) {
	par, err := dynamodbattribute.MarshalMap(entity)
	if err != nil {
		return
	}

	dynamo.Get(entity)
	gen, err := dynamodbattribute.MarshalMap(entity)
	if err != nil {
		return
	}

	for keyA, valA := range par {
		gen[keyA] = valA
	}

	err = dynamodbattribute.UnmarshalMap(gen, entity)
	if err != nil {
		return
	}

	err = dynamo.Put(entity)
	return
}

//-----------------------------------------------------------------------------
//
// Pattern Match
//
//-----------------------------------------------------------------------------

// SeqS3 is an iterator over match results
type SeqS3 struct {
	at    int
	items []*string
	err   error
	s3    *S3
}

// Head selects the first element of matched collection.
func (seq *SeqS3) Head(v interface{}) error {
	if seq.at == -1 {
		seq.at++
	}

	req := &s3.GetObjectInput{
		Bucket: seq.s3.bucket,
		Key:    seq.items[seq.at],
	}
	val, err := seq.s3.db.GetObject(req)
	if err != nil {
		seq.err = err
		return err
	}
	return json.NewDecoder(val.Body).Decode(v)
}

// Tail selects the all elements except the first one
func (seq *SeqS3) Tail() bool {
	seq.at++
	return seq.err == nil && seq.at < len(seq.items)
}

// Error indicates if any error appears during I/O
func (seq *SeqS3) Error() error {
	return seq.err
}

// Match applies a pattern matching to elements in the bucket
func (dynamo S3) Match(key interface{}) Seq {
	iri, err := toIRI(key)
	if err != nil {
		return &SeqS3{-1, nil, err, nil}
	}

	req := &s3.ListObjectsV2Input{
		Bucket:  dynamo.bucket,
		MaxKeys: aws.Int64(1000),
		Prefix:  aws.String(iri.Prefix),
	}

	val, err := dynamo.db.ListObjectsV2(req)
	if err != nil {
		return &SeqS3{-1, nil, err, nil}
	}

	items := make([]*string, 0)
	for _, x := range val.Contents {
		items = append(items, x.Key)
	}

	return &SeqS3{-1, items, nil, &dynamo}
}

//-----------------------------------------------------------------------------
//
// internal helpers
//
//-----------------------------------------------------------------------------

func toIRI(val interface{}) (IRI, error) {
	gen, err := dynamodbattribute.MarshalMap(val)
	if err != nil {
		return IRI{}, err
	}

	prefix, isPrefix := gen["prefix"]
	suffix, isSuffix := gen["suffix"]

	if isPrefix && isSuffix {
		return IRI{aws.StringValue(prefix.S), aws.StringValue(suffix.S)}, nil
	}

	if isPrefix {
		return IRI{Prefix: aws.StringValue(prefix.S)}, nil
	}

	return IRI{}, nil
}
