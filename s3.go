package dynamo

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"reflect"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/fogfish/curie"
)

// S3 is a service connection handle
type S3 struct {
	io     *session.Session
	db     s3iface.S3API
	bucket *string
}

func newS3(io *session.Session, spec *dbURL) *S3 {
	db := &S3{io: io, db: s3.New(io)}

	// config bucket name
	seq := spec.segments(2)
	db.bucket = seq[0]

	return db
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
func (dynamo S3) Get(entity Thing) (err error) {
	req := &s3.GetObjectInput{
		Bucket: dynamo.bucket,
		Key:    aws.String(curie.Path(entity.Identity())),
	}
	val, err := dynamo.db.GetObject(req)
	if err != nil {
		switch v := err.(type) {
		case awserr.Error:
			if v.Code() == s3.ErrCodeNoSuchKey {
				return NotFound{curie.Path(entity.Identity())}
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
func (dynamo S3) Put(entity Thing, _ ...Constrain) (err error) {
	gen, err := json.Marshal(entity)
	if err != nil {
		return
	}

	req := &s3.PutObjectInput{
		Bucket: dynamo.bucket,
		Key:    aws.String(curie.Path(entity.Identity())),
		Body:   aws.ReadSeekCloser(bytes.NewReader(gen)),
	}

	_, err = dynamo.db.PutObject(req)
	return
}

// Remove discards the entity from the bucket
func (dynamo S3) Remove(entity Thing, _ ...Constrain) (err error) {
	req := &s3.DeleteObjectInput{
		Bucket: dynamo.bucket,
		Key:    aws.String(curie.Path(entity.Identity())),
	}

	_, err = dynamo.db.DeleteObject(req)
	return

}

type tGen map[string]interface{}

func (z tGen) Identity() curie.IRI { return z["id"].(curie.IRI) }

// Update applies a partial patch to entity and returns new values
func (dynamo S3) Update(entity Thing, _ ...Constrain) (err error) {
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

	err = json.Unmarshal(genbin, &entity)
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

// s3Gen is type alias for generic representation
type s3Gen struct {
	s3  *S3
	key *string
}

// ID lifts generic representation to its Identity
func (gen s3Gen) ID() (*ID, error) {
	if gen.key == nil {
		return nil, errors.New("End Of Stream")
	}

	var id ID
	seq := strings.SplitN(*gen.key, "/", 2)
	switch {
	case len(seq) == 2:
		id = MkID(curie.New(strings.Join(seq, ":")))
	default:
		id = MkID(curie.New(*gen.key))
	}

	return &id, nil
}

// Lifts generic representation to Thing
func (gen s3Gen) To(thing Thing) error {
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

// s3Seq is an iterator over matched results
type s3Seq struct {
	s3     *S3
	q      *s3.ListObjectsV2Input
	at     int
	items  []*string
	stream bool
	err    error
}

func mkS3Seq(s3 *S3, q *s3.ListObjectsV2Input, err error) *s3Seq {
	return &s3Seq{
		s3:     s3,
		q:      q,
		at:     0,
		items:  nil,
		stream: true,
		err:    err,
	}
}

func (seq *s3Seq) maybeSeed() error {
	if !seq.stream {
		return fmt.Errorf("End of Stream")
	}

	return seq.seed()
}

func (seq *s3Seq) seed() error {
	if seq.items != nil && seq.q.StartAfter == nil {
		return fmt.Errorf("End of Stream")
	}

	val, err := seq.s3.db.ListObjectsV2(seq.q)
	if err != nil {
		seq.err = err
		return err
	}

	if *val.KeyCount == 0 {
		return fmt.Errorf("End of Stream")
	}

	items := make([]*string, 0)
	for _, x := range val.Contents {
		items = append(items, x.Key)
	}

	seq.at = 0
	seq.items = items
	if len(items) > 0 && val.NextContinuationToken != nil {
		seq.q.StartAfter = items[len(items)-1]
	}
	return nil
}

// FMap transforms sequence
func (seq *s3Seq) FMap(f FMap) ([]Thing, error) {
	things := []Thing{}
	for seq.Tail() {
		thing, err := f(s3Gen{s3: seq.s3, key: seq.items[seq.at]})
		if err != nil {
			return nil, err
		}
		things = append(things, thing)
	}
	return things, nil
}

// Head selects the first element of matched collection.
func (seq *s3Seq) Head(thing Thing) error {
	if seq.items == nil {
		if err := seq.seed(); err != nil {
			return err
		}
	}

	return s3Gen{s3: seq.s3, key: seq.items[seq.at]}.To(thing)
}

// Tail selects the all elements except the first one
func (seq *s3Seq) Tail() bool {
	seq.at++

	switch {
	case seq.err != nil:
		return false
	case seq.items == nil:
		err := seq.seed()
		return err == nil
	case seq.err == nil && seq.at >= len(seq.items):
		err := seq.maybeSeed()
		return err == nil
	default:
		return true
	}
}

// Cursor is the global position in the sequence
func (seq *s3Seq) Cursor() *curie.IRI {
	if seq.q.StartAfter != nil {
		iri := curie.New(aws.StringValue(seq.q.StartAfter))
		return &iri
	}
	return nil
}

// Error indicates if any error appears during I/O
func (seq *s3Seq) Error() error {
	return seq.err
}

// Limit sequence to N elements
func (seq *s3Seq) Limit(n int64) Seq {
	seq.q.MaxKeys = aws.Int64(n)
	seq.stream = false
	return seq
}

// Continue limited sequence from the cursor
func (seq *s3Seq) Continue(cursor *curie.IRI) Seq {
	if cursor != nil {
		seq.q.StartAfter = aws.String(curie.Path(*cursor))
	}
	return seq
}

// Reverse order of sequence
func (seq *s3Seq) Reverse() Seq {
	return seq
}

// Match applies a pattern matching to elements in the bucket
func (dynamo S3) Match(key Thing) Seq {
	req := &s3.ListObjectsV2Input{
		Bucket:  dynamo.bucket,
		MaxKeys: aws.Int64(1000),
		Prefix:  aws.String(curie.Path(key.Identity())),
	}

	return mkS3Seq(&dynamo, req, nil)
}

//-----------------------------------------------------------------------------
//
// Streaming
//
//-----------------------------------------------------------------------------

// URL returns absolute URL downloadable using HTTPS protocol
func (dynamo S3) URL(entity Thing, expire time.Duration) (string, error) {
	req := &s3.GetObjectInput{
		Bucket: dynamo.bucket,
		Key:    aws.String(curie.Path(entity.Identity())),
	}

	item, _ := dynamo.db.GetObjectRequest(req)
	return item.Presign(expire)
}

// Recv establishes ingress bytes stream to S3 object
func (dynamo S3) Recv(entity Thing) (io.ReadCloser, error) {
	url, err := dynamo.URL(entity, 20*time.Minute)
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

// Send establishes egress bytes stream to S3 object
func (dynamo S3) Send(entity Thing, stream io.Reader, opts ...Content) error {
	up := s3manager.NewUploader(dynamo.io)

	req := &s3manager.UploadInput{
		Bucket: dynamo.bucket,
		Key:    aws.String(curie.Path(entity.Identity())),
		Body:   stream,
	}

	for _, f := range opts {
		f(req)
	}
	_, err := up.Upload(req)

	return err
}

// Content configures properties of content distribution
type Content func(*s3manager.UploadInput)

// UseHTTP type create configuration for Content distribution
type UseHTTP string

const (
	// HTTP is default configuration for Content distribution using HTTP
	HTTP = UseHTTP("content.http")
)

// CacheControl header
func (UseHTTP) CacheControl(val string) Content {
	return func(x *s3manager.UploadInput) {
		x.CacheControl = aws.String(val)
	}
}

// ContentEncoding header
func (UseHTTP) ContentEncoding(val string) Content {
	return func(x *s3manager.UploadInput) {
		x.ContentEncoding = aws.String(val)
	}
}

// ContentLanguage header
func (UseHTTP) ContentLanguage(val string) Content {
	return func(x *s3manager.UploadInput) {
		x.ContentLanguage = aws.String(val)
	}
}

// ContentType header
func (UseHTTP) ContentType(val string) Content {
	return func(x *s3manager.UploadInput) {
		x.ContentType = aws.String(val)
	}
}

// Expires header
func (UseHTTP) Expires(val time.Time) Content {
	return func(x *s3manager.UploadInput) {
		x.Expires = &val
	}
}
