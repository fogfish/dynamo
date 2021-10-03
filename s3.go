package dynamo

import (
	"bytes"
	"context"
	"encoding/json"
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
)

// ds3 is a S3 client
type ds3 struct {
	io     *session.Session
	db     s3iface.S3API
	bucket *string
}

func newS3(io *session.Session, spec *dbURL) KeyVal {
	db := &ds3{io: io, db: s3.New(io)}

	// config bucket name
	seq := spec.segments(2)
	db.bucket = seq[0]

	return db
}

// Mock S3 I/O channel
func (dynamo *ds3) Mock(db s3iface.S3API) {
	dynamo.db = db
}

//-----------------------------------------------------------------------------
//
// Key Value
//
//-----------------------------------------------------------------------------

func (dynamo *ds3) pathOf(entity Thing) string {
	hkey, skey := entity.Identity()
	if skey == "" {
		return hkey
	}
	return hkey + "/_/" + skey
}

// Get fetches the entity identified by the key.
func (dynamo *ds3) Get(ctx context.Context, entity Thing) (err error) {
	req := &s3.GetObjectInput{
		Bucket: dynamo.bucket,
		Key:    aws.String(dynamo.pathOf(entity)),
	}
	val, err := dynamo.db.GetObjectWithContext(ctx, req)
	if err != nil {
		switch v := err.(type) {
		case awserr.Error:
			if v.Code() == s3.ErrCodeNoSuchKey {
				hkey, skey := entity.Identity()
				err = NotFound{HashKey: hkey, SortKey: skey}
			}
			return
		default:
			return
		}
	}

	err = json.NewDecoder(val.Body).Decode(entity)
	return
}

// Put writes entity
func (dynamo *ds3) Put(ctx context.Context, entity Thing, _ ...Constrain) (err error) {
	gen, err := json.Marshal(entity)
	if err != nil {
		return
	}

	req := &s3.PutObjectInput{
		Bucket: dynamo.bucket,
		Key:    aws.String(dynamo.pathOf(entity)),
		Body:   aws.ReadSeekCloser(bytes.NewReader(gen)),
	}

	_, err = dynamo.db.PutObjectWithContext(ctx, req)
	return
}

// Remove discards the entity from the bucket
func (dynamo *ds3) Remove(ctx context.Context, entity Thing, _ ...Constrain) (err error) {
	req := &s3.DeleteObjectInput{
		Bucket: dynamo.bucket,
		Key:    aws.String(dynamo.pathOf(entity)),
	}

	_, err = dynamo.db.DeleteObjectWithContext(ctx, req)
	return
}

type tGen map[string]interface{}

// Update applies a partial patch to entity and returns new values
func (dynamo *ds3) Update(ctx context.Context, entity Thing, _ ...Constrain) (err error) {
	var gen, par tGen

	req := &s3.GetObjectInput{
		Bucket: dynamo.bucket,
		Key:    aws.String(dynamo.pathOf(entity)),
	}
	val, err := dynamo.db.GetObjectWithContext(ctx, req)
	err = json.NewDecoder(val.Body).Decode(&gen)

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

	err = dynamo.Put(ctx, entity)
	return
}

//-----------------------------------------------------------------------------
//
// Pattern Match
//
//-----------------------------------------------------------------------------

// Match applies a pattern matching to elements in the bucket
func (dynamo *ds3) Match(ctx context.Context, key Thing) Seq {
	req := &s3.ListObjectsV2Input{
		Bucket:  dynamo.bucket,
		MaxKeys: aws.Int64(1000),
		Prefix:  aws.String(dynamo.pathOf(key)),
	}

	return mkS3Seq(ctx, dynamo, req, nil)
}

// s3Gen is type alias for generic representation
type s3Gen struct {
	ctx context.Context
	s3  *ds3
	key *string
}

// ID lifts generic representation to its Identity
func (gen s3Gen) ID() (string, string) {
	if gen.key == nil {
		return "", ""
	}

	seq := strings.Split(*gen.key, "/_/")
	if len(seq) == 1 {
		return seq[0], ""
	}

	return seq[0], seq[1]
}

// Lifts generic representation to Thing
func (gen s3Gen) To(thing Thing) error {
	req := &s3.GetObjectInput{
		Bucket: gen.s3.bucket,
		Key:    gen.key,
	}
	val, err := gen.s3.db.GetObjectWithContext(gen.ctx, req)
	if err != nil {
		return err
	}

	return json.NewDecoder(val.Body).Decode(thing)
}

// s3Seq is an iterator over matched results
type s3Seq struct {
	ctx    context.Context
	s3     *ds3
	q      *s3.ListObjectsV2Input
	at     int
	items  []*string
	stream bool
	err    error
}

func mkS3Seq(ctx context.Context, s3 *ds3, q *s3.ListObjectsV2Input, err error) *s3Seq {
	return &s3Seq{
		ctx:    ctx,
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

	val, err := seq.s3.db.ListObjectsV2WithContext(seq.ctx, seq.q)
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
func (seq *s3Seq) FMap(f func(Gen) error) error {
	for seq.Tail() {
		err := f(s3Gen{ctx: seq.ctx, s3: seq.s3, key: seq.items[seq.at]})
		if err != nil {
			return err
		}
	}
	return nil
}

// Head selects the first element of matched collection.
func (seq *s3Seq) Head(thing Thing) error {
	if seq.items == nil {
		if err := seq.seed(); err != nil {
			return err
		}
	}

	return s3Gen{ctx: seq.ctx, s3: seq.s3, key: seq.items[seq.at]}.To(thing)
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
func (seq *s3Seq) Cursor() (string, string) {
	if seq.q.StartAfter != nil {
		seq := strings.Split(*seq.q.StartAfter, "/_/")
		if len(seq) == 1 {
			return seq[0], ""
		}
		return seq[0], seq[1]
	}
	return "", ""
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
func (seq *s3Seq) Continue(prefix, suffix string) Seq {
	if prefix != "" {
		if suffix == "" {
			seq.q.StartAfter = aws.String(prefix)
		} else {
			seq.q.StartAfter = aws.String(prefix + "/_/" + suffix)
		}
	}
	return seq
}

// Reverse order of sequence
func (seq *s3Seq) Reverse() Seq {
	return seq
}

//-----------------------------------------------------------------------------
//
// Streaming
//
//-----------------------------------------------------------------------------

// SourceURL returns absolute URL downloadable using HTTPS protocol
func (dynamo *ds3) SourceURL(ctx context.Context, entity Thing, expire time.Duration) (string, error) {
	req := &s3.GetObjectInput{
		Bucket: dynamo.bucket,
		Key:    aws.String(dynamo.pathOf(entity)),
	}

	item, _ := dynamo.db.GetObjectRequest(req)
	item.SetContext(ctx)
	return item.Presign(expire)
}

// Recv establishes ingress bytes stream to S3 object
func (dynamo *ds3) Read(ctx context.Context, entity Thing) (io.ReadCloser, error) {
	url, err := dynamo.SourceURL(ctx, entity, 20*time.Minute)
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

// Write establishes egress bytes stream to S3 object
func (dynamo *ds3) Write(ctx context.Context, entity ThingStream, opts ...Content) error {
	up := s3manager.NewUploader(dynamo.io)
	body, err := entity.Reader()
	if err != nil {
		return err
	}

	req := &s3manager.UploadInput{
		Bucket: dynamo.bucket,
		Key:    aws.String(dynamo.pathOf(entity)),
		Body:   body,
	}

	for _, f := range opts {
		f(req)
	}
	_, err = up.UploadWithContext(ctx, req)

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
