package stream

import (
	"context"
	"net"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/fogfish/dynamo"
)

// ds3 is a S3 client
type ds3 struct {
	io     *session.Session
	s3     s3iface.S3API
	codec  Codec
	bucket *string
}

func New[T dynamo.ThingV2](
	io *session.Session,
	spec *dynamo.URL,
) dynamo.KeyValV2[T] {
	db := &ds3{io: io, s3: s3.New(io)}

	// config bucket name
	seq := spec.Segments(2)
	db.bucket = seq[0]

	//
	db.codec = Codec{}

	return dynamo.KeyValV2[dynamo.StreamV2](db).(dynamo.KeyValV2[T])
}

//-----------------------------------------------------------------------------
//
// Key Value
//
//-----------------------------------------------------------------------------

func (db *ds3) sourceURL(ctx context.Context, key dynamo.StreamV2, expire time.Duration) (string, error) {
	req := &s3.GetObjectInput{
		Bucket: db.bucket,
		Key:    aws.String(db.codec.EncodeKey(key)),
	}

	item, _ := db.s3.GetObjectRequest(req)
	item.SetContext(ctx)
	return item.Presign(expire)
}

// Get item from storage
func (db *ds3) Get(ctx context.Context, key dynamo.StreamV2) (*dynamo.StreamV2, error) {
	url, err := db.sourceURL(ctx, key, 20*time.Minute)
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

	return &dynamo.StreamV2{
		ThingV2: key,
		Reader:  in.Body,
	}, nil
}

// Put writes entity
func (db *ds3) Put(ctx context.Context, entity dynamo.StreamV2, config ...dynamo.ConstrainV2[dynamo.StreamV2]) error {
	up := s3manager.NewUploader(db.io)

	req := &s3manager.UploadInput{
		Bucket: db.bucket,
		Key:    aws.String(db.codec.EncodeKey(entity)),
		Body:   entity,
	}

	// for _, f := range opts {
	// 	f(req)
	// }
	_, err := up.UploadWithContext(ctx, req)
	return err
}

// Remove discards the entity from the table
func (db *ds3) Remove(ctx context.Context, key dynamo.StreamV2, config ...dynamo.ConstrainV2[dynamo.StreamV2]) error {
	req := &s3.DeleteObjectInput{
		Bucket: db.bucket,
		Key:    aws.String(db.codec.EncodeKey(key)),
	}

	_, err := db.s3.DeleteObjectWithContext(ctx, req)

	return err
}

// Update applies a partial patch to entity and returns new values
func (db *ds3) Update(ctx context.Context, entity dynamo.StreamV2, config ...dynamo.ConstrainV2[dynamo.StreamV2]) (*dynamo.StreamV2, error) {
	return nil, nil
}

func (db *ds3) Match(ctx context.Context, key dynamo.StreamV2) dynamo.SeqV2[dynamo.StreamV2] {
	return nil
}
