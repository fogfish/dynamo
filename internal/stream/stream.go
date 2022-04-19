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
type ds3[T dynamo.StreamVI] struct {
	io     *session.Session
	s3     s3iface.S3API
	codec  Codec
	bucket *string
}

func New[T dynamo.StreamVI](
	io *session.Session,
	spec *dynamo.URL,
) dynamo.KeyValV2[T] {
	db := &ds3[T]{io: io, s3: s3.New(io)}

	// config bucket name
	seq := spec.Segments(2)
	db.bucket = seq[0]

	//
	db.codec = Codec{}

	return db
	//dynamo.KeyValV2[T](db).(dynamo.KeyValV2[T])
	// db
	//dynamo.KeyValV2[dynamo.StreamVI](db)
	//.(dynamo.KeyValV2[T])
}

//-----------------------------------------------------------------------------
//
// Key Value
//
//-----------------------------------------------------------------------------

func (db *ds3[T]) sourceURL(ctx context.Context, key T, expire time.Duration) (string, error) {
	req := &s3.GetObjectInput{
		Bucket: db.bucket,
		Key:    aws.String(db.codec.EncodeKey(key)),
	}

	item, _ := db.s3.GetObjectRequest(req)
	item.SetContext(ctx)
	return item.Presign(expire)
}

// Get item from storage
func (db *ds3[T]) Get(ctx context.Context, key T) (*T, error) {
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

	yy := key.New(in.Body).(T)
	return &yy, nil
}

// Put writes entity
func (db *ds3[T]) Put(ctx context.Context, entity T, config ...dynamo.ConstrainV2[T]) error {
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
func (db *ds3[T]) Remove(ctx context.Context, key T, config ...dynamo.ConstrainV2[T]) error {
	req := &s3.DeleteObjectInput{
		Bucket: db.bucket,
		Key:    aws.String(db.codec.EncodeKey(key)),
	}

	_, err := db.s3.DeleteObjectWithContext(ctx, req)

	return err
}

// Update applies a partial patch to entity and returns new values
func (db *ds3[T]) Update(ctx context.Context, entity T, config ...dynamo.ConstrainV2[T]) (*T, error) {
	return nil, nil
}

func (db *ds3[T]) Match(ctx context.Context, key T) dynamo.SeqV2[T] {
	return nil
}
