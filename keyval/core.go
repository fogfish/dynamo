//
// Copyright (C) 2019 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

package keyval

// import (
// 	"fmt"
// 	"net/url"
// 	"strings"

// 	"github.com/aws/aws-sdk-go/aws/session"
// )

// /*

// NewNoContext creates a client to access Things at AWS storage service.
// The connection URI controls the access parameters.

// Supported scheme:
//   s3:///my-bucket
//   ddb:///my-table/my-index?prefix=hashkey&suffix=sortkey
// */
// func NewNoContext(uri string, defSession ...*session.Session) (KeyValNoContext, error) {
// 	kv, err := New(uri, defSession...)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return NewKeyValContextDefault(kv), nil
// }

// /*

// ReadOnly establishes read-only connection with AWS Storage service.
// */
// func ReadOnly(uri string, defSession ...*session.Session) (KeyValReader, error) {
// 	return New(uri, defSession...)
// }

// /*

// ReadOnlyNoContext establishes read-only connection with AWS Storage service.
// */
// func ReadOnlyNoContext(uri string, defSession ...*session.Session) (KeyValReaderNoContext, error) {
// 	return NewNoContext(uri, defSession...)
// }

// /*
// MustReadOnly is a helper function to ensure KeyValReader interface is valid and there was no
// error when calling a New function.

// This helper is intended to be used in variable initialization to load the
// interface and configuration at startup. Such as:

//    var io = dynamo.MustReadOnly(dynamo.ReadOnly())
// */
// func MustReadOnly(kv KeyValReader, err error) KeyValReader {
// 	if err != nil {
// 		panic(err)
// 	}
// 	return kv
// }

// /*

// NewStream establishes bytes stream connection with AWS Storage service,
// use URI to specify service and name of the bucket.
// Supported scheme:
//   s3:///my-bucket
// */
// func NewStream(uri string, defSession ...*session.Session) (Stream, error) {
// 	awsSession, err := maybeNewSession(defSession)
// 	if err != nil {
// 		return nil, err
// 	}

// 	creator, spec, err := factory(uri, defSession...)
// 	if err != nil {
// 		return nil, err
// 	}

// 	keyval := creator(awsSession, spec)
// 	stream, ok := keyval.(Stream)
// 	if !ok {
// 		return nil, fmt.Errorf("Streaming is not supported by %s", uri)
// 	}

// 	return stream, nil
// }

// /*

// NewStreamNoContext establishes bytes stream connection with AWS Storage service,
// use URI to specify service and name of the bucket.
// Supported scheme:
//   s3:///my-bucket
// */
// func NewStreamNoContext(uri string, defSession ...*session.Session) (StreamNoContext, error) {
// 	stream, err := NewStream(uri, defSession...)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return NewStreamContextDefault(stream), nil
// }

// /*

// MustStream is a helper function to ensure KeyValReader interface is valid and there was no
// error when calling a New function.

// This helper is intended to be used in variable initialization to load the
// interface and configuration at startup. Such as:

//    var io = dynamo.MustStream(dynamo.Stream())
// */
// func MustStream(stream Stream, err error) Stream {
// 	if err != nil {
// 		panic(err)
// 	}
// 	return stream
// }

// /*

// creator is a factory function
// */
// type creator func(io *session.Session, spec *dbURL) KeyVal

// //
// func factory(uri string, defSession ...*session.Session) (creator, *dbURL, error) {
// 	spec, _ := url.Parse(uri)
// 	switch {
// 	case spec == nil:
// 		return nil, nil, fmt.Errorf("Invalid url: %s", uri)
// 	case len(spec.Path) < 2:
// 		return nil, nil, fmt.Errorf("Invalid url, path to data storage is not defined: %s", uri)
// 	case spec.Scheme == "s3":
// 		return newS3, (*dbURL)(spec), nil
// 	case spec.Scheme == "ddb":
// 		return newDynamo, (*dbURL)(spec), nil
// 	default:
// 		return nil, nil, fmt.Errorf("Unsupported schema: %s", uri)
// 	}
// }

// //
// func maybeNewSession(defSession []*session.Session) (*session.Session, error) {
// 	if len(defSession) != 0 {
// 		return defSession[0], nil
// 	}

// 	awsSession, err := session.NewSessionWithOptions(session.Options{
// 		SharedConfigState: session.SharedConfigEnable,
// 	})

// 	if err != nil {
// 		return nil, err
// 	}

// 	return awsSession, nil
// }

// /*

// dbURL custom type with helper functions
// */
// type dbURL url.URL

// // query parameters
// func (uri *dbURL) query(key, def string) string {
// 	val := (*url.URL)(uri).Query().Get(key)

// 	if val == "" {
// 		return def
// 	}

// 	return val
// }

// // path segments of length
// func (uri *dbURL) segments(n int) []*string {
// 	seq := make([]*string, n)

// 	seg := strings.Split((*url.URL)(uri).Path, "/")[1:]
// 	for i, x := range seg {
// 		val := x
// 		seq[i] = &val
// 	}

// 	return seq
// }
