//
// Copyright (C) 2022 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

package keyval

import (
	"fmt"
	"net/url"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/fogfish/dynamo"
	"github.com/fogfish/dynamo/internal/common"
	"github.com/fogfish/dynamo/internal/ddb"
	"github.com/fogfish/dynamo/internal/s3"
)

/*

New creates a key/val client to access Things at AWS storage service.
The connection URI controls the access parameters.

Supported scheme:
  s3:///my-bucket
  ddb:///my-table/my-index?prefix=hashkey&suffix=sortkey
*/
func New[T dynamo.Thing](
	uri string,
	defSession ...*session.Session,
) (dynamo.KeyVal[T], error) {
	awsSession, err := maybeNewSession(defSession)
	if err != nil {
		return nil, err
	}

	creator, spec, err := factory[T](uri, defSession...)
	if err != nil {
		return nil, err
	}

	return creator(awsSession, spec), nil
}

/*

Must is a helper function to ensure KeyVal interface is valid and there was no
error when calling a New function.

This helper is intended to be used in variable initialization to load the
interface and configuration at startup. Such as:

  var io = dynamo.Must(dynamo.New())
*/
func Must[T dynamo.Thing](kv dynamo.KeyVal[T], err error) dynamo.KeyVal[T] {
	if err != nil {
		panic(err)
	}
	return kv
}

/*

ReadOnly establishes read-only connection with AWS Storage service.
*/
func ReadOnly[T dynamo.Thing](
	uri string,
	defSession ...*session.Session,
) (dynamo.KeyValReader[T], error) {
	return New[T](uri, defSession...)
}

/*

creator is a factory function
*/
type creator[T dynamo.Thing] func(
	io *session.Session,
	spec *common.URL,
) dynamo.KeyVal[T]

/*

parses connector url
*/
func factory[T dynamo.Thing](
	uri string,
	defSession ...*session.Session,
) (creator[T], *common.URL, error) {
	spec, err := url.Parse(uri)
	if err != nil {
		return nil, nil, err
	}

	switch {
	case spec == nil:
		return nil, nil, fmt.Errorf("Invalid url: %s", uri)
	case len(spec.Path) < 2:
		return nil, nil, fmt.Errorf("Invalid url, path to data storage is not defined: %s", uri)
	case spec.Scheme == "s3":
		return s3.New[T], (*common.URL)(spec), nil
	case spec.Scheme == "ddb":
		return ddb.New[T], (*common.URL)(spec), nil
	default:
		return nil, nil, fmt.Errorf("Unsupported schema: %s", uri)
	}
}

/*

creates default session with AWS API
*/
func maybeNewSession(defSession []*session.Session) (*session.Session, error) {
	if len(defSession) != 0 {
		return defSession[0], nil
	}

	awsSession, err := session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	})

	if err != nil {
		return nil, err
	}

	return awsSession, nil
}
