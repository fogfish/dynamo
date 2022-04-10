package session

import (
	"fmt"
	"net/url"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/fogfish/dynamo"
	"github.com/fogfish/dynamo/internal/ddb"
)

func NewV2[T dynamo.ThingV2](
	uri string,
	defSession ...*session.Session,
) (dynamo.KeyValV2[T], error) {
	awsSession, err := maybeNewSession(defSession)
	if err != nil {
		return nil, err
	}

	creator, spec, err := factoryV2[T](uri, defSession...)
	if err != nil {
		return nil, err
	}

	return creator(awsSession, spec), nil
}

/*

creator is a factory function
*/
type creatorV2[T dynamo.ThingV2] func(io *session.Session, spec *dynamo.URL) dynamo.KeyValV2[T]

//
func factoryV2[T dynamo.ThingV2](uri string, defSession ...*session.Session) (creatorV2[T], *dynamo.URL, error) {
	spec, err := url.Parse(uri)
	if err != nil {
		return nil, nil, err
	}

	switch {
	case spec == nil:
		return nil, nil, fmt.Errorf("Invalid url: %s", uri)
	case len(spec.Path) < 2:
		return nil, nil, fmt.Errorf("Invalid url, path to data storage is not defined: %s", uri)
	// case spec.Scheme == "s3":
	// 	return newS3, (*dbURL)(spec), nil
	case spec.Scheme == "ddb":
		f := ddb.New[T]
		return f, (*dynamo.URL)(spec), nil
	default:
		return nil, nil, fmt.Errorf("Unsupported schema: %s", uri)
	}
}

//
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
