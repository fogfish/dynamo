package keyval

// func NewV2[T dynamo.Thing](
// 	uri string,
// 	defSession ...*session.Session,
// ) (dynamo.KeyVal[T], error) {
// 	awsSession, err := maybeNewSession(defSession)
// 	if err != nil {
// 		return nil, err
// 	}

// 	creator, spec, err := factoryV2[T](uri, defSession...)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return creator(awsSession, spec), nil
// }

// func Stream[T dynamo.Stream](
// 	uri string,
// 	defSession ...*session.Session,
// ) (dynamo.KeyVal[T], error) {
// 	awsSession, err := maybeNewSession(defSession)
// 	if err != nil {
// 		return nil, err
// 	}

// 	creator, spec, err := factoryVI[T](uri, defSession...)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return creator(awsSession, spec), nil
// }

// /*

// creator is a factory function
// */
// type creatorV2[T dynamo.Thing] func(io *session.Session, spec *dynamo.URL) dynamo.KeyVal[T]

// //
// func factoryV2[T dynamo.Thing](uri string, defSession ...*session.Session) (creatorV2[T], *dynamo.URL, error) {
// 	spec, err := url.Parse(uri)
// 	if err != nil {
// 		return nil, nil, err
// 	}

// 	switch {
// 	case spec == nil:
// 		return nil, nil, fmt.Errorf("Invalid url: %s", uri)
// 	case len(spec.Path) < 2:
// 		return nil, nil, fmt.Errorf("Invalid url, path to data storage is not defined: %s", uri)
// 	// case spec.Scheme == "s3":
// 	// 	return s3.New[T], (*dynamo.URL)(spec), nil
// 	case spec.Scheme == "ddb":
// 		return ddb.New[T], (*dynamo.URL)(spec), nil
// 	default:
// 		return nil, nil, fmt.Errorf("Unsupported schema: %s", uri)
// 	}
// }

// /*

// creator is a factory function
// */
// type creatorVI[T dynamo.Stream] func(io *session.Session, spec *dynamo.URL) dynamo.KeyVal[T]

// //
// func factoryVI[T dynamo.Stream](uri string, defSession ...*session.Session) (creatorVI[T], *dynamo.URL, error) {
// 	spec, err := url.Parse(uri)
// 	if err != nil {
// 		return nil, nil, err
// 	}

// 	switch {
// 	case spec == nil:
// 		return nil, nil, fmt.Errorf("Invalid url: %s", uri)
// 	case len(spec.Path) < 2:
// 		return nil, nil, fmt.Errorf("Invalid url, path to data storage is not defined: %s", uri)
// 	// case spec.Scheme == "s3":
// 	// 	return s3.New[T], (*dynamo.URL)(spec), nil
// 	case spec.Scheme == "ddb":
// 		return ddb.New[T], (*dynamo.URL)(spec), nil
// 	// case spec.Scheme == "stream":
// 	// 	return stream.New[T], (*dynamo.URL)(spec), nil
// 	default:
// 		return nil, nil, fmt.Errorf("Unsupported schema: %s", uri)
// 	}
// }
