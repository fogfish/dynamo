//
// Copyright (C) 2022 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

package ddb

import (
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/fogfish/dynamo/v2"
	"github.com/fogfish/golem/pure/hseq"
)

//
// Internal data structure to manage type schema
//

// Schema is utility that decodes type into projection expression
type schema[T dynamo.Thing] struct {
	ExpectedAttributeNames map[string]string
	Projection             *string
}

func newSchema[T dynamo.Thing]() *schema[T] {
	seq := hseq.FMap(
		hseq.New[T](),
		func(t hseq.Type[T]) string {
			tag := t.StructField.Tag.Get("dynamodbav")
			key := strings.Split(tag, ",")
			if len(key) == 0 {
				return t.Name
			}
			return key[0]
		},
	)

	names := make(map[string]string, len(seq))
	attrs := make([]string, len(seq))

	for i, x := range seq {
		name := "#__" + x + "__"
		names[name] = x
		attrs[i] = name
	}

	return &schema[T]{
		ExpectedAttributeNames: names,
		Projection:             aws.String(strings.Join(attrs, ", ")),
	}
}
