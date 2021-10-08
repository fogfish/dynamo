//
// Copyright (C) 2019 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

package dynamo

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

/*

Custom codec for struct
*/

/*

Type ...
*/
type Type struct{ reflect.Type }

/*

Lens ...
*/
type Lens []string

/*

Coder ...
*/
type Coder func(map[string]*dynamodb.AttributeValue) (map[string]*dynamodb.AttributeValue, error)

/*

Codec ...
*/
func Codec(t interface{}) Type {
	typeof := reflect.TypeOf(t)
	if typeof.Kind() == reflect.Ptr {
		typeof = typeof.Elem()
	}

	return Type{typeof}
}

/*

Fields ...
*/
func (t Type) Fields(names ...string) Lens {
	lens := make([]string, len(names))

	for i, name := range names {
		field, exists := t.Type.FieldByName(name)
		if !exists {
			panic(fmt.Errorf("Type %s does not have field %s", t.Type.Name(), name))
		}

		tag := field.Tag.Get("dynamodbav")
		if tag == "" {
			panic(fmt.Errorf("Type %s does not have dynamodbav tag on field %s", t.Type.Name(), name))
		}

		lens[i] = strings.Split(tag, ",")[0]
	}

	return lens
}

/*

Decode ...
*/
func (lenses Lens) Decode(vals ...interface{}) Coder {
	return func(gen map[string]*dynamodb.AttributeValue) (map[string]*dynamodb.AttributeValue, error) {
		for i, val := range vals {
			field := lenses[i]
			if gval, exists := gen[field]; exists {
				if err := dynamodbattribute.Unmarshal(gval, val); err != nil {
					return nil, err
				}
				delete(gen, field)
			}
		}
		return gen, nil
	}
}

/*

Encode ...
*/
func (lenses Lens) Encode(vals ...interface{}) Coder {
	return func(gen map[string]*dynamodb.AttributeValue) (map[string]*dynamodb.AttributeValue, error) {
		for i, val := range vals {
			gval, err := dynamodbattribute.Marshal(val)
			if err != nil {
				return nil, err
			}

			field := lenses[i]
			gen[field] = gval
		}
		return gen, nil
	}
}

/*

DecodeX ...
*/
func DecodeX(av *dynamodb.AttributeValue, val interface{}, coder Coder) (err error) {
	av.M, err = coder(av.M)
	if err != nil {
		return err
	}

	return dynamodbattribute.Unmarshal(av, val)
}

/*

EncodeX ...
*/
func EncodeX(av *dynamodb.AttributeValue, val interface{}, coder Coder) (err error) {
	gen, err := dynamodbattribute.Marshal(val)
	if err != nil {
		return err
	}

	gen.M, err = coder(gen.M)
	if err != nil {
		return err
	}

	*av = *gen
	return nil
}
