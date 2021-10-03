//
// Copyright (C) 2019 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

package dynamo

import (
	"encoding/json"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/fogfish/curie"
)

/*

IRI is an alias to compact URI type.
The alias ensures compact URI serialization into DynamoDB schema.
*/
type IRI curie.IRI

/*

NewIRI transform category of strings to dynamo.IRI
*/
func NewIRI(iri string, args ...interface{}) IRI {
	return IRI(curie.New(iri, args...))
}

// String is helper function to transform IRI to string
func (iri IRI) String() string {
	return curie.IRI(iri).String()
}

/*

MarshalDynamoDBAttributeValue `IRI ⟼ "prefix:suffix"`
*/
func (iri IRI) MarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	if curie.Rank(curie.IRI(iri)) == 0 {
		av.NULL = aws.Bool(true)
		return nil
	}

	// Note: we are using string representation to allow linked data in dynamo tables
	val, err := dynamodbattribute.Marshal(curie.IRI(iri).String())
	if err != nil {
		return err
	}

	av.S = val.S
	return nil
}

/*

UnmarshalDynamoDBAttributeValue `"prefix:suffix" ⟼ IRI`
*/
func (iri *IRI) UnmarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	*iri = IRI(curie.New(aws.StringValue(av.S)))
	return nil
}

/*
MarshalJSON `IRI ⟼ "[prefix:suffix]"`
*/
func (iri IRI) MarshalJSON() ([]byte, error) {
	return json.Marshal(curie.IRI(iri))
}

/*
UnmarshalJSON `"[prefix:suffix]" ⟼ IRI`
*/
func (iri *IRI) UnmarshalJSON(b []byte) error {
	var val curie.IRI

	err := json.Unmarshal(b, &val)
	if err != nil {
		return err
	}

	*iri = IRI(val)
	return nil
}

/*

Encode is a helper function to encode core domain types into struct.
The helper ensures compact URI serialization into DynamoDB schema.

  func (x MyType) MarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
    type tStruct MyType
    return dynamo.Encode(av, &x.HashKey, &x.SortKey, tStruct(x))
  }
*/
func Encode(av *dynamodb.AttributeValue, hashkey, sortkey, val interface{}) error {
	gen, err := dynamodbattribute.Marshal(val)
	if err != nil {
		return err
	}

	if gen.M == nil {
		gen.M = make(map[string]*dynamodb.AttributeValue)
	}

	if hashkey != nil {
		hkey, err := dynamodbattribute.Marshal(hashkey)
		if err != nil {
			return err
		}
		if hkey.S != nil {
			gen.M["__prefix"] = hkey
		}
	}

	if sortkey != nil {
		skey, err := dynamodbattribute.Marshal(sortkey)
		if err != nil {
			return err
		}
		if skey.S != nil {
			gen.M["__suffix"] = skey
		}
	}

	*av = *gen
	return nil
}

/*

Decode is a helper function to decode core domain types from Dynamo DB format.
The helper ensures compact URI de-serialization from DynamoDB schema.

  func (x *MyType) UnmarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
    type tStruct *MyType
    return dynamo.Decode(av, &x.HashKey, &x.SortKey, tStruct(x))
  }
*/
func Decode(av *dynamodb.AttributeValue, hashkey, sortkey, val interface{}) error {
	dynamodbattribute.Unmarshal(av, val)

	if hkey, exists := av.M["__prefix"]; exists {
		if err := dynamodbattribute.Unmarshal(hkey, hashkey); err != nil {
			return err
		}
	}

	if skey, exists := av.M["__suffix"]; exists {
		if err := dynamodbattribute.Unmarshal(skey, sortkey); err != nil {
			return err
		}
	}

	return nil
}

/*

Identities sequence of Identities
*/
type Identities [][]string

/*

Join lifts sequence of matched objects to seq of IDs
	seq := dynamo.IDs{}
	dynamo.Match(...).FMap(seq.Join)
*/
func (seq *Identities) Join(gen Gen) error {
	prefix, suffix := gen.ID()

	*seq = append(*seq, []string{prefix, suffix})
	return nil
}
