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
    return dynamo.Encode(av, x.ID, tStruct(x))
  }
*/
func Encode(av *dynamodb.AttributeValue, id curie.IRI, val interface{}) error {
	gen, err := dynamodbattribute.Marshal(val)
	if err != nil {
		return err
	}

	uid, err := dynamodbattribute.Marshal(IRI(id))
	if err != nil {
		return err
	}

	gen.M["id"] = uid

	*av = *gen
	return nil
}

/*

Decode is a helper function to decode core domain types from Dynamo DB format.
The helper ensures compact URI de-serialization from DynamoDB schema.

  func (x *MyType) UnmarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
    type tStruct *MyType
    return dynamo.Decode(av, &x.ID, tStruct(x))
  }
*/
func Decode(av *dynamodb.AttributeValue, id *curie.IRI, val interface{}) error {
	dynamodbattribute.Unmarshal(av, val)

	var xx IRI
	dynamodbattribute.Unmarshal(av.M["id"], &xx)
	*id = curie.IRI(xx)

	return nil
}

/*


ID is compact URI (CURIE) type for struct tagging, It declares unique identity
of a thing. The tagged struct belongs to Thing category so that the struct is
manageable by dynamo interfaces

  type MyStruct struct {
    dynamo.ID
  }
*/
type ID struct {
	IRI IRI `dynamodbav:"id" json:"@id"`
}

/*

TODO: NewIDf
NewID transform category of strings to dynamo.ID.
*/
func NewID(iri string, args ...interface{}) ID {
	return ID{IRI(curie.New(iri, args...))}
}

/*

TODO: NewID
MkID transform category of curie.IRI to dynamo.ID.
*/
func MkID(iri curie.IRI) ID {
	return ID{IRI(iri)}
}

/*

Identity makes CURIE compliant to Thing interface so that embedding ID makes any
struct to be Thing.
*/
func (id ID) Identity() curie.IRI {
	return curie.IRI(id.IRI)
}

/*

Ref return reference to dynamo.IRI
*/
func (id ID) Unwrap() *IRI {
	return &id.IRI
}

/*

IDs sequence of Identities
*/
type IDs []ID

/*

Join lifts sequence of matched objects to seq of IDs
	seq := dynamo.IDs{}
	dynamo.Match(...).FMap(seq.Join)
*/
func (seq *IDs) Join(gen Gen) error {
	iri, err := gen.ID()
	if err != nil {
		return err
	}

	*seq = append(*seq, MkID(*iri))
	return nil
}
