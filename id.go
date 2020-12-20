//
// Copyright (C) 2019 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

package dynamo

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/fogfish/curie"
)

/*

IRI is an alias to compact URI type.
The alias ensures compact URI serialization into DynamoDB schema.
*/
type IRI struct{ curie.IRI }

/*

MarshalDynamoDBAttributeValue `IRI ⟼ "prefix:suffix"`
*/
func (iri IRI) MarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	if iri.Rank() == 0 {
		av.NULL = aws.Bool(true)
		return nil
	}

	// Note: we are using string representation to allow linked data in dynamo tables
	val, err := dynamodbattribute.Marshal(iri.String())
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
	*iri = IRI{curie.New(aws.StringValue(av.S))}
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
	IRI IRI `dynamodbav:"id" json:"id"`
}

/*

NewID transform category of strings to dynamo.ID.
*/
func NewID(iri string, args ...interface{}) ID {
	return ID{IRI{curie.New(iri, args...)}}
}

/*

MkID transform category of curie.IRI to dynamo.ID.
*/
func MkID(iri curie.IRI) ID {
	return ID{IRI{iri}}
}

/*

Identity makes CURIE compliant to Thing interface so that embedding ID makes any
struct to be Thing.
*/
func (id ID) Identity() curie.IRI {
	return id.IRI.IRI
}

/*


Thing is the most generic type of item. The interfaces declares anything with
unique identifier. Embedding CURIE ID into struct makes it Thing compatible.
*/
type Thing interface {
	// The identifier property represents any kind of identifier for
	// any kind of Thing
	Identity() curie.IRI
}
