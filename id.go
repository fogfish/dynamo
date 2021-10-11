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
