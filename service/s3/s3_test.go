//
// Copyright (C) 2022 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

package s3_test

import (
	"context"
	"testing"

	"github.com/fogfish/curie"
	"github.com/fogfish/dynamo/v2/internal/dynamotest"
	"github.com/fogfish/dynamo/v2/internal/s3test"
	"github.com/fogfish/it"
)

func codec(p dynamotest.Person) (dynamotest.Person, error) {
	return p, nil
}

func TestS3(t *testing.T) {
	dynamotest.TestGet(t, codec, s3test.GetObject[dynamotest.Person])
	dynamotest.TestPut(t, codec, s3test.PutObject[dynamotest.Person])
	dynamotest.TestRemove(t, codec, s3test.DeleteObject[dynamotest.Person])
	dynamotest.TestUpdate(t, codec, s3test.GetPutObject[dynamotest.Person])

	dynamotest.TestMatch(t, codec, s3test.GetListObjects[dynamotest.Person])
}

//-----------------------------------------------------------------------------
//
// Corrupted Update
//
//-----------------------------------------------------------------------------

// dynamodbattribute.MarshalMap / dynamodbattribute.UnmarshalMap corrupts struct(s)
// it do not resets the slice to zero when decoding generic structure back to the interface
// as the result old values might leakout while doing s3 update
// this test case ensures correctness of update function
type seqItem struct {
	ID    string `json:"id,omitempty"`
	Flag  bool   `json:"flag,omitempty"`
	Label string `json:"label,omitempty"`
}

type seqType struct {
	ID   string    `json:"id,omitempty"`
	List []seqItem `json:"list,omitempty"`
}

func (seq seqType) HashKey() curie.IRI { return curie.IRI(seq.ID) }
func (seq seqType) SortKey() curie.IRI { return "" }

func seqLong() seqType {
	return seqType{
		ID: "seq",
		List: []seqItem{
			{ID: "1", Flag: true, Label: "a"},
			{ID: "2", Flag: true, Label: "b"},
			{ID: "3", Label: "c"},
			{ID: "4", Label: "d"},
		},
	}
}

func seqShort() seqType {
	return seqType{
		ID: "seq",
		List: []seqItem{
			{ID: "5", Label: "e"},
			{ID: "6", Label: "f"},
		},
	}
}

func TestSeqS3Update(t *testing.T) {
	valS := seqShort()
	valL := seqLong()
	api := s3test.GetPutObject(&seqType{ID: "seq"}, &valS, &valL)

	val, err := api.Update(context.TODO(), valS)

	it.Ok(t).
		If(err).Should().Equal(nil).
		If(val).Should().Equal(valS)
}
