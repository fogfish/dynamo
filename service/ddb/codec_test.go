//
// Copyright (C) 2022 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

package ddb_test

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/fogfish/curie/v2"
	dynamo "github.com/fogfish/dynamo/v3/service/ddb"
	"github.com/fogfish/it"
)

// Testing custom codecs
type codecType struct{ Val string }

type codecTypeDB codecType

func (x codecTypeDB) MarshalDynamoDBAttributeValue() (types.AttributeValue, error) {
	return &types.AttributeValueMemberS{Value: x.Val}, nil
}

func (x *codecTypeDB) UnmarshalDynamoDBAttributeValue(av types.AttributeValue) error {
	tv, ok := av.(*types.AttributeValueMemberS)
	if !ok {
		return &attributevalue.UnmarshalTypeError{
			Value: fmt.Sprintf("%T", av),
			Type:  reflect.TypeOf((*codecTypeDB)(nil)),
		}
	}

	x.Val = tv.Value
	return nil
}

type codecStruct struct {
	ID   codecType `dynamodbav:"id"`
	Type codecType `dynamodbav:"type"`
	Name string    `dynamodbav:"name"`
	City string    `dynamodbav:"city"`
}

func (s codecStruct) HashKey() curie.IRI { return curie.IRI(s.ID.Val) }
func (s codecStruct) SortKey() curie.IRI { return curie.IRI(s.Type.Val) }

var (
	lensCodecID   = dynamo.Codec[codecStruct, codecTypeDB]("ID")
	lensCodecType = dynamo.Codec[codecStruct, codecTypeDB]("Type")
)

func (x codecStruct) MarshalDynamoDBAttributeValue() (types.AttributeValue, error) {
	type tStruct codecStruct
	return dynamo.Encode(tStruct(x),
		lensCodecID.Encode((codecTypeDB)(x.ID)),
		lensCodecType.Encode((codecTypeDB)(x.Type)),
	)
}

func (x *codecStruct) UnmarshalDynamoDBAttributeValue(av types.AttributeValue) error {
	type tStruct *codecStruct
	return dynamo.Decode(av, tStruct(x),
		lensCodecID.Decode((*codecTypeDB)(&x.ID)),
		lensCodecType.Decode((*codecTypeDB)(&x.Type)),
	)
}

func TestCodecDecode(t *testing.T) {
	av := &types.AttributeValueMemberM{
		Value: map[string]types.AttributeValue{
			"id":   &types.AttributeValueMemberS{Value: "myID"},
			"type": &types.AttributeValueMemberS{Value: "myType"},
			"name": &types.AttributeValueMemberS{Value: "myName"},
			"city": &types.AttributeValueMemberS{Value: "myCity"},
		},
	}

	var val codecStruct
	err := attributevalue.Unmarshal(av, &val)

	it.Ok(t).
		IfNil(err).
		If(val.ID.Val).Equal("myID").
		If(val.Type.Val).Equal("myType").
		If(val.Name).Equal("myName").
		If(val.City).Equal("myCity")
}

func TestCodecEncode(t *testing.T) {
	val := codecStruct{
		ID:   codecType{Val: "myID"},
		Type: codecType{Val: "myType"},
		Name: "myName",
		City: "myCity",
	}

	av, err := attributevalue.Marshal(val)
	tv, ok := av.(*types.AttributeValueMemberM)

	it.Ok(t).
		IfNil(err).
		IfTrue(ok).
		If(tv.Value["id"].(*types.AttributeValueMemberS).Value).Equal("myID").
		If(tv.Value["type"].(*types.AttributeValueMemberS).Value).Equal("myType").
		If(tv.Value["name"].(*types.AttributeValueMemberS).Value).Equal("myName").
		If(tv.Value["city"].(*types.AttributeValueMemberS).Value).Equal("myCity")
}

type codecMyType struct {
	HKey curie.IRI  `dynamodbav:"hkey,omitempty"`
	SKey curie.IRI  `dynamodbav:"skey,omitempty"`
	Link *curie.IRI `dynamodbav:"link,omitempty"`
}

func (s codecMyType) HashKey() curie.IRI { return s.HKey }
func (s codecMyType) SortKey() curie.IRI { return s.SKey }

func TestCodecEncodeDecode(t *testing.T) {
	link := curie.IRI("test:a/b/c")
	core := codecMyType{
		HKey: curie.IRI("test:a/b"),
		SKey: curie.IRI("c/d"),
		Link: &link,
	}

	av, err := attributevalue.Marshal(core)
	it.Ok(t).IfNil(err)

	var some codecMyType
	err = attributevalue.Unmarshal(av, &some)
	it.Ok(t).IfNil(err)

	it.Ok(t).
		IfTrue(core.HKey == some.HKey).
		IfTrue(core.SKey == some.SKey).
		IfTrue(*core.Link == *some.Link)
}

func TestCodecEncodeDecodeKeyOnly(t *testing.T) {
	core := codecMyType{
		HKey: curie.IRI("test:a/b"),
		SKey: curie.IRI("c/d"),
	}

	av, err := attributevalue.Marshal(core)
	it.Ok(t).IfNil(err)

	var some codecMyType
	err = attributevalue.Unmarshal(av, &some)
	it.Ok(t).IfNil(err)

	it.Ok(t).
		IfTrue(core.HKey == some.HKey).
		IfTrue(core.SKey == some.SKey)
}

func TestCodecEncodeDecodeKeyOnlyHash(t *testing.T) {
	core := codecMyType{
		HKey: curie.IRI("test:a/b"),
	}

	av, err := attributevalue.Marshal(core)
	it.Ok(t).IfNil(err)

	var some codecMyType
	err = attributevalue.Unmarshal(av, &some)
	it.Ok(t).IfNil(err)

	it.Ok(t).
		IfTrue(core.HKey == some.HKey).
		IfTrue(core.SKey == some.SKey)
}

type codecTypeBad codecType

func (x codecTypeBad) MarshalDynamoDBAttributeValue() (types.AttributeValue, error) {
	return nil, fmt.Errorf("Encode error.")
}

func (x *codecTypeBad) UnmarshalDynamoDBAttributeValue(types.AttributeValue) error {
	return fmt.Errorf("Decode error.")
}

type codecBadType struct {
	HKey curie.IRI    `dynamodbav:"hkey"`
	SKey curie.IRI    `dynamodbav:"skey"`
	Link codecTypeBad `dynamodbav:"link,omitempty"`
}

func (s codecBadType) HashKey() curie.IRI { return s.HKey }
func (s codecBadType) SortKey() curie.IRI { return s.SKey }

func TestCodecEncodeBadType(t *testing.T) {
	core := codecBadType{
		HKey: curie.IRI("test:a/b"),
		SKey: curie.IRI("c/d"),
		Link: codecTypeBad{Val: "test:a/b/c"},
	}

	_, err := attributevalue.Marshal(core)
	it.Ok(t).IfNotNil(err)
}

func TestCodecDecodeBadType(t *testing.T) {
	av := &types.AttributeValueMemberM{
		Value: map[string]types.AttributeValue{
			"hkey": &types.AttributeValueMemberS{Value: "hkey"},
			"skey": &types.AttributeValueMemberS{Value: "skey"},
			"link": &types.AttributeValueMemberS{Value: "link"},
		},
	}

	var val codecBadType
	err := attributevalue.Unmarshal(av, &val)
	it.Ok(t).IfNotNil(err)
}

type codecBadStruct struct {
	HKey codecType `dynamodbav:"hkey"`
	SKey codecType `dynamodbav:"skey"`
	Link codecType `dynamodbav:"link"`
}

func (s codecBadStruct) HashKey() string { return s.HKey.Val }
func (s codecBadStruct) SortKey() string { return s.SKey.Val }

var (
	lensCodecBadsHKey = dynamo.Codec[codecBadType, codecTypeBad]("HKey")
	lensCodecBadsSKey = dynamo.Codec[codecBadType, codecTypeBad]("SKey")
	lensCodecBadsLink = dynamo.Codec[codecBadType, codecTypeBad]("Link")
)

func (x codecBadStruct) MarshalDynamoDBAttributeValue() (types.AttributeValue, error) {
	type tStruct codecBadStruct
	return dynamo.Encode(tStruct(x),
		lensCodecBadsHKey.Encode(codecTypeBad(x.HKey)),
		lensCodecBadsSKey.Encode(codecTypeBad(x.SKey)),
		lensCodecBadsLink.Encode(codecTypeBad(x.Link)),
	)
}

func (x *codecBadStruct) UnmarshalDynamoDBAttributeValue(av types.AttributeValue) error {
	type tStruct *codecBadStruct
	return dynamo.Decode(av, tStruct(x),
		lensCodecBadsHKey.Decode((*codecTypeBad)(&x.HKey)),
		lensCodecBadsSKey.Decode((*codecTypeBad)(&x.SKey)),
		lensCodecBadsLink.Decode((*codecTypeBad)(&x.Link)),
	)
}

func TestCodecEncodeBadStruct(t *testing.T) {
	core := codecBadStruct{
		HKey: codecType{Val: "test:a/b"},
		SKey: codecType{Val: "c/d"},
		Link: codecType{Val: "test:a/b/c"},
	}

	_, err := attributevalue.Marshal(core)
	it.Ok(t).IfNotNil(err)
}

func TestCodecDecodeBadStruct(t *testing.T) {
	av := &types.AttributeValueMemberM{
		Value: map[string]types.AttributeValue{
			"hkey": &types.AttributeValueMemberS{Value: "hkey"},
			"skey": &types.AttributeValueMemberS{Value: "skey"},
			"link": &types.AttributeValueMemberS{Value: "link"},
		},
	}

	var val codecBadStruct
	err := attributevalue.Unmarshal(av, &val)
	it.Ok(t).IfNotNil(err)
}

type Item struct {
	Prefix curie.IRI  `json:"prefix,omitempty"  dynamodbav:"prefix,omitempty"`
	Suffix curie.IRI  `json:"suffix,omitempty"  dynamodbav:"suffix,omitempty"`
	Ref    *curie.IRI `json:"ref,omitempty"  dynamodbav:"ref,omitempty"`
	Tag    string     `json:"tag,omitempty"  dynamodbav:"tag,omitempty"`
}

func fixtureItem() Item {
	ref := curie.IRI("foo:a/suffix")
	return Item{
		Prefix: curie.IRI("foo:prefix"),
		Suffix: curie.IRI("suffix"),
		Ref:    &ref,
		Tag:    "tag",
	}
}

func fixtureJson() string {
	return "{\"prefix\":\"[foo:prefix]\",\"suffix\":\"[suffix]\",\"ref\":\"[foo:a/suffix]\",\"tag\":\"tag\"}"
}

func fixtureDynamo() map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		"prefix": &types.AttributeValueMemberS{Value: "foo:prefix"},
		"suffix": &types.AttributeValueMemberS{Value: "suffix"},
		"ref":    &types.AttributeValueMemberS{Value: "foo:a/suffix"},
		"tag":    &types.AttributeValueMemberS{Value: "tag"},
	}
}

func fixtureEmptyItem() Item {
	return Item{
		Prefix: curie.IRI("foo:prefix"),
		Suffix: curie.IRI("suffix"),
	}
}

func fixtureEmptyJson() string {
	return "{\"prefix\":\"[foo:prefix]\",\"suffix\":\"[suffix]\"}"
}

func TestMarshalJSON(t *testing.T) {
	bytes, err := json.Marshal(fixtureItem())

	it.Ok(t).
		If(err).Should().Equal(nil).
		If(string(bytes)).Should().Equal(fixtureJson())
}

func TestMarshalEmptyJSON(t *testing.T) {
	bytes, err := json.Marshal(fixtureEmptyItem())

	it.Ok(t).
		If(err).Should().Equal(nil).
		If(string(bytes)).Should().Equal(fixtureEmptyJson())
}

func TestUnmarshalJSON(t *testing.T) {
	var item Item

	it.Ok(t).
		If(json.Unmarshal([]byte(fixtureJson()), &item)).Should().Equal(nil).
		If(item).Should().Equal(fixtureItem())
}

func TestUnmarshalEmptyJSON(t *testing.T) {
	var item Item

	it.Ok(t).
		If(json.Unmarshal([]byte(fixtureEmptyJson()), &item)).Should().Equal(nil).
		If(item).Should().Equal(fixtureEmptyItem())
}

func TestMarshalDynamo(t *testing.T) {
	gen, err := attributevalue.MarshalMap(fixtureItem())

	it.Ok(t).
		If(err).Should().Equal(nil).
		If(gen).Should().Equal(fixtureDynamo())
}

func TestUnmarshalDynamo(t *testing.T) {
	var item Item

	it.Ok(t).
		If(attributevalue.UnmarshalMap(fixtureDynamo(), &item)).Should().Equal(nil).
		If(item).Should().Equal(fixtureItem())
}
