package dynamo_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/fogfish/curie"
	"github.com/fogfish/dynamo"
	"github.com/fogfish/it"
)

type codecType struct{ Val string }

type codecTypeDB codecType

func (x codecTypeDB) MarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	av.S = aws.String(x.Val)
	return nil
}

func (x *codecTypeDB) UnmarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	x.Val = *av.S
	return nil
}

type codecStruct struct {
	ID   codecType `dynamodbav:"id"`
	Type codecType `dynamodbav:"type"`
	Name string    `dynamodbav:"name"`
	City string    `dynamodbav:"city"`
}

func (s codecStruct) HashKey() string { return s.ID.Val }
func (s codecStruct) SortKey() string { return s.Type.Val }

var lensCodecID, lensCodecType = dynamo.Codec2[codecStruct, codecTypeDB, codecTypeDB]("ID", "Type")

func (x codecStruct) MarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	type tStruct codecStruct
	return dynamo.Encode(av, tStruct(x),
		lensCodecID.Encode((codecTypeDB)(x.ID)), lensCodecType.Encode((codecTypeDB)(x.Type)),
	)
}

func (x *codecStruct) UnmarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	type tStruct *codecStruct
	return dynamo.Decode(av, tStruct(x),
		lensCodecID.Decode((*codecTypeDB)(&x.ID)), lensCodecType.Decode((*codecTypeDB)(&x.Type)),
	)
}

func TestCodecDecode(t *testing.T) {
	av := &dynamodb.AttributeValue{
		M: map[string]*dynamodb.AttributeValue{
			"id":   {S: aws.String("myID")},
			"type": {S: aws.String("myType")},
			"name": {S: aws.String("myName")},
			"city": {S: aws.String("myCity")},
		},
	}

	var val codecStruct
	err := dynamodbattribute.Unmarshal(av, &val)

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

	av, err := dynamodbattribute.Marshal(val)

	it.Ok(t).
		IfNil(err).
		If(*av.M["id"].S).Equal("myID").
		If(*av.M["type"].S).Equal("myType").
		If(*av.M["name"].S).Equal("myName").
		If(*av.M["city"].S).Equal("myCity")
}

//
//
//
type codecMyType struct {
	HKey curie.IRI     `dynamodbav:"hkey,omitempty"`
	SKey curie.IRI     `dynamodbav:"skey,omitempty"`
	Link *curie.String `dynamodbav:"link,omitempty"`
}

func (s codecMyType) HashKey() string { return s.HKey.String() }
func (s codecMyType) SortKey() string { return s.SKey.String() }

var lensCodecHKey, lensCodecSKey = dynamo.Codec2[codecMyType, dynamo.IRI, dynamo.IRI]("HKey", "SKey")

func (x codecMyType) MarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	type tStruct codecMyType
	return dynamo.Encode(av, tStruct(x),
		lensCodecHKey.Encode(dynamo.IRI(x.HKey)), lensCodecSKey.Encode(dynamo.IRI(x.SKey)),
	)
}

func (x *codecMyType) UnmarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	type tStruct *codecMyType
	return dynamo.Decode(av, tStruct(x),
		lensCodecHKey.Decode((*dynamo.IRI)(&x.HKey)), lensCodecSKey.Decode((*dynamo.IRI)(&x.SKey)),
	)
}

func TestCodecEncodeDecode(t *testing.T) {
	core := codecMyType{
		HKey: curie.New("test:a/b"),
		SKey: curie.New("c/d"),
		Link: curie.Safe(curie.New("test:a/b/c")),
	}

	av, err := dynamodbattribute.Marshal(core)
	it.Ok(t).IfNil(err)

	var some codecMyType
	err = dynamodbattribute.Unmarshal(av, &some)
	it.Ok(t).IfNil(err)

	it.Ok(t).
		IfTrue(curie.Eq(core.HKey, some.HKey)).
		IfTrue(curie.Eq(core.SKey, some.SKey)).
		IfTrue(*core.Link == *some.Link)
}

func TestCodecEncodeDecodeKeyOnly(t *testing.T) {
	core := codecMyType{
		HKey: curie.New("test:a/b"),
		SKey: curie.New("c/d"),
	}

	av, err := dynamodbattribute.Marshal(core)
	it.Ok(t).IfNil(err)

	var some codecMyType
	err = dynamodbattribute.Unmarshal(av, &some)
	it.Ok(t).IfNil(err)

	it.Ok(t).
		IfTrue(curie.Eq(core.HKey, some.HKey)).
		IfTrue(curie.Eq(core.SKey, some.SKey))
}

func TestCodecEncodeDecodeKeyOnlyHash(t *testing.T) {
	core := codecMyType{
		HKey: curie.New("test:a/b"),
	}

	av, err := dynamodbattribute.Marshal(core)
	it.Ok(t).IfNil(err)

	var some codecMyType
	err = dynamodbattribute.Unmarshal(av, &some)
	it.Ok(t).IfNil(err)

	it.Ok(t).
		IfTrue(curie.Eq(core.HKey, some.HKey)).
		IfTrue(curie.Eq(core.SKey, some.SKey))
}

//
//
//
type codecTypeBad codecType

func (x codecTypeBad) MarshalDynamoDBAttributeValue(*dynamodb.AttributeValue) error {
	return fmt.Errorf("Encode error.")
}

func (x *codecTypeBad) UnmarshalDynamoDBAttributeValue(*dynamodb.AttributeValue) error {
	return fmt.Errorf("Decode error.")
}

type codecBadType struct {
	HKey curie.IRI    `dynamodbav:"hkey"`
	SKey curie.IRI    `dynamodbav:"skey"`
	Link codecTypeBad `dynamodbav:"link,omitempty"`
}

func (s codecBadType) HashKey() string { return s.HKey.String() }
func (s codecBadType) SortKey() string { return s.SKey.String() }

var lensCodecBadHKey, lensCodecBadSKey = dynamo.Codec2[codecBadType, dynamo.IRI, dynamo.IRI]("HKey", "SKey")

func (x codecBadType) MarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	type tStruct codecBadType
	return dynamo.Encode(av, tStruct(x),
		lensCodecBadHKey.Encode(dynamo.IRI(x.HKey)), lensCodecBadSKey.Encode(dynamo.IRI(x.SKey)),
	)
}

func (x *codecBadType) UnmarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	type tStruct *codecBadType
	return dynamo.Decode(av, tStruct(x),
		lensCodecBadHKey.Decode((*dynamo.IRI)(&x.HKey)), lensCodecBadSKey.Decode((*dynamo.IRI)(&x.SKey)),
	)
}

func TestCodecEncodeBadType(t *testing.T) {
	core := codecBadType{
		HKey: curie.New("test:a/b"),
		SKey: curie.New("c/d"),
		Link: codecTypeBad{Val: "test:a/b/c"},
	}

	_, err := dynamodbattribute.Marshal(core)
	it.Ok(t).IfNotNil(err)
}

func TestCodecDecodeBadType(t *testing.T) {
	av := &dynamodb.AttributeValue{
		M: map[string]*dynamodb.AttributeValue{
			"hkey": {S: aws.String("hkey")},
			"skey": {S: aws.String("skey")},
			"link": {S: aws.String("link")},
		},
	}

	var val codecBadType
	err := dynamodbattribute.Unmarshal(av, &val)
	it.Ok(t).IfNotNil(err)
}

type codecBadStruct struct {
	HKey codecType `dynamodbav:"hkey"`
	SKey codecType `dynamodbav:"skey"`
	Link codecType `dynamodbav:"link"`
}

func (s codecBadStruct) HashKey() string { return s.HKey.Val }
func (s codecBadStruct) SortKey() string { return s.SKey.Val }

var lensCodecBadsHKey, lensCodecBadsSKey, lensCodecBadsLink = dynamo.Codec3[codecBadType, codecTypeBad, codecTypeBad, codecTypeBad]("HKey", "SKey", "Link")

func (x codecBadStruct) MarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	type tStruct codecBadStruct
	return dynamo.Encode(av, tStruct(x),
		lensCodecBadsHKey.Encode(codecTypeBad(x.HKey)),
		lensCodecBadsSKey.Encode(codecTypeBad(x.SKey)),
		lensCodecBadsLink.Encode(codecTypeBad(x.Link)),
	)
}

func (x *codecBadStruct) UnmarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
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

	_, err := dynamodbattribute.Marshal(core)
	it.Ok(t).IfNotNil(err)
}

func TestCodecDecodeBadStruct(t *testing.T) {
	av := &dynamodb.AttributeValue{
		M: map[string]*dynamodb.AttributeValue{
			"hkey": {S: aws.String("hkey")},
			"skey": {S: aws.String("skey")},
			"link": {S: aws.String("link")},
		},
	}

	var val codecBadStruct
	err := dynamodbattribute.Unmarshal(av, &val)
	it.Ok(t).IfNotNil(err)
}
