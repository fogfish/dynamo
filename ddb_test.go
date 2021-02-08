package dynamo_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/fogfish/dynamo"
	"github.com/fogfish/it"
)

type person struct {
	dynamo.ID
	Name    string `dynamodbav:"name,omitempty"`
	Age     int    `dynamodbav:"age,omitempty"`
	Address string `dynamodbav:"address,omitempty"`
}

func entity() person {
	return person{
		ID:      dynamo.NewID("dead:beef"),
		Name:    "Verner Pleishner",
		Age:     64,
		Address: "Blumenstrasse 14, Berne, 3013",
	}
}

func TestDdbGet(t *testing.T) {
	val := person{ID: dynamo.NewID("dead:beef")}
	err := apiDB().Get(&val)

	it.Ok(t).
		If(err).Should().Equal(nil).
		If(val).Should().Equal(entity())
}

func TestDdbPut(t *testing.T) {
	it.Ok(t).If(apiDB().Put(entity())).Should().Equal(nil)
}

func TestDdbRemove(t *testing.T) {
	it.Ok(t).If(apiDB().Remove(entity())).Should().Equal(nil)
}

func TestDdbUpdate(t *testing.T) {
	val := person{
		ID:  dynamo.NewID("dead:beef"),
		Age: 65,
	}
	err := apiDB().Update(&val)

	it.Ok(t).
		If(err).Should().Equal(nil).
		If(val).Should().Equal(entity())
}

func TestDdbMatch(t *testing.T) {
	cnt := 0
	seq := apiDB().Match(dynamo.NewID("dead:"))

	for seq.Tail() {
		cnt++
		val := person{}
		err := seq.Head(&val)

		it.Ok(t).
			If(err).Should().Equal(nil).
			If(val).Should().Equal(entity())
	}

	it.Ok(t).
		If(seq.Error()).Should().Equal(nil).
		If(cnt).Should().Equal(2)
}

func TestDdbMatchHead(t *testing.T) {
	seq := apiDB().Match(dynamo.NewID("dead:"))

	val := person{}
	err := seq.Head(&val)

	it.Ok(t).
		If(err).Should().Equal(nil).
		If(val).Should().Equal(entity())
}

//
// Use type aliases and methods to implement FMap
type persons []person

func (seq *persons) Join(gen dynamo.Gen) (dynamo.Thing, error) {
	val := person{}
	if fail := gen.To(&val); fail != nil {
		return nil, fail
	}
	*seq = append(*seq, val)
	return &val, nil
}

func TestDdbMatchWithFMap(t *testing.T) {
	pseq := persons{}
	tseq, err := apiDB().Match(dynamo.NewID("dead:")).FMap(pseq.Join)

	thing := entity()
	it.Ok(t).
		If(err).Should().Equal(nil).
		If(tseq).Should().Equal([]dynamo.Thing{&thing, &thing}).
		If(pseq).Should().Equal(persons{thing, thing})
}

func TestDdbMatchIDsWithFMap(t *testing.T) {
	seq := dynamo.IDs{}
	_, err := apiDB().Match(dynamo.NewID("dead:")).FMap(seq.Join)

	thing := entity().ID
	it.Ok(t).
		If(err).Should().Equal(nil).
		If(seq).Should().Equal(dynamo.IDs{thing, thing})
}

//-----------------------------------------------------------------------------
//
// Mock Dynamo DB
//
//-----------------------------------------------------------------------------

func apiDB() *dynamo.DB {
	client := &dynamo.DB{}
	client.Mock(&mockDDB{})
	return client
}

type mockDDB struct {
	dynamodbiface.DynamoDBAPI
}

func (mockDDB) GetItem(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	expect := map[string]*dynamodb.AttributeValue{
		"prefix": {S: aws.String("dead:")},
		"suffix": {S: aws.String("beef")},
	}
	if !reflect.DeepEqual(expect, input.Key) {
		return nil, errors.New("Unexpected entity.")
	}

	return &dynamodb.GetItemOutput{
		Item: map[string]*dynamodb.AttributeValue{
			"prefix":  {S: aws.String("dead:")},
			"suffix":  {S: aws.String("beef")},
			"address": {S: aws.String("Blumenstrasse 14, Berne, 3013")},
			"name":    {S: aws.String("Verner Pleishner")},
			"age":     {N: aws.String("64")},
		},
	}, nil
}

func (mockDDB) PutItem(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	expect := map[string]*dynamodb.AttributeValue{
		"prefix":  {S: aws.String("dead:")},
		"suffix":  {S: aws.String("beef")},
		"address": {S: aws.String("Blumenstrasse 14, Berne, 3013")},
		"name":    {S: aws.String("Verner Pleishner")},
		"age":     {N: aws.String("64")},
	}

	if !reflect.DeepEqual(expect, input.Item) {
		return nil, errors.New("Unexpected entity.")
	}
	return &dynamodb.PutItemOutput{}, nil
}

func (mockDDB) DeleteItem(input *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
	expect := map[string]*dynamodb.AttributeValue{
		"prefix": {S: aws.String("dead:")},
		"suffix": {S: aws.String("beef")},
	}
	if !reflect.DeepEqual(expect, input.Key) {
		return nil, errors.New("Unexpected entity.")
	}

	return &dynamodb.DeleteItemOutput{}, nil
}

func (mockDDB) UpdateItem(input *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
	expect := map[string]*dynamodb.AttributeValue{
		"prefix": {S: aws.String("dead:")},
		"suffix": {S: aws.String("beef")},
	}
	if !reflect.DeepEqual(expect, input.Key) {
		return nil, errors.New("Unexpected entity.")
	}

	update := map[string]*dynamodb.AttributeValue{
		":age": {N: aws.String("65")},
	}
	if !reflect.DeepEqual(update, input.ExpressionAttributeValues) {
		return nil, errors.New("Unexpected entity.")
	}

	return &dynamodb.UpdateItemOutput{
		Attributes: map[string]*dynamodb.AttributeValue{
			"prefix":  {S: aws.String("dead:")},
			"address": {S: aws.String("Blumenstrasse 14, Berne, 3013")},
			"name":    {S: aws.String("Verner Pleishner")},
			"suffix":  {S: aws.String("beef")},
			"age":     {N: aws.String("64")},
		},
	}, nil
}

func (mockDDB) Query(input *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
	expect := map[string]*dynamodb.AttributeValue{
		":prefix": {S: aws.String("dead:")},
	}
	if !reflect.DeepEqual(expect, input.ExpressionAttributeValues) {
		return nil, errors.New("Unexpected entity.")
	}

	return &dynamodb.QueryOutput{
		ScannedCount: aws.Int64(2),
		Count:        aws.Int64(2),
		Items: []map[string]*dynamodb.AttributeValue{
			{
				"prefix":  {S: aws.String("dead:")},
				"address": {S: aws.String("Blumenstrasse 14, Berne, 3013")},
				"name":    {S: aws.String("Verner Pleishner")},
				"suffix":  {S: aws.String("beef")},
				"age":     {N: aws.String("64")},
			},
			{
				"prefix":  {S: aws.String("dead:")},
				"address": {S: aws.String("Blumenstrasse 14, Berne, 3013")},
				"name":    {S: aws.String("Verner Pleishner")},
				"suffix":  {S: aws.String("beef")},
				"age":     {N: aws.String("64")},
			},
		},
	}, nil
}
