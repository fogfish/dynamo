package dynamo_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
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
		ID:      dynamo.NewfID("dead:beef"),
		Name:    "Verner Pleishner",
		Age:     64,
		Address: "Blumenstrasse 14, Berne, 3013",
	}
}

func TestDdbGet(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		val := person{ID: dynamo.NewfID("dead:beef")}
		ddb := mockGetItem(map[string]*dynamodb.AttributeValue{
			"prefix":  {S: aws.String("dead:beef")},
			"suffix":  {S: aws.String("_")},
			"address": {S: aws.String("Blumenstrasse 14, Berne, 3013")},
			"name":    {S: aws.String("Verner Pleishner")},
			"age":     {N: aws.String("64")},
		})

		err := ddb.Get(&val)
		it.Ok(t).
			If(err).Should().Equal(nil).
			If(val).Should().Equal(entity())
	})

	t.Run("Not Found", func(t *testing.T) {
		val := person{ID: dynamo.NewfID("dead:beef")}
		ddb := mockGetItem(nil)

		err := ddb.Get(&val)
		it.Ok(t).
			If(err).ShouldNot().Equal(nil).
			If(err).Should().Be().Like(dynamo.NotFound{})
	})

	t.Run("I/O Error", func(t *testing.T) {
		val := person{ID: dynamo.NewfID("some:key")}
		ddb := mockGetItem(nil)

		err := ddb.Get(&val)
		it.Ok(t).
			If(err).ShouldNot().Equal(nil)
	})
}

func TestDdbPut(t *testing.T) {
	val := entity()
	ddb := mockPutItem(map[string]*dynamodb.AttributeValue{
		"prefix":  {S: aws.String("dead:beef")},
		"suffix":  {S: aws.String("_")},
		"address": {S: aws.String("Blumenstrasse 14, Berne, 3013")},
		"name":    {S: aws.String("Verner Pleishner")},
		"age":     {N: aws.String("64")},
	})

	it.Ok(t).If(ddb.Put(val)).Should().Equal(nil)
}

func TestDdbRemove(t *testing.T) {
	val := entity()
	ddb := mockDeleteItem(map[string]*dynamodb.AttributeValue{
		"prefix": {S: aws.String("dead:beef")},
		"suffix": {S: aws.String("_")},
	})

	it.Ok(t).If(ddb.Remove(val)).Should().Equal(nil)
}

func TestDdbUpdate(t *testing.T) {
	val := person{
		ID:  dynamo.NewfID("dead:beef"),
		Age: 65,
	}
	ddb := mockUpdateItem(
		map[string]*dynamodb.AttributeValue{
			"prefix": {S: aws.String("dead:beef")},
			"suffix": {S: aws.String("_")},
		},
		map[string]*dynamodb.AttributeValue{
			":age": {N: aws.String("65")},
		},
	)

	err := ddb.Update(&val)
	it.Ok(t).
		If(err).Should().Equal(nil).
		If(val).Should().Equal(entity())
}

/*
func TestDdbMatch(t *testing.T) {
}
*/

func TestDdbMatch(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		ddb := mockQuery(
			map[string]*dynamodb.AttributeValue{
				":prefix": {S: aws.String("dead:beef")},
			},
			0,
		)

		seq := ddb.Match(dynamo.NewfID("dead:beef"))

		it.Ok(t).
			IfFalse(seq.Tail()).
			If(seq.Error()).Should().Equal(nil)
	})

	t.Run("One", func(t *testing.T) {
		ddb := mockQuery(
			map[string]*dynamodb.AttributeValue{
				":prefix": {S: aws.String("dead:beef")},
			},
			1,
		)

		seq := ddb.Match(dynamo.NewfID("dead:beef"))

		val := person{}
		err := seq.Head(&val)

		it.Ok(t).
			IfFalse(seq.Tail()).
			If(seq.Error()).Should().Equal(nil).
			If(err).Should().Equal(nil).
			If(val).Should().Equal(entity())
	})

	t.Run("Many", func(t *testing.T) {
		ddb := mockQuery(
			map[string]*dynamodb.AttributeValue{
				":prefix": {S: aws.String("dead:beef")},
			},
			5,
		)

		cnt := 0
		seq := ddb.Match(dynamo.NewfID("dead:beef"))

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
			If(cnt).Should().Equal(5)
	})
}

//
// Use type aliases and methods to implement FMap
type persons []person

func (seq *persons) Join(gen dynamo.Gen) error {
	val := person{}
	if fail := gen.To(&val); fail != nil {
		return fail
	}
	*seq = append(*seq, val)
	return nil
}

func TestDdbMatchFMap(t *testing.T) {
	t.Run("Only Prefix", func(t *testing.T) {
		seq := persons{}
		ddb := mockQuery(
			map[string]*dynamodb.AttributeValue{
				":prefix": {S: aws.String("dead:beef")},
			},
			2,
		)
		thing := entity()

		err := ddb.Match(dynamo.NewfID("dead:beef")).FMap(seq.Join)
		it.Ok(t).
			If(err).Should().Equal(nil).
			If(seq).Should().Equal(persons{thing, thing})
	})

	t.Run("Empty", func(t *testing.T) {
		seq := persons{}
		ddb := mockQuery(
			map[string]*dynamodb.AttributeValue{
				":prefix": {S: aws.String("dead:beef")},
			},
			0,
		)

		err := ddb.Match(dynamo.NewfID("dead:beef")).FMap(seq.Join)
		it.Ok(t).
			If(err).Should().Equal(nil).
			If(seq).Should().Equal(persons{})
	})

	t.Run("With Suffix", func(t *testing.T) {
		seq := persons{}
		ddb := mockQuery(
			map[string]*dynamodb.AttributeValue{
				":prefix": {S: aws.String("dead:beef")},
				":suffix": {S: aws.String("a/b/c")},
			},
			2,
		)
		thing := entity()

		err := ddb.Match(dynamo.NewfID("dead:beef#a/b/c")).FMap(seq.Join)
		it.Ok(t).
			If(err).Should().Equal(nil).
			If(seq).Should().Equal(persons{thing, thing})
	})

	t.Run("Match IDs", func(t *testing.T) {
		seq := dynamo.IDs{}
		ddb := mockQuery(
			map[string]*dynamodb.AttributeValue{
				":prefix": {S: aws.String("dead:beef")},
			},
			2,
		)
		thing := entity().ID

		err := ddb.Match(dynamo.NewfID("dead:beef")).FMap(seq.Join)
		it.Ok(t).
			If(err).Should().Equal(nil).
			If(seq).Should().Equal(dynamo.IDs{thing, thing})
	})
}

//-----------------------------------------------------------------------------
//
// Mock Dynamo DB
//
//-----------------------------------------------------------------------------

//
//
type ddbGetItem struct {
	dynamodbiface.DynamoDBAPI
	returnVal map[string]*dynamodb.AttributeValue
}

func mockGetItem(returnVal map[string]*dynamodb.AttributeValue) dynamo.KeyValNoContext {
	return mockDynamoDB(&ddbGetItem{returnVal: returnVal})
}

func (mock *ddbGetItem) GetItemWithContext(ctx aws.Context, input *dynamodb.GetItemInput, opts ...request.Option) (*dynamodb.GetItemOutput, error) {
	expect := map[string]*dynamodb.AttributeValue{
		"prefix": {S: aws.String("dead:beef")},
		"suffix": {S: aws.String("_")},
	}

	if !reflect.DeepEqual(expect, input.Key) {
		return nil, errors.New("Unexpected entity.")
	}

	return &dynamodb.GetItemOutput{Item: mock.returnVal}, nil
}

//
//
type ddbPutItem struct {
	dynamodbiface.DynamoDBAPI
	expectVal map[string]*dynamodb.AttributeValue
}

func mockPutItem(expectVal map[string]*dynamodb.AttributeValue) dynamo.KeyValNoContext {
	return mockDynamoDB(&ddbPutItem{expectVal: expectVal})
}

func (mock *ddbPutItem) PutItemWithContext(ctx aws.Context, input *dynamodb.PutItemInput, opts ...request.Option) (*dynamodb.PutItemOutput, error) {
	if !reflect.DeepEqual(mock.expectVal, input.Item) {
		return nil, errors.New("Unexpected entity.")
	}
	return &dynamodb.PutItemOutput{}, nil
}

//
//
type ddbDeleteItem struct {
	dynamodbiface.DynamoDBAPI
	expectKey map[string]*dynamodb.AttributeValue
}

func mockDeleteItem(expectKey map[string]*dynamodb.AttributeValue) dynamo.KeyValNoContext {
	return mockDynamoDB(&ddbDeleteItem{expectKey: expectKey})
}

func (mock *ddbDeleteItem) DeleteItemWithContext(ctx aws.Context, input *dynamodb.DeleteItemInput, opts ...request.Option) (*dynamodb.DeleteItemOutput, error) {
	if !reflect.DeepEqual(mock.expectKey, input.Key) {
		return nil, errors.New("Unexpected entity.")
	}

	return &dynamodb.DeleteItemOutput{}, nil
}

//
//
type ddbUpdateItem struct {
	dynamodbiface.DynamoDBAPI
	expectKey map[string]*dynamodb.AttributeValue
	expectVal map[string]*dynamodb.AttributeValue
}

func mockUpdateItem(expectKey map[string]*dynamodb.AttributeValue, expectVal map[string]*dynamodb.AttributeValue) dynamo.KeyValNoContext {
	return mockDynamoDB(&ddbUpdateItem{expectKey: expectKey, expectVal: expectVal})
}

func (mock *ddbUpdateItem) UpdateItemWithContext(ctx aws.Context, input *dynamodb.UpdateItemInput, opts ...request.Option) (*dynamodb.UpdateItemOutput, error) {
	if !reflect.DeepEqual(mock.expectKey, input.Key) {
		return nil, errors.New("Unexpected entity.")
	}

	if !reflect.DeepEqual(mock.expectVal, input.ExpressionAttributeValues) {
		return nil, errors.New("Unexpected entity.")
	}

	return &dynamodb.UpdateItemOutput{
		Attributes: map[string]*dynamodb.AttributeValue{
			"prefix":  {S: aws.String("dead:beef")},
			"address": {S: aws.String("Blumenstrasse 14, Berne, 3013")},
			"name":    {S: aws.String("Verner Pleishner")},
			"suffix":  {S: aws.String("_")},
			"age":     {N: aws.String("64")},
		},
	}, nil
}

//
//
type ddbQuery struct {
	dynamodbiface.DynamoDBAPI
	expectKey map[string]*dynamodb.AttributeValue
	returnLen int
}

func mockQuery(expectKey map[string]*dynamodb.AttributeValue, returnLen int) dynamo.KeyValNoContext {
	return mockDynamoDB(&ddbQuery{expectKey: expectKey, returnLen: returnLen})
}

func (mock *ddbQuery) QueryWithContext(ctx aws.Context, input *dynamodb.QueryInput, opts ...request.Option) (*dynamodb.QueryOutput, error) {
	if !reflect.DeepEqual(mock.expectKey, input.ExpressionAttributeValues) {
		return nil, errors.New("Unexpected entity.")
	}

	seq := []map[string]*dynamodb.AttributeValue{}
	for i := 0; i < mock.returnLen; i++ {
		seq = append(seq, map[string]*dynamodb.AttributeValue{
			"prefix":  {S: aws.String("dead:beef")},
			"address": {S: aws.String("Blumenstrasse 14, Berne, 3013")},
			"name":    {S: aws.String("Verner Pleishner")},
			"suffix":  {S: aws.String("_")},
			"age":     {N: aws.String("64")},
		})
	}

	return &dynamodb.QueryOutput{
		ScannedCount: aws.Int64(int64(mock.returnLen)),
		Count:        aws.Int64(int64(mock.returnLen)),
		Items:        seq,
	}, nil
}

//
//
type MockDynamoDB interface {
	Mock(db dynamodbiface.DynamoDBAPI)
}

func mockDynamoDB(mock dynamodbiface.DynamoDBAPI) dynamo.KeyValNoContext {
	client := dynamo.Must(dynamo.New("ddb:///test"))
	switch v := client.(type) {
	case MockDynamoDB:
		v.Mock(mock)
	default:
		panic("Invalid config")
	}

	return dynamo.NewKeyValContextDefault(client)
}
