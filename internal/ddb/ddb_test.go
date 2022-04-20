package ddb_test

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/fogfish/curie"
	"github.com/fogfish/dynamo"
	"github.com/fogfish/dynamo/internal/ddb/ddbtest"
	"github.com/fogfish/dynamo/internal/dynamotest"
	"github.com/fogfish/it"
)

type person struct {
	Prefix  dynamo.IRI `dynamodbav:"prefix,omitempty"`
	Suffix  dynamo.IRI `dynamodbav:"suffix,omitempty"`
	Name    string     `dynamodbav:"name,omitempty"`
	Age     int        `dynamodbav:"age,omitempty"`
	Address string     `dynamodbav:"address,omitempty"`
}

func (p person) HashKey() string { return curie.IRI(p.Prefix).String() }
func (p person) SortKey() string { return curie.IRI(p.Suffix).String() }

// func keyStruct() person {
// 	return person{
// 		Prefix: dynamo.NewIRI("dead:beef"),
// 		Suffix: dynamo.NewIRI("1"),
// 	}
// }

func entityStruct() person {
	return person{
		Prefix:  dynamo.NewIRI("dead:beef"),
		Suffix:  dynamo.NewIRI("1"),
		Name:    "Verner Pleishner",
		Age:     64,
		Address: "Blumenstrasse 14, Berne, 3013",
	}
}

// func keyDynamo() map[string]*dynamodb.AttributeValue {
// 	return map[string]*dynamodb.AttributeValue{
// 		"prefix": {S: aws.String("dead:beef")},
// 		"suffix": {S: aws.String("1")},
// 	}
// }

func entityDynamo() map[string]*dynamodb.AttributeValue {
	return map[string]*dynamodb.AttributeValue{
		"prefix":  {S: aws.String("dead:beef")},
		"suffix":  {S: aws.String("1")},
		"address": {S: aws.String("Blumenstrasse 14, Berne, 3013")},
		"name":    {S: aws.String("Verner Pleishner")},
		"age":     {N: aws.String("64")},
	}
}

func codec(p dynamotest.Person) (map[string]*dynamodb.AttributeValue, error) {
	return dynamodbattribute.MarshalMap(p)
}

func TestDynamoDB(t *testing.T) {
	dynamotest.TestGet(t, codec, ddbtest.GetItem[dynamotest.Person])
	dynamotest.TestPut(t, codec, ddbtest.PutItem[dynamotest.Person])
	dynamotest.TestRemove(t, codec, ddbtest.DeleteItem[dynamotest.Person])
	dynamotest.TestUpdate(t, codec, ddbtest.UpdateItem[dynamotest.Person])
	dynamotest.TestMatch(t, codec, ddbtest.Query[dynamotest.Person])
}

// func TestDdbGetSuccess(t *testing.T) {
// 	key := person{
// 		Prefix: dynamo.NewIRI("dead:beef"),
// 		Suffix: dynamo.NewIRI("1"),
// 	}
// 	ddb := ddbtest.GetItem[person](keyDynamo(), entityDynamo())

// 	val, err := ddb.Get(key)
// 	it.Ok(t).
// 		If(err).Should().Equal(nil).
// 		If(*val).Should().Equal(entityStruct())
// }

// func TestDdbGetNotFound(t *testing.T) {
// 	key := keyStruct()
// 	ddb := ddbtest.GetItem[person](keyDynamo(), nil)

// 	val, err := ddb.Get(key)
// 	it.Ok(t).
// 		If(val).Should().Equal(nil).
// 		If(err).ShouldNot().Equal(nil).
// 		If(err).Should().Be().Like(dynamo.NotFound{})
// }

// func TestDdbGetErrorIO(t *testing.T) {
// 	key := keyStruct()
// 	ddb := ddbtest.GetItem[person](keyDynamo(), nil)

// 	val, err := ddb.Get(key)
// 	it.Ok(t).
// 		If(val).Should().Equal(nil).
// 		If(err).ShouldNot().Equal(nil)
// }

// func TestDdbPut(t *testing.T) {
// 	val := entityStruct()
// 	ddb := ddbtest.PutItem[person](entityDynamo())

// 	it.Ok(t).If(ddb.Put(val)).Should().Equal(nil)
// }

func TestDdbPutWithConstrain(t *testing.T) {
	name := dynamo.Schema1[person, string]("Name")
	ddb := ddbtest.Constrains[person](nil)

	success := ddb.Put(entityStruct(), name.Eq("xxx"))
	failure := ddb.Put(entityStruct(), name.Eq("yyy"))

	it.Ok(t).
		If(success).Should().Equal(nil).
		If(failure).Should().Be().Like(dynamo.PreConditionFailed{})
}

// func TestDdbRemove(t *testing.T) {
// 	val := entityStruct()
// 	ddb := ddbtest.DeleteItem[person](keyDynamo())

// 	it.Ok(t).If(ddb.Remove(val)).Should().Equal(nil)
// }

func TestDdbRemoveWithConstrain(t *testing.T) {
	name := dynamo.Schema1[person, string]("Name")
	ddb := ddbtest.Constrains[person](nil)

	success := ddb.Remove(entityStruct(), name.Eq("xxx"))
	failure := ddb.Remove(entityStruct(), name.Eq("yyy"))

	it.Ok(t).
		If(success).Should().Equal(nil).
		If(failure).Should().Be().Like(dynamo.PreConditionFailed{})
}

// func TestDdbUpdate(t *testing.T) {
// 	patch := person{
// 		Prefix: dynamo.NewIRI("dead:beef"),
// 		Suffix: dynamo.NewIRI("1"),
// 		Age:    65,
// 	}
// 	ddb := ddbtest.UpdateItem[person](
// 		keyDynamo(),
// 		map[string]*dynamodb.AttributeValue{
// 			":__age__": {N: aws.String("65")},
// 		},
// 		entityDynamo(),
// 	)

// 	val, err := ddb.Update(patch)
// 	it.Ok(t).
// 		If(err).Should().Equal(nil).
// 		If(*val).Should().Equal(entityStruct())
// }

func TestDdbUpdateWithConstrain(t *testing.T) {
	name := dynamo.Schema1[person, string]("Name")
	ddb := ddbtest.Constrains[person](entityDynamo())
	patch := person{
		Prefix: dynamo.NewIRI("dead:beef"),
		Suffix: dynamo.NewIRI("1"),
		Age:    65,
	}

	_, success := ddb.Update(patch, name.Eq("xxx"))
	_, failure := ddb.Update(patch, name.Eq("yyy"))

	it.Ok(t).
		If(success).Should().Equal(nil).
		If(failure).Should().Be().Like(dynamo.PreConditionFailed{})
}

// func TestDdbMatchNone(t *testing.T) {
// 	ddb := mockQuery[person](
// 		map[string]*dynamodb.AttributeValue{
// 			":prefix": {S: aws.String("dead:beef")},
// 		},
// 		0, nil,
// 	)

// 	seq := ddb.Match(person{Prefix: dynamo.NewIRI("dead:beef")})

// 	it.Ok(t).
// 		IfFalse(seq.Tail()).
// 		If(seq.Error()).Should().Equal(nil)
// }

// func TestDdbMatchOne(t *testing.T) {
// 	ddb := mockQuery[person](
// 		map[string]*dynamodb.AttributeValue{
// 			":prefix": {S: aws.String("dead:beef")},
// 		},
// 		1, nil,
// 	)

// 	seq := ddb.Match(person{Prefix: dynamo.NewIRI("dead:beef")})
// 	val, err := seq.Head()

// 	it.Ok(t).
// 		IfFalse(seq.Tail()).
// 		If(seq.Error()).Should().Equal(nil).
// 		If(err).Should().Equal(nil).
// 		If(*val).Should().Equal(entityStruct())
// }

// func TestDdbMatchMany(t *testing.T) {
// 	ddb := mockQuery[person](
// 		map[string]*dynamodb.AttributeValue{
// 			":prefix": {S: aws.String("dead:beef")},
// 		},
// 		5, nil,
// 	)

// 	cnt := 0
// 	seq := ddb.Match(person{Prefix: dynamo.NewIRI("dead:beef")})

// 	for seq.Tail() {
// 		cnt++

// 		val, err := seq.Head()

// 		it.Ok(t).
// 			If(err).Should().Equal(nil).
// 			If(*val).Should().Equal(entityStruct())
// 	}

// 	it.Ok(t).
// 		If(seq.Error()).Should().Equal(nil).
// 		If(cnt).Should().Equal(5)
// }

// //
// // Use type aliases and methods to implement FMap
// type persons []person

// func (seq *persons) Join(val *person) error {
// 	*seq = append(*seq, *val)
// 	return nil
// }

// func TestDdbFMapNone(t *testing.T) {
// 	seq := persons{}
// 	ddb := mockQuery[person](
// 		map[string]*dynamodb.AttributeValue{
// 			":prefix": {S: aws.String("dead:beef")},
// 		},
// 		0, nil,
// 	)

// 	err := ddb.Match(person{Prefix: dynamo.NewIRI("dead:beef")}).FMap(seq.Join)
// 	it.Ok(t).
// 		If(err).Should().Equal(nil).
// 		If(seq).Should().Equal(persons{})

// }

// func TestDdbFMapPrefixOnly(t *testing.T) {
// 	seq := persons{}
// 	ddb := mockQuery[person](
// 		map[string]*dynamodb.AttributeValue{
// 			":prefix": {S: aws.String("dead:beef")},
// 		},
// 		2, nil,
// 	)
// 	thing := entityStruct()

// 	err := ddb.Match(person{Prefix: dynamo.NewIRI("dead:beef")}).FMap(seq.Join)
// 	it.Ok(t).
// 		If(err).Should().Equal(nil).
// 		If(seq).Should().Equal(persons{thing, thing})
// }

// func TestDdbFMapPrefixAndSuffix(t *testing.T) {
// 	seq := persons{}
// 	ddb := mockQuery[person](
// 		map[string]*dynamodb.AttributeValue{
// 			":prefix": {S: aws.String("dead:beef")},
// 			":suffix": {S: aws.String("a/b/c")},
// 		},
// 		2, nil,
// 	)
// 	thing := entityStruct()

// 	err := ddb.Match(person{
// 		Prefix: dynamo.NewIRI("dead:beef"),
// 		Suffix: dynamo.NewIRI("a/b/c"),
// 	}).FMap(seq.Join)

// 	it.Ok(t).
// 		If(err).Should().Equal(nil).
// 		If(seq).Should().Equal(persons{thing, thing})
// }

// func TestDdbFMapThings(t *testing.T) {
// 	seq := dynamo.Things[person]{}
// 	ddb := mockQuery[person](
// 		map[string]*dynamodb.AttributeValue{
// 			":prefix": {S: aws.String("dead:beef")},
// 		},
// 		2, nil,
// 	)
// 	expect := dynamo.Things[person]{entityStruct(), entityStruct()}

// 	err := ddb.Match(person{Prefix: dynamo.NewIRI("dead:beef")}).FMap(seq.Join)
// 	it.Ok(t).
// 		If(err).Should().Equal(nil).
// 		If(seq).Should().Equal(expect)
// }

// func TestDdbCursorAndContinue(t *testing.T) {
// 	// seq := persons{}
// 	ddb := mockQuery[person](
// 		map[string]*dynamodb.AttributeValue{
// 			":prefix": {S: aws.String("dead:beef")},
// 		},
// 		2,
// 		map[string]*dynamodb.AttributeValue{
// 			"prefix": {S: aws.String("dead:beef")},
// 			"suffix": {S: aws.String("1")},
// 		},
// 	)

// 	dbseq := ddb.Match(person{Prefix: dynamo.NewIRI("dead:beef")})
// 	dbseq.Tail()
// 	cursor0 := dbseq.Cursor()

// 	dbseq = ddb.Match(person{Prefix: dynamo.NewIRI("dead:beef")}).Continue(cursor0)
// 	dbseq.Tail()
// 	cursor1 := dbseq.Cursor()

// 	it.Ok(t).
// 		If(cursor0.HashKey()).Equal("dead:beef").
// 		If(cursor0.SortKey()).Equal("1").
// 		If(cursor1.HashKey()).Equal("dead:beef").
// 		If(cursor1.SortKey()).Equal("1")
// }

// //-----------------------------------------------------------------------------
// //
// // Mock Dynamo DB
// //
// //-----------------------------------------------------------------------------

// //
// //
// type ddbGetItem struct {
// 	dynamodbiface.DynamoDBAPI
// 	returnVal map[string]*dynamodb.AttributeValue
// }

// func mockGetItem[T dynamo.Thing](returnVal map[string]*dynamodb.AttributeValue) dynamo.KeyValNoContext[T] {
// 	return mockDynamoDB[T](&ddbGetItem{returnVal: returnVal})
// }

// func (mock *ddbGetItem) GetItemWithContext(ctx aws.Context, input *dynamodb.GetItemInput, opts ...request.Option) (*dynamodb.GetItemOutput, error) {
// 	expect := map[string]*dynamodb.AttributeValue{
// 		"prefix": {S: aws.String("dead:beef")},
// 		"suffix": {S: aws.String("1")},
// 	}

// 	if !reflect.DeepEqual(expect, input.Key) {
// 		return nil, errors.New("Unexpected entity.")
// 	}

// 	return &dynamodb.GetItemOutput{Item: mock.returnVal}, nil
// }

// //
// //
// type ddbPutItem struct {
// 	dynamodbiface.DynamoDBAPI
// 	expectVal map[string]*dynamodb.AttributeValue
// }

// func mockPutItem[T dynamo.Thing](expectVal map[string]*dynamodb.AttributeValue) dynamo.KeyValNoContext[T] {
// 	return mockDynamoDB[T](&ddbPutItem{expectVal: expectVal})
// }

// func (mock *ddbPutItem) PutItemWithContext(ctx aws.Context, input *dynamodb.PutItemInput, opts ...request.Option) (*dynamodb.PutItemOutput, error) {
// 	if !reflect.DeepEqual(mock.expectVal, input.Item) {
// 		return nil, errors.New("Unexpected entity.")
// 	}
// 	return &dynamodb.PutItemOutput{}, nil
// }

// //
// //
// type ddbDeleteItem struct {
// 	dynamodbiface.DynamoDBAPI
// 	expectKey map[string]*dynamodb.AttributeValue
// }

// func mockDeleteItem[T dynamo.Thing](expectKey map[string]*dynamodb.AttributeValue) dynamo.KeyValNoContext[T] {
// 	return mockDynamoDB[T](&ddbDeleteItem{expectKey: expectKey})
// }

// func (mock *ddbDeleteItem) DeleteItemWithContext(ctx aws.Context, input *dynamodb.DeleteItemInput, opts ...request.Option) (*dynamodb.DeleteItemOutput, error) {
// 	if !reflect.DeepEqual(mock.expectKey, input.Key) {
// 		return nil, errors.New("Unexpected entity.")
// 	}

// 	return &dynamodb.DeleteItemOutput{}, nil
// }

// //
// //
// type ddbUpdateItem struct {
// 	dynamodbiface.DynamoDBAPI
// 	expectKey map[string]*dynamodb.AttributeValue
// 	expectVal map[string]*dynamodb.AttributeValue
// }

// func mockUpdateItem[T dynamo.Thing](expectKey map[string]*dynamodb.AttributeValue, expectVal map[string]*dynamodb.AttributeValue) dynamo.KeyValNoContext[T] {
// 	return mockDynamoDB[T](&ddbUpdateItem{expectKey: expectKey, expectVal: expectVal})
// }

// func (mock *ddbUpdateItem) UpdateItemWithContext(ctx aws.Context, input *dynamodb.UpdateItemInput, opts ...request.Option) (*dynamodb.UpdateItemOutput, error) {
// 	if !reflect.DeepEqual(mock.expectKey, input.Key) {
// 		return nil, errors.New("Unexpected entity.")
// 	}

// 	if !reflect.DeepEqual(mock.expectVal, input.ExpressionAttributeValues) {
// 		return nil, errors.New("Unexpected entity.")
// 	}

// 	return &dynamodb.UpdateItemOutput{
// 		Attributes: map[string]*dynamodb.AttributeValue{
// 			"prefix":  {S: aws.String("dead:beef")},
// 			"address": {S: aws.String("Blumenstrasse 14, Berne, 3013")},
// 			"name":    {S: aws.String("Verner Pleishner")},
// 			"suffix":  {S: aws.String("1")},
// 			"age":     {N: aws.String("64")},
// 		},
// 	}, nil
// }

// //
// //
// type ddbQuery struct {
// 	dynamodbiface.DynamoDBAPI
// 	expectKey     map[string]*dynamodb.AttributeValue
// 	returnLen     int
// 	returnLastKey map[string]*dynamodb.AttributeValue
// }

// func mockQuery[T dynamo.Thing](
// 	expectKey map[string]*dynamodb.AttributeValue,
// 	returnLen int,
// 	returnLastKey map[string]*dynamodb.AttributeValue,
// ) dynamo.KeyValNoContext[T] {
// 	return mockDynamoDB[T](&ddbQuery{expectKey: expectKey, returnLen: returnLen, returnLastKey: returnLastKey})
// }

// func (mock *ddbQuery) QueryWithContext(ctx aws.Context, input *dynamodb.QueryInput, opts ...request.Option) (*dynamodb.QueryOutput, error) {
// 	if !reflect.DeepEqual(mock.expectKey, input.ExpressionAttributeValues) {
// 		return nil, errors.New("Unexpected entity.")
// 	}

// 	seq := []map[string]*dynamodb.AttributeValue{}
// 	for i := 0; i < mock.returnLen; i++ {
// 		seq = append(seq, map[string]*dynamodb.AttributeValue{
// 			"prefix":  {S: aws.String("dead:beef")},
// 			"address": {S: aws.String("Blumenstrasse 14, Berne, 3013")},
// 			"name":    {S: aws.String("Verner Pleishner")},
// 			"suffix":  {S: aws.String("1")},
// 			"age":     {N: aws.String("64")},
// 		})
// 	}

// 	return &dynamodb.QueryOutput{
// 		ScannedCount:     aws.Int64(int64(mock.returnLen)),
// 		Count:            aws.Int64(int64(mock.returnLen)),
// 		Items:            seq,
// 		LastEvaluatedKey: mock.returnLastKey,
// 	}, nil
// }

// //
// //
// type MockDynamoDB interface {
// 	Mock(db dynamodbiface.DynamoDBAPI)
// }

// func mockDynamoDB[T dynamo.Thing](mock dynamodbiface.DynamoDBAPI) dynamo.KeyValNoContext[T] {
// 	client, _ := session.NewV2[T]("ddb:///test")
// 	//dynamo.Must(dynamo.New("ddb:///test"))
// 	switch v := client.(type) {
// 	case MockDynamoDB:
// 		v.Mock(mock)
// 	default:
// 		panic("Invalid config")
// 	}

// 	return dynamo.NewKeyValContextDefault(client)
// }
