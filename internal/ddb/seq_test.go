package ddb_test

// func TestDdbMatchNone(t *testing.T) {
// 	ddb := ddbtest.Query[person](
// 		map[string]*dynamodb.AttributeValue{
// 			":prefix": {S: aws.String("dead:beef")},
// 		},
// 		0, entityDynamo(), nil,
// 	)

// 	seq := ddb.Match(person{Prefix: dynamo.NewIRI("dead:beef")})

// 	it.Ok(t).
// 		IfFalse(seq.Tail()).
// 		If(seq.Error()).Should().Equal(nil)
// }

// func TestDdbMatchOne(t *testing.T) {
// 	ddb := ddbtest.Query[person](
// 		map[string]*dynamodb.AttributeValue{
// 			":prefix": {S: aws.String("dead:beef")},
// 		},
// 		1, entityDynamo(), nil,
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
// 	ddb := ddbtest.Query[person](
// 		map[string]*dynamodb.AttributeValue{
// 			":prefix": {S: aws.String("dead:beef")},
// 		},
// 		5, entityDynamo(), nil,
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

//
// Use type aliases and methods to implement FMap
type persons []person

func (seq *persons) Join(val *person) error {
	*seq = append(*seq, *val)
	return nil
}

// func TestDdbFMapNone(t *testing.T) {
// 	seq := persons{}
// 	ddb := ddbtest.Query[person](
// 		map[string]*dynamodb.AttributeValue{
// 			":prefix": {S: aws.String("dead:beef")},
// 		},
// 		0, entityDynamo(), nil,
// 	)

// 	err := ddb.Match(person{Prefix: dynamo.NewIRI("dead:beef")}).FMap(seq.Join)
// 	it.Ok(t).
// 		If(err).Should().Equal(nil).
// 		If(seq).Should().Equal(persons{})

// }

// func TestDdbFMapPrefixOnly(t *testing.T) {
// 	seq := persons{}
// 	ddb := ddbtest.Query[person](
// 		map[string]*dynamodb.AttributeValue{
// 			":prefix": {S: aws.String("dead:beef")},
// 		},
// 		2, entityDynamo(), nil,
// 	)
// 	thing := entityStruct()

// 	err := ddb.Match(person{Prefix: dynamo.NewIRI("dead:beef")}).FMap(seq.Join)
// 	it.Ok(t).
// 		If(err).Should().Equal(nil).
// 		If(seq).Should().Equal(persons{thing, thing})
// }

// func TestDdbFMapPrefixAndSuffix(t *testing.T) {
// 	seq := persons{}
// 	ddb := ddbtest.Query[person](
// 		map[string]*dynamodb.AttributeValue{
// 			":prefix": {S: aws.String("dead:beef")},
// 			":suffix": {S: aws.String("a/b/c")},
// 		},
// 		2, entityDynamo(), nil,
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
// 	ddb := ddbtest.Query[person](
// 		map[string]*dynamodb.AttributeValue{
// 			":prefix": {S: aws.String("dead:beef")},
// 		},
// 		2, entityDynamo(), nil,
// 	)
// 	expect := dynamo.Things[person]{entityStruct(), entityStruct()}

// 	err := ddb.Match(person{Prefix: dynamo.NewIRI("dead:beef")}).FMap(seq.Join)
// 	it.Ok(t).
// 		If(err).Should().Equal(nil).
// 		If(seq).Should().Equal(expect)
// }

// func TestDdbCursorAndContinue(t *testing.T) {
// 	ddb := ddbtest.Query[person](
// 		map[string]*dynamodb.AttributeValue{
// 			":prefix": {S: aws.String("dead:beef")},
// 		},
// 		2, entityDynamo(), keyDynamo(),
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
