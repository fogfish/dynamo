package dynamo_test

/*
type MyType struct {
	HKey curie.IRI     `dynamodbav:"-"`
	SKey curie.IRI     `dynamodbav:"-"`
	Link *curie.String `dynamodbav:"link,omitempty"`
}

func (x MyType) MarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	type tStruct MyType
	return dynamo.Encode(av, dynamo.IRI(x.HKey), dynamo.IRI(x.SKey), tStruct(x))
}

func (x *MyType) UnmarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	type tStruct *MyType
	return dynamo.Decode(av, (*dynamo.IRI)(&x.HKey), (*dynamo.IRI)(&x.SKey), tStruct(x))
}

func TestEncodeDecode(t *testing.T) {
	core := MyType{
		HKey: curie.New("test:a/b"),
		SKey: curie.New("c/d"),
		Link: curie.Safe(curie.New("test:a/b/c")),
	}

	av, err := dynamodbattribute.Marshal(core)
	it.Ok(t).IfNil(err)

	var some MyType
	err = dynamodbattribute.Unmarshal(av, &some)
	it.Ok(t).IfNil(err)

	it.Ok(t).
		IfTrue(curie.Eq(core.HKey, some.HKey)).
		IfTrue(curie.Eq(core.SKey, some.SKey)).
		IfTrue(*core.Link == *some.Link)
}

func TestEncodeDecodeKeyOnly(t *testing.T) {
	core := MyType{
		HKey: curie.New("test:a/b"),
		SKey: curie.New("c/d"),
	}

	av, err := dynamodbattribute.Marshal(core)
	it.Ok(t).IfNil(err)

	var some MyType
	err = dynamodbattribute.Unmarshal(av, &some)
	it.Ok(t).IfNil(err)

	it.Ok(t).
		IfTrue(curie.Eq(core.HKey, some.HKey)).
		IfTrue(curie.Eq(core.SKey, some.SKey))
}

func TestEncodeDecodeKeyOnlyHash(t *testing.T) {
	core := MyType{
		HKey: curie.New("test:a/b"),
	}

	av, err := dynamodbattribute.Marshal(core)
	it.Ok(t).IfNil(err)

	var some MyType
	err = dynamodbattribute.Unmarshal(av, &some)
	it.Ok(t).IfNil(err)

	it.Ok(t).
		IfTrue(curie.Eq(core.HKey, some.HKey)).
		IfTrue(curie.Eq(core.SKey, some.SKey))
}

type BadString string

func (x BadString) MarshalDynamoDBAttributeValue(*dynamodb.AttributeValue) error {
	return fmt.Errorf("Encode error.")
}

func (x *BadString) UnmarshalDynamoDBAttributeValue(*dynamodb.AttributeValue) error {
	return fmt.Errorf("Decode error.")
}

type BadType struct {
	HKey curie.IRI `dynamodbav:"-"`
	SKey curie.IRI `dynamodbav:"-"`
	Link BadString `dynamodbav:"link,omitempty"`
}

func (x BadType) MarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	type tStruct BadType
	return dynamo.Encode(av, dynamo.IRI(x.HKey), dynamo.IRI(x.SKey), tStruct(x))
}

func (x *BadType) UnmarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	type tStruct *BadType
	return dynamo.Decode(av, (*dynamo.IRI)(&x.HKey), (*dynamo.IRI)(&x.SKey), tStruct(x))
}

func TestEncodeBadType(t *testing.T) {
	core := BadType{
		HKey: curie.New("test:a/b"),
		SKey: curie.New("c/d"),
		Link: BadString("test:a/b/c"),
	}

	_, err := dynamodbattribute.Marshal(core)
	it.Ok(t).IfNotNil(err)
}

type BadHKey struct {
	HKey BadString `dynamodbav:"-"`
	SKey curie.IRI `dynamodbav:"-"`
}

func (x BadHKey) MarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	type tStruct BadHKey
	return dynamo.Encode(av, x.HKey, dynamo.IRI(x.SKey), tStruct(x))
}

func (x *BadHKey) UnmarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	type tStruct *BadHKey
	return dynamo.Decode(av, &x.HKey, (*dynamo.IRI)(&x.SKey), tStruct(x))
}

func TestEncodeBadHKey(t *testing.T) {
	core := BadHKey{
		HKey: BadString("test:a/b"),
		SKey: curie.New("c/d"),
	}

	_, err := dynamodbattribute.Marshal(core)
	it.Ok(t).IfNotNil(err)
}

type BadSKey struct {
	HKey curie.IRI `dynamodbav:"-"`
	SKey BadString `dynamodbav:"-"`
}

func (x BadSKey) MarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	type tStruct BadSKey
	return dynamo.Encode(av, dynamo.IRI(x.HKey), x.SKey, tStruct(x))
}

func (x *BadSKey) UnmarshalDynamoDBAttributeValue(av *dynamodb.AttributeValue) error {
	type tStruct *BadSKey
	return dynamo.Decode(av, (*dynamo.IRI)(&x.HKey), &x.SKey, tStruct(x))
}

func TestEncodeBadSKey(t *testing.T) {
	core := BadSKey{
		HKey: curie.New("c/d"),
		SKey: BadString("test:a/b"),
	}

	_, err := dynamodbattribute.Marshal(core)
	it.Ok(t).IfNotNil(err)
}
*/
