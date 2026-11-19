//
// Copyright (C) 2025 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

package ddb_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/fogfish/dynamo/v4"
	"github.com/fogfish/dynamo/v4/service/ddb"
	"github.com/fogfish/gold"
	"github.com/fogfish/it/v2"
)

//
// The tests verify the identity model built with gold types:
// gold.IRI[T] and gold.Imply[A, B] are stored as DynamoDB strings,
// are validated on read/write and behave the same in expressions.
//

type University struct{}
type Course struct{}
type Pers struct{}

// Imply is declared as an alias, otherwise it drops methods of gold.Imply
type Enrollment = gold.Imply[University, Course]

type Student struct {
	Key    Enrollment       `dynamodbav:"prefix"`
	ID     gold.IRI[Pers]   `dynamodbav:"suffix,omitempty"`
	Mentor *gold.IRI[Pers]  `dynamodbav:"mentor,omitempty"`
	Prev   *Enrollment      `dynamodbav:"prev,omitempty"`
	Name   string           `dynamodbav:"name,omitempty"`
	Grade  gold.IRI[Course] `dynamodbav:"grade,omitempty"`
}

func (s Student) HashKey() string { return s.Key.String() }
func (s Student) SortKey() string { return string(s.ID) }

// Enrolled uses Imply as a sort key
type Enrolled struct {
	Org gold.IRI[University] `dynamodbav:"prefix"`
	Key Enrollment           `dynamodbav:"suffix,omitempty"`
}

func (e Enrolled) HashKey() string { return string(e.Org) }
func (e Enrolled) SortKey() string { return e.Key.String() }

func fixtureStudent() Student {
	mentor := gold.ToIRI[Pers]("neumann")
	prev := gold.ImplyFrom(gold.ToIRI[University]("kiel"), gold.ToIRI[Course]("cs100"))
	return Student{
		Key:    gold.ImplyFrom(gold.ToIRI[University]("kiel"), gold.ToIRI[Course]("cs101")),
		ID:     gold.ToIRI[Pers]("8980789222"),
		Mentor: &mentor,
		Prev:   &prev,
		Name:   "Verner Pleishner",
		Grade:  gold.ToIRI[Course]("cs101"),
	}
}

// mock of DynamoDB that captures requests
type goldMock struct {
	ddb.DynamoDB
	calls int

	put    *dynamodb.PutItemInput
	update *dynamodb.UpdateItemInput
	query  *dynamodb.QueryInput

	item  map[string]types.AttributeValue
	items []map[string]types.AttributeValue
}

func (m *goldMock) PutItem(_ context.Context, in *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	m.calls++
	m.put = in
	return &dynamodb.PutItemOutput{}, nil
}

func (m *goldMock) GetItem(_ context.Context, in *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	m.calls++
	return &dynamodb.GetItemOutput{Item: m.item}, nil
}

func (m *goldMock) UpdateItem(_ context.Context, in *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	m.calls++
	m.update = in
	return &dynamodb.UpdateItemOutput{Attributes: m.item}, nil
}

func (m *goldMock) Query(_ context.Context, in *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	m.calls++
	m.query = in
	return &dynamodb.QueryOutput{Items: m.items, Count: int32(len(m.items))}, nil
}

func goldStorage[T dynamo.Thing](m *goldMock) *ddb.Storage[T] {
	return ddb.Must(ddb.New[T]("test", ddb.WithDynamoDB(m)))
}

func isS(t *testing.T, name string, av types.AttributeValue) {
	t.Helper()
	if _, ok := av.(*types.AttributeValueMemberS); !ok {
		t.Errorf("%s is expected to be S, got %T", name, av)
	}
}

func valueS(av types.AttributeValue) string {
	if s, ok := av.(*types.AttributeValueMemberS); ok {
		return s.Value
	}
	return ""
}

// 1. Imply is stored as string
func TestGoldImplyStoredAsString(t *testing.T) {
	m := &goldMock{}
	db := goldStorage[Student](m)

	err := db.Put(context.Background(), fixtureStudent())
	it.Then(t).Should(it.Nil(err))

	for _, k := range []string{"prefix", "suffix", "mentor", "prev", "grade"} {
		isS(t, k, m.put.Item[k])
	}

	it.Then(t).Should(
		it.Equal(valueS(m.put.Item["prefix"]), "university:kiel|course:cs101"),
		it.Equal(valueS(m.put.Item["suffix"]), "pers:8980789222"),
		it.Equal(valueS(m.put.Item["mentor"]), "pers:neumann"),
		it.Equal(valueS(m.put.Item["prev"]), "university:kiel|course:cs100"),
	)
}

// 2. round trip
func TestGoldRoundTrip(t *testing.T) {
	m := &goldMock{}
	db := goldStorage[Student](m)

	err := db.Put(context.Background(), fixtureStudent())
	it.Then(t).Should(it.Nil(err))

	m.item = m.put.Item
	val, err := db.Get(context.Background(), fixtureStudent())
	it.Then(t).Should(
		it.Nil(err),
		it.Equal(val.Key, fixtureStudent().Key),
		it.Equal(val.ID, fixtureStudent().ID),
		it.Equal(*val.Mentor, *fixtureStudent().Mentor),
		it.Equal(*val.Prev, *fixtureStudent().Prev),
		it.Equal(val.Grade, fixtureStudent().Grade),
	)
}

// 3. strict read
func TestGoldStrictRead(t *testing.T) {
	bad := func() map[string]types.AttributeValue {
		m := &goldMock{}
		_ = goldStorage[Student](m).Put(context.Background(), fixtureStudent())
		item := map[string]types.AttributeValue{}
		for k, v := range m.put.Item {
			item[k] = v
		}
		// the identity of the wrong schema, blog:x instead of pers:x
		item["suffix"] = &types.AttributeValueMemberS{Value: "blog:x"}
		return item
	}

	t.Run("Get", func(t *testing.T) {
		m := &goldMock{item: bad()}
		_, err := goldStorage[Student](m).Get(context.Background(), fixtureStudent())
		it.Then(t).ShouldNot(it.Nil(err))
		it.Then(t).Should(it.True(strings.Contains(err.Error(), "invalid entity")))
	})

	t.Run("Match", func(t *testing.T) {
		good := &goldMock{}
		_ = goldStorage[Student](good).Put(context.Background(), fixtureStudent())

		m := &goldMock{items: []map[string]types.AttributeValue{good.put.Item, bad()}}
		seq, _, err := goldStorage[Student](m).Match(context.Background(), fixtureStudent())
		it.Then(t).ShouldNot(it.Nil(err))
		it.Then(t).Should(
			it.True(strings.Contains(err.Error(), "invalid entity")),
			it.Seq(seq).BeEmpty(),
		)
	})

	t.Run("BadImply", func(t *testing.T) {
		item := bad()
		item["suffix"] = &types.AttributeValueMemberS{Value: "pers:1"}
		item["prefix"] = &types.AttributeValueMemberS{Value: "university:kiel|blog:cs101"}
		m := &goldMock{item: item}
		_, err := goldStorage[Student](m).Get(context.Background(), fixtureStudent())
		it.Then(t).ShouldNot(it.Nil(err))
	})
}

// 4. strict write
func TestGoldStrictWrite(t *testing.T) {
	t.Run("IRI", func(t *testing.T) {
		m := &goldMock{}
		val := fixtureStudent()
		val.ID = gold.IRI[Pers]("alice") // not normalized, no Norm()

		err := goldStorage[Student](m).Put(context.Background(), val)
		it.Then(t).ShouldNot(it.Nil(err))
		it.Then(t).Should(it.Equal(m.calls, 0))
	})

	t.Run("Imply", func(t *testing.T) {
		m := &goldMock{}
		val := fixtureStudent()
		val.Key = gold.ImplyFrom(gold.IRI[University]("kiel"), gold.ToIRI[Course]("cs101"))

		err := goldStorage[Student](m).Put(context.Background(), val)
		it.Then(t).ShouldNot(it.Nil(err))
		it.Then(t).Should(it.Equal(m.calls, 0))
	})
}

// 5. sort key placeholder
func TestGoldSortKeyPlaceholder(t *testing.T) {
	val := fixtureStudent()
	val.ID = ""

	m := &goldMock{}
	db := goldStorage[Student](m)
	err := db.Put(context.Background(), val)
	it.Then(t).Should(it.Nil(err))
	it.Then(t).Should(it.Equal(valueS(m.put.Item["suffix"]), "_"))

	// the placeholder is not leaked to application type
	stored := m.put.Item
	m.item = stored
	m.items = []map[string]types.AttributeValue{stored}

	got, err := db.Get(context.Background(), val)
	it.Then(t).Should(
		it.Nil(err),
		it.Equal(got.ID, gold.IRI[Pers]("")),
	)

	seq, _, err := db.Match(context.Background(), val)
	it.Then(t).Should(
		it.Nil(err),
		it.Equal(len(seq), 1),
		it.Equal(seq[0].ID, gold.IRI[Pers]("")),
	)

	// caller's map is not mutated
	it.Then(t).Should(it.Equal(valueS(stored["suffix"]), "_"))
}

// 5b. sort key stored as NULL or "" is replaced by the placeholder
func TestGoldSortKeyPlaceholderOfEmpty(t *testing.T) {
	m := &goldMock{}
	db := goldStorage[Enrolled](m)

	err := db.Put(context.Background(), Enrolled{Org: gold.ToIRI[University]("kiel")})
	it.Then(t).Should(it.Nil(err))
	// empty Imply is not dropped by omitempty, it is stored as "" and replaced
	it.Then(t).Should(it.Equal(valueS(m.put.Item["suffix"]), "_"))
}

// 6. empty Imply hash key
func TestGoldEmptyHashKey(t *testing.T) {
	m := &goldMock{}
	db := goldStorage[Student](m)

	err := db.Put(context.Background(), Student{ID: gold.ToIRI[Pers]("1")})
	it.Then(t).ShouldNot(it.Nil(err))
	it.Then(t).Should(
		it.True(strings.Contains(err.Error(), "invalid key")),
		it.Equal(m.calls, 0),
	)

	_, err = db.Update(context.Background(), Student{ID: gold.ToIRI[Pers]("1")})
	it.Then(t).ShouldNot(it.Nil(err))
	it.Then(t).Should(it.Equal(m.calls, 0))
}

// 7. expressions
func TestGoldExpressions(t *testing.T) {
	kiel := gold.ToIRI[University]("kiel")
	cs := func(id string) Enrollment { return gold.ImplyFrom(kiel, gold.ToIRI[Course](id)) }

	mentor := ddb.UpdateFor[Student, gold.IRI[Pers]]("Mentor")
	prev := ddb.UpdateFor[Student, Enrollment]("Prev")
	key := ddb.ClauseFor[Student, Enrollment]("Key")

	// all values of the expression must be strings, as the stored ones
	check := func(t *testing.T, cond interface{ WriterOpt(Student) }, expect map[string]string) {
		t.Helper()

		m := &goldMock{item: map[string]types.AttributeValue{
			"prefix": &types.AttributeValueMemberS{Value: "university:kiel|course:cs101"},
			"suffix": &types.AttributeValueMemberS{Value: "pers:1"},
		}}
		db := goldStorage[Student](m)

		_, err := db.UpdateWith(context.Background(),
			ddb.Updater(fixtureStudent(),
				mentor.Set(gold.ToIRI[Pers]("neumann")),
				prev.Set(cs("cs100")),
			),
			cond,
		)
		if err != nil {
			t.Fatalf("update failed: %v", err)
		}

		vals := m.update.ExpressionAttributeValues
		for k, v := range vals {
			isS(t, k, v)
		}

		expect[":__mentor__"] = "pers:neumann"
		expect[":__prev__"] = "university:kiel|course:cs100"
		for k, v := range expect {
			it.Then(t).Should(it.Equal(valueS(vals[k]), v))
		}
		it.Then(t).Should(it.Equal(len(vals), len(expect)))
	}

	t.Run("Eq", func(t *testing.T) {
		check(t, key.Eq(cs("cs101")), map[string]string{
			":__c_prefix__": "university:kiel|course:cs101",
		})
	})

	t.Run("In", func(t *testing.T) {
		check(t, key.In(cs("cs101"), cs("cs102")), map[string]string{
			":__c_prefix_0__": "university:kiel|course:cs101",
			":__c_prefix_1__": "university:kiel|course:cs102",
		})
	})

	t.Run("Between", func(t *testing.T) {
		check(t, key.Between(cs("cs100"), cs("cs200")), map[string]string{
			":__c_prefix_a__": "university:kiel|course:cs100",
			":__c_prefix_b__": "university:kiel|course:cs200",
		})
	})

	t.Run("HasPrefix", func(t *testing.T) {
		check(t, key.HasPrefix(gold.ImplyFrom(kiel, gold.IRI[Course](""))), map[string]string{
			":__c_prefix__": "university:kiel|",
		})
	})

	t.Run("Put", func(t *testing.T) {
		m := &goldMock{}
		err := goldStorage[Student](m).Put(context.Background(), fixtureStudent(), key.Eq(cs("cs101")))
		it.Then(t).Should(it.Nil(err))
		isS(t, ":__c_prefix__", m.put.ExpressionAttributeValues[":__c_prefix__"])
	})
}

// 8. prefix match
func TestGoldPrefixMatch(t *testing.T) {
	t.Run("Any", func(t *testing.T) {
		m := &goldMock{}
		db := goldStorage[Student](m)

		_, _, err := db.Match(context.Background(), Student{
			Key: gold.ImplyFrom(gold.ToIRI[University]("kiel"), gold.ToIRI[Course]("cs101")),
			ID:  gold.Any[Pers](),
		})
		it.Then(t).Should(it.Nil(err))
		it.Then(t).Should(
			it.Equal(*m.query.KeyConditionExpression, "prefix = :__prefix__ and begins_with(suffix, :__suffix__)"),
			it.Equal(valueS(m.query.ExpressionAttributeValues[":__suffix__"]), "pers:"),
		)
	})

	t.Run("PartialImply", func(t *testing.T) {
		m := &goldMock{}
		db := goldStorage[Enrolled](m)

		_, _, err := db.Match(context.Background(), Enrolled{
			Org: gold.ToIRI[University]("kiel"),
			Key: gold.ImplyFrom(gold.ToIRI[University]("kiel"), gold.IRI[Course]("")),
		})
		it.Then(t).Should(it.Nil(err))
		it.Then(t).Should(
			it.Equal(*m.query.KeyConditionExpression, "prefix = :__prefix__ and begins_with(suffix, :__suffix__)"),
			it.Equal(valueS(m.query.ExpressionAttributeValues[":__suffix__"]), "university:kiel|"),
		)
	})

	t.Run("HashOnly", func(t *testing.T) {
		m := &goldMock{}
		db := goldStorage[Enrolled](m)

		_, _, err := db.Match(context.Background(), Enrolled{Org: gold.ToIRI[University]("kiel")})
		it.Then(t).Should(it.Nil(err))
		it.Then(t).Should(it.Equal(*m.query.KeyConditionExpression, "prefix = :__prefix__"))
	})
}

// 9. the library calls attributevalue (un)marshalers only through av.go
func TestNoDirectAttributeValueCalls(t *testing.T) {
	direct := regexp.MustCompile(`attributevalue\.(Marshal|MarshalMap|Unmarshal|UnmarshalMap)\(`)

	files, err := filepath.Glob("*.go")
	it.Then(t).Should(it.Nil(err))

	for _, file := range files {
		if file == "av.go" || strings.HasSuffix(file, "_test.go") {
			continue
		}

		b, err := os.ReadFile(file)
		it.Then(t).Should(it.Nil(err))
		if direct.Match(b) {
			t.Errorf("%s calls attributevalue directly, use av.go helpers", file)
		}
	}
}

// 10. the core of the library does not depend on identity libraries
func TestNoIdentityDependencies(t *testing.T) {
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go tool is not available")
	}

	for _, pkg := range []string{".", "../.."} {
		out, err := exec.Command("go", "list", "-deps", pkg).CombinedOutput()
		if err != nil {
			t.Skipf("go list failed: %v: %s", err, out)
		}

		for _, line := range strings.Split(string(out), "\n") {
			if strings.Contains(line, "curie") || strings.Contains(line, "fogfish/gold") {
				t.Errorf("%s depends on %s", pkg, line)
			}
		}
	}
}
