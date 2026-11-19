# dynamo

The library implements a simple key-value abstraction to store algebraic, linked-data data types at AWS DynamoDB.

[![Version](https://img.shields.io/github/v/tag/fogfish/dynamo?label=version)](https://github.com/fogfish/dynamo/releases)
[![Documentation](https://pkg.go.dev/badge/github.com/fogfish/dynamo/v4)](https://pkg.go.dev/github.com/fogfish/dynamo/v4)
[![Build Status](https://github.com/fogfish/dynamo/workflows/build/badge.svg)](https://github.com/fogfish/dynamo/actions/)
[![Git Hub](https://img.shields.io/github/last-commit/fogfish/dynamo.svg)](https://github.com/fogfish/dynamo)
[![Coverage Status](https://coveralls.io/repos/github/fogfish/dynamo/badge.svg?branch=main)](https://coveralls.io/github/fogfish/dynamo?branch=main)
[![Go Report Card](https://goreportcard.com/badge/github.com/fogfish/dynamo/v4)](https://goreportcard.com/report/github.com/fogfish/dynamo/v4)
[![Mentioned in Awesome Go](https://awesome.re/mentioned-badge-flat.svg)](https://github.com/avelino/awesome-go)


## Inspiration

The library encourages developers to use Golang struct to define domain models, write correct, maintainable code. Using this library, the application can achieve the ideal data model that would require a single request to DynamoDB and model one-to-one, one-to-many and even many-to-many relations. The library uses generic programming style to implement actual storage I/O, while expose external domain object as `[T dynamo.Thing]` with implicit conversion back and forth between a concrete struct(s). The library uses [AWS Golang SDK v2](https://github.com/aws/aws-sdk-go-v2) under the hood.

Essentially, the library implement a following generic key-value trait to access domain objects. 

```go
type KeyVal[T dynamo.Thing] interface {
  Put(T) error
  Get(T) (T, error)
  Remove(T) (T, error)
  Update(T) (T, error)
  Match(T) ([]T, error)
}
```

The library philosophy and use-cases are covered in depth at the post
[How To Model Any Relational Data in DynamoDB With dynamo library](example/relational/README.md) or continue reading the Getting started section.


## Getting started

The library requires Go **1.24** or later due to usage of [generics](https://go.dev/blog/intro-generics).

The latest version of the library is available at its `main` branch. All development, including new features and bug fixes, take place on the `main` branch using forking and pull requests as described in contribution guidelines. The stable version is available via Golang modules.


- [dynamo](#dynamo)
  - [Inspiration](#inspiration)
  - [Getting started](#getting-started)
    - [Data types definition](#data-types-definition)
    - [Identity with gold](#identity-with-gold)
    - [DynamoDB I/O](#dynamodb-io)
    - [Error Handling](#error-handling)
    - [Hierarchical structures](#hierarchical-structures)
    - [Sequences and Pagination](#sequences-and-pagination)
    - [Linked data](#linked-data)
    - [Type projections](#type-projections)
    - [Custom codecs for core domain types](#custom-codecs-for-core-domain-types)
    - [DynamoDB Expressions](#dynamodb-expressions)
      - [Projection Expression](#projection-expression)
      - [Conditional Expression](#conditional-expression)
      - [Update Expression](#update-expression)
      - [Set Types](#set-types)
    - [Optimistic Locking](#optimistic-locking)
    - [Batch I/O](#batch-io)
    - [Configure DynamoDB](#configure-dynamodb)
    - [Migrating from v3](#migrating-from-v3)
  - [How To Contribute](#how-to-contribute)
    - [commit message](#commit-message)
    - [bugs](#bugs)
  - [License](#license)


### Data types definition

Data types definition is an essential part of development with `dynamo` library. Golang structs declares domain of your application. Public fields are serialized into DynamoDB attributes, the field tag `dynamodbav` controls marshal/unmarshal processes.

The library demands from each structure implementation of `Thing` interface. This type acts as struct annotation -- Golang compiler raises an error at compile time if other data type is supplied to the dynamo library. Secondly, each structure defines unique "composite primary key", both partition and sort keys are opaque `string`s to the library. The library encourages definition of the keys using type-safe identities of [gold](https://github.com/fogfish/gold) library: `gold.IRI[T]` and `gold.Imply[A, B]`, see [Identity with gold](#identity-with-gold).

```go
type University struct{}
type Person struct{}

type Professor struct {
  Org     gold.IRI[University] `dynamodbav:"prefix,omitempty"`
  ID      gold.IRI[Person]     `dynamodbav:"suffix,omitempty"`
  Name    string               `dynamodbav:"name,omitempty"`
  Age     int                  `dynamodbav:"age,omitempty"`
  Address string               `dynamodbav:"address,omitempty"`
}

//
// Identity implements thing interface
func (p Professor) HashKey() string { return string(p.Org) }
func (p Professor) SortKey() string { return string(p.ID) }

//
// this data type is a normal Golang struct
// just create an instance, fill required fields
var professor := Professor{
  Org:     gold.ToIRI[University]("kiel"),   // "university:kiel"
  ID:      gold.ToIRI[Person]("8980789222"), // "person:8980789222"
  Name:    "Verner Pleishner",
  Age:     64,
  Address: "Blumenstrasse 14, Berne, 3013",
}
```

This is it! Your application is ready to read/write data to/form DynamoDB tables.


### DynamoDB I/O

Please [see and try examples](examples). Its cover all basic use-cases with runnable code snippets, check the post [How To Model Any Relational Data in DynamoDB With dynamo library](examples/relational/README.md) for deep-dive into library philosophy.

```bash
go run examples/keyval/main.go my-table
```

The following code snippet shows a typical I/O patterns

```go
import (
  "github.com/fogfish/dynamo/v4/service/ddb"
  "github.com/fogfish/gold"
)

//
// Create dynamodb client and bind it with the table.
// The client is type-safe and support I/O with a single type (e.g. Professor).
// Use options to specify params.
db, err := ddb.New[Professor]("my-table")

//
// Write the struct with Put
if err := db.Put(context.TODO(), professor); err != nil {
}

//
// Lookup the struct using Get. This function takes input structure as key
// and return a new copy upon the completion. The only requirement - ID has to
// be defined.
val, err := db.Get(context.TODO(),
  Professor{
    Org: gold.ToIRI[University]("kiel"),
    ID:  gold.ToIRI[Person]("8980789222"),
  },
)

switch {
case nil:
  // success
case recoverNotFound(err):
  // not found
default:
  // other i/o error
}

//
// Apply a partial update using Update function. This function takes 
// a partially defined structure, patches the instance at storage and 
// returns remaining attributes.
val, err := db.Update(context.TODO(),
  Professor{
    Org:     gold.ToIRI[University]("kiel"),
    ID:      gold.ToIRI[Person]("8980789222"),
    Address: "Viktoriastrasse 37, Berne, 3013",
  },
)

if err != nil { /* ... */ }

//
// Remove the struct using Remove give partially defined struct with ID
_, err := db.Remove(context.TODO(),
  Professor{
    Org: gold.ToIRI[University]("kiel"),
    ID:  gold.ToIRI[Person]("8980789222"),
  },
)

if err != nil { /* ... */ }
```

### Error Handling

The library enforces for "assert errors for behavior, not type" as the error handling strategy, see [the post](https://tech.fog.fish/2022/07/05/assert-golang-errors-for-behavior.html) for details. 

Use following behaviors to recover from errors:

```go
type ErrorCode interface{ ErrorCode() string }

type NotFound interface { NotFound() string }

type PreConditionFailed interface { PreConditionFailed() bool }

type Conflict interface { Conflict() bool }

type Gone interface { Gone() bool }
```

**Strict decoding.** Every gold value is validated when it is read from the table. A row with an attribute that does not match its type (e.g. `blog:x` in a `gold.IRI[Person]` field) makes `Get` fail with the `invalid entity` error. `Match` fails the whole page, it never returns a partially decoded sequence. This is intended, a corrupted identity should never reach the application. Similarly, a key without a partition key (e.g. empty `gold.Imply`) is rejected by `Put`/`Update` with the `invalid key` error before any request is sent to DynamoDB.


### Hierarchical structures

The library support definition of `A ⟼ B` relation for data elements. Let's consider message threads as a classical examples for such hierarchies:

```
A
├ B
├ C
│ ├ D  
│ └ E
│   └ F
└ G
```

Composite sort key is core concept to organize hierarchies. It facilitates linked-data, hierarchical structures and cheap relations between data items. An application declares node path using composite sort key design pattern. For example, the root is `thread:A`, 2nd rank node `⟨thread:A, message:B⟩`, 3rd rank node `⟨thread:A, message:C/D⟩` and so on `⟨thread:A, message:C/E/F⟩`. Each `id` declares partition and sub nodes. The hierarchy inside the `id` is managed by the application, `gold.IRI` itself is flat. The library implement a `Match` function, supply the node identity and it returns sequence of child elements.

```go
//
// Match uses partition key to match DynamoDB entries.
// It returns a sequence of matched data elements.  
db.Match(context.TODO(), Message{Thread: gold.ToIRI[Thread]("A")})

//
// Match uses partition key and partial sort key to match DynamoDB entries.
// The partial key is the prefix of the sort key, e.g. "message:C/" 
db.Match(context.TODO(), Message{Thread: gold.ToIRI[Thread]("A"), ID: gold.ToIRI[Message]("C/")})

//
// gold.Any[T]() matches every identity of the class T, e.g. "message:"
db.Match(context.TODO(), Message{Thread: gold.ToIRI[Thread]("A"), ID: gold.Any[Message]()})
```

See [advanced example](examples/relational/) for details on managing linked-data. 


### Sequences and Pagination

Hierarchical structures is the way to organize collections, lists, sets, etc. The `Match` returns a lazy [Sequence](https://pkg.go.dev/github.com/fogfish/dynamo?readme=expanded#Seq) that represents your entire collection. Sometimes, your need to split the collection into sequence of pages.

```go
// 1. Set the limit on the stream 
seq, cursor, err := db.Match(context.TODO(),
  Message{Thread: gold.ToIRI[Thread]("A"), ID: gold.ToIRI[Message]("C/")},
  dynamo.Limit[Message](25),
)

// 2. Continue I/O with a new stream, supply the cursor
seq := db.Match(context.TODO(),
  Message{Thread: gold.ToIRI[Thread]("A"), ID: gold.ToIRI[Message]("C/")},
  dynamo.Limit[Message](25),
  cursor,
)
```


### Linked data

Cross-linking of structured data is an essential part of type safe domain driven design. The library helps developers to model relations between data instances using familiar data type.

```go
type Person struct {
  Org     gold.IRI[University] `dynamodbav:"prefix,omitempty"`
  ID      gold.IRI[Person]     `dynamodbav:"suffix,omitempty"`
  Leader  *gold.IRI[Person]    `dynamodbav:"leader,omitempty"`
}
```

`ID` and `Leader` are sibling, equivalent data types. `ID` is only used as primary identity, `Leader` is a "pointer" to linked-data. The identities are compact IRIs `{schema}:{id}`, the schema is derived from the type. The compiler rejects an attempt to link `Leader` with the identity of other class. The optional link is a pointer with `omitempty`. See [Identity with gold](#identity-with-gold) for details.


### Identity with gold

The library does not interpret the keys, `Thing` returns plain strings. The application models keys and links with types of [gold](https://github.com/fogfish/gold) library, which makes the identities type-safe at compile time.

```go
type University struct{}
type Person struct{}

type Professor struct {
  Org  gold.IRI[University] `dynamodbav:"prefix,omitempty"`
  ID   gold.IRI[Person]     `dynamodbav:"suffix,omitempty"`
  Name string               `dynamodbav:"name,omitempty"`
}

func (p Professor) HashKey() string { return string(p.Org) }
func (p Professor) SortKey() string { return string(p.ID) }

p := Professor{
  Org: gold.ToIRI[University]("kiel"),   // "university:kiel"
  ID:  gold.ToIRI[Person]("8980789222"), // "person:8980789222"
}
```

Composite keys are modeled with `gold.Imply`:

```go
type Course struct{}
type Enrollment = gold.Imply[University, Course] // alias — stored as "university:kiel|course:cs101"

type Student struct {
  Key Enrollment       `dynamodbav:"prefix"`
  ID  gold.IRI[Person] `dynamodbav:"suffix,omitempty"`
}

func (s Student) HashKey() string { return s.Key.String() }
func (s Student) SortKey() string { return string(s.ID) }
```

The rules:

* Key and link fields are `gold.IRI[T]` or `gold.Imply[A, B]`. They are type-checked at compile time, stored as DynamoDB strings (`S`, never a map) and validated when read. The library enables the standard `encoding.TextMarshaler`/`TextUnmarshaler` for every attribute (including values used in update and condition expressions), so a value in an expression is encoded exactly as the stored one.
* `Imply` must be declared as an **alias** (`=`). A defined type (`type Enrollment gold.Imply[A, B]`) drops methods of gold and is stored as a map, unless it forwards `MarshalText`/`UnmarshalText`.
* A non-key `Imply` field that may be absent should be a **pointer** with `omitempty` (`*Enrollment`). `omitempty` does not drop a zero `Imply` value, it is stored as an empty string.
* Optional links are `*gold.IRI[T]` with `omitempty`.
* Prefix queries (`Match`): `gold.Any[T]()` selects every identity of the class `T` (`"person:"`). A partial `Imply`, `gold.ImplyFrom(a, gold.IRI[B](""))`, is encoded as `"a|"`, which is prefix-safe.
* Plain IRI prefixes are still string prefixes: `person:neu` matches `person:neumann`. That is inherent to `begins_with`.
* The schema (`person`, `university`) is derived from the Go type name. Use `gold.Register` for any type that appears in stored keys and pin `gold.Schemas()` in a golden test, so renaming a Go type cannot silently change the keys.
* Write path is strict: a non-normalised IRI (e.g. `gold.IRI[Person]("alice")` without `.Norm()`) is rejected by `Put`/`Update`. Keys passed to `Get`/`Remove`/`Match` go through `HashKey()`/`SortKey()` as plain strings and are **not** validated.
* The sort key is optional. An undefined sort key is stored as the `_` placeholder and is never decoded into an application type, the field stays at its zero value. A partition key is mandatory.


### Type projections

Often, there is an established system of the types in the application. It is not convenient to inject dependencies to the `dynamo` library. Also, the usage of secondary indexes requires multiple projections of core type.  

```go
// 
// original core type
type Person struct {
  Org     string `dynamodbav:"prefix,omitempty"`
  ID      string `dynamodbav:"suffix,omitempty"`
  Name    string `dynamodbav:"name,omitempty"`
  Age     int    `dynamodbav:"age,omitempty"`
  Country string `dynamodbav:"country,omitempty"`
}

//
// the core type projection that uses ⟨Org, ID⟩ as composite key
// e.g. this projection supports writes to DynamoDB table
type dbPerson Person

func (p dbPerson) HashKey() string { return p.Org }
func (p dbPerson) SortKey() string { return p.ID }

//
// the core type projection that uses ⟨Org, Name⟩ as composite key
// e.g. the projection support lookup of employer
type dbNamedPerson Person

func (p dbNamedPerson) HashKey() string { return p.Org }
func (p dbNamedPerson) SortKey() string { return p.Name }

//
// the core type projection that uses ⟨Country, Name⟩ as composite key
type dbCitizen Person

func (p dbCitizen) HashKey() string { return p.Country }
func (p dbCitizen) SortKey() string { return p.Name }
```

### Custom codecs for core domain types

Development of complex Golang application might lead developers towards [Standard Package Layout](https://medium.com/@benbjohnson/standard-package-layout-7cdbc8391fc1). It becomes extremely difficult to isolate dependencies from core data types to this library and AWS SDK. The library support serialization of core type to dynamo using custom codecs 

```go
/*** core.go ***/

// 1. complex domain type is defined
type ID struct {/* ... */}

// 2. structure with core types is defined, no deps to dynamo library
type Person struct {
  Org      ID  `dynamodbav:"prefix,omitempty"`
  ID       ID  `dynamodbav:"suffix,omitempty"`
}

/*** aws/ddb/ddb.go ***/

import (
  "github.com/fogfish/dynamo/v4/service/ddb"
)

// 3. declare codecs for complex core domain type 
type id core.ID

func (id) MarshalDynamoDBAttributeValue() (types.AttributeValue, error) {
  /* ...*/
}

func (*id) UnmarshalDynamoDBAttributeValue(types.AttributeValue) error {
  /* ...*/
}

// aws/ddb/ddb.go
// 2. type alias to core type implements dynamo custom codec
type dbPerson Person

// 3. custom codec for structure field is defined 
var (
  codecHashKey = ddb.Codec[dbPerson, id]("Org")
  codecSortKey = ddb.Codec[dbPerson, id]("ID")
)

// 4. use custom codec
func (p dbPerson) MarshalDynamoDBAttributeValue() (types.AttributeValue, error) {
  type tStruct dbPerson
  return ddb.Encode(av, tStruct(p),
    codecHashKey.Encode(id(p.Org)),
    codecSortKey.Encode(id(p.ID))),
  )
}

func (x *dbPerson) UnmarshalDynamoDBAttributeValue(av types.AttributeValue) error {
  type tStruct *dbPerson
  return ddb.Decode(av, tStruct(x),
    codecHashKey.Decode((*id)(&x.Org)),
    codecSortKey.Decode((*id)(&x.ID))),
  )
}
```

### DynamoDB Expressions

In Amazon DynamoDB, there is [a concept of expressions](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.html) to denote the attributes to read; indicate various conditions while doing I/O.


#### Projection Expression

[Projection expression](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ProjectionExpressions.html) defines attributes to be read from the table. The library automatically defines projection expression for each request. The expression is derived from the datatype definition. 

```go
// The type projects: prefix, suffix & name attributes.
type Identity struct {
  Org     gold.IRI[University] `dynamodbav:"prefix,omitempty"`
  ID      gold.IRI[Person]     `dynamodbav:"suffix,omitempty"`
  Name    string               `dynamodbav:"name,omitempty"`
}

// The type projects: prefix, suffix, name, age & address.
type Person struct {
  Org     gold.IRI[University] `dynamodbav:"prefix,omitempty"`
  ID      gold.IRI[Person]     `dynamodbav:"suffix,omitempty"`
  Name    string               `dynamodbav:"name,omitempty"`
  Age     int                  `dynamodbav:"age,omitempty"`
  Address string               `dynamodbav:"address,omitempty"`
}
```

#### Conditional Expression

[Condition expression](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ConditionExpressions.html) helps to implement conditional manipulation of items. The expression defines boolean predicate to determine which items should be modified. If the condition expression evaluates to true, the operation succeeds; otherwise, the operation fails. The library defines a special type `Schema`, which translates a Golang declaration into DynamoDB syntax:

```go
type Person struct {
  Name    string    `dynamodbav:"name,omitempty"`
}

// defines the builder of conditional expression
var ifName = ddb.ClauseFor[Person, string]("Name")

db.Update(/* ... */, ifName.NotExists())
db.Update(/* ... */, ifName.Eq("Verner Pleishner"))
```

Conditions and updates accept gold types, the value is encoded exactly as it is stored:

```go
var ifKey = ddb.ClauseFor[Student, Enrollment]("Key")

db.Update(/* ... */, ifKey.Eq(gold.ImplyFrom(kiel, cs101)))
db.Update(/* ... */, ifKey.HasPrefix(gold.ImplyFrom(kiel, gold.IRI[Course](""))))
```

See [constraint.go](service/ddb/constraint.go) for the list of supported conditional expressions:
* Comparison: `Eq`, `Ne`, `Lt`, `Le`, `Gt`, `Ge`, `Is`
* Unary checks: `Exists`, `NotExists`
* Set checks: `Between`, `In`
* String: `HasPrefix`, `Contains`
* Concurrency control: `Optimistic`

The conditional expressions are composable using `OneOf` or `AllOf` expression. `OneOf` joins multiple constraint into higher-order constraint that is true when one of defined is true. It is OR expression. `AllOf` is conjunctive join. 

```go
db.Update(/* ... */,
  ddb.OneOf(
    ifName.NotExists(),
    ifName.Eq("Verner Pleishner"),
  ),
)

db.Update(/* ... */,
  ddb.AllOf(
    ifName.HasPrefix("Verner"),
    ifName.Contains("Pleishner"),
  ),
)
```

#### Update Expression

[Update expression](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.UpdateExpressions.html) specifies how update operation will modify the attributes of an item. Unfortunately, this abstraction do not fit into the key-value concept advertised by the library. However, update expression are useful to implement counters, set management, etc. 

The `dynamo` library implements `UpdateWith` method together with a simple DSL that supports all DynamoDB update expression actions: SET, REMOVE, ADD, and DELETE.

**Basic Usage**

```go
type Person struct {
  Name    string    `dynamodbav:"name,omitempty"`
  Age     int       `dynamodbav:"age,omitempty"`
  Address string    `dynamodbav:"address,omitempty"`
  Hobbies  []string `dynamodbav:"hobbies,omitempty"`
}

// defines the builder of updater expression
var (
  Name = ddb.UpdateFor[Person, string]("Name")
  Age  = ddb.UpdateFor[Person, int]("Age")
  Address = ddb.UpdateFor[Person, string]("Address")
  Hobbies = ddb.UpdateFor[Person, []string]("Hobbies")
)

db.UpdateWith(context.Background(),
  ddb.Updater(
    Person{
      Org: gold.ToIRI[University]("kiel"),
      ID:  gold.ToIRI[Person]("8980789222"),
    },
    Address.Set("Viktoriastrasse 37, Berne, 3013"),
    Age.Inc(64),
  ),
)
```

**SET** The SET action adds or replaces attributes in an item:

```go
// Set attribute value
Name.Set("John Doe")                     // SET name = :value

// Set attribute only if it doesn't exist
Name.SetNotExists("Default Name")        // SET name = if_not_exists(name, :value)

// Increment/decrement numeric values
Age.Inc(5)                              // SET age = age + :value
Age.Dec(2)                              // SET age = age - :value

// Append to list
Hobbies.Append([]string{"reading"})     // SET hobbies = list_append(hobbies, :value)

// Prepend to list
Hobbies.Prepend([]string{"writing"})    // SET hobbies = list_append(:value, hobbies)
```

**REMOVE** The REMOVE action deletes attributes from an item:

```go
// Remove attribute entirely
Name.Remove()                          // REMOVE name
```

**Type Safety** The library provides compile-time type safety by binding update expressions to specific struct fields and their types:

```go
// This will cause a compile error if types don't match
var Age = ddb.UpdateFor[Person, int]("age")
Age.Set("not a number") // ❌ Compile error
Age.Set(25)             // ✅ Correct
```

#### Set Types

The library automatically handles DynamoDB set types when the struct field has appropriate tags:

```go
type Item struct {
  StringSet []string `dynamodbav:"ss,stringset"`
  NumberSet []int    `dynamodbav:"ns,numberset"`  
  BinarySet [][]byte `dynamodbav:"bs,binaryset"`
}

var StringSet = ddb.UpdateFor[Item, []string]("StringSet")

// These operations work with actual DynamoDB sets
StringSet.Union([]string{"new", "items"})  // ADD ss :value
StringSet.Minus([]string{"old", "items"})  // DELETE ss :value
```



### Optimistic Locking

Optimistic Locking is a lightweight approach to ensure causal ordering of read, write operations to database. AWS made a great post about [Optimistic Locking with Version Number](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBMapper.OptimisticLocking.html).

The `dynamo` library implements type safe conditional expressions to achieve optimistic locking. This feature is vital when your serverless application concurrently updates same entity in the database.

Let's consider a following example. 

```go
type Person struct {
  Org     string `dynamodbav:"prefix,omitempty"`
  ID      string `dynamodbav:"suffix,omitempty"`
  Name    string `dynamodbav:"anothername,omitempty"`
}
```

An optimistic locking on this structure is straightforward from DynamoDB perspective. Just make a request with conditional expression:

```golang
&dynamodb.UpdateItemInput{
  ConditionExpression: "anothername = :anothername",
  ExpressionAttributeValues: /* ":anothername" : {S: "Verner Pleishner"} */
}
```

However, the application operates with struct types. How to define a condition expression on the field `Name`? Golang struct defines and refers the field by `Name` but DynamoDB stores it under the attribute `anothername`. Struct field `dynamodbav` tag specifies serialization rules. Golang does not support a typesafe approach to build a correspondence between `Name` ⟷ `anothername`. Developers have to utilize dynamodb attribute name(s) in conditional expression and Golang struct name in rest of the code. It becomes confusing and hard to maintain. The library defines set of helper types and functions to declare and use conditional expression in type safe manner:

```go
type Person struct {
  Org     string `dynamodbav:"prefix,omitempty"`
  ID      string `dynamodbav:"suffix,omitempty"`
  Name    string `dynamodbav:"anothername,omitempty"`
}

// defines the builder of conditional expression
var Name = ddb.ClauseFor[Person, string]("Name")

val, err := db.Update(context.TODO(), &person, Name.Eq("Verner Pleishner"))
switch err.(type) {
case nil:
  // success
case dynamo.PreConditionFailed:
  // not found
default:
  // other i/o error
}
```

The library provides helper function to implement the concurency control. The optimistic locking constraint `Optimistic` is built-in `OneOf` combinator for `NotExists` or `Eq`.

```go
db.Put(/* ... */, Name.Optimistic("Verner Pleishner"))
```

See the [go doc](https://pkg.go.dev/github.com/fogfish/dynamo?tab=doc) for all supported constraints.

### Batch I/O

The library supports batch interface to read/write objects from DynamoDB tables:
* `BatchGet` takes sequence of keys and return sequence of values.
* `BatchPut` takes sequence of object to store.
* `BatchRemove` takes sequence of keys to delete.


### Configure DynamoDB

The `dynamo` library is optimized to operate with generic Dynamo DB that declares both partition and sort keys with fixed names. Use the following schema:


```typescript
const Schema = (): ddb.TableProps => ({
  tableName: 'my-table',
  partitionKey: {type: ddb.AttributeType.STRING, name: 'prefix'},
  sortKey: {type: ddb.AttributeType.STRING, name: 'suffix'},
})
```

If table uses other names for `partitionKey` and `sortKey` then config options allows to re-declare it.

```go
db, err := ddb.New[Person]("my-table",
  // Optionally set Global Secondary Index for the session
  ddb.WithGlobalSecondaryIndex("my-index"),

  // Optionally declare other keys to be user for attribute projection
  ddb.WithHashKey("someHashKey"),
  ddb.WithSortKey("someSortKey"),
)
```

The following [post](example/relational/README.md) discusses in depth and shows example DynamoDB table configuration and covers aspect of secondary indexes. 


### Migrating from v3

v4 (`github.com/fogfish/dynamo/v4`) is a breaking release, v3 is maintained at the `v3` branch. The v3 data is not migrated.

* `Thing` returns `string`: `HashKey() string`, `SortKey() string`. The core API does not name any identity library.
* `curie` is replaced by [gold](https://github.com/fogfish/gold): `curie.IRI` becomes `gold.IRI[T]`, composite keys become `gold.Imply[A, B]`, `curie.New("a", "")` as match-all prefix becomes `gold.Any[A]()`. Gold values are stored as strings and validated, see [Identity with gold](#identity-with-gold).
* Decoding is strict: an attribute that does not pass validation fails `Get` and `Match` with `invalid entity`.
* AWS S3 backend (`service/s3`) is removed.
* `ddb.WithPrefixes` is removed, it did nothing for DynamoDB.
* Go 1.24 is required.


## How To Contribute

The library is [MIT](LICENSE) licensed and accepts contributions via GitHub pull requests:

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Added some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request


The build and testing process requires [Go](https://golang.org) version 1.24 or later.

**build** and **test** library.

```bash
git clone https://github.com/fogfish/dynamo
cd dynamo
go test ./...
staticcheck ./...
```

Update dependency with [go-check-updates](https://github.com/fogfish/go-check-updates)

```bash
go-check-updates
go-check-updates -u --push github
``` 

### commit message

The commit message helps us to write a good release note, speed-up review process. The message should address two question what changed and why. The project follows the template defined by chapter [Contributing to a Project](http://git-scm.com/book/ch5-2.html) of Git book.

### bugs

If you experience any issues with the library, please let us know via [GitHub issues](https://github.com/fogfish/dynamo/issue). We appreciate detailed and accurate reports that help us to identity and replicate the issue. 

## License

[![See LICENSE](https://img.shields.io/github/license/fogfish/dynamo.svg?style=for-the-badge)](LICENSE)
