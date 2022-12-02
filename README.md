# dynamo

The library implements a simple key-value abstraction to store algebraic, linked-data data types at AWS storage services: AWS DynamoDB and AWS S3.

[![Version](https://img.shields.io/github/v/tag/fogfish/dynamo?label=version)](https://github.com/fogfish/dynamo/releases)
[![Documentation](https://pkg.go.dev/badge/github.com/fogfish/dynamo)](https://pkg.go.dev/github.com/fogfish/dynamo)
[![Build Status](https://github.com/fogfish/dynamo/workflows/build/badge.svg)](https://github.com/fogfish/dynamo/actions/)
[![Git Hub](https://img.shields.io/github/last-commit/fogfish/dynamo.svg)](https://github.com/fogfish/dynamo)
[![Coverage Status](https://coveralls.io/repos/github/fogfish/dynamo/badge.svg?branch=main)](https://coveralls.io/github/fogfish/dynamo?branch=main)
[![Go Report Card](https://goreportcard.com/badge/github.com/fogfish/dynamo)](https://goreportcard.com/report/github.com/fogfish/dynamo)
[![Maintainability](https://api.codeclimate.com/v1/badges/8a8746f9cbaba81bb44b/maintainability)](https://codeclimate.com/github/fogfish/dynamo/maintainability)


## Inspiration

The library encourages developers to use Golang struct to define domain models, write correct, maintainable code. Using this library, the application can achieve the ideal data model that would require a single request to DynamoDB and model one-to-one, one-to-many and even many-to-many relations. The library uses generic programming style to implement actual storage I/O, while expose external domain object as `[T dynamo.Thing]` with implicit conversion back and forth between a concrete struct(s). The library uses [AWS Golang SDK v2](https://github.com/aws/aws-sdk-go-v2) under the hood.

Essentially, the library implement a following generic key-value trait to access domain objects. 

```go
type KeyVal[T dynamo.Thing] interface {
  Put(T) error
  Get(T) (T, error)
  Remove(T) error
  Update(T) (T, error)
  Match(T) []T
}
```

The library philosophy and use-cases are covered in depth at the post
[How To Model Any Relational Data in DynamoDB With dynamo library](example/relational/README.md) or continue reading the Getting started section.


## Getting started

The library requires Go **1.18** or later due to usage of [generics](https://go.dev/blog/intro-generics).

The latest version of the library is available at its `main` branch. All development, including new features and bug fixes, take place on the `main` branch using forking and pull requests as described in contribution guidelines. The stable version is available via Golang modules.


- [Getting Started](#getting-started)
  - [Data types definition](#data-types-definition)
  - [DynamoDB IO](#dynamodb-io)
  - [Error Handling](#error-handling)
  - [Hierarchical structures](#hierarchical-structures)
  - [Sequences and Pagination](#sequences-and-pagination)
  - [Linked data](#linked-data)
  - [Type projections](#type-projections)
  - [Custom codecs for core domain types](#custom-codecs-for-core-domain-types)
  - [Optimistic Locking](#optimistic-locking)
  - [Configure DynamoDB](#configure-dynamodb)
  - [AWS S3 Support](#aws-s3-support)


### Data types definition

Data types definition is an essential part of development with `dynamo` library. Golang structs declares domain of your application. Public fields are serialized into DynamoDB attributes, the field tag `dynamodbav` controls marshal/unmarshal processes.

The library demands from each structure implementation of `Thing` interface. This type acts as struct annotation -- Golang compiler raises an error at compile time if other data type is supplied to the dynamo library. Secondly, each structure defines unique "composite primary key". The library encourages definition of both partition and sort keys using a special data type `curie.IRI`. This type is a synonym to compact Internationalized Resource Identifiers, which facilitates linked-data, hierarchical structures and cheap relations between data items. `curie.IRI` is a synonym to the built-in `string` type so that anything castable to string suite to model the keys as alternative solution.   

```go
type Person struct {
  Org     curie.IRI `dynamodbav:"prefix,omitempty"`
  ID      curie.IRI `dynamodbav:"suffix,omitempty"`
  Name    string    `dynamodbav:"name,omitempty"`
  Age     int       `dynamodbav:"age,omitempty"`
  Address string    `dynamodbav:"address,omitempty"`
}

//
// Identity implements thing interface
func (p Person) HashKey() curie.IRI { return p.Org }
func (p Person) SortKey() curie.IRI { return p.ID }

//
// this data type is a normal Golang struct
// just create an instance, fill required fields
var person := Person{
  Org:     curie.IRI("University:Kiel"),
  ID:      curie.IRI("Professor:8980789222"),
  Name:    "Verner Pleishner",
  Age:     64,
  Address: "Blumenstrasse 14, Berne, 3013",
}
```

This is it! Your application is ready to read/write data to/form DynamoDB tables.


### DynamoDB I/O

Please [see and try examples](examples). Its cover all basic use-cases with runnable code snippets, check the post [How To Model Any Relational Data in DynamoDB With dynamo library](examples/relational/README.md) for deep-dive into library philosophy.

```bash
go run examples/keyval/main.go ddb:///my-table
```

The following code snippet shows a typical I/O patterns

```go
import (
  "github.com/fogfish/dynamo/v2/service/ddb"
)

//
// Create dynamodb client and bind it with the table.
// The client is type-safe and support I/O with a single type (e.g. Person).
// Use URI notation to specify the diver (ddb://) and the table (/my-table).
db := ddb.Must(ddb.New[Person]("ddb:///my-table"))

//
// Write the struct with Put
if err := db.Put(context.TODO(), person); err != nil {
}

//
// Lookup the struct using Get. This function takes input structure as key
// and return a new copy upon the completion. The only requirement - ID has to
// be defined.
val, err := db.Get(context.TODO(),
  Person{
    Org: curie.IRI("University:Kiel"),
    ID:  curie.IRI("Professor:8980789222"),
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
  Person{
    Org:     curie.IRI("University:Kiel"),
    ID:      curie.IRI("Professor:8980789222"),
    Address: "Viktoriastrasse 37, Berne, 3013",
  }
)

if err != nil { /* ... */ }

//
// Remove the struct using Remove give partially defined struct with ID
err := db.Remove(context.TODO(),
  Person{
    Org: curie.IRI("University:Kiel"),
    ID:  curie.IRI("Professor:8980789222"),
  }
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

Composite sort key is core concept to organize hierarchies. It facilitates linked-data, hierarchical structures and cheap relations between data items. An application declares node path using composite sort key design pattern. For example, the root is `thread:A`, 2nd rank node `⟨thread:A, B⟩`, 3rd rank node `⟨thread:A, C/D⟩` and so on `⟨thread:A, C/E/F⟩`. Each `id` declares partition and sub nodes. The library implement a `Match` function, supply the node identity and it returns sequence of child elements.

```go
//
// Match uses partition key to match DynamoDB entries.
// It returns a sequence of matched data elements.  
db.Match(context.TODO(), Message{Thread: "thread:A"})

//
// Match uses partition key and partial sort key to match DynamoDB entries. 
db.Match(context.TODO(), Message{Thread: "thread:A", ID: "C"})
```

See [advanced example](examples/relational/) for details on managing linked-data. 


### Sequences and Pagination

Hierarchical structures is the way to organize collections, lists, sets, etc. The `Match` returns a lazy [Sequence](https://pkg.go.dev/github.com/fogfish/dynamo?readme=expanded#Seq) that represents your entire collection. Sometimes, your need to split the collection into sequence of pages.

```go
// 1. Set the limit on the stream 
seq := db.Match(context.TODO(),
  Message{Thread: "thread:A", ID: "C"},
  dynamo.Limit(25),
)

// 2. Read cursor value
cursor := seq[len(seq)-1]

// 3. Continue I/O with a new stream, supply the cursor
seq := db.Match(context.TODO(),
  Message{Thread: "thread:A", ID: "C"},
  dynamo.Limit(25),
  dynamo.Cursor(cursor),
)
```


### Linked data

Cross-linking of structured data is an essential part of type safe domain driven design. The library helps developers to model relations between data instances using familiar data type.

```go
type Person struct {
  Org     curie.IRI  `dynamodbav:"prefix,omitempty"`
  ID      curie.IRI  `dynamodbav:"suffix,omitempty"`
  Leader  *curie.IRI `dynamodbav:"leader,omitempty"`
}
```

`ID` and `Leader` are sibling, equivalent data types. `ID` is only used as primary identity, `Leader` is a "pointer" to linked-data. The library advices usage of compact Internationalized Resource Identifiers (`curie.IRI`) for this purpose. Semantic Web publishes structured data using this type so that it can be interlinked by applications.


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

func (p dbPerson) HashKey() curie.IRI { return curie.IRI(p.Org) }
func (p dbPerson) SortKey() curie.IRI { return curie.IRI(p.ID) }

//
// the core type projection that uses ⟨Org, Name⟩ as composite key
// e.g. the projection support lookup of employer
type dbNamedPerson Person

func (p dbNamedPerson) HashKey() curie.IRI { return curie.IRI(p.Org) }
func (p dbNamedPerson) SortKey() curie.IRI { return curie.IRI(p.Name) }

//
// the core type projection that uses ⟨Country, Name⟩ as composite key
type dbCitizen Person

func (p dbCitizen) HashKey() curie.IRI { return curie.IRI(p.Country) }
func (p dbCitizen) SortKey() curie.IRI { return curie.IRI(p.Name) }
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
  "github.com/fogfish/dynamo/service/ddb"
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
var Name = dynamo.Schema[Person, string]("Name")

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

See the [go doc](https://pkg.go.dev/github.com/fogfish/dynamo?tab=doc) for all supported constraints.


### Configure DynamoDB

The `dynamo` library is optimized to operate with generic Dynamo DB that declares both partition and sort keys with fixed names. Use the following schema:


```typescript
const Schema = (): ddb.TableProps => ({
  tableName: 'my-table',
  partitionKey: {type: ddb.AttributeType.STRING, name: 'prefix'},
  sortKey: {type: ddb.AttributeType.STRING, name: 'suffix'},
})
```

If table uses other names for `partitionKey` and `sortKey` then connect URI allows to re-declare them

```go
//
// Create client and bind it with DynamoDB the table
db := keyval.Must(
  keyval.New[Person]("ddb:///my-table?prefix=someHashKey&suffix=someSortKey", nil, nil),
)
```

The following [post](example/relational/README.md) discusses in depth and shows example DynamoDB table configuration and covers aspect of secondary indexes. 


### AWS S3 Support

The library advances its simple I/O interface to AWS S3 bucket, allowing to persist data types to multiple storage simultaneously.

```go
import (
  "github.com/fogfish/dynamo/v2/service/ddb"
  "github.com/fogfish/dynamo/v2/service/s3"
)


//
// Create client and bind it with DynamoDB the table
db := ddb.Must(ddb.New("ddb:///my-table", nil, nil))

//
// Create client and bind it with S3 bucket
db := s3.Must(s3.New("s3:///my-bucket", nil, nil))
```

There are few fundamental differences about AWS S3 bucket
* use `s3` schema of connection URI;
* compose primary key is serialized to S3 bucket path. (e.g. `⟨thread:A, C/E/F⟩ ⟼ thread/A/_/C/E/F`);
* storage persists struct to JSON, use `json` field tags to specify serialization rules;
* optimistic locking is not supported yet, any conditional expression is silently ignored;
* `Update` is not thread safe.



## How To Contribute

The library is [MIT](LICENSE) licensed and accepts contributions via GitHub pull requests:

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Added some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request


The build and testing process requires [Go](https://golang.org) version 1.13 or later.

**build** and **test** library.

```bash
git clone https://github.com/fogfish/dynamo
cd dynamo
go test
```

### commit message

The commit message helps us to write a good release note, speed-up review process. The message should address two question what changed and why. The project follows the template defined by chapter [Contributing to a Project](http://git-scm.com/book/ch5-2.html) of Git book.

### bugs

If you experience any issues with the library, please let us know via [GitHub issues](https://github.com/fogfish/dynamo/issue). We appreciate detailed and accurate reports that help us to identity and replicate the issue. 

## License

[![See LICENSE](https://img.shields.io/github/license/fogfish/dynamo.svg?style=for-the-badge)](LICENSE)
