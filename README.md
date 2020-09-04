# dynamo

The library implements a simple key-value abstraction to store algebraic data types at AWS storage services: AWS DynamoDB and AWS S3.

[![Documentation](https://godoc.org/github.com/fogfish/dynamo?status.svg)](http://godoc.org/github.com/fogfish/dynamo)
[![Build Status](https://secure.travis-ci.org/fogfish/dynamo.svg?branch=master)](http://travis-ci.org/fogfish/dynamo)
[![Git Hub](https://img.shields.io/github/last-commit/fogfish/dynamo.svg)](http://travis-ci.org/fogfish/dynamo)
[![Coverage Status](https://coveralls.io/repos/github/fogfish/dynamo/badge.svg?branch=master)](https://coveralls.io/github/fogfish/dynamo?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/fogfish/dynamo)](https://goreportcard.com/report/github.com/fogfish/dynamo)
[![Maintainability](https://api.codeclimate.com/v1/badges/8a8746f9cbaba81bb44b/maintainability)](https://codeclimate.com/github/fogfish/dynamo/maintainability)


## Inspiration

The library encourages developers to use Golang struct to define domain models, write correct, maintainable code. The library uses generic programming style to implement actual storage I/O, while expose external domain object as `interface{}` with implicit conversion back and forth
between a concrete struct(s). The library uses [AWS Golang SDK](https://aws.amazon.com/sdk-for-go/) under the hood

Essentially, the library implement a following generic key-value trait to access domain objects. 

```scala
trait KeyVal[T] {
  def put(entity: T): T
  def get(pattern: T): T
  def remove(pattern: T): T
  def update(entity: T): T
  def match(pattern: T): Seq[T]
}
```

## Getting started

The latest version of the library is available at its `master` branch. All development, including new features and bug fixes, take place on the `master` branch using forking and pull requests as described in contribution guidelines.

- [Getting Started](#getting-started)
  - [Data types definition](#data-types-definition)
  - [DynamoDB IO](#dynamodb-io)
  - [Hierarchical structures](#hierarchical-structures)
  - [Linked data](#linked-data)
  - [Optimistic Locking](#optimistic-locking)
  - [Configure DynamoDB](#configure-dynamodb)
  - [Other storages](#other-storages)


### Data types definition

Data types definition is an essential part of development with `dynamo` library. Golang structs declares domain of your application. Public fields are serialized into DynamoDB attributes, the field tag `dynamodbav` controls marshal/unmarshal process. 

The library demands from each structure embedding of `iri.ID` type (this type is implemented by another [package](https://github.com/fogfish/iri)). This type acts as struct annotation -- Golang compiler raises an error at compile time if other data type is supplied for DynamoDB I/O. Secondly, this type facilitates linked-data, hierarchical structures and cheap relations between data elements.

```go
import "github.com/fogfish/iri"

type Person struct {
  iri.ID
  Name    string `dynamodbav:"name,omitempty"`
  Age     int    `dynamodbav:"age,omitempty"`
  Address string `dynamodbav:"address,omitempty"`
}

//
// this data type is a normal Golang struct
// just create and instance filling required fields
// ID is own data type thus use iri.New(...)
var person := Person{
  ID:      iri.New("8980789222")
  Name:    "Verner Pleishner",
  Age:     64,
  Address: "Blumenstrasse 14, Berne, 3013",
}
```

This is it! Your application is ready to read/write data to/form DynamoDB tables.


### DynamoDB I/O

Please see [the code example](example/keyval/main.go) and try it

```bash
go run example/keyval/main.go ddb:///my-table
```

The following code snippet shows a typical I/O patterns

```go
import (
  "github.com/fogfish/dynamo"
)

//
// Create dynamodb client and bind it with the table
// Use URI notation to specify the diver (ddb://) and the table (/my-table) 
db := dynamo.Must(dynamo.New("ddb:///my-table"))

//
// Write the struct with Put
if err := db.Put(person); err != nil {
}

//
// Lookup the struct using Get. This function takes "empty" structure as a placeholder and
// fill it with a data upon the completion. The only requirement - ID has to be defined.
person := Person{ID: iri.New("8980789222")}
switch err := db.Get(&person).(type) {
case nil:
  // success
case dynamo.NotFound:
  // not found
default:
  // other i/o error
}

//
// Apply a partial update using Update function. This function takes a partially defined
// structure, patches the instance at storage and returns remaining attributes.
person := Person{
  ID:      iri.New("8980789222"),
  Address: "Viktoriastrasse 37, Berne, 3013",
}
if err := db.Update(&person); err != nil {
}

//
// Remove the struct using Remove. Either give struct or ID to it
if err := db.Remove(iri.New("8980789222")); err != nil {
}
```

### Hierarchical structures

The library support definition of `A ⟼ B` relation for data. Message threads are a classical
examples for such hierarchies:

```
A
├ B
├ C
│ ├ D  
│ └ E
│   └ F
└ G
```

The data type `iri.ID` is core type to organize hierarchies. An application declares node path by colon separated strings. For example, the root is `iri.New("A")`, 2nd rank node `iri.New("A:C")` and so on `iri.New("A:C:E:F")`. Each `id` declares either node or its sub children. The library implement a `Match` function, supply the node identity and it returns sequence of child elements. 

```go
//
// Match uses iri.ID to match DynamoDB entries. It returns a sequence of 
// generic representations that has to be transformed into actual data types
// FMap is an utility it takes a closure function that lifts generic to the struct. 
db.Match(iri.New("A:C")).FMap(
  func(gen dynamo.Gen) (iri.Thing, error) {
    p := person{}
    return gen.To(&p)
  }
)

//
// Type aliases is the best approach to lift generic sequence in type safe one.
type persons []person

// Join is a monoid to append generic element into sequence 
func (seq *persons) Join(gen dynamo.Gen) (iri.Thing, error) {
  val := person{}
  if fail := gen.To(&val); fail != nil {
    return nil, fail
  }
  *seq = append(*seq, val)
  return &val, nil
}

// and final magic to discover hierarchy of elements
seq := persons{}
db.Match(iri.New("A:C")).FMap(seq.Join)
```

See the [go doc](https://pkg.go.dev/github.com/fogfish/dynamo?tab=doc) for api spec and [advanced example](example) app.


### Linked data

Cross-linking of structured data is an essential part of type safe domain driven design. The library helps developers to model relations between data instances using familiar data type:

```go
type Person struct {
  iri.ID
  Account *iri.IRI `dynamodbav:"name,omitempty"`
}
```

`iri.ID` and `iri.IRI` are sibling, equivalent data types. `ID` is only used as primary key, `IRI` is a "pointer" to linked-data.


### Optimistic Locking

Optimistic Locking is a lightweight approach to ensure causal ordering of read, write operations to database. AWS made a great post about [Optimistic Locking with Version Number](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBMapper.OptimisticLocking.html).

The `dynamo` library implements type safe conditional expressions to achieve optimistic locking. This feature is vital when your serverless application concurrently updates same entity in the database.

Let's consider a following example. 

```go
type Person struct {
  iri.ID
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

However, the application operates with struct types. How to define a condition expression on the field `Name`? Golang struct defines and refers the field by `Name` but DynamoDB stores it under the attribute `anothername`. Struct field `dynamodbav` tag specifies serialization rules. Golang does not support a typesafe approach to build a correspondence between `Name` ⟷ `anothername`. Developers have to utilize dynamodb attribute name(s) in conditional expression and
Golang struct name in rest of the code. It becomes confusing and hard to maintain. The library defines set of helper types and functions to declare and use conditional expression in type safe manner:

```go
type Person struct {
  iri.ID
  Name    string `dynamodbav:"anothername,omitempty"`
}
var Name = Thing(Person{}).Field("Name")

db.Update(&person, Name.Eq("Verner Pleishner"))
```

See the [go doc](https://pkg.go.dev/github.com/fogfish/dynamo?tab=doc) for all supported constrains.


### Configure DynamoDB

The `dynamo` library is optimized to operate with generic Dynamo DB that declares both partition and sort keys with fixed names. Use the following schema:


```typescript
const Schema = (): ddb.TableProps => ({
  tableName: 'my-table',
  partitionKey: {type: ddb.AttributeType.STRING, name: 'prefix'},
  sortKey: {type: ddb.AttributeType.STRING, name: 'suffix'},
})
```


### Other storages

The library advances its simple I/O interface to AWS S3 bucket, allowing to persist data types to multiple storage simultaneously.

```go
//
// Create client and bind it with DynamoDB the table
db := dynamo.Must(dynamo.New("ddb:///my-table"))

//
// Create client and bind it with S3 bucket
s3 := dynamo.Must(dynamo.New("s3:///my-bucket"))
```

There are few fundamental differences about AWS S3 bucket
* use `s3` schema of connection URI;
* primary key `iri.ID` is serialized to S3 bucket path. (e.g. `iri.New("A:C:E:F") ⟼ A/C/E/F`);
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
