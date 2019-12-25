# dynamo

The library implements a driver to AWS DynamoDB that operates
with generic representation of algebraic data types.

[![Documentation](https://godoc.org/github.com/fogfish/dynamo?status.svg)](http://godoc.org/github.com/fogfish/dynamo)
[![Build Status](https://secure.travis-ci.org/fogfish/dynamo.svg?branch=master)](http://travis-ci.org/fogfish/dynamo)
[![Git Hub](https://img.shields.io/github/last-commit/fogfish/dynamo.svg)](http://travis-ci.org/fogfish/dynamo)
[![Coverage Status](https://coveralls.io/repos/github/fogfish/dynamo/badge.svg?branch=master)](https://coveralls.io/github/fogfish/dynamo?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/fogfish/dynamo)](https://goreportcard.com/report/github.com/fogfish/dynamo)

## Inspiration

The library encourages developers to use Golang struct to define domain
models, write correct, maintainable code. The library uses generic
programming style to implement actual storage I/O, while expose external
domain object as interface{} with implicit conversion back and forth
between a concrete struct(s).

Essentially, it implement a following generic key-value trait to access
domain objects. The library AWS Go SDK under the hood

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

The library is optimized to operate with generic Dynamo DB schemas:

```typescript
const Schema = (): ddb.TableProps => ({
  partitionKey: {type: ddb.AttributeType.STRING, name: 'prefix'},
  readCapacity: 1,
  sortKey: {type: ddb.AttributeType.STRING, name: 'suffix'},
  tableName: 'my-table',
  writeCapacity: 1,
})
```

Import the library in your code

```go
import (
  "github.com/fogfish/dynamo"
)

type Person struct {
  dynamo.IRI
  Name    string `dynamodbav:"name,omitempty"`
  Age     int    `dynamodbav:"age,omitempty"`
  Address string `dynamodbav:"address,omitempty"`
}

func main() {
  db := dynamo.New("my-table")

  //
  err := db.Put(
    Person{
      dynamo.IRI{"dead", "beef"},
      "Verner Pleishner",
      64,
      "Blumenstrasse 14, Berne, 3013",
    }
  )

  //
  person := Person{IRI: dynamo.IRI{"dead", "beef"}}
  err = db.Get(&person{})

  //
  seq := db.Match(dynamo.IRI{Prefix: "dead"})
  for seq.Tail() {
    p := &person{}
    err = seq.Head(p)
  }
  if seq.Fail != nil {/* ... */}

  //
  db.Remove(dynamo.IRI{"dead", "beef"})
}
```

See the [go doc](http://godoc.org/github.com/fogfish/dynamo) for api spec and [advanced example](example) app.


## How To Contribute

The library is [MIT](LICENSE) licensed and accepts contributions via GitHub pull requests:

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Added some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request


The build and testing process requires [Go](https://golang.org) version 1.13 or later.

**Build** and **run** service in your development console. The following command boots Erlang virtual machine and opens Erlang shell.

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
