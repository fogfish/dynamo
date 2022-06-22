# How To Model Any Relational Data in DynamoDB With dynamo library

AWS DynamoDB is a NoSQL database, the traditional [data normalization](https://en.wikipedia.org/wiki/Database_normalization) techniques would not work with this database. Instead, the overall data design is based on understanding access patterns. The access pattern is only the thing to be considered. The ideal data model would require a single request to DynamoDB. Aspects of schema design for DynamoDB are well covered by existing publications, the reference sections link most notable posts on this subject. 

The `dynamo` library has been developed to streamline the data design process using pure Golang structures for definition of domain models. This post, guides through this process using practical examples supported by code snippets.

## Data Access and Query Patterns

> The first step in designing your DynamoDB application is to identify the specific query patterns that the system must satisfy.

The data writing is an "easiest" part in the process. Entire data lifecycle management is built with three operations: create, update and remove data items. These operations are defined over pure Golang structs (a type) - public fields are serialized into DynamoDB attributes, the field tag `dynamodbav` controls marshal/unmarshal process.

```go
type Author struct {
  Name    string `dynamodbav:"name,omitempty"`
  Address string `dynamodbav:"address,omitempty"`
}
```

The data reading requires thoughtful work upfront. Typically, all data is denormalized due to absence of joins and desire to minimize the number of round-trips to DynamoDB. It is possible to achieve one-to-one, one-to-many and even many-to-many relations using the `dynamo` library but the access patterns need to be identified. As the result of the access pattern study process, the list might look like the following (an example, arxiv.org like application has been considered):
* As an author I want to register a profile ...
* As an author I want to publish an article to the system ...
* As a reader I want to fetch the article ...
* As a reader I want to list all articles written by the author ...
* As a reader I want to look up articles titles for given keywords ...
* As a reader I want to look up article titles written by the author for a given keyword ... 
* As a reader I want to look up all keywords of the article ...
* As a reader I want to look up all articles for a given category in chronological order ...
* As a reader I want to list all articles written by the author in chronological order ...

The list of access patterns for real applications looks complicated at times. This example, represents all I/O patterns solvable with `dynamo` library and reflect real production challenges.

In the context of DynamoDB, the implementation of all access patterns is achieved either with composite sort key or secondary indexes. The composite sort key design is the next important step in data modelling.


## Composite Sort Key  

> Related items can be grouped together and queried efficiently if their key design causes them to sort together. This is an important NoSQL design strategy.

AWS DynamoDB supports either simple (a partition key only) or composite (a partition key combined with a sort key) to uniquely identify items. The `dynamo` library is 100% compatible with standard Golang AWS SDK, the composite key is defined by pair of attributes and annotated with the field tag `dynamodbav`. However, the library requires that each type implements `Thing` interface and its methods: `HashKey`, `SortKey` that returns composite key pair. The `Thing` interface acts as struct annotation -- Golang compiler raises an error at compile time if other data type is supplied.

The library does not enforce any special type for key modelling, anything castable to string suite but the `dynamo` library uses a special data type `curie.IRI` for the purpose of composite key modelling. This type is a compact [Internationalized Resource Identifiers](https://github.com/fogfish/curie), which facilitates linked-data, hierarchical structures and cheap relations between data items. 

```go
type Article struct {
	Author   curie.IRI `dynamodbav:"prefix,omitempty"`
	ID       curie.IRI `dynamodbav:"suffix,omitempty"`
  // ...
}

func (article Article) HashKey() curie.IRI { return article.Author }
func (article Article) SortKey() curie.IRI { return article.ID }


article := Article{
  Author: curie.New("author:%s", "neumann"),
  ID:     curie.New("article:%s", "theory_of_automata"),
  // ...
}
```

Let's emphasize a few fundamental design problems solved by the `curie.IRI` data type.

**Single table** is a design pattern to address network I/O bottlenecks by retrieving heterogenous item types using a single request. It recommends putting all data items into one table and forgetting the classical relational approach of using different tables per entity. Steep learning curve and "leaks" of identity are two well-known issues in this pattern.

Let's consider our application, what the access pattern has been defined for. It operates with three concepts: `author`, `article` and `keyword`. Haskell Curry author's identity might collide with an article identity about Haskell programming language or some keywords about functional programming. 

The data type `curie.IRI` makes a formal definition of the logical partition the identity belongs to. The scheme explicitly defines the purpose of the identity and protects from accidental "collisions". 

```go
curie.New("author:haskell")
curie.New("article:haskell")
curie.New("keyword:haskell")
```

**Sharding** is a technique for distributing loads more evenly across data partitions. The imbalanced or "hot" partition is a well-known issue with DynamoDB. Either random or calculated suffixes is the strategy for load distribution evenly across a partition key space.

The data type `curie.IRI` makes formal rules of building keys from multiple segments. The application has a common interface to construct keys of any complexity to resolve data sharding aspects. 

```go
curie.New("author:smith/%d", 1)
curie.New("author:smith/%d", 2)
```

**Composite key** is built from partition key combined with a sort key. It helps an application to keep related data together in one "place" so that it can be efficiently accessed, effectively building one-to-many relation. Well-crafted composite sort keys define a hierarchical structures that can be queries at any level of the hierarchy. For example, the following key is efficiently listing nested geographical locations `country/region/state/county/city/district`.

The data type `curie.IRI` simplifies data modelling, and identities of data items, which are built from a well defined type that is exchangeable between application, DynamoDB and other systems.

## Data Access

Let's follow up previously specified access patterns and this composite primary key type to model algebraic data types for fictional arxiv.org application:

The access patterns for authors follow a classical key-value I/O.
* As an author I want to register a profile ...

The scheme `author` and author unique identity is a candidate for
partition key

```go
/*
HashKey is
curie.New("author:%s", "neumann")
  ⟿ author:neumann
*/
type Author struct {
  ID curie.IRI `dynamodbav:"prefix,omitempty"`
}
```

The access patterns for an article follows one-to-many I/O
- As an author I want to publish an article to the system ...
- As a reader I want to fetch the article ...
- As a reader I want to list all articles written by the author ...

The article is either referenced directly or looked as a descendant of
the author. Eventually building relation one author to many articles.
The composed sort key is a pattern to build the relation. Author is
the partition key, article id is a sort key. Any instance of author identity can be casted to article identity and back.

```go
/*
HashKey is
curie.New("author:%s", "neumann")
  ⟿ author:neumann

SortKey is
curie.New("article:%s", "theory_of_automata")
  ⟿ article:theory_of_automata
*/
type Article struct {
	Author   curie.IRI `dynamodbav:"prefix,omitempty"`
	ID       curie.IRI `dynamodbav:"suffix,omitempty"`
}
```

The access patterns for an article - keyword is a classical many-to-many I/O
- As a reader I want to look up articles titles for given keywords ...
- As a reader I want to look up articles titles written by the author for a given keyword ...
- As a reader I want to look up all keywords of the article ...

[Adjacency List design pattern](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/bp-adjacency-graphs.html#bp-adjacency-lists) is one way to solve many-to-many relation but it requires a global secondary index on the sort key, which might cause unnecessary overhead in single table design.

The global secondary index implicitly maintains two adjacency lists.
The first one is the forward article-to-keyword, the second one is
an inverse keyword-to-article. It is possible to craft these lists
explicitly with following keys.

```go
/*

HashKey is
curie.New("keyword:%s", "theory")
  ⟿ keyword:theory

SortKey is
curie.New("article:%s/%s", "neumann", "theory_of_automata")
  ⟿ article:neumann/theory_of_automata

and inverse

HashKey is
  ⟿ article:neumann/theory_of_automata

SortKey is
  ⟿ keyword:theory
*/
type Keyword struct {
	HashKey curie.IRI `dynamodbav:"prefix,omitempty"`
	SortKey curie.IRI `dynamodbav:"suffix,omitempty"`
}
```

There are only a few limited ways to query data efficiently from DynamoDB. The composite sort key together with `curie.IRI` data type let application retrieve hierarchy of related items using range queries with expressions `begins_with`, `between`, `>`, `<`, etc. The `dynamo` library automates construction of these expressions.


## Secondary indexes

> Secondary indexes are often essential to support the query patterns that your application requires.

Composite sort key supports hierarchical one-to-many relations. Additional orthogonal access patterns might require secondary indexes over existing data sets. For example a two orthogonal access patterns:
* As a reader I want to list all articles written by the author ...
* As a reader I want to look up all articles for a given category in chronological order ...

The first access pattern is addressed by composite sort key `author ⟼ article`, the second one requires another `category ⟼ year` key. One approach is an explicit projection of data but secondary indexes are easy. DynamoDB implicitly copies data from the main table into the secondary index using another pair of attributes to establish identity. Therefore a new access dimension is unlocked. Eventual consistency is only the aspect to consider while using indexes. The local secondary indexes provide strong consistency but general advice to favour global indexes.

The `dynamo` library supports both global and local secondary indexes with particular behavior:
* it creates a client instance per table, therefore each index requires own instance of the client;
* it automatically projects type attributes into partition (HASH) and sort (RANGE) keys attributes. The default values of attributes are `prefix` and `suffix`. If table design uses other attribute names, which is always a cases of secondary indexes, then connection URI shall give a hint about new name;
* application shall instantiate read-only clients for secondary indexes.

```go
// client to access to "main" table
ddb := keyval.Must(
  keyval.New[Article](
    dynamo.WithURI("ddb:///example-dynamo-relational"),
  ),
)

// client to access global secondary index
gsi := keyval.Must(
  keyval.ReadOnly[Category](
    dynamo.WithURI("ddb:///example-dynamo-relational/example-dynamo-relational-category-year?prefix=category&suffix=year"),
  ),
)
```

```go
/*

The access patterns for an article on orthogonal direction 
follows same one-to-many I/O
- As a reader I want to look up all articles for a given category in chronological order ...

The article shall define additional attributes, they would be projected
by DynamoDB into partition and sort keys. Eventually building additional
one subject to many year relations. 

*/
type Article struct {
  // ...
  Category string     `dynamodbav:"category,omitempty"`
  Year     string     `dynamodbav:"year,omitempty"`
}
```

AWS DynamoDB gives a recommendation to favor global secondary indexes rather than local secondary indexes. Each table in DynamoDB can have up to 20 global secondary indexes and 5 local secondary indexes. The local indexes must be designed at the time of the table creation.

DynamoDB table schema for fictional arxiv.org is defined at [schema.sh](schema.sh) and Golang types at [types.go](types.go)


## Writing and Reading DynamoDB  

Actual reads and writes into DynamoDB tables are very straightforward with the `dynamo` library. It has been well covered by the [api documentation](https://github.com/fogfish/dynamo). Let's only highlight a simple example for each access pattern, which has been defined earlier. 

**As an author I want to publish an article to the system ...**

The example application instantiates `Author` type, defining primary key and other attributes. The instance is `Put` to DynamoDB  

```go
author := Author{
  ID: curie.New("author:%s", "neumann"),
  // ...
} 

db.Put(author)
```

**As an author I want to publishes an article to system ...**

Fundamentally, there is no difference what data type the application writes to DynamoDB. It is all about instantiation right structure with right composite key

```go
article := Article{
  Author: curie.New("author:%s", "neumann"),
  ID:     curie.New("article:%s", "theory_of_automata"),
  // ...
}

db.Put(article)
```

The example arxiv.org application does not use a secondary index to support implicit search by keywords (so called adjacency list design pattern). Therefore, the application needs to publish keywords explicitly 

```go
keyword := curie.New("keyword:%s", "theory")
article := curie.New("article:%s/%s", "neumann", "theory_of_automata"),

forward := Keyword{HashKey: keyword, SortKey: article, /* ... */} 
inverse := Keyword{HashKey: article, SortKey: keyword, /* ... */}

db.Put(forward)
db.Put(inverse)
```

**As a reader I want to fetch the article ...**

The lookup of concrete instances of items requires knowledge about the full composite sort key. The application uses `Get`, providing an "empty" structure as a key. 

```go
articleID := Article{
  Author: curie.New("author:%s", "neumann"),
  ID:     curie.New("article:%s", "theory_of_automata"),
}

article, err := db.Get(&articleID)
```

**As a reader I want to list all articles written by the author ...**

This is one of the primary one-to-many access patterns supported by composite sort keys. The application defines partition key (author) and queries all associated articles. The `dynamo` library implements `Match` function, which uses `curie.IRI` as a pattern of composite sort key. The function returns a lazy sequence of generic representations that has to be transformed into actual data types. `FMap` is a utility that takes a closure function that lifts generic to the struct. The example below uses a monoid pattern to materialize a sequence of generic elements, please see [api documentation](https://github.com/fogfish/dynamo) for details about this pattern.

```go
var seq Articles

db.Match(Article{
  Author: curie.New("author:%s", "neumann"),
}).FMap(seq.Join)
```

**As a reader I want to look up articles titles for given keywords ...**

The table contains forwards and inverse lists of all keywords. The application crafts `curie.IRI` to query keywords partition and return all articles.

```go
var seq Keywords

db.Match(Keyword{
  HashKey: curie.New("keyword:%s", "theory"),
}).FMap(seq.Join)
```

**As a reader I want to look up articles titles written by the author for a given keyword ...**

The access pattern is implemented using composite sort keys ability to encode hierarchical structures. It requires scope articles by two dimensions keyword and author. Like in the previous access pattern, the keyword partition is a primary dimension to query articles. However the `begins_with` constraints of sort keys limits articles to "written by" subset because sort key is designed to maintain hierarchical relation `keyword ⟼ author ⟼ article`. The `dynamo` library automatically deducts this and constructs the correct query if the application uses `curie.IRI` data type to supply identity to the collection.

```go
var seq Keywords
ddb.Match(Keyword{
  HashKey: curie.New("keyword:%s", "theory"),
  SortKey: curie.New("article:%s", "neumann"),
}).FMap(seq.Join)
```

**As a reader I want to look up all keywords of the article ...**

The `article ⟼ keyword` is one-to-many relation supported by the inverse keywords list. The application crafts `curie.IRI` to query a partition dedicated for articles metadata and filters keywords only.

```go
var seq Keywords
ddb.Match(Keyword{
  HashKey: curie.IRI("article:%s/%s", "neumann", "theory_of_automata"),
  SortKey: curie.IRI("keyword:")
}).FMap(seq.Join)
```

**As a reader I want to look up all articles for a given category in chronological order ...**

The access pattern requires a global secondary index. Otherwise, the lookup is similar to other access patterns.  

```go
gsi := keyval.Must(
  keyval.ReadOnly[Category](
    dynamo.WithURI("ddb:///example-dynamo-relational/example-dynamo-relational-category-year?prefix=category&suffix=year"),
  ),
)

var seq Articles
gsi.Match(Category{
  Category: "Computer Science",
}).FMap(seq.Join)
```

**As a reader I want to list all articles written by the author in chronological order ...**

The access pattern requires a local secondary index. As it has been discussed earlier, the usage of local secondary indexes is required only when consistent reads are required. Here access pattern shows ability of the `dynamo` library to query local secondary index

```go
// client to access global secondary index
lsi := keyval.Must(
  keyval.ReadOnly[Article](
    dynamo.WithURI("ddb:///example-dynamo-relational/example-dynamo-relational-year?suffix=year"),
  ),
)

var seq Articles
lsi.Match(Article{
  Author: curie.New("article:%s", "neumann"),
}).FMap(seq.Join)
```

## Afterwords

AWS DynamoDB is a managed NoSQL database that provides predictable performance. DynamoDB is quickly becoming the service of choice for traditional and serverless development. The development against DynamoDB requires consideration of access patterns. The `dynamo` library has been developed to streamline the data design process using pure Golang structures for definition of domain models. Using the library, the application can achieve the ideal data model that would require a single request to DynamoDB and model one-to-one, one-to-many and even many-to-many relations.


## References

1. [Best Practices for Designing and Architecting with DynamoDB](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/best-practices.html)
2. [The Ten Rules for Data Modeling with DynamoDB](https://www.trek10.com/blog/the-ten-rules-for-data-modeling-with-dynamodb)
3. [The What, Why, and When of Single-Table Design with DynamoDB](https://www.alexdebrie.com/posts/dynamodb-single-table)

