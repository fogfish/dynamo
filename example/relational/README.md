# How To Model Any Relational Data in DynamoDB With dynamo library

AWS DynamoDB is a NoSQL database, the traditional [data normalization](https://en.wikipedia.org/wiki/Database_normalization) techniques would not work with this database. Instead, the overall data design is based on the understanding access patterns. The access pattern is only the thing to be considered. The ideal data model would require a single request to DynamoDB. Aspects of schema design for DynamoDB are well covered by existing publications, the reference sections links most notable posts on this subject. 

The `dynamo` library has been developed to streamline the data design process using pure Golang structures for definition of domain models. This post, guides your through this process using practical example supported by code snippets.

## Data Access and Query Patterns

> The first step in designing your DynamoDB application is to identify the specific query patterns that the system must satisfy.

The data writing is an "easiest" part in the process. Entire data lifecycle management is built with three operations: create, update and remove data items. These operations are defined over pure Golang structs - public fields are serialized into DynamoDB attributes, the field tag `dynamodbav` controls marshal/unmarshal process.

```go
type Author struct {
  Name    string `dynamodbav:"name,omitempty"`
  Address string `dynamodbav:"address,omitempty"`
}
```

The data reading requires thoughtful work upfront. Typically, all data is de-normalized due to absence of joins and desire to minimize number of round-trips to DynamoDB. It is possible to achieve one-to-one, one-to-many and even many-to-many relations using `dynamo` library but the access patterns needs to be identified. As the result of the access pattern study process, the list might look like the following (an example, arxiv.org like application has been considered):
* As an author I want to register a profile ...
* As an author I want to publishes an article to system ...
* As a reader I want to fetch the article ...
* As a reader I want to list all articles written by the author ...
* As a reader I want to lookup articles titles for given keyword ...
* As a reader I want to lookup articles titles written by the author for given keyword ... 
* As a reader I want to lookup all keywords of the article ...
* As a reader I want to lookup all articles for given category in chronological order ...
* As a reader I want to list all articles written by the author in chronological order ...

The list of access pattern for real application looks complicated at times. This example, represents all I/O patterns solvable with `dynamo` library and reflect real production challenges.

In the context of DynamoDB, the implementation of all access pattern is achieved either with composite sort key or secondary indexes. The key design is the next important step in the data modelling.


## Composite Sort Key  

> Related items can be grouped together and queried efficiently if their key design causes them to sort together. This is an important NoSQL design strategy.

AWS DynamoDB support either simple (a partition key only) or composite (a partition key combined with a sort key) to uniquely identifies items. The `dynamo` library defines a special data type `dynamo.ID` for the purpose of composite key modelling. This type is a synonym to compact Internationalized Resource Identifiers (`curie.IRI`), which facilitates linked-data, hierarchical structures and cheap relations between data items. The library demands from each pure Golang structure embeds `dynamo.ID`. This type acts as struct annotation -- Golang compiler raises an error at compile time if other data type is supplied for DynamoDB I/O.

```go
type Author struct {
  dynamo.ID
  // ...
}
```

Let's emphasis a few fundamental design problem solved by this data type.

**Single table** is a design pattern to address network I/O bottlenecks by retrieving heterogenous item types using a single request. It recommends to put all data items into one table and forget classical relational approach of using different tables per entity. Steep learning curve and "leaks" of identity is two well-known issue in this pattern.

Let's consider our application, what the access pattern have been defined for. It operates with three concepts: `author`, `article` and `keyword`. Haskell Curry author's identity might collides with an article identity about Haskell programming language or a keyword about functional programming. 

The data type `dynamo.ID` (`curie.IRI`) makes a formal definition of the logical partition the identity belongs to. The scheme explicitly defines the purpose of the identity and protects from accidental "collisions". 

```go
dynamo.NewID("author:haskell")
dynamo.NewID("article:haskell")
dynamo.NewID("keyword:haskell")
```

**Sharding** is a technique for distributing loads more evenly across a data partitions. The imbalanced or "hot" partition is well-known issue with DynamoDB. Either random or calculated suffixes is the strategy for load distribution evenly across a partition key space.

The data type `dynamo.ID` (`curie.IRI`) makes a formal rules of building keys from multiple segments. The application has a common interface to construct keys of any complexity to resolve data sharding aspects. 

```go
dynamo.NewID("author:smith/%d", 1)
dynamo.NewID("author:smith/%d", 2)
```

**Composite key** is built from partition key combined with a sort key. The sort key helps an application to keeping related data together in one "place" so that it can be efficiently accessed, effectively building one-to-many relation. Well-crafted composite sort keys defines a hierarchical structures that can be queries at any level of the hierarchy. For example, the following key is efficient listing nested geographical locations `country/region/state/county/city/district`.

The data type `dynamo.ID` (`curie.IRI`) is composed of two elements prefix and suffix, which are automatically serialized into DynamoDB attributes partition (HASH) and sort (RANGE) keys. This approach simplifies data modelling, identities of data items is build from well defined type that is exchangeable between application, DynamoDB and other systems.

Let's follow up previously specified access patterns, this composite sort key type to model algebraic data types for fictional arxiv.org application.

```go
/*

The access patterns for author follows a classical Key-Val I/O.
- As an author I want to register a profile ...

The scheme author and author unique identity is a candidate for
primary key, sharding suffix can also be employed if needed.

dynamo.NewID("author:%s", "neumann")
  ⟿ author:neumann
*/
type Author struct {
  dynamo.ID
}

```

```go
/*

The access patterns for an article follows one-to-many I/O
- As an author I want to publishes an article to system ...
- As a reader I want to fetch the article ...
- As a reader I want to list all articles written by the author ...

The article is either referenced directly or looked as a descendant of
the author. Eventually building one author to many articles relations.
The composed sort key is a pattern to build the relation. Author is
the partition key, article id is a sort key

dynamo.NewID("article:%s#%s", "neumann", "theory_of_automata")
  ⟿ article:neumann#theory_of_automata

*/
type Article struct {
  dynamo.ID
}

/*

Any instance of author identity can be casted to article identity and back
*/
curie.Join(
  curie.NewScheme(curie.IRI(author), "article"),
  "theory_of_automata",
)
```

```go
/*

The access patterns for an article - keyword is a classical many-to-many I/O
- As a reader I want to lookup articles titles for given keyword ...
- As a reader I want to lookup articles titles written by the author for given keyword ...
- As a reader I want to lookup all keywords of the article ...

Adjacency List design pattern is one way to solve many-to-many relation but it requires a global secondary index on the sort key, which might cause unnecessary overhead in single table design.
https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/bp-adjacency-graphs.html#bp-adjacency-lists

The global secondary index implicitly maintains two adjacency list.
The first one is the forward article-to-keyword, the second one is
an inverse keyword-to-article. It is possible to craft these lists
explicitly. The composed sort key builds for this lists: 

dynamo.NewID("keyword:%s#article/%s/%s",
  "theory", "neumann", "theory_of_automata")
    ⟿ keyword:theory#article/neumann/theory_of_automata

dynamo.NewID("article:%s/%s#keyword/%s",
  "neumann", "theory_of_automata", "theory")
    ⟿ article:neumann/theory_of_automata#keyword:theory

*/
type Keyword struct {
  dynamo.ID
}
```

There are only few limited ways how to query data efficiently from DynamoDB. The composite sort key together with `dynamo.ID` (`curie.IRI`) data type let application retrieve hierarchy of related items using range queries with expressions `begins_with`, `between`, `>`, `<`, etc.


## Secondary indexes

Composite sort key supports a hierarchical one-to-many relations. Additional orthogonal access pattern might require secondary indexes over existing data sets. For example:
* As a reader I want to list all articles written by the author ...
* As a reader I want to lookup all articles for given category in chronological order ...

The first access pattern is addressed by composite sort key `author ⟼ article`, the second one requires another `category ⟼ year` key. One approach is an explicit projection of data but secondary indexes are easy. DynamoDB implicitly copies data from main table into the secondary index in a redesigned shape, therefore a new access dimension is unlocked. Eventual consistency is only the feature to consider. The local secondary indexes provides strong consistency but general advice to favour global indexes.

The `dynamo` library supports both global and local secondary indexes with particular behavior:
* it creates a client instance per table, therefore each index requires own instance of the client;
* it automatically projects `dynamo.ID` (`curie.IRI`) into partition (HASH) and sort (RANGE) keys attributes. The default values of attributes is `prefix` and `suffix`. If table design uses other attribute names, which is always a cases of secondary indexes, then connection URI shall give a hint about new name;
* application shall instantiate read-only client for secondary indexes.

```go
// client to access to "main" table
ddb := dynamo.Must(dynamo.New("ddb:///example-dynamo-relational"))

// client to access global secondary index
gsi := dynamo.Must(dynamo.ReadOnly("ddb:///example-dynamo-relational/example-dynamo-relational-category-year?prefix=category&suffix=year"))
```

```go
/*

The access patterns for an article on orthogonal direction 
follows same one-to-many I/O
- As a reader I want to lookup all articles for given category in chronological order ...

The article shall define additional attributes, they would be projected
by DynamoDB into partition and sort keys. Eventually building additional
one subject to many year relations. 

dynamo.NewID("%s#%s", "Computer Science", "1991")
  ⟿ Computer Science#1991

Note: the key here is missing schema definition because secondary index
attributes are usually natural key, in the contrast with previous examples where surrogate were used.

*/
type Article struct {
  dynamo.ID
  Category string `dynamodbav:"category,omitempty"`
  Year     string `dynamodbav:"year,omitempty"`
}
```

AWS DynamoDB gives a recommendation to favor global secondary indexes rather than local secondary indexes. Each table in DynamoDB can have up to 20 global secondary indexes and 5 local secondary indexes. The local indexes must be designed at the time of the table creation.

DynamoDB table schema for fictional arxiv.org is defined at [schema.sh](schema.sh) and Golang types at [types.go](types.go)


## Writing and Reading DynamoDB  

Actual reads and writes into DynamoDB tables is very straightforward with the `dynamo` library. It has been well covered by the [api documentation](https://github.com/fogfish/dynamo). Let's only highlight a simple example for each access pattern, which has been defined earlier. 

**As an author I want to register a profile ...**

The example application instantiates `Author` type, defining composite sort key and other attributes. The instance is `Put` to DynamoDB  

```go
author := Author{
  ID: dynamo.NewID("author:%s", "neumann"),
  // ...
} 

db.Put(author)
```

**As an author I want to publishes an article to system ...**

Fundamentally, there are no difference what data type does application write to DynamoDB. It is all about type instantiation structure with right composite key

```go
article := Article{
  ID: dynamo.NewID("article:%s#%s", "neumann", "theory_of_automata"),
  // ...
}

db.Put(article)
```

The example arxiv.org application does not use a secondary indexes to support implicit search by keywords (so called adjacency list design pattern). Therefore, the application needs to publish keywords explicitly 

```go
forward := Keyword{
  ID: dynamo.NewID("keyword:%s#article/%s/%s",
        "theory", "neumann", "theory_of_automata"),
  // ...  
}
inverse := Keyword{
  ID: dynamo.NewID("article:%s/%s#keyword/%s",
        "neumann", "theory_of_automata", "theory")
  // ...
}

db.Put(forward)
db.Put(inverse)
```

**As a reader I want to fetch the article ...**

The lookup of concrete instance of item requires knowledge about the full composite sort key. The application uses `Get`, providing "empty" structure as a placeholder.

```go
article := Article{
  ID: dynamo.NewID("article:%s#%s", "neumann", "theory_of_automata"),
}

db.Get(&article)
```

**As a reader I want to list all articles written by the author ...**

This is one of the primary one-to-many access pattern supported by composite sort key. The application defines partition key (author) and queries all associated articles. The `dynamo` library implements `Match` function, which uses `dynamo.ID` (`curie.IRI`) as pattern of composite sort key. The function returns a lazy sequence of generic representations that has to be transformed into actual data types. `FMap` is an utility it takes a closure function that lifts generic to the struct. The example below uses monoid pattern to materialize sequence of generic element, please see [api documentation](https://github.com/fogfish/dynamo) for details about this pattern.

```go
id := dynamo.NewID("article:%s", "neumann")

var seq Articles
db.Match(id).FMap(seq.Join)
```

**As a reader I want to lookup articles titles for given keyword ...**

The table contains forwards and inverse list of all keywords. The application crafts `dynamo.ID` (`curie.IRI`) to query keywords partition and return all articles.

```go
id := dynamo.NewID("keyword:%s", "theory")

var seq Keywords
db.Match(id).FMap(seq.Join)
```

**As a reader I want to lookup articles titles written by the author for given keyword ...**

The access pattern is implemented using composite sort keys ability to encode hierarchical structures. It requires to scope articles by two dimensions keyword and author. Like in the previous access pattern, the keyword partition is a primary dimension to query articles. However the `begins_with` constrain of sort keys limits articles to "written by" subset because sort key is designed to maintain hierarchical relation `keyword ⟼ author ⟼ article`. The `dynamo` library automatically deducts this and constructs correct query if application uses `dynamo.ID` (`curie.IRI`) data type to supply identity to the collection.

```go
id := dynamo.NewID("keyword:%s#article/%s", "theory", "neumann")

var seq Keywords
ddb.Match(id).FMap(seq.Join)
```

**As a reader I want to lookup all keywords of the article ...**

The `article ⟼ keyword` is one-to-many relation supported by the inverse keywords list. The application crafts `dynamo.ID` (`curie.IRI`) to query a partition dedicated for articles metadata and filters keywords only.

```go
id := dynamo.NewID("article:%s/%s#keyword", "neumann", "theory_of_automata")

var seq Keywords
ddb.Match(id).FMap(seq.Join)
```

**As a reader I want to lookup all articles for given category in chronological order ...**

The access pattern requires global secondary index. Otherwise, the lookup is similar other access patterns.  

```go
gsi := dynamo.Must(dynamo.ReadOnly("ddb:///example-dynamo-relational/example-dynamo-relational-category-year?prefix=category&suffix=year"))

id := dynamo.NewID("%s", "Computer Science")

var seq Articles
gsi.Match(id).FMap(seq.Join)
```

**As a reader I want to list all articles written by the author in chronological order ...**

The access pattern requires local secondary index. As it has been discussed earlier, the usage of local secondary indexes is required only when consistent reads required. Here access pattern shows ability of the `dynamo` library to query local secondary index

```go
// client to access global secondary index
lsi := dynamo.Must(dynamo.ReadOnly("ddb:///example-dynamo-relational/example-dynamo-relational-year?suffix=year"))

id := dynamo.NewID("article:%s", "neumann")

var seq Articles
lsi.Match(id).FMap(seq.Join)
```

## Afterwords

AWS DynamoDB is a managed NoSQL database that provides predictable performance. DynamoDB is quickly becoming the service of choice for traditional and serverless development. The development against DynamoDB requires consideration of access patterns. The `dynamo` library has been developed to streamline the data design process using pure Golang structures for definition of domain models. Using the library, the application can achieve the ideal data model, that would require a single request to DynamoDB.


## References

1. [Best Practices for Designing and Architecting with DynamoDB](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/best-practices.html)
2. [The Ten Rules for Data Modeling with DynamoDB](https://www.trek10.com/blog/the-ten-rules-for-data-modeling-with-dynamodb)
3. [The What, Why, and When of Single-Table Design with DynamoDB](https://www.alexdebrie.com/posts/dynamodb-single-table)


