package main

import (
	"fmt"
	"math/rand"

	"github.com/fogfish/dynamo"
)

/*

Author of articles with-in fictional arxiv.org application.

The access patterns for authors follow a classical Key-Val I/O.
- As an author I want to register a profile ...

The author unique identity is a candidate for partition key,
sharding suffix can also be employed if needed.

HashKey is
dynamo.NewIRI("author:%s", "neumann")
  ⟿ author:neumann
*/
type Author struct {
	ID   dynamo.IRI `dynamodbav:"prefix,omitempty"`
	Name string     `dynamodbav:"name,omitempty" json:"name,omitempty"`
}

// Identity implements Thing interface
func (author Author) HashKey() string { return author.ID.String() }
func (author Author) SortKey() string { return "_" }

/*

NewAuthor creates instance of author
*/
func NewAuthor(id, name string) Author {
	return Author{
		ID:   dynamo.NewIRI("author:%s", id),
		Name: name,
	}
}

/*

Article is published by Authors (Author ¹⟼ⁿ Article)

The access patterns for an article follows one-to-many I/O
- As an author I want to publish an article to the system ...
- As a reader I want to fetch the article ...
- As a reader I want to list all articles written by the author ...

The article is either referenced directly or looked as a descendant of
the author. Eventually building relation one author to many articles.
The composed sort key is a pattern to build the relation. Author is
the partition key, article id is a sort key

HashKey is
dynamo.NewIRI("author:%s", "neumann")
  ⟿ author:neumann

SortKey is
dynamo.NewIRI("article:%s", "theory_of_automata")
  ⟿ article:theory_of_automata
*/
type Article struct {
	Author   dynamo.IRI `dynamodbav:"prefix,omitempty"`
	ID       dynamo.IRI `dynamodbav:"suffix,omitempty"`
	Title    string     `dynamodbav:"title,omitempty" json:"title,omitempty"`
	Category string     `dynamodbav:"category,omitempty" json:"category,omitempty"`
	Year     string     `dynamodbav:"year,omitempty" json:"year,omitempty"`
}

// Identity implements Thing interface
func (article Article) HashKey() string { return article.Author.String() }
func (article Article) SortKey() string { return article.ID.String() }

/*

NewArticle creates instance of Article
*/
func NewArticle(author string, id, title string) Article {
	category := "Math"
	if rand.Float64() < 0.5 {
		category = "Computer Science"
	}

	return Article{
		Author:   dynamo.NewIRI("author:%s", author),
		ID:       dynamo.NewIRI("article:%s", id),
		Title:    title,
		Category: category,
		Year:     fmt.Sprintf("%d", 1930+rand.Intn(40)),
	}
}

/*

Articles is sequence of Articles.

This code snippet shows the best approach to lift generic sequence of DynamoDB
items into the sequence of articles. The pattern uses concept of monoid.
*/
type Articles dynamo.Things[Article]

// Join generic element into sequence
func (seq *Articles) Join(val *Article) error {
	*seq = append(*seq, *val)
	return nil
}

/*

Keyword is contained by Article (Keyword ⁿ⟼ⁿ Article)

The access patterns for an article - keyword is a classical many-to-many I/O
- As a reader I want to look up articles titles for given keywords ...
- As a reader I want to look up articles titles written by the author for a given keyword ...
- As a reader I want to look up all keywords of the article ...

Adjacency List design pattern is one way to solve many-to-many relation but it requires a global secondary index on the sort key, which might cause unnecessary overhead in single table design.
https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/bp-adjacency-graphs.html#bp-adjacency-lists

The global secondary index implicitly maintains two adjacency lists.
The first one is the forward article-to-keyword, the second one is
an inverse keyword-to-article. It is possible to craft these lists
explicitly. The composed sort key builds for this lists:

HashKey is
dynamo.NewIRI("keyword:%s", "theory")
  ⟿ keyword:theory

SortKey is
dynamo.NewIRI("article:%s/%s", "neumann", "theory_of_automata")
  ⟿ article:neumann/theory_of_automata

and inverse

HashKey is
  ⟿ article:neumann/theory_of_automata

SortKey is
  ⟿ keyword:theory
*/
type Keyword struct {
	HKey dynamo.IRI `dynamodbav:"prefix,omitempty"`
	SKey dynamo.IRI `dynamodbav:"suffix,omitempty"`
	Text string     `dynamodbav:"text,omitempty" json:"text,omitempty"`
}

// Identity implements Thing interface
func (keyword Keyword) HashKey() string { return keyword.HKey.String() }
func (keyword Keyword) SortKey() string { return keyword.SKey.String() }

/*

NewKeyword explicitly creates pair of Keyword ⟼ Article and
Article ⟼ Keyword relations.
*/
func NewKeyword(author, article, title, keyword string) []Keyword {
	hashKey := dynamo.NewIRI("keyword:%s", keyword)
	sortKey := dynamo.NewIRI("article:%s/%s", author, article)

	return []Keyword{
		{HKey: hashKey, SKey: sortKey, Text: title},
		{HKey: sortKey, SKey: hashKey, Text: keyword},
	}
}

/*

Keywords is a sequence of Keywords
*/
type Keywords []Keyword

// Join generic element into sequence
func (seq *Keywords) Join(val *Keyword) error {
	*seq = append(*seq, *val)
	return nil
}
