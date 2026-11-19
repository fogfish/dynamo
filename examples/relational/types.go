package main

import (
	"fmt"
	"math/rand"

	"github.com/fogfish/gold"
)

// Author of articles with-in fictional arxiv.org application.
//
// The access patterns for authors follow a classical Key-Val I/O.
// - As an author I want to register a profile ...
//
// The author unique identity is a candidate for partition key,
// sharding suffix can also be employed if needed.
//
// HashKey is
// gold.ToIRI[Author]("neumann")
// ⟿ author:neumann
//
// SortKey is undefined, the library stores it as a placeholder.
type Author struct {
	ID   gold.IRI[Author] `dynamodbav:"prefix,omitempty"`
	Name string           `dynamodbav:"name,omitempty" json:"name,omitempty"`
}

// Identity implements Thing interface
func (author Author) HashKey() string { return string(author.ID) }
func (author Author) SortKey() string { return "" }

// NewAuthor creates instance of author
func NewAuthor(id, name string) Author {
	return Author{
		ID:   gold.ToIRI[Author](id),
		Name: name,
	}
}

// Article is published by Authors (Author ¹⟼ⁿ Article)
//
// The access patterns for an article follows one-to-many I/O
// - As an author I want to publish an article to the system ...
// - As a reader I want to fetch the article ...
// - As a reader I want to list all articles written by the author ...
//
// The article is either referenced directly or looked as a descendant of
// the author. Eventually building relation one author to many articles.
// The composed sort key is a pattern to build the relation. Author is
// the partition key, article id is a sort key
//
// HashKey is
// gold.ToIRI[Author]("neumann")
// ⟿ author:neumann
//
// SortKey is
// gold.ToIRI[Article]("theory_of_automata")
// ⟿ article:theory_of_automata
//
// All articles of the author are looked up using the prefix gold.Any[Article]()
// ⟿ article:
type Article struct {
	Author   gold.IRI[Author]  `dynamodbav:"prefix,omitempty"`
	ID       gold.IRI[Article] `dynamodbav:"suffix,omitempty"`
	Title    string            `dynamodbav:"title,omitempty" json:"title,omitempty"`
	Category string            `dynamodbav:"category,omitempty" json:"category,omitempty"`
	Year     string            `dynamodbav:"year,omitempty" json:"year,omitempty"`
}

// Identity implements Thing interface
func (article Article) HashKey() string { return string(article.Author) }
func (article Article) SortKey() string { return string(article.ID) }

// Category is a projection of the Article to different index
type Category Article

// Identity implements Thing interface
func (article Category) HashKey() string { return article.Category }
func (article Category) SortKey() string { return article.Year }

// NewArticle creates instance of Article
func NewArticle(author string, id, title string) Article {
	category := "Math"
	if rand.Float64() < 0.5 {
		category = "Computer Science"
	}

	return Article{
		Author:   gold.ToIRI[Author](author),
		ID:       gold.ToIRI[Article](id),
		Title:    title,
		Category: category,
		Year:     fmt.Sprintf("%d", 1930+rand.Intn(40)),
	}
}

// ArticleRef is a globally unique reference to the article. The article's
// identity is unique only within the scope of the author, therefore the
// reference is a composite (Imply) of the author and the article.
//
//	author:neumann|article:theory_of_automata
//
// Imply is declared as an alias, the type keeps methods of gold.Imply and
// is stored as a string.
type ArticleRef = gold.Imply[Author, Article]

// Keyword is contained by Article (Keyword ⁿ⟼ⁿ Article)
//
// The access patterns for an article - keyword is a classical many-to-many I/O
// - As a reader I want to look up articles titles for given keywords ...
// - As a reader I want to look up articles titles written by the author for a given keyword ...
// - As a reader I want to look up all keywords of the article ...
//
// Adjacency List design pattern is one way to solve many-to-many relation but it requires a global secondary index on the sort key, which might cause unnecessary overhead in single table design.
// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/bp-adjacency-graphs.html#bp-adjacency-lists
//
// The global secondary index implicitly maintains two adjacency lists.
// The first one is the forward keyword-to-article, the second one is
// an inverse article-to-keyword. It is possible to craft these lists
// explicitly. Each direction is a distinct type, the type-safe identity
// makes it impossible to mix them.
//
// Keyword (forward)
//
// HashKey is
// gold.ToIRI[Keyword]("theory")
// ⟿ keyword:theory
//
// SortKey is
// gold.ImplyFrom(gold.ToIRI[Author]("neumann"), gold.ToIRI[Article]("theory_of_automata"))
// ⟿ author:neumann|article:theory_of_automata
//
// The partial Imply gold.ImplyFrom(gold.ToIRI[Author]("neumann"), gold.IRI[Article](""))
// ⟿ author:neumann|
// selects all articles of the author. The trailing separator is prefix-safe.
type Keyword struct {
	HKey gold.IRI[Keyword] `dynamodbav:"prefix,omitempty"`
	SKey ArticleRef        `dynamodbav:"suffix,omitempty"`
	Text string            `dynamodbav:"text,omitempty" json:"text,omitempty"`
}

// Identity implements Thing interface
func (keyword Keyword) HashKey() string { return string(keyword.HKey) }
func (keyword Keyword) SortKey() string { return keyword.SKey.String() }

// ArticleKeyword is an inverse relation Article ⟼ Keyword
//
// HashKey is
// ⟿ author:neumann|article:theory_of_automata
//
// SortKey is
// ⟿ keyword:theory
type ArticleKeyword struct {
	HKey ArticleRef        `dynamodbav:"prefix"`
	SKey gold.IRI[Keyword] `dynamodbav:"suffix,omitempty"`
	Text string            `dynamodbav:"text,omitempty" json:"text,omitempty"`
}

// Identity implements Thing interface
func (keyword ArticleKeyword) HashKey() string { return keyword.HKey.String() }
func (keyword ArticleKeyword) SortKey() string { return string(keyword.SKey) }

// NewKeyword explicitly creates pair of Keyword ⟼ Article and
// Article ⟼ Keyword relations.
func NewKeyword(author, article, title, keyword string) (Keyword, ArticleKeyword) {
	kw := gold.ToIRI[Keyword](keyword)
	ref := gold.ImplyFrom(gold.ToIRI[Author](author), gold.ToIRI[Article](article))

	return Keyword{HKey: kw, SKey: ref, Text: title},
		ArticleKeyword{HKey: ref, SKey: kw, Text: keyword}
}
