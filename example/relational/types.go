package main

import (
	"fmt"
	"math/rand"

	"github.com/fogfish/curie"
	"github.com/fogfish/dynamo"
)

/*

Author of articles with-in fictional arxiv.org application.

The access patterns for authors follow a classical Key-Val I/O.
- As an author I want to register a profile ...

The author unique identity is a candidate for partition key,
sharding suffix can also be employed if needed.

dynamo.NewID("author:%s", "neumann")
  ⟿ author:neumann
*/
type Author struct {
	dynamo.ID
	Name string `dynamodbav:"name,omitempty" json:"name,omitempty"`
}

/*

NewAuthor creates instance of author
*/
func NewAuthor(id, name string) Author {
	return Author{
		ID:   dynamo.NewID("author:%s", id),
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

dynamo.NewID("article:%s#%s", "neumann", "theory_of_automata")
  ⟿ article:neumann#theory_of_automata

*/
type Article struct {
	dynamo.ID
	Title    string `dynamodbav:"title,omitempty" json:"title,omitempty"`
	Category string `dynamodbav:"category,omitempty" json:"category,omitempty"`
	Year     string `dynamodbav:"year,omitempty" json:"year,omitempty"`
}

/*

NewArticle creates instance of Article
*/
func NewArticle(author dynamo.ID, id, title string) Article {
	iri := curie.Join(curie.NewScheme(curie.IRI(author.IRI), "article"), id)

	category := "Math"
	if rand.Float64() < 0.5 {
		category = "Computer Science"
	}

	return Article{
		ID:       dynamo.MkID(iri),
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
type Articles []Article

// Join generic element into sequence
func (seq *Articles) Join(gen dynamo.Gen) (dynamo.Thing, error) {
	val := Article{}
	if fail := gen.To(&val); fail != nil {
		return nil, fail
	}
	*seq = append(*seq, val)
	return &val, nil
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

dynamo.NewID("keyword:%s#article/%s/%s",
  "theory", "neumann", "theory_of_automata")
    ⟿ keyword:theory#article/neumann/theory_of_automata

dynamo.NewID("article:%s/%s#keyword/%s",
  "neumann", "theory_of_automata", "theory")
    ⟿ article:neumann/theory_of_automata#keyword:theory

*/
type Keyword struct {
	dynamo.ID
	Text string `dynamodbav:"text,omitempty" json:"text,omitempty"`
}

/*

NewKeyword explicitly creates pair of Keyword ⟼ Article and
Article ⟼ Keyword relations.
*/
func NewKeyword(article dynamo.ID, title string, keyword string) []Keyword {
	keywordID := curie.Split(curie.Heir(curie.New("keyword:%s", keyword), curie.IRI(article.IRI)), 3)
	articleID := curie.Join(curie.IRI(article.IRI), "keyword", keyword)

	return []Keyword{
		{ID: dynamo.MkID(keywordID), Text: title},
		{ID: dynamo.MkID(articleID), Text: keyword},
	}
}

/*

Keywords is a sequence of Keywords
*/
type Keywords []Keyword

// Join generic element into sequence
func (seq *Keywords) Join(gen dynamo.Gen) (dynamo.Thing, error) {
	val := Keyword{}
	if fail := gen.To(&val); fail != nil {
		return nil, fail
	}
	*seq = append(*seq, val)
	return &val, nil
}
