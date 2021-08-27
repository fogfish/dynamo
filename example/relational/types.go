package main

import (
	"fmt"
	"math/rand"

	"github.com/fogfish/curie"
	"github.com/fogfish/dynamo"
)

/*

Author ...

Each author will have many articles.

author:john
*/
type Author struct {
	dynamo.ID
	Name string `dynamodbav:"name,omitempty" json:"name,omitempty"`
}

/*

NewAuthor ...
*/
func NewAuthor(id, name string) Author {
	return Author{
		ID:   dynamo.NewID("author:%s", id),
		Name: name,
	}
}

/*

Article ...

Author ¹⟼ⁿ Article

Each article can be in many keywords,

one - to - many relation: requires a partition key and sort key

article:john/the_system_design

*/
type Article struct {
	dynamo.ID
	Title    string `dynamodbav:"title,omitempty" json:"title,omitempty"`
	Category string `dynamodbav:"category,omitempty" json:"subject,omitempty"`
	Year     string `dynamodbav:"year,omitempty" json:"year,omitempty"`
}

/*

NewArticle ...
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

//
type Articles []Article

//
func (seq *Articles) Join(gen dynamo.Gen) (dynamo.Thing, error) {
	val := Article{}
	if fail := gen.To(&val); fail != nil {
		return nil, fail
	}
	*seq = append(*seq, val)
	return &val, nil
}

/*

Keyword ...

Adj list pattern

Keyword ⁿ⟼ⁿ Article

and each keyword can also be in many articles.

(Composite sort key pattern pk=keyword:queueing sk=article/john)

keyword:queueing/article/john/the_system_design

article:john/the_system_design/keyword/queueing
*/
type Keyword struct {
	dynamo.ID
	Text string `dynamodbav:"text,omitempty" json:"text,omitempty"`
}

//
func NewKeyword(article dynamo.ID, keyword string) []Keyword {
	keywordID := curie.Split(curie.Heir(curie.New("keyword:%s", keyword), curie.IRI(article.IRI)), 3)
	articleID := curie.Join(curie.IRI(article.IRI), "keyword", keyword)

	return []Keyword{
		{ID: dynamo.MkID(keywordID), Text: keyword},
		{ID: dynamo.MkID(articleID), Text: keyword},
	}
}

//
type References []curie.IRI

//
func (seq *References) Join(gen dynamo.Gen) (dynamo.Thing, error) {
	id, err := gen.ID()
	if err != nil {
		return nil, err
	}

	iri := curie.Child(curie.IRI(id.IRI))
	*seq = append(*seq, iri)
	return nil, nil
}
