package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/fogfish/dynamo"
)

func main() {
	//
	// create DynamoDB clients for the main table (ddb), local secondary index (lsi),
	// global secondary index (gsi)
	ddb := dynamo.NewKeyValContextDefault(
		dynamo.Must(dynamo.New("ddb:///example-dynamo-relational")),
	)
	lsi := dynamo.NewKeyValContextDefault(
		dynamo.Must(dynamo.New("ddb:///example-dynamo-relational/example-dynamo-relational-year?suffix=year")),
	)
	gsi := dynamo.NewKeyValContextDefault(
		dynamo.Must(dynamo.New("ddb:///example-dynamo-relational/example-dynamo-relational-category-year?prefix=category&suffix=year")),
	)

	//
	// As an author I want to register a profile ...
	// As an author I want to publish an article to the system ...
	assert(articlesOfJohnVonNeumann(ddb))
	assert(articlesOfLeonardKleinrock(ddb))

	//
	// As a reader I want to fetch the article ...
	assert(fetchArticle(ddb))

	//
	// As a reader I want to list all articles written by the author ...
	assert(lookupArticlesByAuthor(ddb, "neumann"))
	assert(lookupArticlesByAuthor(ddb, "kleinrock"))

	//
	// As a reader I want to look up articles titles for given keywords ...
	assert(lookupArticlesByKeyword(ddb, "theory"))

	//
	// As a reader I want to look up articles titles written by the author for a given keyword
	assert(lookupArticlesByKeywordAuthor(ddb, "theory", "neumann"))

	//
	// As a reader I want to look up all keywords of the article ...
	assert(fetchArticleKeywords(ddb))

	//
	// As a reader I want to look up all articles for a given category in chronological order ...
	assert(lookupArticlesByCategory(gsi, "Computer Science"))
	assert(lookupArticlesByCategory(gsi, "Math"))

	//
	// As a reader I want to list all articles written by the author in chronological order ...
	assert(lookupByAuthor(lsi, "neumann"))
}

/*

As a reader I want to fetch the article ...
*/
func fetchArticle(db dynamo.KeyValNoContext) error {
	log.Printf("==> fetch article: An axiomatization of set theory\n")

	article := Article{
		AuthorID: fmt.Sprintf("author:%s", "neumann"),
		ID:       fmt.Sprintf("article:%s", "theory_of_automata"),
	}

	if err := db.Get(&article); err != nil {
		return err
	}

	return stdio(article)
}

/*

As a reader I want to list all articles written by the author ...
*/
func lookupArticlesByAuthor(db dynamo.KeyValNoContext, author string) error {
	log.Printf("==> lookup articles by author: %s\n", author)

	var seq Articles
	err := db.Match(Article{
		AuthorID: fmt.Sprintf("author:%s", author),
		ID:       "article",
	}).FMap(seq.Join)
	if err != nil {
		return err
	}

	return stdio(seq)
}

/*

As a reader I want to look up articles titles for given keywords ...
*/
func lookupArticlesByKeyword(db dynamo.KeyValNoContext, keyword string) error {
	log.Printf("==> lookup articles by keyword: %s\n", keyword)

	return lookupKeywords(db, fmt.Sprintf("keyword:%s", keyword), "")
}

/*

As a reader I want to look up articles titles written by the author for a given keyword
*/
func lookupArticlesByKeywordAuthor(db dynamo.KeyValNoContext, keyword, author string) error {
	log.Printf("==> lookup articles by keyword %s and author: %s\n", keyword, author)

	return lookupKeywords(db, fmt.Sprintf("keyword:%s", keyword), fmt.Sprintf("article:%s", author))
}

/*

As a reader I want to look up all keywords of the article ...
*/
func fetchArticleKeywords(db dynamo.KeyValNoContext) error {
	log.Printf("==> lookup keyword for An axiomatization of set theory\n")

	return lookupKeywords(db, "article:neumann/theory_of_set", "keyword:")
}

/*

As a reader I want to look up all articles for a given category in chronological order ...
*/
func lookupArticlesByCategory(db dynamo.KeyValNoContext, category string) error {
	log.Printf("==> lookup articles by category: %s\n", category)

	var seq Articles
	err := db.Match(Article{
		Category: category,
	}).FMap(seq.Join)
	if err != nil {
		return err
	}

	return stdio(seq)
}

/*

As a reader I want to list all articles written by the author in chronological order ...
*/
func lookupByAuthor(db dynamo.KeyValNoContext, author string) error {
	log.Printf("==> lookup articles: %s", author)

	var seq Articles
	err := db.Match(Article{
		AuthorID: fmt.Sprintf("author:%s", author),
	}).FMap(seq.Join)
	if err != nil {
		return err
	}

	return stdio(seq)
}

//
func articlesOfJohnVonNeumann(db dynamo.KeyValNoContext) error {
	if err := registerAuthor(db, "neumann", "John von Neumann"); err != nil {
		return err
	}

	err := publishArticle(db, "neumann",
		"theory_of_set",
		"An axiomatization of set theory",
		[]string{"theory", "math"},
	)
	if err != nil {
		return err
	}

	err = publishArticle(db, "neumann",
		"theory_of_automata",
		"The general and logical theory of automata",
		[]string{"theory", "computer"},
	)
	if err != nil {
		return err
	}

	return nil
}

//
func articlesOfLeonardKleinrock(db dynamo.KeyValNoContext) error {
	if err := registerAuthor(db, "kleinrock", "Leonard Kleinrock"); err != nil {
		return err
	}

	err := publishArticle(db, "kleinrock",
		"queueing_sys_vol1",
		"Queueing Systems: Volume I – Theory",
		[]string{"queue", "theory"},
	)
	if err != nil {
		return err
	}

	err = publishArticle(db, "kleinrock",
		"queueing_sys_vol2",
		"Queueing Systems: Volume II – Computer Applications",
		[]string{"queue", "computer"},
	)
	if err != nil {
		return err
	}

	return nil
}

/*

As an author I want to register a profile ...
*/
func registerAuthor(db dynamo.KeyValNoContext, id, name string) error {
	log.Printf("==> register: %s", name)

	author := NewAuthor(id, name)
	if err := db.Put(author); err != nil {
		return err
	}

	return nil
}

/*

As an author I want to publish an article to the system ...
*/
func publishArticle(db dynamo.KeyValNoContext, author, id, title string, keywords []string) error {
	log.Printf("==> publish: %s", title)

	article := NewArticle(author, id, title)
	if err := db.Put(article); err != nil {
		return err
	}

	for _, keyword := range keywords {
		seq := NewKeyword(author, id, title, keyword)
		for _, k := range seq {
			if err := db.Put(k); err != nil {
				return err
			}
		}
	}

	return nil
}

//
func lookupKeywords(db dynamo.KeyValNoContext, hashkey, sortkey string) error {
	var seq Keywords
	err := db.Match(Keyword{HashKey: hashkey, SortKey: sortkey}).FMap(seq.Join)
	if err != nil {
		return err
	}

	return stdio(seq)
}

// stdio outputs query result
func stdio(data interface{}) error {
	b, err := json.MarshalIndent(data, "|", "  ")
	if err != nil {
		return err
	}

	log.Printf(string(b))
	return nil
}

// assert error
func assert(err error) {
	if err != nil {
		panic(err)
	}
}
