//
// Copyright (C) 2022 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/fogfish/curie"
	"github.com/fogfish/dynamo"
	"github.com/fogfish/dynamo/service/ddb"
)

func main() {
	//
	// create DynamoDB clients for the main table (ddb), local secondary index (lsi),
	// global secondary index (gsi)
	db := ddb.Must(ddb.New[Author](nil, "ddb:///example-dynamo-relational", nil))
	dba := ddb.Must(ddb.New[Article](nil, "ddb:///example-dynamo-relational", nil))
	dbk := ddb.Must(ddb.New[Keyword](nil, "ddb:///example-dynamo-relational", nil))

	lsi := ddb.Must(ddb.New[Article](nil, "ddb:///example-dynamo-relational/example-dynamo-relational-year?suffix=year", nil))
	gsi := ddb.Must(ddb.New[Category](nil, "ddb:///example-dynamo-relational/example-dynamo-relational-category-year?prefix=category&suffix=year", nil))

	//
	// As an author I want to register a profile ...
	// As an author I want to publish an article to the system ...
	assert(articlesOfJohnVonNeumann(db, dba, dbk))
	assert(articlesOfLeonardKleinrock(db, dba, dbk))

	//
	// As a reader I want to fetch the article ...
	assert(fetchArticle(dba))

	//
	// As a reader I want to list all articles written by the author ...
	assert(lookupArticlesByAuthor(dba, "neumann"))
	assert(lookupArticlesByAuthor(dba, "kleinrock"))

	//
	// As a reader I want to look up articles titles for given keywords ...
	assert(lookupArticlesByKeyword(dbk, "theory"))

	//
	// As a reader I want to look up articles titles written by the author for a given keyword
	assert(lookupArticlesByKeywordAuthor(dbk, "theory", "neumann"))

	//
	// As a reader I want to look up all keywords of the article ...
	assert(fetchArticleKeywords(dbk))

	//
	// As a reader I want to look up all articles for a given category in chronological order ...
	assert(lookupArticlesByCategory(gsi, "Computer Science"))
	assert(lookupArticlesByCategory(gsi, "Math"))

	//
	// As a reader I want to list all articles written by the author in chronological order ...
	assert(lookupByAuthorOrderedByTime(lsi, "neumann"))
}

/*

As a reader I want to fetch the article ...
*/
func fetchArticle(db dynamo.KeyVal[Article]) error {
	log.Printf("==> fetch article: An axiomatization of set theory\n")

	article, err := db.Get(context.Background(),
		Article{
			Author: curie.New("author:%s", "neumann"),
			ID:     curie.New("article:%s", "theory_of_automata"),
		},
	)
	if err != nil {
		return err
	}

	return stdio(article)
}

/*

As a reader I want to list all articles written by the author ...
*/
func lookupArticlesByAuthor(db dynamo.KeyVal[Article], author string) error {
	log.Printf("==> lookup articles by author: %s\n", author)

	var seq Articles
	err := db.Match(context.Background(), Article{
		Author: curie.New("author:%s", author),
		ID:     curie.New("article:"),
	}).FMap(seq.Join)

	if err != nil {
		return err
	}

	return stdio(seq)
}

/*

As a reader I want to look up articles titles for given keywords ...
*/
func lookupArticlesByKeyword(db dynamo.KeyVal[Keyword], keyword string) error {
	log.Printf("==> lookup articles by keyword: %s\n", keyword)

	var seq Keywords
	err := db.Match(context.Background(),
		Keyword{
			HKey: curie.New("keyword:%s", keyword),
		},
	).FMap(seq.Join)

	if err != nil {
		return err
	}

	return stdio(seq)
}

/*

As a reader I want to look up articles titles written by the author for a given keyword
*/
func lookupArticlesByKeywordAuthor(db dynamo.KeyVal[Keyword], keyword, author string) error {
	log.Printf("==> lookup articles by keyword %s and author: %s\n", keyword, author)

	var seq Keywords
	err := db.Match(context.Background(),
		Keyword{
			HKey: curie.New("keyword:%s", keyword),
			SKey: curie.New("article:%s", author),
		},
	).FMap(seq.Join)

	if err != nil {
		return err
	}

	return stdio(seq)
}

/*

As a reader I want to look up all keywords of the article ...
*/
func fetchArticleKeywords(db dynamo.KeyVal[Keyword]) error {
	log.Printf("==> lookup keyword for An axiomatization of set theory\n")

	var seq Keywords
	err := db.Match(context.Background(),
		Keyword{
			HKey: curie.New("article:%s/%s", "neumann", "theory_of_set"),
			SKey: curie.New("keyword:"),
		},
	).FMap(seq.Join)

	if err != nil {
		return err
	}

	return stdio(seq)
}

/*

As a reader I want to look up all articles for a given category in chronological order ...
*/
func lookupArticlesByCategory(db dynamo.KeyVal[Category], category string) error {
	log.Printf("==> lookup articles by category: %s\n", category)

	var seq dynamo.Things[Category]
	err := db.Match(context.Background(),
		Category{
			Category: category,
		},
	).FMap(seq.Join)

	if err != nil {
		return err
	}

	return stdio(seq)
}

/*

As a reader I want to list all articles written by the author in chronological order ...
*/
func lookupByAuthorOrderedByTime(db dynamo.KeyVal[Article], author string) error {
	log.Printf("==> lookup articles in chronological order: %s", author)

	var seq Articles
	err := db.Match(context.Background(),
		Article{
			Author: curie.New("author:%s", author),
		},
	).FMap(seq.Join)

	if err != nil {
		return err
	}

	return stdio(seq)
}

//
func articlesOfJohnVonNeumann(
	db dynamo.KeyVal[Author],
	dba dynamo.KeyVal[Article],
	dbk dynamo.KeyVal[Keyword],
) error {
	if err := registerAuthor(db, "neumann", "John von Neumann"); err != nil {
		return err
	}

	err := publishArticle(dba, dbk, "neumann",
		"theory_of_set",
		"An axiomatization of set theory",
		[]string{"theory", "math"},
	)
	if err != nil {
		return err
	}

	err = publishArticle(dba, dbk, "neumann",
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
func articlesOfLeonardKleinrock(
	db dynamo.KeyVal[Author],
	dba dynamo.KeyVal[Article],
	dbk dynamo.KeyVal[Keyword],
) error {
	if err := registerAuthor(db, "kleinrock", "Leonard Kleinrock"); err != nil {
		return err
	}

	err := publishArticle(dba, dbk, "kleinrock",
		"queueing_sys_vol1",
		"Queueing Systems: Volume I - Theory",
		[]string{"queue", "theory"},
	)
	if err != nil {
		return err
	}

	err = publishArticle(dba, dbk, "kleinrock",
		"queueing_sys_vol2",
		"Queueing Systems: Volume II - Computer Applications",
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
func registerAuthor(db dynamo.KeyVal[Author], id, name string) error {
	log.Printf("==> register: %s", name)

	author := NewAuthor(id, name)
	if err := db.Put(context.Background(), author); err != nil {
		return err
	}

	return nil
}

/*

As an author I want to publish an article to the system ...
*/
func publishArticle(
	dba dynamo.KeyVal[Article],
	dbk dynamo.KeyVal[Keyword],
	author, id, title string,
	keywords []string,
) error {
	log.Printf("==> publish: %s", title)

	article := NewArticle(author, id, title)
	if err := dba.Put(context.Background(), article); err != nil {
		return err
	}

	for _, keyword := range keywords {
		seq := NewKeyword(author, id, title, keyword)
		for _, k := range seq {
			if err := dbk.Put(context.Background(), k); err != nil {
				return err
			}
		}
	}

	return nil
}

// stdio outputs query result
func stdio(data interface{}) error {
	b, err := json.MarshalIndent(data, "|", "  ")
	if err != nil {
		return err
	}

	log.Println(string(b))
	return nil
}

// assert error
func assert(err error) {
	if err != nil {
		panic(err)
	}
}
