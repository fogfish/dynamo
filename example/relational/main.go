package main

import (
	"encoding/json"
	"log"

	"github.com/fogfish/curie"
	"github.com/fogfish/dynamo"
)

func main() {
	db := dynamo.Must(dynamo.New("ddb:///example-dynamo-relational"))
	dby := dynamo.Must(dynamo.New("ddb:///example-dynamo-relational/example-dynamo-relational-year"))
	dbp := dynamo.Must(dynamo.New("ddb:///example-dynamo-relational/example-dynamo-relational-publisher-year?prefix=publisher&suffix=year"))

	// id := dynamo.MkID(curie.New("article:john/x"))
	// keywords := NewKeyword(id, "theory")
	// for _, k := range keywords {
	// 	// fmt.Println(k)
	// 	// fmt.Println(curie.Prefix(curie.IRI(k.IRI)))
	// 	// fmt.Println(curie.Suffix(curie.IRI(k.IRI)))
	// 	if err := db.Put(k); err != nil {
	// 		panic(err)
	// 	}
	// }

	// var seq dynamo.IDs
	// db.Match(dynamo.NewID("keyword:theory#article/john")).FMap(seq.Join)
	// fmt.Println(seq)

	if err := articlesOfJohnVonNeumann(db); err != nil {
		panic(err)
	}

	if err := articlesOfLeonardKleinrock(db); err != nil {
		panic(err)
	}

	if err := lookupArticlesByAuthor(db, curie.New("author:neumann")); err != nil {
		panic(err)
	}

	if err := lookupArticlesByAuthor(db, curie.New("author:kleinrock")); err != nil {
		panic(err)
	}

	if err := lookupArticlesByKeyword(db, "theory"); err != nil {
		panic(err)
	}

	if err := lookupVonNeumannArticlesByKeyword(db, "theory"); err != nil {
		panic(err)
	}

	if err := lookupArticlesByAuthor(dby, curie.New("author:neumann")); err != nil {
		panic(err)
	}

	if err := lookupArticlesByAuthor(dby, curie.New("author:kleinrock")); err != nil {
		panic(err)
	}

	if err := lookupArticlesByPublisher(dbp, "Academic Press."); err != nil {
		panic(err)
	}

	if err := lookupArticlesByPublisher(dbp, "Allen Press."); err != nil {
		panic(err)
	}
}

//
func lookupArticlesByAuthor(db dynamo.KeyVal, author curie.IRI) error {
	log.Printf("==> lookup author: %v", author)

	pattern := dynamo.MkID(curie.NewScheme(author, "article"))

	var seq Articles
	_, err := db.Match(pattern).FMap(seq.Join)
	if err != nil {
		return err
	}

	return StdIO(seq)
}

//
func lookupArticlesByKeyword(db dynamo.KeyVal, keyword string) error {
	log.Printf("==> lookup keyword: %v", keyword)

	pattern := dynamo.MkID(curie.New("keyword:%s", keyword))

	var seq References
	_, err := db.Match(pattern).FMap(seq.Join)
	if err != nil {
		return err
	}

	return StdIO(seq)
}

//
func lookupVonNeumannArticlesByKeyword(db dynamo.KeyVal, keyword string) error {
	log.Printf("==> lookup keyword: %v in John von Neumann", keyword)

	pattern := dynamo.MkID(curie.New("keyword:%s#article/neumann", keyword))

	var seq References
	_, err := db.Match(pattern).FMap(seq.Join)
	if err != nil {
		return err
	}

	return StdIO(seq)
}

//
func lookupArticlesByPublisher(db dynamo.KeyVal, publisher string) error {
	log.Printf("==> lookup publisher: %v", publisher)

	pattern := dynamo.MkID(curie.New(publisher))

	var seq Articles
	_, err := db.Match(pattern).FMap(seq.Join)
	if err != nil {
		return err
	}

	return StdIO(seq)
}

//
func articlesOfJohnVonNeumann(db dynamo.KeyVal) error {
	log.Println("==> publishing: John von Neumann")

	//
	author := NewAuthor("neumann", "John von Neumann")
	if err := db.Put(author); err != nil {
		return err
	}

	//
	article := NewArticle(author.ID, "set_theory", "An axiomatization of set theory")
	if err := db.Put(article); err != nil {
		return err
	}

	keywords := NewKeyword(article.ID, "theory")
	for _, k := range keywords {
		if err := db.Put(k); err != nil {
			return err
		}
	}

	//
	article = NewArticle(author.ID, "automata_theory", "The general and logical theory of automata")
	if err := db.Put(article); err != nil {
		return err
	}

	keywords = NewKeyword(article.ID, "theory")
	for _, k := range keywords {
		if err := db.Put(k); err != nil {
			return err
		}
	}

	keywords = NewKeyword(article.ID, "computer")
	for _, k := range keywords {
		if err := db.Put(k); err != nil {
			return err
		}
	}

	return nil
}

//
func articlesOfLeonardKleinrock(db dynamo.KeyVal) error {
	log.Println("==> publishing: Leonard Kleinrock")

	//
	author := NewAuthor("kleinrock", "Leonard Kleinrock")
	if err := db.Put(author); err != nil {
		return err
	}

	//
	article := NewArticle(author.ID, "queueing_sys_vol1", "Queueing Systems: Volume I – Theory")
	if err := db.Put(article); err != nil {
		return err
	}

	keywords := NewKeyword(article.ID, "queue")
	for _, k := range keywords {
		if err := db.Put(k); err != nil {
			return err
		}
	}

	keywords = NewKeyword(article.ID, "theory")
	for _, k := range keywords {
		if err := db.Put(k); err != nil {
			return err
		}
	}

	//
	article = NewArticle(author.ID, "queueing_sys_vol2", "Queueing Systems: Volume II – Computer Applications")
	if err := db.Put(article); err != nil {
		return err
	}

	keywords = NewKeyword(article.ID, "queue")
	for _, k := range keywords {
		if err := db.Put(k); err != nil {
			return err
		}
	}

	keywords = NewKeyword(article.ID, "computer")
	for _, k := range keywords {
		if err := db.Put(k); err != nil {
			return err
		}
	}

	return nil
}

// StdIO outputs query result
func StdIO(data interface{}) error {
	b, err := json.MarshalIndent(data, "|", "  ")
	if err != nil {
		return err
	}

	log.Printf(string(b))
	return nil
}
