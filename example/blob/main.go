package main

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/fogfish/dynamo"
	"github.com/fogfish/iri"
)

func main() {
	db, err := dynamo.Stream(os.Args[1])
	if err != nil {
		panic(err)
	}

	in, err := db.Recv(iri.New(os.Args[2]))
	if err != nil {
		panic(err)
	}

	gz, err := gzip.NewReader(in)
	if err != nil {
		panic(err)
	}

	ta := tar.NewReader(gz)
	for {
		header, err := ta.Next()
		if err == io.EOF {
			return
		} else if err != nil {
			panic(err)
		}

		switch header.Typeflag {
		case tar.TypeDir:
			fmt.Println("==> dir " + header.Name)
		case tar.TypeReg:
			fmt.Printf("==> file %s ", header.Name)
			data := make([]byte, header.Size)
			ta.Read(data)
			fmt.Printf(" %v\n", header.Size)
		default:
			log.Fatalf("unknown type: %v in %s", header.Typeflag, header.Name)
		}
	}
}
