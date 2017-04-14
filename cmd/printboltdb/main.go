package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/relab/raft"
	"github.com/relab/raft/commonpb"
)

func main() {
	var path = flag.String("path", "", "path to bolt storage")
	flag.Parse()

	if len(*path) == 0 {
		fmt.Print("-path argument is required\n\n")
		flag.Usage()
		os.Exit(1)
	}

	storage, err := raft.NewFileStorage(*path, false)

	if err != nil {
		log.Fatal(err)
	}

	nextIndex, err := storage.NextIndex()
	if err != nil {
		log.Fatal(err)
	}
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Found: %d entries.\n", nextIndex-firstIndex+1)

	entries := make([]*commonpb.Entry, nextIndex-firstIndex+1)

	for i := firstIndex; i < nextIndex; i++ {
		entry, err := storage.GetEntry(i)

		if err != nil {
			entry = &commonpb.Entry{Data: []byte("missing")}
		}

		entries[i-firstIndex] = entry
	}

	for _, entry := range entries {
		fmt.Println(entry)
	}
}
