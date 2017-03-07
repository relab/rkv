package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/relab/raft/commonpb"
	"github.com/relab/raft/raftgorums"
)

func main() {
	var path = flag.String("path", "", "path to bolt storage")
	flag.Parse()

	if len(*path) == 0 {
		fmt.Print("-path argument is required\n\n")
		flag.Usage()
		os.Exit(1)
	}

	storage, err := raftgorums.NewFileStorage(*path, false)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Found: %d entries.\n", storage.NextIndex()-storage.FirstIndex())

	entries := make([]*commonpb.Entry, storage.NextIndex()-storage.FirstIndex())

	for i := storage.FirstIndex(); i < storage.NextIndex(); i++ {
		entry, err := storage.GetEntry(i)

		if err != nil {
			entry = &commonpb.Entry{Data: []byte("missing")}
		}

		entries[i-storage.FirstIndex()] = entry
	}

	for _, entry := range entries {
		fmt.Println(entry)
	}
}
