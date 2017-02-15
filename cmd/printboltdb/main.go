package main

import (
	"flag"
	"fmt"
	"log"
	"os"

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

	fmt.Printf("Found: %d entries.\n", storage.NumEntries())

	entries, err := storage.GetEntries(0, storage.NumEntries())

	if err != nil {
		log.Fatal(err)
	}

	for _, entry := range entries {
		fmt.Println(entry)
	}
}
