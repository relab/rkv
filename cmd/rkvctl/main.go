package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/relab/rkv/rkvpb"
)

func main() {
	rand.Seed(time.Now().UnixNano())

	var (
		cluster = flag.String("cluster", ":9201,:9202,:9203", "comma separated cluster servers")
		leader  = flag.Int("leader", 0, "index of leader in cluster")
		ops     = flag.Int("ops", 10, "number of write and read operations (total = 1 + 2*ops)")
	)

	flag.Parse()

	servers := strings.Split(*cluster, ",")

	if len(servers) == 0 {
		fmt.Print("-cluster argument is required\n\n")
		flag.Usage()
		os.Exit(1)
	}

	cc, err := grpc.Dial(
		servers[*leader],
		grpc.WithBlock(),
		grpc.WithInsecure(),
		grpc.WithTimeout(1*time.Second),
	)

	if err != nil {
		log.Fatal(err)
	}

	c := rkvpb.NewRKVClient(cc)

	res, err := c.Register(context.Background(), &rkvpb.RegisterRequest{})

	if err != nil {
		log.Fatal(err)
	}

	clientID := res.ClientID

	var wg sync.WaitGroup

	for i := 0; i < *ops; i++ {
		wg.Add(1)
		go func(i int) {
			_, err := c.Insert(context.Background(), &rkvpb.InsertRequest{
				ClientID:  clientID,
				ClientSeq: uint64(i + 1),
				Key:       fmt.Sprintf("key%d", i),
				Value:     fmt.Sprintf("value%d", i),
			})

			if err != nil {
				log.Fatal(err)
			}

			wg.Done()
		}(i)
	}

	wg.Wait()

	for i := 0; i < *ops; i++ {
		wg.Add(1)
		go func(i int) {
			res, err := c.Lookup(context.Background(), &rkvpb.LookupRequest{
				Key: fmt.Sprintf("key%d", i),
			})

			if err != nil {
				log.Fatal(err)
			}

			expected := fmt.Sprintf("value%d", i)

			if res.Value != expected {
				panic(fmt.Sprintf("got %s wanted %s\n", res.Value, expected))
			}

			wg.Done()
		}(i)
	}

	wg.Wait()

	if err := cc.Close(); err != nil {
		log.Fatal(err)
	}
}
