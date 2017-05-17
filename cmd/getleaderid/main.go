package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/relab/rkv/rkvpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
)

func main() {
	var (
		cluster = flag.String("cluster", ":9201,:9202,:9203", "comma separated cluster servers")
	)
	flag.Parse()

	discard := log.New(ioutil.Discard, "", 0)
	grpclog.SetLogger(discard)

	servers := strings.Split(*cluster, ",")

	var wg sync.WaitGroup
	wg.Add(len(servers))
	done := make(chan struct{})
	result := make(chan int)

	go func() {
		wg.Wait()
		done <- struct{}{}
	}()

	for i, server := range servers {
		go func(id int, server string) {
			defer wg.Done()

			cc, err := grpc.Dial(server,
				grpc.WithInsecure(),
				grpc.WithBlock(),
				grpc.WithTimeout(100*time.Millisecond),
			)

			if err != nil {
				return
			}

			client := rkvpb.NewRKVClient(cc)

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			_, err = client.Register(ctx, &rkvpb.RegisterRequest{})
			cancel()

			if err != nil {
				return
			}

			result <- id
		}(i+1, server)
	}

	select {
	case id := <-result:
		fmt.Println(id)
	case <-done:
		fmt.Println("no leader")
	}
}
