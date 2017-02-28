package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"strings"
	"time"

	"github.com/relab/raft/raftgorums"
	"github.com/relab/rkv/rkvpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
)

func main() {
	var (
		id               = flag.Uint64("id", 0, "server ID")
		cluster          = flag.String("cluster", ":9201", "comma separated cluster servers")
		bench            = flag.Bool("quiet", false, "Silence log output")
		recover          = flag.Bool("recover", false, "Recover from stable storage")
		batch            = flag.Bool("batch", true, "enable batching")
		electionTimeout  = flag.Duration("election", 2*time.Second, "How long servers wait before starting an election")
		heartbeatTimeout = flag.Duration("heartbeat", 250*time.Millisecond, "How often a heartbeat should be sent")
		maxAppendEntries = flag.Uint64("maxappend", 5000, "Max entries per AppendEntries message")
	)

	flag.Parse()
	rand.Seed(time.Now().UnixNano())

	if *id == 0 {
		fmt.Print("-id argument is required\n\n")
		flag.Usage()
		os.Exit(1)
	}

	nodes := strings.Split(*cluster, ",")

	if len(nodes) == 0 {
		fmt.Print("-cluster argument is required\n\n")
		flag.Usage()
		os.Exit(1)
	}

	if *maxAppendEntries < 1 {
		fmt.Print("-maxappend must be atleast 1\n\n")
		flag.Usage()
		os.Exit(1)
	}

	if *bench {
		log.SetOutput(ioutil.Discard)
		silentLogger := log.New(ioutil.Discard, "", log.LstdFlags)
		grpclog.SetLogger(silentLogger)
		grpc.EnableTracing = false
	}

	storage, err := raftgorums.NewFileStorage(fmt.Sprintf("db%.2d.bolt", *id), !*recover)

	if err != nil {
		log.Fatal(err)
	}

	lis, err := net.Listen("tcp", nodes[*id-1])

	if err != nil {
		log.Fatal(err)
	}

	grpcServer := grpc.NewServer()

	node := raftgorums.NewNode(grpcServer, NewStore(), &raftgorums.Config{
		ID:               *id,
		Nodes:            nodes,
		Batch:            *batch,
		Storage:          storage,
		ElectionTimeout:  *electionTimeout,
		HeartbeatTimeout: *heartbeatTimeout,
		MaxAppendEntries: *maxAppendEntries,
		Logger:           log.New(os.Stderr, "raft| ", log.LstdFlags),
	})

	service := NewService(node.Raft)
	rkvpb.RegisterRKVServer(grpcServer, service)

	go func() {
		log.Fatal(grpcServer.Serve(lis))
	}()

	log.Fatal(node.Run())
}
