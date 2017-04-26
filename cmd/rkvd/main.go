package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/relab/raft"
	"github.com/relab/raft/raftgorums"
	"github.com/relab/rkv/rkvpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
)

func main() {
	var (
		id                = flag.Uint64("id", 0, "server ID")
		servers           = flag.String("servers", ":9201,:9202,:9203,:9204,:9205,:9206,:9207", "comma separated list of server addresses")
		cluster           = flag.String("cluster", "1,2,3", "comma separated list of server ids to form cluster with, [1 >= id <= len(servers)]")
		bench             = flag.Bool("quiet", false, "Silence log output")
		recover           = flag.Bool("recover", false, "Recover from stable storage")
		batch             = flag.Bool("batch", true, "enable batching")
		electionTimeout   = flag.Duration("election", time.Second, "How long servers wait before starting an election")
		heartbeatTimeout  = flag.Duration("heartbeat", 20*time.Millisecond, "How often a heartbeat should be sent")
		entriesPerMsg     = flag.Uint64("entriespermsg", 64, "Entries per Appendentries message")
		catchupMultiplier = flag.Uint64("catchupmultiplier", 160, "How many more times entries per message allowed during catch up")
	)

	flag.Parse()
	rand.Seed(time.Now().UnixNano())

	if *id == 0 {
		fmt.Print("-id argument is required\n\n")
		flag.Usage()
		os.Exit(1)
	}

	nodes := strings.Split(*servers, ",")

	if len(nodes) == 0 {
		fmt.Print("-server argument is required\n\n")
		flag.Usage()
		os.Exit(1)
	}

	selected := strings.Split(*cluster, ",")

	var ids []uint64

	for _, sid := range selected {
		id, err := strconv.ParseUint(sid, 10, 64)

		if err != nil {
			fmt.Print("could not parse -cluster argument\n\n")
			flag.Usage()
			os.Exit(1)
		}

		if id <= 0 || id > uint64(len(nodes)) {
			fmt.Print("invalid -cluster argument\n\n")
			flag.Usage()
			os.Exit(1)
		}

		ids = append(ids, id)
	}

	if len(ids) == 0 {
		fmt.Print("-cluster argument is required\n\n")
		flag.Usage()
		os.Exit(1)
	}

	if len(ids) > len(nodes) {
		fmt.Print("-cluster specifies too many servers\n\n")
		flag.Usage()
		os.Exit(1)
	}

	if *entriesPerMsg < 1 {
		fmt.Print("-entriespermsg must be atleast 1\n\n")
		flag.Usage()
		os.Exit(1)
	}

	if *catchupMultiplier < 1 {
		fmt.Print("-catchupmultiplier must be atleast 1\n\n")
		flag.Usage()
		os.Exit(1)
	}

	logFile, err := os.OpenFile(
		fmt.Sprintf("%s%sraft%.2d.log", os.TempDir(), string(filepath.Separator), *id),
		os.O_CREATE|os.O_TRUNC|os.O_APPEND|os.O_WRONLY, 0666,
	)

	if err != nil {
		logrus.Fatal(err)
	}

	logger := logrus.New()
	logger.Hooks.Add(NewLogToFileHook(logFile))

	if *bench {
		logger.Out = ioutil.Discard
		grpc.EnableTracing = false
	}

	grpclog.SetLogger(logger)

	storage, err := raft.NewFileStorage(fmt.Sprintf("db%.2d.bolt", *id), !*recover)
	storageWithCache := raft.NewCacheStorage(storage, 20000)

	if err != nil {
		logrus.Fatal(err)
	}

	lis, err := net.Listen("tcp", nodes[*id-1])

	if err != nil {
		logrus.Fatal(err)
	}

	grpcServer := grpc.NewServer()

	raft := raftgorums.NewRaft(NewStore(), &raftgorums.Config{
		ID:                *id,
		Servers:           nodes,
		InitialCluster:    ids,
		Batch:             *batch,
		Storage:           storageWithCache,
		ElectionTimeout:   *electionTimeout,
		HeartbeatTimeout:  *heartbeatTimeout,
		EntriesPerMsg:     *entriesPerMsg,
		CatchupMultiplier: *catchupMultiplier,
		Logger:            logger,
		MetricsEnabled:    true,
	})

	service := NewService(raft)
	rkvpb.RegisterRKVServer(grpcServer, service)

	go func() {
		logrus.Fatal(grpcServer.Serve(lis))
	}()

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		logrus.Fatal(http.ListenAndServe(":5"+nodes[*id-1][1:], nil))
	}()

	logrus.Fatal(raft.Run(grpcServer))
}
