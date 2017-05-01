package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	etcdraft "github.com/coreos/etcd/raft"
	"github.com/relab/raft"
	etcd "github.com/relab/raft/etcd"
	"github.com/relab/raft/raftgorums"
	"github.com/relab/rkv/rkvpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
)

const (
	bgorums    = "gorums"
	betcd      = "etcd"
	bhashicorp = "hashicorp"
)

var (
	bench             = flag.Bool("quiet", false, "Silence log output")
	recover           = flag.Bool("recover", false, "Recover from stable storage")
	batch             = flag.Bool("batch", true, "enable batching")
	electionTimeout   = flag.Duration("election", time.Second, "How long servers wait before starting an election")
	heartbeatTimeout  = flag.Duration("heartbeat", 20*time.Millisecond, "How often a heartbeat should be sent")
	entriesPerMsg     = flag.Uint64("entriespermsg", 64, "Entries per Appendentries message")
	catchupMultiplier = flag.Uint64("catchupmultiplier", 160, "How many more times entries per message allowed during catch up")
)

func main() {
	var (
		id      = flag.Uint64("id", 0, "server ID")
		servers = flag.String("servers", ":9201,:9202,:9203,:9204,:9205,:9206,:9207", "comma separated list of server addresses")
		cluster = flag.String("cluster", "1,2,3", "comma separated list of server ids to form cluster with, [1 >= id <= len(servers)]")
		backend = flag.String("backend", "gorums", "Raft backend to use [gorums|etcd|hashicorp]")
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

	lis, err := net.Listen("tcp", nodes[*id-1])

	if err != nil {
		logger.Fatal(err)
	}

	grpcServer := grpc.NewServer()

	switch *backend {
	case bgorums:
		rungorums(logger, lis, grpcServer, *id, ids, nodes)
	case betcd:
		runetcd(logger, lis, grpcServer, *id, ids, nodes)
	}

}

func runetcd(logger logrus.FieldLogger, lis net.Listener, grpcServer *grpc.Server, id uint64, ids []uint64, nodes []string) {
	host, port, err := net.SplitHostPort(nodes[id-1])
	if err != nil {
		logger.Fatal(err)
	}
	p, _ := strconv.Atoi(port)
	selflis := host + ":" + strconv.Itoa(p-100)

	ids = append(ids[:id-1], ids[id:]...)
	nodes = append(nodes[:id-1], nodes[id:]...)

	peers := make([]etcdraft.Peer, len(nodes))

	for i, addr := range nodes {
		nid := ids[i]

		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			logger.Fatal(err)
		}
		p, _ := strconv.Atoi(port)

		ur, err := url.Parse("http://" + addr)
		ur.Host = host + ":" + strconv.Itoa(p-100)
		if err != nil {
			logger.Fatal(err)
		}

		peers[i] = etcdraft.Peer{
			ID:      nid,
			Context: []byte(ur.String()),
		}
	}

	storage := etcdraft.NewMemoryStorage()
	node := etcd.NewRaft(
		logger,
		NewStore(),
		storage,
		&etcdraft.Config{
			ID:              id,
			ElectionTick:    int(*electionTimeout / *heartbeatTimeout),
			HeartbeatTick:   1,
			Storage:         storage,
			MaxSizePerMsg:   *entriesPerMsg,
			MaxInflightMsgs: 256,
			Logger:          logger,
		},
		peers,
		*heartbeatTimeout,
	)

	service := NewService(node)
	rkvpb.RegisterRKVServer(grpcServer, service)

	go func() {
		logrus.Fatal(grpcServer.Serve(lis))
	}()

	lishttp, err := net.Listen("tcp", selflis)

	if err != nil {
		logger.Fatal(err)
	}

	logger.Fatal(http.Serve(lishttp, node.Handler()))
}

func rungorums(logger logrus.FieldLogger, lis net.Listener, grpcServer *grpc.Server, id uint64, ids []uint64, nodes []string) {
	storage, err := raft.NewFileStorage(fmt.Sprintf("db%.2d.bolt", id), !*recover)

	if err != nil {
		logger.Fatal(err)
	}

	storageWithCache := raft.NewCacheStorage(storage, 20000)

	node := raftgorums.NewRaft(NewStore(), &raftgorums.Config{
		ID:                id,
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

	service := NewService(node)
	rkvpb.RegisterRKVServer(grpcServer, service)

	go func() {
		logrus.Fatal(grpcServer.Serve(lis))
	}()

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		logrus.Fatal(http.ListenAndServe(":5"+nodes[id-1][1:], nil))
	}()

	logger.Fatal(node.Run(grpcServer))
}
