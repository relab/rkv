package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"math"
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
	hashic "github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"github.com/relab/raft"
	etcd "github.com/relab/raft/etcd"
	hraft "github.com/relab/raft/hashicorp"
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
	serverMetrics     = flag.Bool("servermetrics", true, "enable server-side metrics")
	electionTimeout   = flag.Duration("election", time.Second, "How long servers wait before starting an election")
	heartbeatTimeout  = flag.Duration("heartbeat", 20*time.Millisecond, "How often a heartbeat should be sent")
	entriesPerMsg     = flag.Uint64("entriespermsg", 64, "Entries per Appendentries message")
	catchupMultiplier = flag.Uint64("catchupmultiplier", 1024, "How many more times entries per message allowed during catch up")
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

	logger := logrus.New()

	logFile, err := os.OpenFile(
		fmt.Sprintf("%s%sraft%.2d.log", os.TempDir(), string(filepath.Separator), *id),
		os.O_CREATE|os.O_TRUNC|os.O_APPEND|os.O_WRONLY, 0600,
	)

	if err != nil {
		logger.Fatal(err)
	}

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
	case bhashicorp:
		runhashicorp(logger, lis, grpcServer, *id, ids, nodes)
	}

}

func runhashicorp(
	logger logrus.FieldLogger,
	lis net.Listener, grpcServer *grpc.Server,
	id uint64, ids []uint64, nodes []string,
) {
	var selflis string
	selflis, ids, nodes = selfIDsNodes(logger, id, ids, nodes)

	servers := make([]hashic.Server, len(nodes)+1)
	servers[0] = hashic.Server{
		Suffrage: hashic.Voter,
		ID:       hashic.ServerID(selflis),
		Address:  hashic.ServerAddress(selflis),
	}
	for i, addr := range nodes {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			logger.Fatal(err)
		}
		p, _ := strconv.Atoi(port)
		addr = host + ":" + strconv.Itoa(p-100)

		servers[i+1] = hashic.Server{
			Suffrage: hashic.Voter,
			ID:       hashic.ServerID(addr),
			Address:  hashic.ServerAddress(addr),
		}
	}

	addr, err := net.ResolveTCPAddr("tcp", selflis)
	if err != nil {
		logger.Fatal(err)
	}
	trans, err := hashic.NewTCPTransport(selflis, addr, len(ids), 10*time.Second, os.Stderr)
	if err != nil {
		logger.Fatal(err)
	}

	path := fmt.Sprintf("hashicorp%.2d.bolt", id)
	overwrite := !*recover
	// Check if file already exists.
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			// We don't need to overwrite a file that doesn't exist.
			overwrite = false
		} else {
			// If we are unable to verify the existence of the file,
			// there is probably a permission problem.
			logger.Fatal(err)
		}
	}
	if overwrite {
		if err := os.Remove(path); err != nil {
			logger.Fatal(err)
		}
	}
	logs, err := raftboltdb.NewBoltStore(path)
	if err != nil {
		logger.Fatal(err)
	}

	snaps := hashic.NewInmemSnapshotStore()

	cfg := &hashic.Config{
		LocalID:            hashic.ServerID(selflis),
		ProtocolVersion:    hashic.ProtocolVersionMax,
		HeartbeatTimeout:   *electionTimeout,
		ElectionTimeout:    *electionTimeout,
		CommitTimeout:      *heartbeatTimeout,
		MaxAppendEntries:   int(*entriesPerMsg),
		ShutdownOnRemove:   true,
		TrailingLogs:       math.MaxUint64,
		SnapshotInterval:   120 * time.Hour,
		SnapshotThreshold:  math.MaxUint64,
		LeaderLeaseTimeout: *electionTimeout / 2,
	}

	node := hraft.NewRaft(logger, NewStore(), cfg, servers, trans, logs, logs, snaps)

	service := NewService(node)
	rkvpb.RegisterRKVServer(grpcServer, service)

	if *serverMetrics {
		_, port, err := net.SplitHostPort(selflis)

		if err != nil {
			logger.Fatal(err)
		}

		go func() {
			http.Handle("/metrics", promhttp.Handler())
			logger.Fatal(http.ListenAndServe(":5"+port, nil))
		}()
	}

	logger.Fatal(grpcServer.Serve(lis))
}

func runetcd(
	logger logrus.FieldLogger,
	lis net.Listener, grpcServer *grpc.Server,
	id uint64, ids []uint64, nodes []string,
) {
	var selflis string
	selflis, ids, nodes = selfIDsNodes(logger, id, ids, nodes)

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
			ID:            id,
			ElectionTick:  int(*electionTimeout / *heartbeatTimeout),
			HeartbeatTick: 1,
			Storage:       storage,
			MaxSizePerMsg: *entriesPerMsg,
			// etcdserver says: Never overflow the rafthttp buffer,
			// which is 4096. We keep the same constant.
			MaxInflightMsgs: 4096 / 8,
			Logger:          logger,
		},
		peers,
		*heartbeatTimeout,
	)

	service := NewService(node)
	rkvpb.RegisterRKVServer(grpcServer, service)

	go func() {
		logger.Fatal(grpcServer.Serve(lis))
	}()

	if *serverMetrics {
		_, port, err := net.SplitHostPort(selflis)

		if err != nil {
			logger.Fatal(err)
		}

		go func() {
			http.Handle("/metrics", promhttp.Handler())
			logger.Fatal(http.ListenAndServe(":5"+port, nil))
		}()
	}

	lishttp, err := net.Listen("tcp", selflis)

	if err != nil {
		logger.Fatal(err)
	}

	logger.Fatal(http.Serve(lishttp, node.Handler()))
}

func rungorums(
	logger logrus.FieldLogger,
	lis net.Listener, grpcServer *grpc.Server,
	id uint64, ids []uint64, nodes []string,
) {
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
		logger.Fatal(grpcServer.Serve(lis))
	}()

	if *serverMetrics {
		_, port, err := net.SplitHostPort(nodes[id-1])

		if err != nil {
			logger.Fatal(err)
		}

		go func() {
			http.Handle("/metrics", promhttp.Handler())
			logger.Fatal(http.ListenAndServe(":5"+port, nil))
		}()
	}

	logger.Fatal(node.Run(grpcServer))
}

func selfIDsNodes(logger logrus.FieldLogger, id uint64, ids []uint64, nodes []string) (string, []uint64, []string) {
	host, port, err := net.SplitHostPort(nodes[id-1])
	if err != nil {
		logger.Fatal(err)
	}
	p, _ := strconv.Atoi(port)
	selflis := host + ":" + strconv.Itoa(p-100)

	ids = append(ids[:id-1], ids[id:]...)
	nodes = append(nodes[:id-1], nodes[id:]...)

	return selflis, ids, nodes
}
