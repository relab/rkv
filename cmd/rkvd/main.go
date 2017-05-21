package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	etcdraft "github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/wal"
	"github.com/coreos/etcd/wal/walpb"
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
	batch             = flag.Bool("batch", true, "Enable batching")
	serverMetrics     = flag.Bool("servermetrics", true, "Enable server-side metrics")
	electionTimeout   = flag.Duration("election", time.Second, "How long servers wait before starting an election")
	heartbeatTimeout  = flag.Duration("heartbeat", 20*time.Millisecond, "How often a heartbeat should be sent")
	entriesPerMsg     = flag.Uint64("entriespermsg", 64, "Entries per Appendentries message")
	catchupMultiplier = flag.Uint64("catchupmultiplier", 1024, "How many more times entries per message allowed during catch up")
	cache             = flag.Int("cache", 1024*1024*64, "How many entries should be kept in memory") // ~1GB @ 16bytes per entry.
	maxgrpc           = flag.Int("maxgrpc", 128<<20, "Max GRPC message size")                        // ~128MB.
	checkQuorum       = flag.Bool("checkquorum", false, "Require a quorum of responses to a heartbeat to retain leadership")
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

	grpcServer := grpc.NewServer(grpc.MaxMsgSize(*maxgrpc))

	if *serverMetrics {
		go func() {
			http.Handle("/metrics", promhttp.Handler())
			logger.Fatal(http.ListenAndServe(fmt.Sprintf(":590%d", *id), nil))
		}()
	}

	lat := raft.NewLatency()
	event := raft.NewEvent()

	var once sync.Once
	writeData := func() {
		lat.Write(fmt.Sprintf("./latency-%v.csv", time.Now().UnixNano()))
		event.Write(fmt.Sprintf("./event-%v.csv", time.Now().UnixNano()))
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		event.Record(raft.EventTerminated)
		once.Do(writeData)
		os.Exit(1)
	}()
	defer func() {
		once.Do(writeData)
	}()

	switch *backend {
	case bgorums:
		rungorums(logger, lis, grpcServer, *id, ids, nodes, lat, event)
	case betcd:
		runetcd(logger, lis, grpcServer, *id, ids, nodes, lat, event)
	case bhashicorp:
		runhashicorp(logger, lis, grpcServer, *id, ids, nodes, lat, event)
	}

}

func runhashicorp(
	logger logrus.FieldLogger,
	lis net.Listener, grpcServer *grpc.Server,
	id uint64, ids []uint64, nodes []string,
	lat *raft.Latency, event *raft.Event,
) {
	servers := make([]hashic.Server, len(nodes))
	for i, addr := range nodes {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			logger.Fatal(err)
		}
		p, _ := strconv.Atoi(port)
		addr = host + ":" + strconv.Itoa(p-100)

		suffrage := hashic.Voter

		if !contains(uint64(i+1), ids) {
			suffrage = hashic.Nonvoter
		}

		servers[i] = hashic.Server{
			Suffrage: suffrage,
			ID:       hashic.ServerID(addr),
			Address:  hashic.ServerAddress(addr),
		}
	}

	addr, err := net.ResolveTCPAddr("tcp", string(servers[id-1].Address))
	if err != nil {
		logger.Fatal(err)
	}
	trans, err := hashic.NewTCPTransport(string(servers[id-1].Address), addr, len(nodes)+1, 10*time.Second, os.Stderr)
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
		LocalID:            servers[id-1].ID,
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

	leaderOut := make(chan struct{})

	node := hraft.NewRaft(logger, NewStore(), cfg, servers, trans, logs, logs, snaps, ids, lat, event, leaderOut)

	service := NewService(logger, node, leaderOut)
	rkvpb.RegisterRKVServer(grpcServer, service)

	logger.Fatal(grpcServer.Serve(lis))
}

func runetcd(
	logger logrus.FieldLogger,
	lis net.Listener, grpcServer *grpc.Server,
	id uint64, ids []uint64, nodes []string,
	lat *raft.Latency, event *raft.Event,
) {
	peers := make([]etcdraft.Peer, len(ids))

	for i, nid := range ids {
		addr := nodes[i]
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

	dir := fmt.Sprintf("etcdwal%.2d", id)

	switch {
	case wal.Exist(dir) && !*recover:
		if err := os.RemoveAll(dir); err != nil {
			logger.Fatal(err)
		}
		fallthrough
	case !wal.Exist(dir):
		if err := os.Mkdir(dir, 0750); err != nil {
			logger.Fatalf("rkvd: cannot create dir for wal (%v)", err)
		}

		w, err := wal.Create(dir, nil)
		if err != nil {
			logger.Fatalf("rkvd: create wal error (%v)", err)
		}
		w.Close()
	}

	walsnap := walpb.Snapshot{}
	w, err := wal.Open(dir, walsnap)
	if err != nil {
		logger.Fatalf("rkvd: error loading wal (%v)", err)
	}

	_, st, ents, err := w.ReadAll()
	if err != nil {
		log.Fatalf("rkvd: failed to read WAL (%v)", err)
	}

	storage := etcdraft.NewMemoryStorage()
	storage.SetHardState(st)
	storage.Append(ents)

	leaderOut := make(chan struct{})

	node := etcd.NewRaft(
		logger,
		NewStore(),
		storage,
		w,
		&etcdraft.Config{
			ID:            id,
			ElectionTick:  int(*electionTimeout / *heartbeatTimeout),
			HeartbeatTick: 1,
			Storage:       storage,
			MaxSizePerMsg: *entriesPerMsg,
			// etcdserver says: Never overflow the rafthttp buffer,
			// which is 4096. We keep the same constant.
			MaxInflightMsgs: 4096 / 8,
			CheckQuorum:     *checkQuorum,
			PreVote:         true,
			Logger:          logger,
		},
		peers,
		*heartbeatTimeout,
		!contains(id, ids),
		nodes,
		lat, event,
		leaderOut,
	)

	service := NewService(logger, node, leaderOut)
	rkvpb.RegisterRKVServer(grpcServer, service)

	go func() {
		logger.Fatal(grpcServer.Serve(lis))
	}()

	host, port, err := net.SplitHostPort(nodes[id-1])
	if err != nil {
		logger.Fatal(err)
	}
	p, _ := strconv.Atoi(port)
	selflis := host + ":" + strconv.Itoa(p-100)

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
	lat *raft.Latency, event *raft.Event,
) {
	storage, err := raft.NewFileStorage(fmt.Sprintf("db%.2d.bolt", id), !*recover)

	if err != nil {
		logger.Fatal(err)
	}

	storageWithCache := raft.NewCacheStorage(storage, *cache)

	leaderOut := make(chan struct{})

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
		CheckQuorum:       *checkQuorum,
		MetricsEnabled:    true,
	}, lat, event, leaderOut)

	service := NewService(logger, node, leaderOut)
	rkvpb.RegisterRKVServer(grpcServer, service)

	go func() {
		logger.Fatal(grpcServer.Serve(lis))
	}()

	logger.Fatal(node.Run(grpcServer))
}

func contains(x uint64, xs []uint64) bool {
	for _, y := range xs {
		if y == x {
			return true
		}
	}
	return false
}
