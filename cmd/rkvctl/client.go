package main

import (
	"context"
	"math/rand"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/go-kit/kit/metrics"
	"github.com/relab/rkv/rkvpb"
	"google.golang.org/grpc"
)

type client struct {
	l             *uint64
	currentLeader uint64
	servers       []rkvpb.RKVClient

	id  uint64
	seq uint64

	zipf *rand.Zipf

	s *stats
}

func newClient(leader *uint64, servers []string, zipf *rand.Zipf, s *stats) (*client, error) {
	conns := make([]rkvpb.RKVClient, len(servers))

	for i, server := range servers {
		cc, err := grpc.Dial(server, grpc.WithBlock(), grpc.WithInsecure())

		if err != nil {
			return nil, err
		}

		conns[i] = rkvpb.NewRKVClient(cc)
	}

	c := &client{
		l:       leader,
		servers: conns,
		zipf:    zipf,
		s:       s,
	}

	var res *rkvpb.RegisterResponse
	for i := 0; i < len(servers); i++ {
		var err error
		res, err = c.register()
		if err != nil {
			if i+1 == len(servers) {
				return nil, err
			}
			continue
		}
		break
	}

	c.id = res.ClientID
	c.seq = 1

	return c, nil
}

func (c *client) register() (*rkvpb.RegisterResponse, error) {
	c.s.writeReqs.Add(1)
	timer := metrics.NewTimer(c.s.writeLatency)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	res, err := c.leader().Register(ctx, &rkvpb.RegisterRequest{})

	if err != nil {
		c.nextLeader()
		c.s.writes.Add(1)
		timer.ObserveDuration()
	}

	return res, err
}

func (c *client) lookup() (*rkvpb.LookupResponse, error) {
	c.s.readReqs.Add(1)
	timer := metrics.NewTimer(c.s.readLatency)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	res, err := c.leader().Lookup(ctx, &rkvpb.LookupRequest{
		Key: strconv.FormatUint(c.zipf.Uint64(), 10),
	})

	if err != nil {
		c.nextLeader()
		c.s.reads.Add(1)
		timer.ObserveDuration()
	}

	return res, err
}

func (c *client) insert() (*rkvpb.InsertResponse, error) {
	c.s.writeReqs.Add(1)
	timer := metrics.NewTimer(c.s.writeLatency)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	res, err := c.leader().Insert(ctx, &rkvpb.InsertRequest{
		ClientID:  c.id,
		ClientSeq: atomic.AddUint64(&c.seq, 1),
		Key:       strconv.FormatUint(c.zipf.Uint64(), 10),
		Value:     strconv.FormatUint(c.zipf.Uint64(), 10),
	})

	if err != nil {
		c.nextLeader()
		c.s.writes.Add(1)
		timer.ObserveDuration()
	}

	return res, err
}

func (c *client) leader() rkvpb.RKVClient {
	return c.servers[c.getLeader()]
}

func (c *client) nextLeader() {
	newLeader := (c.currentLeader + 1) % uint64(len(c.servers))
	ok := atomic.CompareAndSwapUint64(c.l, c.currentLeader, newLeader)
	if !ok {
		return
	}
	logrus.WithField("leader", newLeader+1).Infoln("Changed leader")
}

func (c *client) getLeader() uint64 {
	c.currentLeader = atomic.LoadUint64(c.l)
	return c.currentLeader
}
