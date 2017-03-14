package main

import (
	"context"
	"errors"
	"math/rand"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/go-kit/kit/metrics"
	"github.com/relab/rkv/rkvpb"
	"google.golang.org/grpc"
)

type client struct {
	l             *uint64
	currentLeader uint64
	servers       []rkvpb.RKVClient

	inFlight int64

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

	res, err := c.register()

	if err != nil {
		return nil, err
	}

	c.id = res.ClientID
	c.seq = 1

	return c, nil
}

const (
	retryPerServer = 10
	sleepPerRound  = 250 * time.Millisecond
	requestTimeout = 10 * time.Minute
	maxInFlight    = 15000
)

// ErrMaxInFlightReached indicates that there are too many messages in-flight.
var ErrMaxInFlightReached = errors.New("reached max in-flight")

func sleep(round int) {
	dur := sleepPerRound * time.Duration(round)
	time.Sleep(dur)
}

func (c *client) register() (*rkvpb.RegisterResponse, error) {
	if atomic.LoadInt64(&c.inFlight) > maxInFlight {
		return nil, ErrMaxInFlightReached
	}
	atomic.AddInt64(&c.inFlight, 1)
	defer func() {
		atomic.AddInt64(&c.inFlight, -1)
	}()

	c.s.writeReqs.Add(1)
	timer := metrics.NewTimer(c.s.writeLatency)

	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()

	var res *rkvpb.RegisterResponse
	var err error

	for i := 0; i < retryPerServer*len(c.servers); i++ {
		if i%len(c.servers) == 0 {
			sleep(i / len(c.servers))
		}

		res, err = c.leader().Register(ctx, &rkvpb.RegisterRequest{})

		if err != nil {
			c.nextLeader()
			continue
		}

		c.s.writes.Add(1)
		timer.ObserveDuration()
		break
	}

	return res, err
}

func (c *client) lookup() (*rkvpb.LookupResponse, error) {
	if atomic.LoadInt64(&c.inFlight) > maxInFlight {
		return nil, ErrMaxInFlightReached
	}
	atomic.AddInt64(&c.inFlight, 1)
	defer func() {
		atomic.AddInt64(&c.inFlight, -1)
	}()

	c.s.readReqs.Add(1)
	timer := metrics.NewTimer(c.s.readLatency)

	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()

	var res *rkvpb.LookupResponse
	var err error

	for i := 0; i < retryPerServer*len(c.servers); i++ {
		if i%len(c.servers) == 0 {
			sleep(i / len(c.servers))
		}

		res, err = c.leader().Lookup(ctx, &rkvpb.LookupRequest{
			Key: strconv.FormatUint(c.zipf.Uint64(), 10),
		})

		if err != nil {
			c.nextLeader()
			continue
		}

		c.s.reads.Add(1)
		timer.ObserveDuration()
		break

	}

	return res, err
}

func (c *client) insert() (*rkvpb.InsertResponse, error) {
	if atomic.LoadInt64(&c.inFlight) > maxInFlight {
		return nil, ErrMaxInFlightReached
	}
	atomic.AddInt64(&c.inFlight, 1)
	defer func() {
		atomic.AddInt64(&c.inFlight, -1)
	}()

	c.s.writeReqs.Add(1)
	timer := metrics.NewTimer(c.s.writeLatency)

	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()

	var res *rkvpb.InsertResponse
	var err error

	for i := 0; i < retryPerServer*len(c.servers); i++ {
		if i%len(c.servers) == 0 {
			sleep(i / len(c.servers))
		}

		res, err = c.leader().Insert(ctx, &rkvpb.InsertRequest{
			ClientID:  c.id,
			ClientSeq: atomic.AddUint64(&c.seq, 1),
			Key:       strconv.FormatUint(c.zipf.Uint64(), 10),
			Value:     strconv.FormatUint(c.zipf.Uint64(), 10),
		})

		if err != nil {
			c.nextLeader()
			continue
		}

		c.s.writes.Add(1)
		timer.ObserveDuration()
		break
	}

	return res, err
}

func (c *client) leader() rkvpb.RKVClient {
	return c.servers[c.getLeader()]
}

func (c *client) nextLeader() {
	newLeader := (c.currentLeader + 1) % uint64(len(c.servers))
	atomic.CompareAndSwapUint64(c.l, c.currentLeader, newLeader)
}

func (c *client) getLeader() uint64 {
	c.currentLeader = atomic.LoadUint64(c.l)
	return c.currentLeader
}
