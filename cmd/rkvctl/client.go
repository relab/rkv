package main

import (
	"context"
	"errors"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/go-kit/kit/metrics"
	"github.com/relab/raft/commonpb"
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

	zipf *z

	s *stats

	payload int
}

// z wraps rand.Zipf with a mutex.
type z struct {
	sync.Mutex
	*rand.Zipf
}

func (zz *z) Uint64() uint64 {
	zz.Lock()
	i := zz.Zipf.Uint64()
	zz.Unlock()
	return i
}

func newClient(leader *uint64, servers []string, zipf *rand.Zipf, s *stats, payload int) (*client, error) {
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
		zipf:    &z{Zipf: zipf},
		s:       s,
		payload: payload,
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
	requestTimeout = 10 * time.Minute
	maxInFlight    = 50000
)

func newBackOff() *backoff.ExponentialBackOff {
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = 1 * time.Millisecond
	b.RandomizationFactor = 0.5
	b.Multiplier = 1.5
	b.MaxInterval = 1 * time.Second
	b.MaxElapsedTime = 15 * time.Minute
	b.Reset()
	return b
}

func notify(err error, d time.Duration) {
	// TODO For debugging.
}

// ErrMaxInFlightReached indicates that there are too many messages in-flight.
var ErrMaxInFlightReached = errors.New("reached max in-flight")

func (c *client) addServer(serverID uint64) (*commonpb.ReconfResponse, error) {
	return c.reconf(serverID, commonpb.ReconfAdd)
}

func (c *client) removeServer(serverID uint64) (*commonpb.ReconfResponse, error) {
	return c.reconf(serverID, commonpb.ReconfRemove)
}

func (c *client) reconf(serverID uint64, reconfType commonpb.ReconfType) (*commonpb.ReconfResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()

	var res *commonpb.ReconfResponse

	op := func() error {
		var err error
		prevLeader := c.getLeader()
		res, err = c.servers[prevLeader].Reconf(ctx, &commonpb.ReconfRequest{
			ServerID:   serverID,
			ReconfType: reconfType,
		})

		if err != nil {
			c.nextLeader(prevLeader)
			return err
		}

		return nil
	}

	err := backoff.RetryNotify(op, backoff.WithContext(newBackOff(), ctx), notify)

	return res, err
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

	op := func() error {
		var err error
		prevLeader := c.getLeader()
		res, err = c.servers[prevLeader].Register(ctx, &rkvpb.RegisterRequest{})

		if err != nil {
			c.nextLeader(prevLeader)
			return err
		}

		c.s.writes.Add(1)
		timer.ObserveDuration()
		return nil

	}

	err := backoff.RetryNotify(op, backoff.WithContext(newBackOff(), ctx), notify)

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

	req := &rkvpb.LookupRequest{
		Key: c.randPayload(),
	}

	op := func() error {
		var err error
		prevLeader := c.getLeader()
		res, err = c.servers[prevLeader].Lookup(ctx, req)

		if err != nil {
			c.nextLeader(prevLeader)
			return err
		}

		c.s.reads.Add(1)
		timer.ObserveDuration()
		return nil
	}

	err := backoff.RetryNotify(op, backoff.WithContext(newBackOff(), ctx), notify)

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

	req := &rkvpb.InsertRequest{
		ClientID:  c.id,
		ClientSeq: atomic.AddUint64(&c.seq, 1),
		Key:       c.randPayload(),
		Value:     c.randPayload(),
	}

	op := func() error {
		var err error
		prevLeader := c.getLeader()
		res, err = c.servers[prevLeader].Insert(ctx, req)

		if err != nil {
			c.nextLeader(prevLeader)
			return err
		}

		c.s.writes.Add(1)
		timer.ObserveDuration()
		return nil
	}

	err := backoff.RetryNotify(op, backoff.WithContext(newBackOff(), ctx), notify)

	return res, err
}

func (c *client) nextLeader(prevLeader uint64) {
	newLeader := (prevLeader + 1) % uint64(len(c.servers))
	atomic.CompareAndSwapUint64(c.l, prevLeader, newLeader)
}

func (c *client) getLeader() uint64 {
	c.currentLeader = atomic.LoadUint64(c.l)
	return c.currentLeader
}

func (c *client) randPayload() string {
	return strconv.FormatUint(c.zipf.Uint64(), 10) + strings.Repeat("X", c.payload)
}
