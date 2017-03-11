package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"

	"github.com/relab/rkv/rkvpb"
)

type controller struct {
	leader int32
	n      int
	conns  []rkvpb.RKVClient
}

func newController(servers []string) (*controller, error) {
	n := len(servers)

	conns := make([]rkvpb.RKVClient, n)

	for i, server := range servers {
		cc, err := grpc.Dial(
			server,
			grpc.WithBlock(),
			grpc.WithInsecure(),
		)

		if err != nil {
			return nil, err
		}

		conns[i] = rkvpb.NewRKVClient(cc)
	}

	return &controller{
		leader: 0,
		n:      n,
		conns:  conns,
	}, nil
}

type request func(leader int32) (interface{}, error)

func (c *controller) do(req request, maxRetry int, i ...int) (interface{}, error) {
	if i == nil {
		i = []int{0}
	}

	for {
		res, err := req(atomic.LoadInt32(&c.leader))

		if err != nil {
			if i[0] < maxRetry {
				serr := grpc.ErrorDesc(err)

				switch {
				case i[0] == 0 && strings.HasPrefix(serr, "not leader"):
					l, _ := strconv.Atoi(serr[len(serr)-4 : len(serr)-3])
					leader := int32(l)
					if leader > 0 {
						atomic.StoreInt32(&c.leader, leader-1)
						break
					}
					fallthrough
				default:
					atomic.StoreInt32(&c.leader, (atomic.LoadInt32(&c.leader)+1)%int32(len(c.conns)))
				}
				<-time.After(2 * time.Second)
				return c.do(req, maxRetry, i[0]+1)
			}

			return nil, err
		}

		return res, nil
	}
}

type zipf struct {
	sync.Mutex
	zf *rand.Zipf
}

func (z *zipf) Uint64() uint64 {
	z.Lock()
	defer z.Unlock()
	return z.zf.Uint64()
}

func main() {
	rand.Seed(time.Now().UnixNano())

	var (
		cluster    = flag.String("cluster", ":9201,:9202,:9203", "comma separated cluster servers")
		rwratio    = flag.Float64("rwratio", 0.2, "read-write ratio")
		clients    = flag.Int("clients", 4, "number of clients")
		throughput = flag.Int("throughput", 500, "requests per second per client")
	)

	flag.Parse()

	servers := strings.Split(*cluster, ",")

	if len(servers) == 0 {
		fmt.Print("-cluster argument is required\n\n")
		flag.Usage()
		os.Exit(1)
	}

	var n uint64 = 10000
	z := &zipf{
		zf: rand.NewZipf(rand.New(rand.NewSource(99)), 1.1, 4, n),
	}

	sleep := time.Second / time.Duration(*throughput)
	var wg sync.WaitGroup
	var startWait sync.WaitGroup
	startWait.Add(1)

	var reqs uint64
	var llock sync.Mutex
	var latency time.Duration

	for i := 0; i < *clients; i++ {
		c, err := newController(servers)

		if err != nil {
			log.Fatal(err)
		}

		res, err := c.do(func(leader int32) (interface{}, error) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			return c.conns[leader].Register(ctx, &rkvpb.RegisterRequest{})
		}, c.n)

		if err != nil {
			log.Fatal(err)
		}

		clientID := res.(*rkvpb.RegisterResponse).ClientID
		var seq uint64

		fmt.Println("ClientID:", clientID)

		wg.Add(1)
		startWait.Add(1)
		go func() {
			startWait.Done()
			startWait.Wait()

			var stop uint64
			for stop == 0 {
				r := rand.Float64()
				if r < *rwratio {
					go func() {
						startRead := time.Now()

						_, err := c.do(func(leader int32) (interface{}, error) {
							ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
							defer cancel()
							return c.conns[leader].Lookup(
								ctx,
								&rkvpb.LookupRequest{
									Key: strconv.FormatUint(z.Uint64(), 10),
								},
							)
						}, 2*c.n)

						if err != nil && stop == 0 {
							log.Println("stopping:", clientID, err)
							atomic.AddUint64(&stop, 1)
							return
						}

						rr := atomic.AddUint64(&reqs, 1)
						r := time.Duration(rr)
						llock.Lock()
						latency -= latency / r
						latency += time.Now().Sub(startRead) / r
						llock.Unlock()
					}()
				} else {
					go func() {
						startRead := time.Now()

						_, err := c.do(func(leader int32) (interface{}, error) {
							ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
							defer cancel()
							return c.conns[leader].Insert(
								ctx,
								&rkvpb.InsertRequest{
									ClientID:  clientID,
									ClientSeq: atomic.AddUint64(&seq, 1),
									Key:       strconv.FormatUint(z.Uint64(), 10),
									Value:     strconv.FormatUint(z.Uint64(), 10),
								},
							)
						}, 2*c.n)

						if err != nil && stop == 0 {
							log.Println("stopping:", clientID, err)
							atomic.AddUint64(&stop, 1)
							return
						}

						rr := atomic.AddUint64(&reqs, 1)
						r := time.Duration(rr)
						llock.Lock()
						latency -= latency / r
						latency += time.Now().Sub(startRead) / r
						llock.Unlock()
					}()
				}
				<-time.After(sleep)
			}

			wg.Done()
		}()
	}

	go func() {
		startWait.Done()
		startWait.Wait()
		start := time.Now()
		for {
			<-time.After(time.Second)
			log.Println("Avg Throughput:", atomic.LoadUint64(&reqs)/uint64(time.Now().Sub(start).Seconds()))
			llock.Lock()
			log.Println("Avg Latency:", latency, float64(latency/(50*time.Millisecond))+1)
			llock.Unlock()
		}
	}()

	wg.Wait()
}
