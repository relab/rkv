package main

import (
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/relab/raft/commonpb"
)

// Arbitrary seed.
const seedStart int64 = 99

func main() {
	var (
		cluster = flag.String("cluster", ":9201,:9202,:9203", "comma separated cluster servers")
		clients = flag.Int("clients", 1, "number of clients")

		add    = flag.Uint64("add", 0, "server to attempt to add to cluster")
		remove = flag.Uint64("remove", 0, "server to attempt to remove from cluster")

		reads      = flag.Float64("reads", 0, "percentage of requests which should be reads")
		throughput = flag.Int("throughput", 1, "send [throughput] requests per second per client")

		forever = flag.Bool("forever", true, "send requests forever")
		dur     = flag.Duration("time", 0, "send requests for [time] duration, overrides forever")
		count   = flag.Uint64("count", 0, "send [count] requests, overrides time and forever")

		keyspace = flag.Uint64("keyspace", 10000, "number of keys to touch")
		zipfs    = flag.Float64("zipfs", 1.1, "zipf s parameter")
		zipfv    = flag.Float64("zipfv", 4, "zipf v parameter")

		payload = flag.Int("payload", 16, "payload in bytes")
	)
	flag.Parse()

	if *payload < 16 {
		panic("payload must be at least 16")
	}

	if *add > 0 && *remove > 0 {
		panic("can only do one reconfiguration at the time")
	}

	if *dur > 0 || *count > 0 {
		*forever = false
	}

	if *dur > 0 && *count > 0 {
		*dur = 0
	}

	var leader uint64
	servers := strings.Split(*cluster, ",")

	var wg sync.WaitGroup
	var wclients sync.WaitGroup

	lat := newLatency()

	var once sync.Once
	writeLatencies := func() { lat.write(fmt.Sprintf("./latency-%v.csv", time.Now().UnixNano())) }

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		once.Do(writeLatencies)
		os.Exit(1)
	}()
	defer func() {
		once.Do(writeLatencies)
	}()

	for i := 0; i < *clients; i++ {
		rndsrc := rand.New(rand.NewSource(seedStart + int64(i)))
		zipf := rand.NewZipf(rndsrc, *zipfs, *zipfv, *keyspace)
		wclients.Add(1)
		go func() {
			defer wclients.Done()

			c, err := newClient(&leader, servers, zipf, s, (*payload-16)/2, lat)

			if err != nil {
				logrus.WithError(err).Panicln("Failed to create client")
			}

			switch {
			case *add > 0:
				addServer(c, *add)
				*forever = false
				return
			case *remove > 0:
				removeServer(c, *remove)
				*forever = false
				return
			default:
				wg.Add(1)
				go runClient(c, &wg, *throughput, *reads, *forever, *dur, *count)
			}
		}()
	}

	wclients.Wait()

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		logrus.Fatal(http.ListenAndServe(":59100", nil))
	}()

	wg.Wait()
}

func addServer(c *client, serverID uint64) {
	res, err := c.addServer(serverID)

	logger := logrus.WithError(err)

	switch res.Status {
	case commonpb.ReconfOK:
		logger.Println("Successfully added server")
	case commonpb.ReconfNotLeader:
		logger.Warnln("Not leader")
	case commonpb.ReconfTimeout:
		logger.Warnln("Either adding server took too long, or the proposed configuration is invalid")
	default:
		logger.Panicln("invalid reconf status")
	}
}

func removeServer(c *client, serverID uint64) {
	res, err := c.removeServer(serverID)

	logger := logrus.WithError(err)

	switch res.Status {
	case commonpb.ReconfOK:
		logger.Println("Successfully removed server")
	case commonpb.ReconfNotLeader:
		logger.Warnln("Not leader")
	case commonpb.ReconfTimeout:
		logger.Warnln("Either removing server took too long, or the proposed configuration is invalid")
	default:
		logger.Panicln("invalid reconf status")
	}
}

func runClient(c *client, wg *sync.WaitGroup, throughput int, reads float64, forever bool, dur time.Duration, count uint64) {
	defer wg.Done()

	sleep := time.Second / time.Duration(throughput)

	loop := func() {
		r := rand.Float64()

		if r < reads {
			go c.lookup()
		} else {
			go c.insert()
		}
		time.Sleep(sleep)
	}

	switch {
	case forever:
		for {
			loop()
		}
	case count > 0:
		for i := uint64(0); i < count; i++ {
			loop()
		}
	default:
		timeout := time.After(dur)

		for {
			select {
			case <-timeout:
				return
			default:
				loop()
			}
		}
	}
}
