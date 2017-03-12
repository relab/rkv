package main

import (
	"flag"
	"math/rand"
	"net/http"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Arbitrary seed.
const seedStart int64 = 99

func main() {
	var (
		cluster = flag.String("cluster", ":9201,:9202,:9203", "comma separated cluster servers")
		clients = flag.Int("clients", 5, "number of clients")

		reads      = flag.Float64("reads", 0, "percentage of requests which should be reads")
		throughput = flag.Int("throughput", 10, "requests per second per client")

		keyspace = flag.Uint64("keyspace", 10000, "number of keys to touch")
		zipfs    = flag.Float64("zipfs", 1.1, "zipf s parameter")
		zipfv    = flag.Float64("zipfv", 4, "zipf v parameter")
	)
	flag.Parse()

	var leader uint64
	servers := strings.Split(*cluster, ",")

	for i := 0; i < *clients; i++ {
		rndsrc := rand.New(rand.NewSource(seedStart + int64(i)))
		zipf := rand.NewZipf(rndsrc, *zipfs, *zipfv, *keyspace)
		c, err := newClient(&leader, servers, zipf, s)

		if err != nil {
			logrus.WithError(err).Panicln("Failed to create client")
		}

		go runClient(c, *throughput, *reads)
	}

	http.Handle("/metrics", promhttp.Handler())
	logrus.Fatal(http.ListenAndServe(":59100", nil))
}

func runClient(c *client, throughput int, reads float64) {
	sleep := time.Second / time.Duration(throughput)

	for {
		r := rand.Float64()

		if r < reads {
			go c.lookup()
		} else {
			go c.insert()
		}
		time.Sleep(sleep)
	}
}
