package main

import (
	"time"

	"github.com/go-kit/kit/metrics"
	"github.com/go-kit/kit/metrics/prometheus"
	promc "github.com/prometheus/client_golang/prometheus"
)

type stats struct {
	readLatency  metrics.Histogram
	writeLatency metrics.Histogram
	readReqs     metrics.Counter
	writeReqs    metrics.Counter
	reads        metrics.Counter
	writes       metrics.Counter
}

var s = &stats{
	readLatency: prometheus.NewSummaryFrom(promc.SummaryOpts{
		Namespace: "raft",
		Subsystem: "client",
		Name:      "read_latency",
		Help:      "Read request response time.",
		MaxAge:    5 * time.Second,
	}, []string{}),
	writeLatency: prometheus.NewSummaryFrom(promc.SummaryOpts{
		Namespace: "raft",
		Subsystem: "client",
		Name:      "write_latency",
		Help:      "Write request response time.",
		MaxAge:    5 * time.Second,
	}, []string{}),
	readReqs: prometheus.NewCounterFrom(promc.CounterOpts{
		Namespace: "raft",
		Subsystem: "client",
		Name:      "read_requests",
		Help:      "Read requests to server.",
	}, []string{}),
	writeReqs: prometheus.NewCounterFrom(promc.CounterOpts{
		Namespace: "raft",
		Subsystem: "client",
		Name:      "write_requests",
		Help:      "Write requests to server.",
	}, []string{}),
	reads: prometheus.NewCounterFrom(promc.CounterOpts{
		Namespace: "raft",
		Subsystem: "client",
		Name:      "reads",
		Help:      "Reads processed.",
	}, []string{}),
	writes: prometheus.NewCounterFrom(promc.CounterOpts{
		Namespace: "raft",
		Subsystem: "client",
		Name:      "writes",
		Help:      "Writes processed.",
	}, []string{}),
}
