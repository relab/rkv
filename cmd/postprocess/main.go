package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/logrusorgru/aurora"
	"github.com/montanaflynn/stats"
)

const countRange = 0.5

const (
	first = iota
	xThroughputYLatency
	xTimeYThroughput
	eventsFromStart
	last
)

func main() {
	if len(os.Args) < 2 {
		errexit("process not specified")
	}

	process, err := strconv.Atoi(os.Args[1])
	if err != nil || !(process > first && process < last) {
		errexit("process must be " + strconv.Itoa(first) + " > x < " + strconv.Itoa(last))
	}

	switch process {
	case xThroughputYLatency:
		if len(os.Args) < 3 {
			errexit("no input files")
		}

		files := os.Args[2:]
		xThroughputYLatencyFunc(files)
	case xTimeYThroughput:
		if len(os.Args) < 4 {
			errexit("no input files")
		}

		files := os.Args[3:]
		t := os.Args[2]
		rstart, err := strconv.ParseInt(t, 10, 64)
		if err != nil {
			errexit("invalid start time")
		}
		start := time.Unix(0, rstart)

		xTimeYThroughputFunc(start, files)
	case eventsFromStart:
		if len(os.Args) != 4 {
			errexit("no input file")
		}

		file := os.Args[3]
		t := os.Args[2]
		rstart, err := strconv.ParseInt(t, 10, 64)
		if err != nil {
			errexit("invalid start time")
		}
		start := time.Unix(0, rstart)
		eventsFromStartFunc(start, file)
	}
}

func eventsFromStartFunc(start time.Time, file string) {
	events, ts := readEvents(file)

	fmt.Printf(
		"Filename: %s\nStart: %d\n%s\t%s\n",
		file,
		start.UnixNano(),
		"event",
		"time s",
	)

	for i, event := range events {
		fmt.Printf("%s\t%f\n", event, ts[i].Sub(start).Seconds())
	}
}

func xTimeYThroughputFunc(start time.Time, files []string) {
	for i, filename := range files {
		msg := "Reading files from experiment"
		fmt.Fprintf(os.Stderr, aurora.Magenta("%s %s into memory\n").String(), msg, filename)

		allstarts, allends, _ := read([]string{filename})
		starts, ends := allstarts[0], allends[0]

		fmt.Printf(
			"Filename: %s\nStart: %d\n%s\t%s\n",
			filename,
			starts[0].UnixNano(),
			"second",
			"throughput",
		)

		ts := throughputwtime(starts, ends)
		var secs []time.Time

		for sec := range ts {
			secs = append(secs, sec)
		}

		sort.Sort(TimeSlice(secs))

		for _, sec := range secs {
			fmt.Printf("%f\t%f\n", sec.Sub(start).Seconds(), ts[sec])
		}

		fmt.Fprintf(os.Stderr, aurora.Magenta("%s --> %03.0f%%\n").String(), strings.Repeat(" ", len(msg)-4), float64(i+1)/float64(len(files))*100)
	}
}

func throughputwtime(starts, ends []time.Time) map[time.Time]float64 {
	ts := make(map[time.Time]float64)
	sec := ends[0].Truncate(time.Second).Add(time.Second)

	for _, end := range ends {
		if end.Before(sec) {
			ts[sec]++
			continue
		}

		sec = sec.Add(time.Second)
	}

	return ts
}

func xThroughputYLatencyFunc(files []string) {
	data := make(map[string][]string)
	var experiments []string

	for _, filename := range files {
		parts := strings.Split(filename, "_")
		if len(parts) < 2 {
			errexit("invalid filename: " + filename)
		}
		lastpart := parts[len(parts)-1]
		lenpostfix := len(filename) - len(lastpart) - 1
		if lenpostfix < 0 {
			errexit("invalid filename: " + filename)
		}
		experiment := filename[:lenpostfix]
		if _, ok := data[experiment]; !ok {
			experiments = append(experiments, experiment)
		}
		data[experiment] = append(data[experiment], filename)
	}

	sort.Strings(experiments)

	fmt.Printf(
		"%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
		"name",
		"throughput/s",
		"throughput/s stdev",
		"latency (average) ms",
		"latency (average) ms stdev",
		"latency (media) ms",
		"Q1",
		"Q3",
		"Min",
		"Max",
	)

	for i, experiment := range experiments {
		msg := "Reading files from experiment"
		fmt.Fprintf(os.Stderr, aurora.Magenta("%s %s into memory\n").String(), msg, experiment)

		allstarts, allends, alldurs := read(data[experiment])

		var allthroughput []float64

		for i := range allstarts {
			t := throughput(allstarts[i], allends[i])
			n := int(countRange * float64(len(t)))
			offset := (len(t) - n) / 2
			allthroughput = append(allthroughput, t[offset:offset+n]...)
		}

		var allatency []float64

		for _, durs := range alldurs {
			n := int(countRange * float64(len(durs)))
			offset := (len(durs) - n) / 2
			allatency = append(allatency, durs[offset:offset+n]...)
		}

		meanthroughput, _ := stats.Mean(allthroughput)
		stdevthroughput, _ := stats.StandardDeviation(allthroughput)
		meanlatency, _ := stats.Mean(allatency)
		stdevlatency, _ := stats.StandardDeviation(allatency)
		medianlatency, _ := stats.Median(allatency)
		qs, _ := stats.Quartile(allatency)
		min, _ := stats.Min(allatency)
		max, _ := stats.Max(allatency)
		fmt.Printf(
			"%s\t%f\t%f\t%f\t%f\t%f\t%f\t%f\t%f\t%f\n",
			experiment,
			meanthroughput,
			stdevthroughput,
			time.Duration(meanlatency).Seconds()*1000,
			time.Duration(stdevlatency).Seconds()*1000,
			time.Duration(medianlatency).Seconds()*1000,
			time.Duration(qs.Q1).Seconds()*1000,
			time.Duration(qs.Q3).Seconds()*1000,
			time.Duration(min).Seconds()*1000,
			time.Duration(max).Seconds()*1000,
		)

		fmt.Fprintf(os.Stderr, aurora.Magenta("%s --> %03.0f%%\n").String(), strings.Repeat(" ", len(msg)-4), float64(i+1)/float64(len(experiments))*100)
	}
}

func throughput(starts, ends []time.Time) []float64 {
	throughput := make(map[time.Time]uint64)

	sec := ends[0].Truncate(time.Second).Add(time.Second)
	start := sec

	for _, end := range ends {
		if end.Before(sec) {
			throughput[sec]++
			continue
		}

		sec = sec.Add(time.Second)
	}

	total := time.Second * time.Duration(len(throughput))

	var throughputs []float64
	for t := start; t.Before(start.Add(total)); t = t.Add(time.Second) {
		throughputs = append(throughputs, float64(throughput[t]))
	}

	return throughputs
}

func read(data []string) ([][]time.Time, [][]time.Time, [][]float64) {
	var allstarts [][]time.Time
	var allends [][]time.Time
	var alldurs [][]float64

	for _, onerun := range data {
		f, err := os.Open(onerun)
		if err != nil {
			errexit("error opening file: " + onerun)
		}

		r := csv.NewReader(f)
		// Skip header.
		if _, err := r.Read(); err != nil {
			panic(err)
		}

		var starts, ends []time.Time
		var durs []float64

		for {
			record, err := r.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				panic(err)
			}

			rstart, err := strconv.ParseInt(record[0], 10, 64)
			if err != nil {
				panic(err)
			}

			rend, err := strconv.ParseInt(record[1], 10, 64)
			if err != nil {
				panic(err)
			}

			start := time.Unix(0, rstart)
			end := time.Unix(0, rend)
			dur := end.Sub(start)

			starts = append(starts, start)
			ends = append(ends, end)
			durs = append(durs, float64(dur.Nanoseconds()))
		}

		allstarts = append(allstarts, starts)
		allends = append(allends, ends)
		alldurs = append(alldurs, durs)
	}

	return allstarts, allends, alldurs
}

func readEvents(file string) ([]string, []time.Time) {
	f, err := os.Open(file)
	if err != nil {
		errexit("error opening file: " + file)
	}

	r := csv.NewReader(f)
	// Skip header.
	if _, err := r.Read(); err != nil {
		panic(err)
	}

	var events []string
	var ts []time.Time

	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}

		rts, err := strconv.ParseInt(record[1], 10, 64)
		if err != nil {
			panic(err)
		}

		ts = append(ts, time.Unix(0, rts))
		events = append(events, record[0])
	}

	return events, ts
}

func errexit(msg string) {
	fmt.Printf("postprocess: %s\n\nUsage:\n", msg)
	fmt.Printf("\tpostprocess x [start time, if x = 2 or 3] test1_1.csv test1_2.csv test2_1.csv ..., where %d > x < %d\n", first, last)
	os.Exit(1)
}

// TimeSlice attaches the methods of sort.Interface to []time.Time, sorting in increasing order.
type TimeSlice []time.Time

func (p TimeSlice) Len() int           { return len(p) }
func (p TimeSlice) Less(i, j int) bool { return p[i].Before(p[j]) }
func (p TimeSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
