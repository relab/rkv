package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type client struct {
	id  string
	seq uint64

	leader  int
	servers []string

	retries int

	http *http.Client
}

func newClient(servers []string) *client {
	return &client{
		leader:  rand.Intn(len(servers)),
		servers: servers,
		http:    &http.Client{},
	}
}

type request interface {
	changeHost(string) (*http.Request, error)
}

type registerReq struct{}

func (r *registerReq) changeHost(addr string) (*http.Request, error) {
	return http.NewRequest(http.MethodGet, "http://"+addr+"/register", nil)
}

type lookupReq struct {
	key string
}

func (r *lookupReq) changeHost(addr string) (*http.Request, error) {
	return http.NewRequest(http.MethodGet, "http://"+addr+"/store/"+r.key, nil)
}

type insertReq struct {
	key   string
	value string
	id    string
	seq   uint64
}

func (r *insertReq) changeHost(addr string) (*http.Request, error) {
	body := strings.NewReader(r.value)
	url := fmt.Sprintf("http://%s/store/%s?id=%s&seq=%d", addr, r.key, r.id, r.seq)
	req, err := http.NewRequest("PUT", url, body)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return req, err

}

func (c *client) newLeader() {
	if len(c.servers) == 1 {
		return
	}

	for {
		leader := rand.Intn(len(c.servers))

		if c.leader != leader {
			c.leader = leader
			break
		}
	}
}

func (c *client) register() error {
	var r registerReq
	id, _, err := c.do(&r)

	if err != nil {
		return err
	}

	c.id = id
	c.seq++
	return nil
}

func (c *client) lookup(key string) (string, error) {
	r := lookupReq{key}
	value, _, err := c.do(&r)

	if err != nil {
		return "", err
	}

	return value, nil
}

func (c *client) insert(key, value string) error {
	if c.id == "" {
		panic("tried to insert without registering first")
	}

	r := insertReq{
		key:   key,
		value: value,
		id:    c.id,
		seq:   c.seq,
	}

	_, status, err := c.do(&r)

	if err != nil {
		return err
	}

	if status != http.StatusOK {
		log.Fatal(http.StatusText(status))
	}

	c.seq++
	return nil
}

func (c *client) do(req request) (string, int, error) {
	r, err := req.changeHost(c.servers[c.leader])

	if err != nil {
		return "", -1, err
	}

	resp, err := c.http.Do(r)

	if err != nil {
		if c.retries >= len(c.servers) {
			return "", -1, err
		}

		c.retries++
		c.newLeader()
		return c.do(req)
	}

	defer resp.Body.Close()

	b, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		return "", -1, err
	}

	if resp.StatusCode == http.StatusTemporaryRedirect {
		c.retries++
		c.newLeader()
		return c.do(req)
	}

	c.retries = 0
	return strings.TrimSuffix(string(b), "\n"), resp.StatusCode, err
}

func main() {
	rand.Seed(time.Now().UnixNano())

	var (
		cluster = flag.String("cluster", ":9201,:9202,:9203", "comma separated cluster servers")
		clients = flag.Int("clients", 1, "number of clients")
	)

	flag.Parse()

	servers := strings.Split(*cluster, ",")

	if len(servers) == 0 {
		fmt.Print("-cluster argument is required\n\n")
		flag.Usage()
		os.Exit(1)
	}

	runClients(*clients, servers)
}

func runClients(n int, servers []string) {
	var wg sync.WaitGroup
	wg.Add(n)

	for i := 0; i < n; i++ {
		go func(j int) {
			c := newClient(servers)

			err := c.register()

			if err != nil {
				log.Fatal(err)
			}

			fmt.Print("W:", j, " ")
			for i := 0; i < 32; i++ {
				key := fmt.Sprintf("c%dkey%d", j, i)
				value := fmt.Sprintf("c%dvalue%d", j, i)
				err := c.insert(key, value)

				if err != nil {
					log.Fatal(err)
				}
			}

			fmt.Print("R:", j, " ")
			for i := 0; i < 32; i++ {
				key := fmt.Sprintf("c%dkey%d", j, i)
				value, err := c.lookup(key)

				if err != nil {
					log.Fatal(err)
				}

				expected := fmt.Sprintf("c%dvalue%d", j, i)

				if value != expected {
					log.Printf("got %s wanted %s\n", value, expected)
				}
			}

			wg.Done()
		}(i)
	}

	wg.Wait()
}
