package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"time"
)

type ec2 struct {
	Results []result
}

type result struct {
	Instances []instance
}

type instance struct {
	ID              string
	Public_IP       string
	Public_DNS_Name string
	Placement       string
}

const cmd = "#!/bin/bash\n\nrkvd -id %d -servers %s -cluster %s -quiet -backend %s &"

func main() {
	f, err := ioutil.ReadFile("./ec2.txt")
	if err != nil {
		panic(err)
	}

	f = bytes.Replace(f, []byte("'"), []byte("\""), -1)
	f = bytes.Replace(f, []byte("True"), []byte("\"True\""), -1)
	f = bytes.Replace(f, []byte("False"), []byte("\"False\""), -1)
	f = bytes.Replace(f, []byte("None"), []byte("\"None\""), -1)

	var s ec2
	err = json.Unmarshal(f, &s)
	if err != nil {
		panic(err)
	}

	var insts []instance

	for _, res := range s.Results {
		for _, ins := range res.Instances {
			insts = append(insts, ins)
		}
	}

	client := http.Client{Timeout: 5 * time.Second}
	res, err := client.Get("http://169.254.169.254/latest/meta-data/instance-id")
	if err != nil {
		panic(err)
	}
	defer res.Body.Close()
	bid, err := ioutil.ReadAll(res.Body)
	if err != nil {
		panic(err)
	}
	id := string(bid)

	serverID := 0
	servers := ""
	cluster := ""

	for i, ins := range insts {
		fmt.Printf("Adding %d (%s): %s\n", i+1, ins.Placement, ins.Public_DNS_Name)
		servers += fmt.Sprintf("%s:9201,", ins.Public_DNS_Name)
		cluster += fmt.Sprintf("%d,", i+1)
		if id == ins.ID {
			serverID = i + 1
		}
	}

	servers = servers[:len(servers)-1]
	cluster = cluster[:len(cluster)-1]

	if serverID == 0 {
		panic("no self ID")
	}

	createScript(serverID, servers, cluster, "gorums")
	createScript(serverID, servers, cluster, "etcd")
	createScript(serverID, servers, cluster, "hashicorp")
}

func createScript(serverID int, servers, cluster, backend string) {
	f, err := os.OpenFile(
		fmt.Sprintf("./inmem/%s", backend),
		os.O_RDWR|os.O_CREATE|os.O_TRUNC,
		0775,
	)
	if err != nil {
		panic(err)
	}

	_, err = f.WriteString(fmt.Sprintf(cmd, serverID, servers, cluster, backend))
	if err != nil {
		panic(err)
	}
}
