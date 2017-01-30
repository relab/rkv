#! /bin/bash

set -e

go build

./byzserver -port=8080 -key keys/server1 &
./byzserver -port=8081 -key keys/server1 &
./byzserver -port=8082 -key keys/server1 &
./byzserver -port=8083 -key keys/server1 &
# ./byzserver -port=8084 -key keys/server1 &

echo "running, enter to stop"

read && killall byzserver 
