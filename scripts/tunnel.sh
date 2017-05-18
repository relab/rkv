#!/bin/bash

# Used to forward prometheus requests to remote servers. Port must be 59100,
# 5901, 5902, 5903, 5904 or 5905, as specified in prometheus.yml. Where 59100 is
# used for the client, and 590x is used for the server with raft id x.

if [[ $# -ne 2 ]]; then
    echo -e "\n\ttunnel.sh establishes a tunnel from localhost:port to remote:port.";
    echo -e "\n\tUsage ./tunnel.sh remote port\n";
    exit;
fi

ssh $1 -L $2:localhost:$2 -q
